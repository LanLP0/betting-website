use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use argon2::{
    Argon2,
    password_hash::{PasswordHasher, SaltString},
};
use betting_common::{
    EventSettled, UserCreated, connect_pg, connect_rmq, exchanges, publish_event_with_trace,
    req_get_request_id,
};
use clickhouse::Client as ClickhouseClient;
use clickhouse::Row;
use futures_util::StreamExt;
use lapin::{options::*, types::FieldTable};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::env;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

#[derive(Row, Serialize, Deserialize, Debug, ToSchema)]
struct EventMetric {
    timestamp: Option<u64>,
    event_id: String,
    event_type: String,
    value1: String,
    value2: String,
    value3: String,
    payload: Option<String>,
    trace_id: Option<String>,
}

#[derive(Deserialize, ToSchema)]
struct AddUserReq {
    username: String,
    email: String,
    password: String,
}

#[derive(Deserialize, ToSchema)]
struct AddEventReq {
    name: String,
    #[schema(value_type = String)]
    start_time: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize, ToSchema)]
struct SettleEventReq {
    winning_selection: String,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        get_health,
        get_metrics,
        add_user,
        delete_user,
        add_event,
        delete_event,
        settle_event
    ),
    components(
        schemas(EventMetric, AddUserReq, AddEventReq, SettleEventReq)
    ),
    tags(
        (name = "Management", description = "Management and System Metrics APIs")
    )
)]
struct ApiDoc;

struct AppState {
    db: PgPool,
    rmq: lapin::Channel,
    clickhouse: ClickhouseClient,
}

#[utoipa::path(
    get,
    path = "/api/v1/management/health",
    responses(
        (status = 200, description = "Service Healthy")
    )
)]
async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

#[utoipa::path(
    get,
    path = "/api/v1/management/metrics",
    responses(
        (status = 200, description = "ClickHouse metrics", body = Vec<EventMetric>),
        (status = 403, description = "Forbidden - Admin access required")
    )
)]
async fn get_metrics(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    let auth_user_role = req
        .headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let mut cursor = match data.clickhouse.query(
        "SELECT timestamp, event_id, event_type, value1, value2, value3, payload, trace_id FROM metrics_schema.events_log ORDER BY timestamp DESC LIMIT 100"
    ).fetch::<EventMetric>() {
        Ok(c) => c,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let mut metrics = Vec::new();
    while let Ok(Some(row)) = cursor.next().await {
        metrics.push(row);
    }

    HttpResponse::Ok().json(metrics)
}

#[utoipa::path(
    post,
    path = "/api/v1/management/users/add",
    request_body = AddUserReq,
    responses(
        (status = 201, description = "User added successfully"),
        (status = 403, description = "Forbidden - Admin access required"),
        (status = 409, description = "User already exists")
    )
)]
async fn add_user(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<AddUserReq>,
) -> impl Responder {
    let auth_user_role = req
        .headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = match argon2.hash_password(body.password.as_bytes(), &salt) {
        Ok(h) => h.to_string(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let id = Uuid::new_v4();
    let query = sqlx::query!(
        "INSERT INTO users_schema.users (id, username, email, password_hash, role) VALUES ($1, $2, $3, $4, 'user')",
        id, &body.username, &body.email, &password_hash
    )
    .execute(&data.db)
    .await;

    if query.is_ok() {
        let trace_id = req_get_request_id(&req);
        let ev = UserCreated {
            id,
            username: body.username.clone(),
        };
        let _ =
            publish_event_with_trace(&data.rmq, exchanges::USER, "user.created", &ev, &trace_id)
                .await;
        let _ = publish_event_with_trace(
            &data.rmq,
            exchanges::WALLET,
            "user.create_wallet",
            &ev,
            &trace_id,
        )
        .await;
        HttpResponse::Created().json(serde_json::json!({ "id": id }))
    } else {
        HttpResponse::Conflict().finish()
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/management/users/{id}",
    params(
        ("id" = String, Path, description = "User ID to delete")
    ),
    responses(
        (status = 200, description = "User deleted"),
        (status = 403, description = "Forbidden - Admin access required")
    )
)]
async fn delete_user(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let auth_user_role = req
        .headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let id = path.into_inner();
    let _ = sqlx::query!("DELETE FROM users_schema.users WHERE id = $1", id)
        .execute(&data.db)
        .await;

    HttpResponse::Ok().finish()
}

#[utoipa::path(
    post,
    path = "/api/v1/management/events/add",
    request_body = AddEventReq,
    responses(
        (status = 201, description = "Event created"),
        (status = 403, description = "Forbidden - Admin access required")
    )
)]
async fn add_event(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<AddEventReq>,
) -> impl Responder {
    let auth_user_role = req
        .headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let id = Uuid::new_v4();
    let _ = sqlx::query!(
        "INSERT INTO events_schema.events (id, name, start_time, status) VALUES ($1, $2, $3, 'OPEN')",
        id, &body.name, body.start_time
    )
    .execute(&data.db)
    .await;

    HttpResponse::Created().json(serde_json::json!({ "id": id }))
}

#[utoipa::path(
    delete,
    path = "/api/v1/management/events/{id}",
    params(
        ("id" = String, Path, description = "Event ID to delete")
    ),
    responses(
        (status = 200, description = "Event deleted"),
        (status = 403, description = "Forbidden - Admin access required")
    )
)]
async fn delete_event(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let auth_user_role = req
        .headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let id = path.into_inner();
    let _ = sqlx::query!("DELETE FROM events_schema.events WHERE id = $1", id)
        .execute(&data.db)
        .await;

    HttpResponse::Ok().finish()
}

#[utoipa::path(
    post,
    path = "/api/v1/management/events/{id}/settle",
    params(
        ("id" = String, Path, description = "Event ID to settle")
    ),
    request_body = SettleEventReq,
    responses(
        (status = 200, description = "Event settled"),
        (status = 403, description = "Forbidden - Admin access required")
    )
)]
async fn settle_event(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    body: web::Json<SettleEventReq>,
) -> impl Responder {
    let auth_user_role = req
        .headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let event_id = path.into_inner();
    let _ = sqlx::query!(
        "UPDATE events_schema.events SET status = 'SETTLED', winning_selection = $1 WHERE id = $2",
        &body.winning_selection,
        event_id
    )
    .execute(&data.db)
    .await;

    let trace_id = req_get_request_id(&req);
    let ev = EventSettled {
        event_id,
        winning_selection: body.winning_selection.clone(),
    };

    let _ = publish_event_with_trace(&data.rmq, exchanges::EVENT, "event.settled", &ev, &trace_id)
        .await;
    HttpResponse::Ok().finish()
}

async fn metrics_rmq_consumer(rmq_chan: lapin::Channel, clickhouse: ClickhouseClient) {
    let q = rmq_chan
        .queue_declare(
            "metrics_clickhouse_queue".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await;

    if let Ok(q) = q {
        let topic_exchanges = vec![
            exchanges::USER,
            exchanges::WALLET,
            exchanges::BETTING,
            exchanges::EVENT,
            exchanges::NOTIFICATION,
        ];

        for ex in topic_exchanges {
            let _ = rmq_chan
                .queue_bind(
                    q.name().to_owned(),
                    ex.into(),
                    "#".into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await;
        }

        if let Ok(mut consumer) = rmq_chan
            .basic_consume(
                q.name().to_owned(),
                "metrics_consumer".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
        {
            while let Some(delivery) = consumer.next().await {
                if let Ok(delivery) = delivery {
                    let routing_key = delivery.routing_key.as_str().to_string();
                    let payload_str = String::from_utf8_lossy(&delivery.data).to_string();
                    let event_id = Uuid::new_v4().to_string();

                    // Extract trace_id from headers or correlation_id
                    let trace_id = delivery
                        .properties
                        .headers()
                        .as_ref()
                        .and_then(|h| h.inner().get("x-trace-id"))
                        .and_then(|v| match v {
                            lapin::types::AMQPValue::LongString(s) => Some(s.to_string()),
                            lapin::types::AMQPValue::ShortString(s) => Some(s.to_string()),
                            _ => None,
                        })
                        .or_else(|| {
                            delivery
                                .properties
                                .correlation_id()
                                .as_ref()
                                .map(|c| c.to_string())
                        });

                    // Extract key fields for fast indexing
                    let mut val1 = String::new();
                    let mut val2 = String::new();
                    let mut val3 = String::new();

                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&payload_str) {
                        if let Some(v) = json.get("user_id").or_else(|| json.get("id")) {
                            val1 = v.to_string().trim_matches('"').to_string();
                        }
                        if let Some(v) = json.get("amount").or_else(|| json.get("status")) {
                            val2 = v.to_string().trim_matches('"').to_string();
                        }
                        if let Some(v) = json.get("bet_id").or_else(|| json.get("event_id")) {
                            val3 = v.to_string().trim_matches('"').to_string();
                        }
                    }

                    let row = EventMetric {
                        timestamp: None,
                        event_id,
                        event_type: routing_key,
                        value1: val1,
                        value2: val2,
                        value3: val3,
                        payload: Some(payload_str),
                        trace_id,
                    };

                    if let Ok(mut insert) = clickhouse
                        .insert::<EventMetric>("metrics_schema.events_log")
                        .await
                    {
                        let _ = insert.write(&row).await;
                        let _ = insert.end().await;
                    }

                    let _ = delivery.ack(BasicAckOptions::default()).await;
                }
            }
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL required");
    let pool = connect_pg(&db_url, 5).await.expect("Failed DB connection");

    let rmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL required");
    let rmq_chan = connect_rmq(&rmq_url, "management-service")
        .await
        .expect("Failed RMQ connection");

    let clickhouse_url =
        env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into());
    let clickhouse = ClickhouseClient::default()
        .with_url(clickhouse_url)
        .with_user("default")
        .with_password("");

    tokio::spawn(metrics_rmq_consumer(rmq_chan.clone(), clickhouse.clone()));

    let state = web::Data::new(AppState {
        db: pool,
        rmq: rmq_chan,
        clickhouse,
    });

    let openapi = ApiDoc::openapi();

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .service(
                SwaggerUi::new("/api/v1/swagger/{_:.*}")
                    .url("/api-docs/openapi.json", openapi.clone()),
            )
            .route("/api/v1/management/health", web::get().to(get_health))
            .route("/api/v1/management/metrics", web::get().to(get_metrics))
            .route("/api/v1/management/users/add", web::post().to(add_user))
            .route(
                "/api/v1/management/users/{id}",
                web::delete().to(delete_user),
            )
            .route("/api/v1/management/events/add", web::post().to(add_event))
            .route(
                "/api/v1/management/events/{id}",
                web::delete().to(delete_event),
            )
            .route(
                "/api/v1/management/events/{id}/settle",
                web::post().to(settle_event),
            )
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
