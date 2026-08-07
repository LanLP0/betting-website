use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
use clickhouse::Client;
use futures_util::stream::StreamExt;
use lapin::{BasicProperties, Connection, ConnectionProperties, options::*, types::FieldTable};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::env;
use uuid::Uuid;

struct AppState {
    db: PgPool,
    rmq: lapin::Channel,
    ch: Client,
}

#[derive(Deserialize)]
struct AddUserReq {
    username: String,
    password_hash: String,
    role: String,
}

#[derive(Deserialize)]
struct AddEventReq {
    name: String,
    start_time: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
struct SettleEventReq {
    winning_selection: String,
}

#[derive(Serialize, clickhouse::Row)]
struct MetricRow {
    timestamp: u32,
    event_type: String,
    payload: String,
}

async fn publish_event(
    channel: &lapin::Channel,
    exchange: &str,
    routing_key: &str,
    payload: impl Serialize,
) {
    let payload = serde_json::to_vec(&payload).unwrap();
    let _ = channel
        .basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default(),
        )
        .await;
}

async fn is_admin(req: &HttpRequest) -> bool {
    if let Some(role) = req.headers().get("X-User-Role") {
        role == "admin"
    } else {
        false
    }
}

async fn add_user(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<AddUserReq>,
) -> impl Responder {
    if !is_admin(&req).await {
        return HttpResponse::Forbidden().finish();
    }
    let id = Uuid::new_v4();
    let _ = sqlx::query!("INSERT INTO users_schema.users (id, username, password_hash, role) VALUES ($1, $2, $3, $4)",
        id, body.username, body.password_hash, body.role).execute(&data.db).await;
    HttpResponse::Created().json(id)
}

async fn add_event(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<AddEventReq>,
) -> impl Responder {
    if !is_admin(&req).await {
        return HttpResponse::Forbidden().finish();
    }
    let id = Uuid::new_v4();
    let _ = sqlx::query!(
        "INSERT INTO events_schema.events (id, name, start_time) VALUES ($1, $2, $3)",
        id,
        body.name,
        body.start_time
    )
    .execute(&data.db)
    .await;
    HttpResponse::Created().json(id)
}

async fn settle_event(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<SettleEventReq>,
) -> impl Responder {
    if !is_admin(&req).await {
        return HttpResponse::Forbidden().finish();
    }
    let id = Uuid::parse_str(req.match_info().get("id").unwrap()).unwrap();

    let _ = sqlx::query!(
        "UPDATE events_schema.events SET status = 'SETTLED', winning_selection = $1 WHERE id = $2",
        body.winning_selection,
        id
    )
    .execute(&data.db)
    .await;

    #[derive(Serialize)]
    struct EventSettled {
        event_id: Uuid,
        winning_selection: String,
    }
    let ev = EventSettled {
        event_id: id,
        winning_selection: body.winning_selection.clone(),
    };
    publish_event(&data.rmq, "event_topic", "event.settled", &ev).await;

    HttpResponse::Ok().finish()
}

async fn get_metrics(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    if !is_admin(&req).await {
        return HttpResponse::Forbidden().finish();
    }

    #[derive(Deserialize, clickhouse::Row, Serialize)]
    struct Cnt {
        count: u64,
    }

    let count: u64 = data
        .ch
        .query("SELECT count() FROM event_log")
        .fetch_one::<Cnt>()
        .await
        .map(|c| c.count)
        .unwrap_or(0);

    HttpResponse::Ok().json(serde_json::json!({ "total_events_logged": count }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db_url = env::var("DATABASE_URL").unwrap();
    let pool = PgPoolOptions::new().connect(&db_url).await.unwrap();

    let ch_url = env::var("CLICKHOUSE_URL").unwrap();
    let ch = Client::default().with_url(&ch_url);

    let rmq_url = env::var("RABBITMQ_URL").unwrap();
    let rmq_conn = Connection::connect(&rmq_url, ConnectionProperties::default())
        .await
        .unwrap();
    let rmq_chan = rmq_conn.create_channel().await.unwrap();
    rmq_chan
        .exchange_declare(
            "event_topic",
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // Consumer for Metrics
    let chan_clone = rmq_chan.clone();
    let ch_clone = Client::default().with_url(&ch_url);
    tokio::spawn(async move {
        let q = chan_clone
            .queue_declare(
                "metrics_firehose",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone
            .basic_qos(100, BasicQosOptions::default())
            .await
            .unwrap();
        // Bind to all exchanges with #
        chan_clone
            .queue_bind(
                q.name().as_str(),
                "user_topic",
                "#",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone
            .queue_bind(
                q.name().as_str(),
                "wallet_topic",
                "#",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone
            .queue_bind(
                q.name().as_str(),
                "betting_topic",
                "#",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone
            .queue_bind(
                q.name().as_str(),
                "event_topic",
                "#",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        let mut consumer = chan_clone
            .basic_consume(
                q.name().as_str(),
                "metrics_c",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        while let Some(delivery) = consumer.next().await {
            if let Ok(delivery) = delivery {
                let rkey = delivery.routing_key.as_str().to_string();
                let payload = String::from_utf8_lossy(&delivery.data).to_string();

                let mut insert = ch_clone.insert("event_log").unwrap();
                let row = MetricRow {
                    timestamp: chrono::Utc::now().timestamp() as u32,
                    event_type: rkey,
                    payload,
                };
                let _ = insert.write(&row).await;
                let _ = insert.end().await;

                let _ = delivery
                    .ack(lapin::options::BasicAckOptions::default())
                    .await;
            }
        }
    });

    let state = web::Data::new(AppState {
        db: pool,
        rmq: rmq_chan,
        ch,
    });
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/api/v1/management/users/add", web::post().to(add_user))
            .route("/api/v1/management/events/add", web::post().to(add_event))
            .route(
                "/api/v1/management/events/{id}/settle",
                web::post().to(settle_event),
            )
            .route("/api/v1/management/metrics", web::get().to(get_metrics))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
