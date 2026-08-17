use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use backon::{ExponentialBuilder, Retryable};
use betting_common::{
    Event, EventOdds, OddsRpcRequest, OddsRpcResponse, connect_pg, connect_rmq, exchanges,
    publish_event_props, publish_event_with_trace, req_get_request_id, verify_hmac_signature,
};
use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use futures_util::stream::StreamExt;
use lapin::{BasicProperties, options::*, types::FieldTable};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::env;
use uuid::Uuid;

struct AppState {
    db: PgPool,
    redis: redis::Client,
    rmq: lapin::Channel,
    webhook_secret: String,
}

async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

// TODO pagination
// Checked
async fn get_events(data: web::Data<AppState>) -> impl Responder {
    let rows_req = {|| async {sqlx::query!(
        "SELECT id, name, description, status, winning_selection, teams, odds, settled_at, created_at FROM events_schema.events ORDER BY created_at DESC"
    )
    .fetch_all(&data.db)
    .await}}.retry(ExponentialBuilder::default().with_jitter()).when(betting_common::sqlx_retry_when).await;

    if rows_req.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    let rows = rows_req.unwrap();

    let res: Vec<_> = rows
        .into_iter()
        .map(|r| Event {
            id: r.id,
            name: r.name,
            description: r.description,
            status: r.status,
            winning_selection: r.winning_selection,
            teams: r.teams,
            odds: r.odds.into_iter().map(|o| o.to_f64().unwrap()).collect(),
            settled_at: r.settled_at,
            created_at: r.created_at.unwrap_or_else(|| chrono::Utc::now()),
        })
        .collect();

    HttpResponse::Ok().json(res)
}

// Checked
async fn get_event(path: web::Path<Uuid>, data: web::Data<AppState>) -> impl Responder {
    let id = path.into_inner();

    let req = {
        || async {
            sqlx::query!(
                "SELECT id, name, description, status, winning_selection, teams, odds, settled_at, created_at FROM events_schema.events WHERE id = $1",
                id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if req.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    if let Some(r) = req.unwrap() {
        HttpResponse::Ok().json(Event {
            id: r.id,
            name: r.name,
            description: r.description,
            status: r.status,
            winning_selection: r.winning_selection,
            teams: r.teams,
            odds: r.odds.into_iter().map(|o| o.to_f64().unwrap()).collect(),
            settled_at: r.settled_at,
            created_at: r.created_at.unwrap_or_else(|| chrono::Utc::now()),
        })
    } else {
        HttpResponse::NotFound().finish()
    }
}

// Checked
async fn get_event_odds(path: web::Path<Uuid>, data: web::Data<AppState>) -> impl Responder {
    let id = path.into_inner();
    let redis_key = format!("odds:{}", id);

    let mut conn = data.redis.get_multiplexed_async_connection().await.ok();

    // 1. Try Redis cache
    if let Some(ref mut c) = conn {
        if let Ok(val) = c.get::<_, String>(&redis_key).await {
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&val) {
                return HttpResponse::Ok().json(json_val);
            }
        }
    }

    // 2. Check if event exists in DB
    let ev_req = {
        || async {
            sqlx::query!(
                "SELECT status, winning_selection, teams, odds FROM events_schema.events WHERE id = $1",
                id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if let Ok(Some(ev)) = ev_req {
        let odds_f = ev
            .odds
            .iter()
            .map(|o| o.to_f64().unwrap_or_default())
            .collect::<Vec<_>>();
        let res_json = serde_json::json!(EventOdds {
            event_id: id,
            status: ev.status,
            winning_selection: ev.winning_selection,
            teams: ev.teams,
            odds: odds_f
        });

        if let Some(mut c) = conn {
            let payload_str = serde_json::to_string(&res_json).unwrap_or_default();
            let _: () = c.set_ex(&redis_key, payload_str, 60).await.unwrap_or(());
        }

        return HttpResponse::Ok().json(res_json);
    }

    HttpResponse::NotFound().body("Event not found or odds currently unavailable")
}

// Checked
async fn events_callback(
    req: HttpRequest,
    data: web::Data<AppState>,
    bytes: web::Bytes,
) -> impl Responder {
    if !verify_hmac_signature(req.headers(), &bytes, &data.webhook_secret) {
        return HttpResponse::Unauthorized().body("Invalid signature");
    }

    let body: EventOdds = match serde_json::from_slice(&bytes) {
        Ok(b) => b,
        Err(_) => return HttpResponse::BadRequest().body("Invalid JSON payload"),
    };

    let redis_key = format!("odds:{}", body.event_id);
    let payload_val = serde_json::json!(EventOdds {
        event_id: body.event_id,
        status: body.status.clone(),
        winning_selection: body.winning_selection.clone(),
        teams: body.teams,
        odds: body.odds.clone()
    });

    if let Ok(mut conn) = data.redis.get_multiplexed_async_connection().await {
        let payload_str = serde_json::to_string(&payload_val).unwrap_or_default();
        let _: () = conn.set_ex(redis_key, payload_str, 60).await.unwrap_or(());
    }

    let odds_d = body
        .odds
        .iter()
        .map(|o| BigDecimal::from_f64(*o).unwrap_or_default())
        .collect::<Vec<BigDecimal>>();

    let _ = {|| async {
        sqlx::query!(
            "UPDATE events_schema.events SET status = $1, winning_selection = $2, odds = $3 WHERE id = $4",
            &body.status, body.winning_selection, odds_d.as_slice(), body.event_id
        )
        .execute(&data.db)
        .await
    }}.retry(ExponentialBuilder::default().with_jitter()).when(betting_common::sqlx_retry_when).await;

    let trace_id = req_get_request_id(&req);
    // Publish event updated to RabbitMQ
    let _ = publish_event_with_trace(
        &data.rmq,
        exchanges::EVENT,
        "event.updated",
        &payload_val,
        &trace_id,
    )
    .await;

    HttpResponse::Ok().finish()
}

// Background RPC responder for internal services querying event odds
async fn rpc_odds_consumer(pool: PgPool, rmq: lapin::Channel, redis_client: redis::Client) {
    let q = rmq
        .queue_declare(
            "event_odds_rpc_queue".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await;

    if let Ok(q) = q {
        let _ = rmq
            .queue_bind(
                q.name().to_owned(),
                exchanges::EVENT.into(),
                "event.odds.rpc_request".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await;

        if let Ok(mut consumer) = rmq
            .basic_consume(
                q.name().to_owned(),
                "events_rpc_c".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
        {
            while let Some(delivery) = consumer.next().await {
                if let Ok(delivery) = delivery {
                    if let Ok(req) = serde_json::from_slice::<OddsRpcRequest>(&delivery.data) {
                        let mut teams = vec!["Home Team".to_string(), "Away Team".to_string()];
                        let mut odds = vec![1.90, 1.90];
                        let mut success = false;

                        let redis_key = format!("odds:{}", req.event_id);
                        if let Ok(mut conn) = redis_client.get_multiplexed_async_connection().await
                        {
                            if let Ok(val) = conn.get::<_, String>(&redis_key).await {
                                if let Ok(json_val) =
                                    serde_json::from_str::<serde_json::Value>(&val)
                                {
                                    if let (Some(t), Some(o)) = (
                                        json_val.get("teams").and_then(|t| t.as_array()),
                                        json_val.get("odds").and_then(|o| o.as_array()),
                                    ) {
                                        teams = t
                                            .iter()
                                            .filter_map(|v| v.as_str().map(String::from))
                                            .collect();
                                        odds = o.iter().filter_map(|v| v.as_f64()).collect();
                                        success = true;
                                    }
                                }
                            }
                        }

                        if !success {
                            if let Ok(Some(_)) = sqlx::query!(
                                "SELECT id FROM events_schema.events WHERE id = $1",
                                req.event_id
                            )
                            .fetch_optional(&pool)
                            .await
                            {
                                success = true;
                            }
                        }

                        if let (Some(reply_to), Some(corr_id)) = (
                            delivery.properties.reply_to().as_ref().map(|s| s.as_str()),
                            delivery.properties.correlation_id().as_ref(),
                        ) {
                            let response = OddsRpcResponse {
                                event_id: req.event_id,
                                success,
                                teams: Some(teams),
                                odds: Some(odds),
                            };

                            let props =
                                BasicProperties::default().with_correlation_id(corr_id.clone());
                            let _ = publish_event_props(&rmq, "", reply_to, response, props).await;
                        }
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
    let rmq_chan = connect_rmq(&rmq_url).await.expect("Failed RMQ connection");

    let redis_url = env::var("REDIS_URL").expect("REDIS_URL required");
    let redis_client = redis::Client::open(redis_url).expect("Failed Redis connection");

    let webhook_secret = env::var("WEBHOOK_SECRET").expect("WEBHOOK_SECRET env var required");

    rmq_chan
        .exchange_declare(
            exchanges::EVENT.into(),
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // Spawn internal RPC responder
    tokio::spawn(rpc_odds_consumer(
        pool.clone(),
        rmq_chan.clone(),
        redis_client.clone(),
    ));

    let state = web::Data::new(AppState {
        db: pool,
        redis: redis_client,
        rmq: rmq_chan,
        webhook_secret,
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .route("/api/v1/events/health", web::get().to(get_health))
            .route("/api/v1/events", web::get().to(get_events))
            .route("/api/v1/events/callback", web::post().to(events_callback))
            .route("/api/v1/events/{id}", web::get().to(get_event))
            .route("/api/v1/events/{id}/odds", web::get().to(get_event_odds))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await

    // TODO subscribe to /mock/api/v1/events/subscribe on startup
}
