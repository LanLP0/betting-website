use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use actix_ws::Message;
use betting_common::{NotificationPush, exchanges};
use futures_util::StreamExt;
use lapin::{options::*, types::FieldTable};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::env;
use uuid::Uuid;

struct AppState {
    db: PgPool,
    redis: redis::Client,
    _rmq: lapin::Channel,
}

#[derive(Serialize)]
struct NotificationResponse {
    id: Uuid,
    notification_type: String,
    title: String,
    message: String,
    payload: serde_json::Value,
    status: String,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Deserialize)]
struct MarkReadReq {
    notification_ids: Option<Vec<Uuid>>,
    all: Option<bool>,
}

async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

async fn get_user_notifications(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_user_id = path.into_inner();

    if !betting_common::req_user_match_id(&req, &target_user_id) {
        return HttpResponse::Forbidden().finish();
    }

    let rows = sqlx::query!(
        "SELECT id, notification_type, title, message, payload, status, created_at FROM notification_schema.user_notifications WHERE user_id = $1 ORDER BY created_at DESC LIMIT 50",
        target_user_id
    )
    .fetch_all(&data.db)
    .await
    .unwrap_or_default();

    let res: Vec<_> = rows
        .into_iter()
        .map(|r| NotificationResponse {
            id: r.id,
            notification_type: r.notification_type,
            title: r.title,
            message: r.message,
            payload: r.payload,
            status: r.status,
            created_at: r.created_at,
        })
        .collect();

    HttpResponse::Ok().json(res)
}

async fn mark_notifications_read(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<MarkReadReq>,
) -> impl Responder {
    let user_id_str = match req.headers().get("X-User-ID") {
        Some(v) => v.to_str().unwrap_or(""),
        None => return HttpResponse::Unauthorized().finish(),
    };
    let user_id = match Uuid::parse_str(user_id_str) {
        Ok(id) => id,
        Err(_) => return HttpResponse::BadRequest().finish(),
    };

    if body.all.unwrap_or(false) {
        let _ = sqlx::query!(
            "UPDATE notification_schema.user_notifications SET status = 'read', read_at = NOW() WHERE user_id = $1 AND status = 'unread'",
            user_id
        )
        .execute(&data.db)
        .await;
    } else if let Some(ref ids) = body.notification_ids {
        let _ = sqlx::query!(
            "UPDATE notification_schema.user_notifications SET status = 'read', read_at = NOW() WHERE user_id = $1 AND id = ANY($2)",
            user_id,
            ids
        )
        .execute(&data.db)
        .await;
    }

    HttpResponse::Ok().finish()
}

async fn ws_notifications(
    req: HttpRequest,
    stream: web::Payload,
    data: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    let user_id_str = req
        .headers()
        .get("X-User-ID")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let user_id = match Uuid::parse_str(user_id_str) {
        Ok(id) => id,
        Err(_) => return Ok(HttpResponse::Unauthorized().finish()),
    };

    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;
    let redis_client = data.redis.clone();

    // Spawn Redis Pub/Sub listener for this user (targeted + broadcast)
    let mut session_clone = session.clone();
    actix_web::rt::spawn(async move {
        let user_channel = format!("notifications:{}", user_id);
        let broadcast_channel = "odds_broadcast".to_string();

        if let Ok(mut pubsub_conn) = redis_client.get_connection() {
            let mut pubsub = pubsub_conn.as_pubsub();
            if pubsub.subscribe(&user_channel).is_ok()
                && pubsub.subscribe(&broadcast_channel).is_ok()
            {
                while let Ok(msg) = pubsub.get_message() {
                    let payload_str: String = msg.get_payload().unwrap_or_default();
                    if session_clone.text(payload_str).await.is_err() {
                        break; // Client disconnected
                    }
                }
            }
        }
    });

    // Spawn client ping/pong stream handler
    actix_web::rt::spawn(async move {
        while let Some(Ok(msg)) = msg_stream.next().await {
            match msg {
                Message::Ping(bytes) => {
                    if session.pong(&bytes).await.is_err() {
                        return;
                    }
                }
                Message::Close(_) => break,
                _ => (),
            }
        }
    });

    Ok(res)
}

async fn rmq_notification_consumer(
    pool: PgPool,
    rmq_chan: lapin::Channel,
    redis_client: redis::Client,
) {
    let q = rmq_chan
        .queue_declare(
            "notification_push_queue".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare notification queue");

    rmq_chan
        .queue_bind(
            q.name().to_owned(),
            exchanges::NOTIFICATION.into(),
            "notification.push".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to bind notification queue");

    let mut consumer = rmq_chan
        .basic_consume(
            q.name().to_owned(),
            "notification_consumer".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to create consumer");

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            if let Ok(notif) = serde_json::from_slice::<NotificationPush>(&delivery.data) {
                // 1. Asynchronously persist to database
                let _ = sqlx::query!(
                    "INSERT INTO notification_schema.user_notifications (user_id, notification_type, title, message, payload) VALUES ($1, $2, $3, $4, $5)",
                    notif.user_id,
                    notif.notification_type,
                    notif.title,
                    notif.message,
                    notif.payload
                )
                .execute(&pool)
                .await;

                // 2. Publish to Redis Pub/Sub for active WebSocket sessions across nodes
                if let Ok(mut r_conn) = redis_client.get_multiplexed_async_connection().await {
                    let channel_name = format!("notifications:{}", notif.user_id);
                    let notif_json = serde_json::to_string(&notif).unwrap_or_default();
                    let _: () = r_conn.publish(channel_name, notif_json).await.unwrap_or(());
                }
            }
            let _ = delivery.ack(BasicAckOptions::default()).await;
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL required");
    let pool = betting_common::connect_pg(&db_url, 5)
        .await
        .expect("Failed to connect to postgres");

    let rmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL required");
    let rmq_chan = betting_common::connect_rmq(&rmq_url)
        .await
        .expect("Failed to connect to RabbitMQ");

    rmq_chan
        .exchange_declare(
            exchanges::NOTIFICATION.into(),
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare notification exchange");

    let redis_url = env::var("REDIS_URL").expect("REDIS_URL required");
    let redis_client = redis::Client::open(redis_url).expect("Failed to initialize Redis client");

    // Spawn RMQ notification consumer background task
    tokio::spawn(rmq_notification_consumer(
        pool.clone(),
        rmq_chan.clone(),
        redis_client.clone(),
    ));

    let state = web::Data::new(AppState {
        db: pool,
        redis: redis_client,
        _rmq: rmq_chan,
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .route("/api/v1/notifications/health", web::get().to(health_check))
            .route(
                "/api/v1/notifications/read",
                web::put().to(mark_notifications_read),
            )
            .route(
                "/api/v1/notifications/websocket",
                web::get().to(ws_notifications),
            )
            .route(
                "/api/v1/notifications/{id}",
                web::get().to(get_user_notifications),
            )
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
