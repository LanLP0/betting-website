use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use actix_ws::Message;
use backon::{ExponentialBuilder, Retryable};
use betting_common::{
    NotificationPush, declare_queue_with_dlx, exchanges, http::req_get_user_id, req_get_user_role,
    req_user_match_id, setup_dlq,
};
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
struct NotificationsResponse {
    user_id: Uuid,
    total_count: i64,
    notifications: Vec<UserNotification>,
}

#[derive(Serialize)]
struct UserNotification {
    id: Uuid,
    notification_type: String,
    title: String,
    payload: serde_json::Value,
    status: String,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Deserialize)]
struct MarkReadReq {
    notification_ids: Option<Vec<Uuid>>,
    all: Option<bool>,
}

#[derive(Deserialize)]
struct NotificationQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

/// Fetch notifications for a target user with RBAC enforcement and retry resilience.
async fn get_user_notifications(
    req: HttpRequest,
    path: web::Path<Uuid>,
    query: web::Query<NotificationQuery>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_user_id = path.into_inner();
    let auth_user_role = req_get_user_role(&req);

    // Enforce authorization: only the resource owner or an admin can view user notifications
    if !req_user_match_id(&req, &target_user_id) && auth_user_role != Some("admin") {
        return HttpResponse::Forbidden().finish();
    }

    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let offset = query.offset.unwrap_or(0).max(0);

    let rows_res = {
        || async {
            sqlx::query!(
                r#"
                SELECT id, notification_type, title, payload, status, created_at 
                FROM notification_schema.user_notifications 
                WHERE user_id = $1 
                ORDER BY created_at DESC 
                LIMIT $2 OFFSET $3
                "#,
                target_user_id,
                limit,
                offset
            )
            .fetch_all(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    let count_res = {
        || async {
            sqlx::query!(
                "SELECT COUNT(*) FROM notification_schema.user_notifications WHERE user_id = $1",
                target_user_id
            )
            .fetch_one(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if rows_res.is_ok() && count_res.is_ok() {
        let (rows, count) = (rows_res.unwrap(), count_res.unwrap());
        let res: Vec<_> = rows
            .into_iter()
            .map(|r| UserNotification {
                id: r.id,
                notification_type: r.notification_type,
                title: r.title,
                payload: r.payload,
                status: r.status,
                created_at: r.created_at,
            })
            .collect();
        HttpResponse::Ok().json(NotificationsResponse {
            user_id: target_user_id,
            total_count: count.count.unwrap(),
            notifications: res,
        })
    } else {
        let e = rows_res.err().unwrap_or(count_res.err().unwrap());
        log::error!("Database query failed in get_user_notifications: {:?}", e);
        HttpResponse::InternalServerError().finish()
    }
}

/// Mark notifications as read for a user (either all unread or selected IDs).
async fn mark_notifications_read(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<MarkReadReq>,
) -> impl Responder {
    let user_id = match req_get_user_id(&req) {
        Some(id) => id,
        None => return HttpResponse::BadRequest().finish(),
    };

    let mark_all = body.all.unwrap_or(false);
    let ids = body.notification_ids.as_deref().unwrap_or(&[]);

    if !mark_all && ids.is_empty() {
        return HttpResponse::BadRequest()
            .body("Either 'all' must be true or 'notification_ids' provided with non-empty list");
    }

    let update_res = if mark_all {
        {
            || async {
                sqlx::query!(
                    "UPDATE notification_schema.user_notifications SET status = 'read', read_at = NOW() WHERE user_id = $1 AND status = 'unread'",
                    user_id
                )
                .execute(&data.db)
                .await
            }
        }
        .retry(ExponentialBuilder::default().with_jitter())
        .when(betting_common::sqlx_retry_when)
        .await
    } else {
        {
            || async {
                sqlx::query!(
                    "UPDATE notification_schema.user_notifications SET status = 'read', read_at = NOW() WHERE user_id = $1 AND id = ANY($2)",
                    user_id,
                    ids
                )
                .execute(&data.db)
                .await
            }
        }
        .retry(ExponentialBuilder::default().with_jitter())
        .when(betting_common::sqlx_retry_when)
        .await
    };

    match update_res {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(e) => {
            log::error!("Failed to mark notifications read: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/*
 * =========================================================================================
 * ARCHITECTURAL NOTICE: REAL-TIME NOTIFICATION WORKER & SCALING DESIGN
 * =========================================================================================
 * Context & Scenario:
 * In a distributed multi-node deployment (e.g. Docker Swarm / Kubernetes), WebSocket clients
 * establish sticky TCP connections to arbitrary pod replicas.
 *
 * 1. Cross-Node Broadcast & Targeted Routing:
 *    - Domain events published to Redis channels (`notifications:<user_id>` and `odds_broadcast`)
 *      enable any worker node to deliver real-time messages to the connected client.
 * 2. Async Non-Blocking Pub/Sub:
 *    - MUST use asynchronous non-blocking Redis Pub/Sub (`redis::aio::PubSub`) rather than
 *      synchronous `get_connection()` / `pubsub.get_message()`. Synchronous IO inside Tokio tasks
 *      will starve the async runtime thread pool, causing total service denial under concurrent load.
 * 3. Connection Teardown & Leak Prevention:
 *    - Active WebSocket sessions and Redis subscriptions must be coordinated via `tokio::select!`
 *      so that when a WebSocket connection drops (or heartbeat times out), the Redis subscription
 *      is cleanly unregistered and the connection resource returned.
 * 4. Future Enhancement (GAP / Scalability):
 *    - Client ack/read synchronization: When a user marks notifications as read on Client A,
 *      publish a `notification.read` event via Redis Pub/Sub to update badge counters in real-time
 *      on Client B (multi-tab/multi-device synchronization).
 * =========================================================================================
 */
async fn ws_notifications(
    req: HttpRequest,
    stream: web::Payload,
    data: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    let user_id = match req_get_user_id(&req) {
        Some(id) => id,
        None => return Ok(HttpResponse::Unauthorized().finish()),
    };

    // TODO check for multiple connections per user and reject if exists

    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;
    let redis_client = data.redis.clone();

    // Spawn unified non-blocking async session handler with Redis Pub/Sub and heartbeat monitoring
    actix_web::rt::spawn(async move {
        let user_channel = format!("notifications:{}", user_id);
        let broadcast_channel = "odds_broadcast".to_string();

        let mut pubsub_conn = match redis_client.get_async_pubsub().await {
            Ok(ps) => ps,
            Err(e) => {
                log::error!(
                    "Failed to obtain async Redis PubSub connection for user {}: {:?}",
                    user_id,
                    e
                );
                let _ = session.close(None).await;
                return;
            }
        };

        if let Err(e) = pubsub_conn.subscribe(&user_channel).await {
            log::error!(
                "Failed to subscribe to user channel {}: {:?}",
                user_channel,
                e
            );
            let _ = session.close(None).await;
            return;
        }

        if let Err(e) = pubsub_conn.subscribe(&broadcast_channel).await {
            log::error!(
                "Failed to subscribe to broadcast channel {}: {:?}",
                broadcast_channel,
                e
            );
            let _ = session.close(None).await;
            return;
        }

        let mut pubsub_stream = pubsub_conn.into_on_message();

        loop {
            tokio::select! {
                // Inbound WebSocket frames from client (ping, pong, close)
                ws_msg = msg_stream.next() => {
                    match ws_msg {
                        Some(Ok(Message::Ping(bytes))) => {
                            if session.pong(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {
                            // Heartbeat response received
                        }
                        Some(Ok(Message::Close(reason))) => {
                            let _ = session.close(reason).await;
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => {
                            // Client disconnected
                            break;
                        }
                    }
                }
                // Inbound Redis Pub/Sub messages (user targeted or broadcast)
                redis_msg = pubsub_stream.next() => {
                    match redis_msg {
                        Some(msg) => {
                            let payload_str: String = msg.get_payload().unwrap_or_default();
                            if session.text(payload_str).await.is_err() {
                                break; // Failed to deliver to WS client, teardown
                            }
                        }
                        None => {
                            // Redis Pub/Sub stream terminated
                            break;
                        }
                    }
                }
            }
        }

        log::debug!(
            "WebSocket notification session closed cleanly for user {}",
            user_id
        );
    });

    Ok(res)
}

/*
 * =========================================================================================
 * ARCHITECTURAL NOTICE: PERSISTENT NOTIFICATION CONSUMER & RESILIENCE
 * =========================================================================================
 * Context & Scenario:
 * Consumes `notification.push` messages from RabbitMQ:
 * 1. Batched / Transactional Persistence:
 *    - User alerts are persisted to PostgreSQL (`notification_schema.user_notifications`).
 *    - Transient database errors are retried using exponential backoff with jitter (`backon`).
 * 2. Cross-Node Fanout:
 *    - Once persisted, the message is fanned out across all active cluster nodes via Redis Pub/Sub.
 * 3. Dead Letter Queue & Deduplication:
 *    - If PostgreSQL or Redis remains permanently unavailable after maximum retries, messages
 *      must be pushed to a Dead Letter Queue (DLQ: `notification.dlq`) with full trace headers
 *      to avoid message loss or head-of-line blocking in the main queue.
 * =========================================================================================
 */
async fn rmq_notification_consumer(
    pool: PgPool,
    rmq_chan: lapin::Channel,
    redis_client: redis::Client,
) {
    let q = declare_queue_with_dlx(
        &rmq_chan,
        "notification_push_queue",
        "notification.push_dead_letter",
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

    // Acquire reusable multiplexed async connection for Redis publishing
    let mut redis_conn_opt = redis_client.get_multiplexed_async_connection().await.ok();

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            match serde_json::from_slice::<NotificationPush>(&delivery.data) {
                Ok(notif) => {
                    // 1. Persist to database with exponential retry
                    let insert_res = {
                        || async {
                            sqlx::query!(
                                "INSERT INTO notification_schema.user_notifications (user_id, notification_type, title, payload) VALUES ($1, $2, $3, $4)",
                                notif.user_id,
                                notif.notification_type,
                                notif.title,
                                notif.payload
                            )
                            .execute(&pool)
                            .await
                        }
                    }
                    .retry(ExponentialBuilder::default().with_jitter())
                    .when(betting_common::sqlx_retry_when)
                    .await;

                    if let Err(e) = insert_res {
                        log::error!(
                            "Failed to persist notification to DB after retries, routing to DLQ: {:?}",
                            e
                        );
                        let _ = delivery
                            .nack(BasicNackOptions {
                                requeue: false,
                                multiple: false,
                            })
                            .await;
                        continue;
                    }

                    // 2. Publish to Redis Pub/Sub for active WebSocket sessions across nodes
                    if redis_conn_opt.is_none() {
                        redis_conn_opt = redis_client.get_multiplexed_async_connection().await.ok();
                    }

                    if let Some(ref mut r_conn) = redis_conn_opt {
                        let channel_name = format!("notifications:{}", notif.user_id);
                        let notif_json = serde_json::to_string(&notif).unwrap_or_default();
                        let pub_res: Result<(), _> = r_conn.publish(channel_name, notif_json).await;
                        if let Err(e) = pub_res {
                            log::warn!("Redis publish failed, resetting connection: {:?}", e);
                            redis_conn_opt = None; // Reconnect on next iteration
                        }
                    }
                    let _ = delivery.ack(BasicAckOptions::default()).await;
                }
                Err(e) => {
                    log::error!(
                        "Malformed NotificationPush payload, routing to DLQ: {:?}",
                        e
                    );
                    let _ = delivery
                        .nack(BasicNackOptions {
                            requeue: false,
                            multiple: false,
                        })
                        .await;
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
    let pool = betting_common::connect_pg(&db_url, 5)
        .await
        .expect("Failed to connect to postgres");

    let rmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL required");
    let rmq_chan = betting_common::connect_rmq(&rmq_url, "notification-service")
        .await
        .expect("Failed to connect to RabbitMQ");

    let _ = rmq_chan
        .exchange_declare(
            exchanges::NOTIFICATION.into(),
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare notification exchange");

    let _ = setup_dlq(&rmq_chan).await;

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
