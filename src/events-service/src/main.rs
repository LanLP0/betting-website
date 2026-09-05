use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use backon::{ExponentialBuilder, Retryable};
use betting_common::{
    Event, EventOdds, EventSettled, EventSubscribeRequest, OddsRpcRequest, OddsRpcResponse,
    PaginationQuery, connect_pg, connect_rmq, declare_queue_with_dlx, exchanges,
    get_odds_for_event, http::BadRequestResponse, publish_event_props, publish_event_with_trace,
    req_get_request_id, req_get_user_role, setup_dlq, verify_hmac_signature,
};
use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use futures_util::stream::StreamExt;
use lapin::{BasicProperties, options::*, types::FieldTable};
use redis::AsyncCommands;
use serde::Deserialize;
use sqlx::PgPool;
use std::{env, time::Duration};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Deserialize)]
struct AddEventReq {
    name: String,
    description: Option<String>,
    teams: Vec<String>,
    odds: Vec<f64>,
}

#[derive(Deserialize)]
struct SettleEventReq {
    winning_selection: String,
}

struct AppState {
    db: PgPool,
    redis: redis::Client,
    rmq: lapin::Channel,
    webhook_secret: String,
}

async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

// Paginated events listing
async fn get_events(
    query: web::Query<PaginationQuery>,
    data: web::Data<AppState>,
) -> impl Responder {
    let limit = query.get_limit(50, 100);
    let offset = query.get_offset();

    let rows_req = {
        || async {
            sqlx::query!(
                "SELECT id, name, description, status, winning_selection, teams, odds, settled_at, created_at, COUNT(*) OVER() AS total_count FROM events_schema.events ORDER BY created_at DESC LIMIT $1 OFFSET $2",
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

    if rows_req.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    let rows = rows_req.unwrap();
    let count: i64 = if rows.is_empty() {
        0
    } else {
        rows[0].total_count.unwrap_or(0)
    };

    let events: Vec<_> = rows
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

    HttpResponse::Ok().json((count, events))
}

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

async fn get_event_odds(path: web::Path<Uuid>, data: web::Data<AppState>) -> impl Responder {
    let id = path.into_inner();

    match get_odds_for_event(id, &data.db, &data.redis).await {
        Some(event_odds) => HttpResponse::Ok().json(event_odds),
        None => HttpResponse::NotFound().body("Event not found or odds currently unavailable"),
    }
}

async fn add_event(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<AddEventReq>,
) -> impl Responder {
    let auth_user_role = req_get_user_role(&req);
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let name = body.name.trim();
    if name.is_empty() || name.len() > 255 {
        return HttpResponse::BadRequest().body("Event name must be between 1 and 255 characters");
    }

    if body.teams.len() < 2 {
        return HttpResponse::BadRequest().body("An event must have at least 2 teams");
    }

    if body.odds.len() != body.teams.len() {
        return HttpResponse::BadRequest().body("Odds array length must match teams array length");
    }

    for (team, odd) in body.teams.iter().zip(body.odds.iter()) {
        if team.trim().is_empty() {
            return HttpResponse::BadRequest().body("Team names cannot be empty");
        }
        if *odd < 1.01 || odd.is_nan() || odd.is_infinite() {
            return HttpResponse::BadRequest().body("All odds values must be at least 1.01");
        }
    }

    let description = body
        .description
        .clone()
        .unwrap_or_else(|| "Sporting match".to_string());

    let odds_dec: Vec<BigDecimal> = body
        .odds
        .iter()
        .map(|o| BigDecimal::from_f64(*o).unwrap_or_else(|| BigDecimal::from(2)))
        .collect();

    let id = Uuid::new_v4();
    let insert_res = {
        || async {
            sqlx::query!(
                r#"
                INSERT INTO events_schema.events (id, name, description, status, teams, odds)
                VALUES ($1, $2, $3, 'open', $4, $5)
                "#,
                id,
                name,
                description,
                &body.teams,
                &odds_dec as &[BigDecimal]
            )
            .execute(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    match insert_res {
        Ok(_) => {
            let trace_id = req_get_request_id(&req);
            let payload_val = serde_json::json!(EventOdds {
                event_id: id,
                status: "open".into(),
                winning_selection: None,
                teams: body.teams.clone(),
                odds: body.odds.clone()
            });

            // Cache in Redis for high-speed FastPath reads
            if let Ok(mut conn) = data.redis.get_multiplexed_async_connection().await {
                let payload_str = serde_json::to_string(&payload_val).unwrap_or_default();
                let redis_key = format!("odds:{}", id);
                let _: () = conn
                    .set_ex(redis_key, payload_str, 3000)
                    .await
                    .unwrap_or(());
            }

            // Publish event.created to Event topic exchange
            let _ = publish_event_with_trace(
                &data.rmq,
                exchanges::EVENT,
                "event.created",
                &payload_val,
                &trace_id,
            )
            .await;

            HttpResponse::Created().json(serde_json::json!({ "id": id }))
        }
        Err(e) => {
            log::error!("Database error in add_event: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

async fn delete_event(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let auth_user_role = req_get_user_role(&req);
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let id = path.into_inner();
    let delete_res = {
        || async {
            sqlx::query!(
                "DELETE FROM events_schema.events WHERE id = $1 RETURNING id",
                id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    match delete_res {
        Ok(Some(_)) => {
            let trace_id = req_get_request_id(&req);

            // Invalidate Redis cache
            if let Ok(mut conn) = data.redis.get_multiplexed_async_connection().await {
                let redis_key = format!("odds:{}", id);
                let _: () = conn.del(redis_key).await.unwrap_or(());
            }

            let ev = serde_json::json!({ "event_id": id });
            let _ = publish_event_with_trace(
                &data.rmq,
                exchanges::EVENT,
                "event.deleted",
                &ev,
                &trace_id,
            )
            .await;

            HttpResponse::Ok().finish()
        }
        Ok(None) => HttpResponse::NotFound().body("Event not found"),
        Err(e) => {
            log::error!("Database error in delete_event: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

async fn settle_event(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    body: web::Json<SettleEventReq>,
) -> impl Responder {
    let auth_user_role = req_get_user_role(&req);
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let event_id = path.into_inner();
    let winning_selection = body.winning_selection.trim();
    if winning_selection.is_empty() {
        return HttpResponse::BadRequest().body("Winning selection cannot be empty");
    }

    // 1. Verify event exists and validate that winning_selection is among match teams
    let event_row = {
        || async {
            sqlx::query!(
                "SELECT status, teams, odds FROM events_schema.events WHERE id = $1",
                event_id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    let event = match event_row {
        Ok(Some(ev)) => ev,
        Ok(None) => return HttpResponse::NotFound().body("Event not found"),
        Err(e) => {
            log::error!(
                "Database query failed in settle_event (ev_id: {}): {:?}",
                event_id,
                e
            );
            return HttpResponse::InternalServerError().finish();
        }
    };

    if event.status == "SETTLED" {
        return HttpResponse::BadRequest().json(BadRequestResponse {
            status: "failed".into(),
            err_code: "event_already_settled".into(),
            should_retry: false,
            msg: Some("Event is already settled".into()),
        });
    }

    if !event.teams.iter().any(|t| t == winning_selection) {
        return HttpResponse::BadRequest().json(BadRequestResponse {
            status: "failed".into(),
            err_code: "invalid_params".into(),
            should_retry: false,
            msg: Some("Winning selection must match one of the participating teams".into()),
        });
    }

    // 2. Atomically settle event in PostgreSQL
    let update_res = {
        || async {
            sqlx::query!(
                r#"
                UPDATE events_schema.events 
                SET status = 'SETTLED', winning_selection = $1, settled_at = NOW() 
                WHERE id = $2 AND status != 'SETTLED'
                RETURNING id
                "#,
                winning_selection,
                event_id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    match update_res {
        Ok(Some(_)) => {
            let trace_id = req_get_request_id(&req);

            // Update Redis cache with SETTLED state
            if let Ok(mut conn) = data.redis.get_multiplexed_async_connection().await {
                let payload_val = serde_json::json!(EventOdds {
                    event_id,
                    status: "SETTLED".into(),
                    winning_selection: Some(winning_selection.to_string()),
                    teams: event.teams,
                    odds: event
                        .odds
                        .into_iter()
                        .map(|o| o.to_f64().unwrap_or(0.0))
                        .collect(),
                });
                let payload_str = serde_json::to_string(&payload_val).unwrap_or_default();
                let redis_key = format!("odds:{}", event_id);
                let _: () = conn
                    .set_ex(redis_key, payload_str, 3000)
                    .await
                    .unwrap_or(());
            }

            let ev = EventSettled {
                event_id,
                winning_selection: winning_selection.to_string(),
            };

            let _ = publish_event_with_trace(
                &data.rmq,
                exchanges::EVENT,
                "event.settled",
                &ev,
                &trace_id,
            )
            .await;

            HttpResponse::Ok().finish()
        }
        Ok(None) => HttpResponse::BadRequest().json(BadRequestResponse {
            status: "failed".into(),
            err_code: "event_already_settled".into(),
            should_retry: false,
            msg: Some("Event already settled or concurrently updated".into()),
        }),
        Err(e) => {
            log::error!("Database error updating event settlement: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

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
        let _: () = conn
            .set_ex(redis_key, payload_str, 3000)
            .await
            .unwrap_or(());
    }

    let odds_d = body
        .odds
        .iter()
        .map(|o| BigDecimal::from_f64(*o).unwrap_or_default())
        .collect::<Vec<BigDecimal>>();

    let _ = {
        || async {
            sqlx::query!(
                "UPDATE events_schema.events SET status = $1, winning_selection = $2, odds = $3 WHERE id = $4",
                &body.status, body.winning_selection, odds_d.as_slice(), body.event_id
            )
            .execute(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

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

// Background RPC responder for internal services querying event odds (with DLQ integration)
async fn rpc_odds_consumer(pool: PgPool, rmq: lapin::Channel, redis_client: redis::Client) {
    let q =
        declare_queue_with_dlx(&rmq, "event_odds_rpc_queue", "event.odds.rpc_dead_letter").await;

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
            let mut retries = 0;
            loop {
                let delivery = consumer.next().await;
                if delivery.is_none() {
                    continue;
                }

                if let Some(Ok(delivery)) = delivery {
                    match serde_json::from_slice::<OddsRpcRequest>(&delivery.data) {
                        Ok(req) => {
                            if let (Some(reply_to), Some(corr_id)) = (
                                delivery.properties.reply_to().as_ref().map(|s| s.as_str()),
                                delivery.properties.correlation_id().as_ref(),
                            ) {
                                let event_odds =
                                    get_odds_for_event(req.event_id, &pool, &redis_client).await;

                                let response = OddsRpcResponse {
                                    event_id: req.event_id,
                                    success: event_odds.is_some(),
                                    event_odds,
                                };

                                let props =
                                    BasicProperties::default().with_correlation_id(corr_id.clone());
                                let _ =
                                    publish_event_props(&rmq, "", reply_to, response, props).await;
                            }
                            let _ = delivery.ack(BasicAckOptions::default()).await;
                        }
                        Err(e) => {
                            log::error!(
                                "Corrupted OddsRpcRequest payload, routing to DLQ: {:?}",
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
                } else {
                    // Consumer stream failure
                    if retries > 5 {
                        log::error!("RPC odds consumer retries exceeded maximum limit");
                    }
                    sleep(Duration::from_secs(1 * 2u64.pow(retries.min(5)))).await;
                    retries += 1;
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
    let rmq_chan = connect_rmq(&rmq_url, "events-service")
        .await
        .expect("Failed RMQ connection");

    let redis_url = env::var("REDIS_URL").expect("REDIS_URL required");
    let redis_client = redis::Client::open(redis_url).expect("Failed Redis connection");

    let webhook_secret = env::var("WEBHOOK_SECRET").expect("WEBHOOK_SECRET env var required");

    // Initialize Event Topic Exchange and Dead-Letter Exchange/Queue
    rmq_chan
        .exchange_declare(
            exchanges::EVENT.into(),
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    let _ = setup_dlq(&rmq_chan).await;

    // Spawn internal RPC responder
    tokio::spawn(rpc_odds_consumer(
        pool.clone(),
        rmq_chan.clone(),
        redis_client.clone(),
    ));

    // Subscribe to mock events supplier on service startup with retry
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();
    let sub_req = EventSubscribeRequest {
        webhook_url: "http://events-service:8080/api/v1/events/callback".into(),
        service_name: "events-service".into(),
    };

    {
        || async {
            let res = http_client
                .post("http://mock-service:8080/mock/api/v1/events/subscribe")
                .json(&sub_req)
                .send()
                .await
                .map_err(|e| e.to_string())?;

            if res.status().is_success() {
                log::info!("Events-service successfully subscribed to mock events feed.");
                Ok(())
            } else {
                Err(format!(
                    "Mock subscription returned status: {}",
                    res.status()
                ))
            }
        }
    }
    .retry(
        ExponentialBuilder::default()
            .with_max_times(10)
            .with_jitter(),
    )
    .await
    .unwrap();

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
            .route("/api/v1/events/mgmt/add", web::post().to(add_event))
            .route("/api/v1/events/mgmt/{id}", web::delete().to(delete_event))
            .route(
                "/api/v1/events/mgmt/{id}/settle",
                web::post().to(settle_event),
            )
            .route("/api/v1/events/callback", web::post().to(events_callback))
            .route("/api/v1/events/{id}", web::get().to(get_event))
            .route("/api/v1/events/{id}/odds", web::get().to(get_event_odds))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
