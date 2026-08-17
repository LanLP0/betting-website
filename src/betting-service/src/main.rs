use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use betting_common::{
    BetCancelled, BetRequested, BetWon, EventSettled, NotificationPush, WalletStatus, connect_pg,
    connect_rmq, exchanges, publish_event, publish_event_with_trace, req_get_request_id,
};
use bigdecimal::ToPrimitive;
use futures_util::stream::StreamExt;
use lapin::{options::*, types::FieldTable};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, types::BigDecimal};
use std::collections::HashMap;
use std::env;
use uuid::Uuid;

struct AppState {
    db: PgPool,
    rmq: lapin::Channel,
    redis: redis::Client,
}

#[derive(Deserialize)]
struct PlaceBetReq {
    event_id: Uuid,
    selection: String,
    amount: f64,
}

#[derive(Serialize, Deserialize)]
struct BetResponse {
    id: Uuid,
    user_id: Uuid,
    event_id: Uuid,
    selection: String,
    odds_at_placement: f64,
    amount: f64,
    status: String,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Serialize)]
struct SelectionMetrics {
    selection: String,
    bet_count: usize,
    volume: f64,
}

#[derive(Serialize)]
struct EventBetsMetricsResponse {
    event_id: Uuid,
    total_bets: usize,
    total_volume: f64,
    bets_by_selection: Vec<SelectionMetrics>,
}

async fn handle_wallet_status(
    pool: &PgPool,
    rmq: &lapin::Channel,
    event: WalletStatus,
    _delivery: &lapin::message::Delivery,
) {
    let status = if event.status == "SUCCESS" {
        "CONFIRMED"
    } else {
        "FAILED"
    };

    let bet = sqlx::query!(
        "UPDATE bets_schema.bets SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING user_id, amount, selection",
        status,
        event.bet_id
    )
    .fetch_optional(pool)
    .await;

    if let Ok(Some(row)) = bet {
        let user_id: Uuid = row.user_id;
        let selection: String = row.selection;
        if status == "CONFIRMED" {
            let notif = NotificationPush {
                user_id,
                notification_type: "bet_confirmed".into(),
                title: "Bet Confirmed".into(),
                message: format!("Your bet on '{}' has been placed successfully.", selection),
                payload: serde_json::json!({ "bet_id": event.bet_id }),
            };
            let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
        }
    }
}

async fn handle_event_settled(pool: &PgPool, rmq: &lapin::Channel, event: EventSettled) {
    let winning = event.winning_selection.clone();

    let _ = sqlx::query!(
        "UPDATE bets_schema.bets SET status = 'WON', updated_at = NOW() WHERE event_id = $1 AND selection = $2 AND status = 'CONFIRMED'",
        event.event_id,
        winning
    )
    .execute(pool)
    .await;

    let _ = sqlx::query!(
        "UPDATE bets_schema.bets SET status = 'LOST', updated_at = NOW() WHERE event_id = $1 AND selection != $2 AND status = 'CONFIRMED'",
        event.event_id,
        winning
    )
    .execute(pool)
    .await;

    let won_bets = sqlx::query!(
        "SELECT id, user_id, amount, odds_at_placement FROM bets_schema.bets WHERE event_id = $1 AND status = 'WON'",
        event.event_id
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    for bet in won_bets {
        let bet_id: Uuid = bet.id;
        let user_id: Uuid = bet.user_id;
        let amount: BigDecimal = bet.amount;
        let odds: BigDecimal = bet.odds_at_placement;

        let amt_f = amount.to_f64().unwrap_or(0.0);
        let odds_f = odds.to_f64().unwrap_or(1.0);
        let payout = amt_f * odds_f;

        let event_msg = BetWon {
            bet_id,
            user_id,
            payout_amount: payout,
        };

        let _ = publish_event(rmq, exchanges::BETTING, "bet.won", event_msg).await;
    }
}

async fn handle_bet_cancel_refunded(pool: &PgPool, event: WalletStatus) {
    if event.status == "SUCCESS" {
        let _ = sqlx::query!(
            "UPDATE bets_schema.bets SET status = 'CANCELLED', updated_at = NOW() WHERE id = $1",
            event.bet_id
        )
        .execute(pool)
        .await;
    }
}

async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

async fn place_bet(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<PlaceBetReq>,
) -> impl Responder {
    let user_id_str = match req.headers().get("X-User-ID") {
        Some(v) => v.to_str().unwrap_or(""),
        None => return HttpResponse::Unauthorized().finish(),
    };
    let user_id = match Uuid::parse_str(user_id_str) {
        Ok(id) => id,
        Err(_) => return HttpResponse::BadRequest().finish(),
    };

    if body.amount <= 0.0 || body.amount.is_nan() || body.amount.is_infinite() {
        return HttpResponse::BadRequest().body("Invalid bet amount");
    }

    // Read odds from Redis
    let redis_key = format!("odds:{}", body.event_id);
    let mut current_odds: Option<f64> = None;

    if let Ok(mut conn) = data.redis.get_multiplexed_async_connection().await {
        if let Ok(val) = conn.get::<_, String>(&redis_key).await {
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&val) {
                if let Some(teams) = json_val.get("teams").and_then(|t| t.as_array()) {
                    if let Some(odds) = json_val.get("odds").and_then(|o| o.as_array()) {
                        for (idx, team_val) in teams.iter().enumerate() {
                            if team_val.as_str() == Some(&body.selection) {
                                if let Some(odd_num) = odds.get(idx).and_then(|o| o.as_f64()) {
                                    current_odds = Some(odd_num);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let odds_val = match current_odds {
        Some(o) => o,
        None => return HttpResponse::BadRequest().body("Odds for event/selection not available"),
    };

    let amount_dec = match BigDecimal::try_from(body.amount) {
        Ok(a) => a,
        Err(_) => return HttpResponse::BadRequest().body("Invalid decimal amount"),
    };
    let odds_dec = match BigDecimal::try_from(odds_val) {
        Ok(o) => o,
        Err(_) => return HttpResponse::BadRequest().body("Invalid odds value"),
    };
    let bet_id = Uuid::new_v4();

    let query = sqlx::query!(
        "INSERT INTO bets_schema.bets (id, user_id, event_id, selection, odds_at_placement, amount, status) VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')",
        bet_id,
        user_id,
        body.event_id,
        body.selection,
        odds_dec,
        amount_dec
    )
    .execute(&data.db)
    .await;

    if query.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    let trace_id = req_get_request_id(&req);
    let event_msg = BetRequested {
        bet_id,
        user_id,
        amount: body.amount,
    };

    if publish_event_with_trace(
        &data.rmq,
        exchanges::BETTING,
        "bet.requested",
        event_msg,
        &trace_id,
    )
    .await
    .is_err()
    {
        let _ = sqlx::query!(
            "UPDATE bets_schema.bets SET status = 'FAILED' WHERE id = $1",
            bet_id
        )
        .execute(&data.db)
        .await;
        return HttpResponse::InternalServerError().body("Failed to queue bet");
    }

    HttpResponse::Accepted().json(serde_json::json!({
        "bet_id": bet_id,
        "status": "PENDING"
    }))
}

async fn cancel_bet(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let bet_id = path.into_inner();
    let user_id_str = match req.headers().get("X-User-ID") {
        Some(v) => v.to_str().unwrap_or(""),
        None => return HttpResponse::Unauthorized().finish(),
    };
    let user_id = match Uuid::parse_str(user_id_str) {
        Ok(id) => id,
        Err(_) => return HttpResponse::BadRequest().finish(),
    };

    let bet = sqlx::query!(
        "SELECT status FROM bets_schema.bets WHERE id = $1 AND user_id = $2",
        bet_id,
        user_id
    )
    .fetch_optional(&data.db)
    .await;

    if let Ok(Some(b)) = bet {
        let status: String = b.status;
        if status == "CONFIRMED" {
            let _ = sqlx::query!(
                "UPDATE bets_schema.bets SET status = 'CANCEL_PENDING' WHERE id = $1",
                bet_id
            )
            .execute(&data.db)
            .await;

            let trace_id = req_get_request_id(&req);
            let _ = publish_event_with_trace(
                &data.rmq,
                exchanges::BETTING,
                "bet.cancel.request_refund",
                BetCancelled { user_id, bet_id },
                &trace_id,
            )
            .await;

            return HttpResponse::Ok().json(serde_json::json!({ "status": "CANCEL_PENDING" }));
        }
    }

    HttpResponse::BadRequest().body("Bet cannot be cancelled")
}

async fn get_bets_by_event(path: web::Path<Uuid>, data: web::Data<AppState>) -> impl Responder {
    let event_id = path.into_inner();

    let rows = sqlx::query!(
        "SELECT selection, amount FROM bets_schema.bets WHERE event_id = $1",
        event_id
    )
    .fetch_all(&data.db)
    .await
    .unwrap_or_default();

    let total_bets = rows.len();
    let total_volume: f64 = rows.iter().map(|r| r.amount.to_f64().unwrap_or(0.0)).sum();

    let mut selection_map: HashMap<String, (usize, f64)> = HashMap::new();
    for r in &rows {
        let amt = r.amount.to_f64().unwrap_or(0.0);
        let entry = selection_map.entry(r.selection.clone()).or_insert((0, 0.0));
        entry.0 += 1;
        entry.1 += amt;
    }

    let bets_by_selection: Vec<SelectionMetrics> = selection_map
        .into_iter()
        .map(|(selection, (bet_count, volume))| SelectionMetrics {
            selection,
            bet_count,
            volume,
        })
        .collect();

    HttpResponse::Ok().json(EventBetsMetricsResponse {
        event_id,
        total_bets,
        total_volume,
        bets_by_selection,
    })
}

async fn get_user_bets(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_user_id = path.into_inner();

    if let Some(h) = req.headers().get("X-User-ID") {
        if let Ok(id_str) = h.to_str() {
            if let Ok(uid) = Uuid::parse_str(id_str) {
                let role = req
                    .headers()
                    .get("X-User-Role")
                    .and_then(|r| r.to_str().ok())
                    .unwrap_or("");
                if uid != target_user_id && role != "admin" {
                    return HttpResponse::Forbidden().finish();
                }
            }
        }
    }

    let rows = sqlx::query!(
        "SELECT id, user_id, event_id, selection, odds_at_placement, amount, status, created_at FROM bets_schema.bets WHERE user_id = $1 ORDER BY created_at DESC",
        target_user_id
    )
    .fetch_all(&data.db)
    .await
    .unwrap_or_default();

    let res: Vec<_> = rows
        .into_iter()
        .map(|r| {
            let odds: BigDecimal = r.odds_at_placement;
            let amt: BigDecimal = r.amount;
            BetResponse {
                id: r.id,
                user_id: r.user_id,
                event_id: r.event_id,
                selection: r.selection,
                odds_at_placement: odds.to_f64().unwrap_or(1.0),
                amount: amt.to_f64().unwrap_or(0.0),
                status: r.status,
                created_at: r.created_at,
            }
        })
        .collect();

    HttpResponse::Ok().json(res)
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

    rmq_chan
        .exchange_declare(
            exchanges::BETTING.into(),
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // Consumer 1: Wallet status responses
    let pool_clone = pool.clone();
    let chan_clone = rmq_chan.clone();
    tokio::spawn(async move {
        let q = chan_clone
            .queue_declare(
                "betting_wallet_responses".into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();

        chan_clone
            .queue_bind(
                q.name().to_owned(),
                exchanges::WALLET.into(),
                "wallet.funds_locked".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone
            .queue_bind(
                q.name().to_owned(),
                exchanges::WALLET.into(),
                "wallet.funds_insufficient".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        let mut consumer = chan_clone
            .basic_consume(
                q.name().to_owned(),
                "betting_c1".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        while let Some(delivery) = consumer.next().await {
            if let Ok(delivery) = delivery {
                if let Ok(ev) = serde_json::from_slice::<WalletStatus>(&delivery.data) {
                    handle_wallet_status(&pool_clone, &chan_clone, ev, &delivery).await;
                }
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
        }
    });

    // Consumer 2: Event settled
    let pool_clone2 = pool.clone();
    let chan_clone2 = rmq_chan.clone();
    tokio::spawn(async move {
        let q = chan_clone2
            .queue_declare(
                "betting_event_settled".into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();

        chan_clone2
            .queue_bind(
                q.name().to_owned(),
                exchanges::EVENT.into(),
                "event.settled".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        let mut consumer = chan_clone2
            .basic_consume(
                q.name().to_owned(),
                "betting_c2".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        while let Some(delivery) = consumer.next().await {
            if let Ok(delivery) = delivery {
                if let Ok(ev) = serde_json::from_slice::<EventSettled>(&delivery.data) {
                    handle_event_settled(&pool_clone2, &chan_clone2, ev).await;
                }
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
        }
    });

    // Consumer 3: Bet cancel refunded confirmation (GAP-11)
    let pool_clone3 = pool.clone();
    let chan_clone3 = rmq_chan.clone();
    tokio::spawn(async move {
        let q = chan_clone3
            .queue_declare(
                "betting_refund_responses".into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();

        chan_clone3
            .queue_bind(
                q.name().to_owned(),
                exchanges::BETTING.into(),
                "bet.cancel.refunded".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        let mut consumer = chan_clone3
            .basic_consume(
                q.name().to_owned(),
                "betting_c3".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        while let Some(delivery) = consumer.next().await {
            if let Ok(delivery) = delivery {
                if let Ok(ev) = serde_json::from_slice::<WalletStatus>(&delivery.data) {
                    handle_bet_cancel_refunded(&pool_clone3, ev).await;
                }
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
        }
    });

    let state = web::Data::new(AppState {
        db: pool,
        rmq: rmq_chan,
        redis: redis_client,
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .route("/api/v1/bets/health", web::get().to(get_health))
            .route("/api/v1/bets", web::post().to(place_bet))
            .route("/api/v1/bets/{id}", web::delete().to(cancel_bet))
            .route("/api/v1/bets/event/{id}", web::get().to(get_bets_by_event))
            .route("/api/v1/bets/user/{id}", web::get().to(get_user_bets))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
