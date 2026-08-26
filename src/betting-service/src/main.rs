use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use backon::{ExponentialBuilder, Retryable};
use betting_common::{
    BetCancelled, BetRequested, BetWon, EventSettled, NotificationPush, WalletStatus, connect_pg,
    connect_rmq, exchanges, get_odds_for_event, publish_event, publish_event_with_trace,
    req_get_request_id,
};
use bigdecimal::{FromPrimitive, RoundingMode, ToPrimitive};
use futures_util::stream::StreamExt;
use lapin::{options::*, types::FieldTable};
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

// Checked
async fn handle_wallet_status(
    pool: &PgPool,
    rmq: &lapin::Channel,
    event: WalletStatus,
    _delivery: &lapin::message::Delivery,
) {
    let bet_status = {
        || async {
            sqlx::query!(
                "SELECT status FROM bets_schema.bets WHERE id = $1",
                event.bet_id
            )
            .fetch_optional(pool)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if bet_status.is_err() {
        // TODO push event back into dead letter queue
        return;
    }

    let bet_status = bet_status.unwrap();

    if bet_status.is_none() {
        if event.status == "SUCCESS" {
            // TODO refund here
        }
        return;
    }

    let s = bet_status.unwrap().status;
    let status = if event.status == "SUCCESS" {
        match s.as_str() {
            "PENDING-LOST" => "LOST", // When the event is settled before wallet respond
            "PENDING-WON" => "WON",
            "PENDING" => "CONFIRMED",
            _ => unreachable!(), // Possible scenario: wallet status event is fired twice (shouldn't happen due to RMQ message acknowledgement)
        }
    } else {
        "FAILED"
    };

    if status == "WON" {
        // TODO payout here - push bet.won event
    }

    let bet = {|| async {sqlx::query!(
        "UPDATE bets_schema.bets SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING user_id, amount, selection",
        status,
        event.bet_id
    )
    .fetch_optional(pool)
    .await}}.retry(ExponentialBuilder::default().with_jitter()).when(betting_common::sqlx_retry_when).await;

    if let Ok(Some(row)) = bet {
        let user_id: Uuid = row.user_id;
        let selection: String = row.selection;
        if status == "CONFIRMED" {
            let notif = NotificationPush {
                user_id,
                notification_type: "bet_confirmed".into(),
                title: "Bet Confirmed".into(),
                payload: serde_json::json!({ "content": [{"type": "text", "text": format!("Your bet on '{}' has been placed successfully.", selection)}], "metadata": {"bet_id": event.bet_id} }),
            };
            let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
        } else {
            // TODO notification
        }
    }
}

// Checked
async fn handle_event_settled(pool: &PgPool, rmq: &lapin::Channel, event: EventSettled) {
    let winning = event.winning_selection.clone();

    let res = {
        || async {
            sqlx::query!(
                r#"
                UPDATE bets_schema.bets 
                SET 
                    status = CASE 
                        WHEN status = 'CONFIRMED' AND selection = $2 THEN 'WON'
                        WHEN status = 'CONFIRMED' AND selection != $2 THEN 'LOST'
                        WHEN status = 'PENDING' AND selection = $2 THEN 'PENDING-WON'
                        WHEN status = 'PENDING' AND selection != $2 THEN 'PENDING-LOST'
                        ELSE status 
                    END,
                    updated_at = NOW()
                WHERE event_id = $1 
                AND status IN ('CONFIRMED', 'PENDING')
                RETURNING id, user_id, amount, odds_at_placement, status
                "#,
                event.event_id,
                winning
            )
            .fetch_all(pool)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if res.is_err() {
        return;
    }

    let updated_bets = res.unwrap();

    for bet in updated_bets {
        if bet.status != "WON" {
            // TODO notification for LOST
            continue;
        }

        let bet_id: Uuid = bet.id;
        let user_id: Uuid = bet.user_id;
        let amount: BigDecimal = bet.amount;
        let odds: BigDecimal = bet.odds_at_placement;
        let payout = amount * odds;

        let payout_f = payout
            .with_scale_round(4, RoundingMode::HalfEven)
            .to_f64()
            .unwrap_or(0.0);

        let event_msg = BetWon {
            bet_id,
            user_id,
            payout_amount: payout_f,
            payout_amount_full: payout.to_plain_string(),
        };

        let _ = publish_event(rmq, exchanges::BETTING, "bet.won", event_msg).await;
    }
}

// Checked
async fn handle_bet_cancel_refunded(pool: &PgPool, event: WalletStatus) {
    if event.status == "SUCCESS" {
        let _ = sqlx::query!(
            "UPDATE bets_schema.bets SET status = 'CANCELLED', updated_at = NOW() WHERE id = $1",
            event.bet_id
        )
        .execute(pool)
        .await;

        // TODO refund notification
    }
}

async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

// Checked
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

    let odds = get_odds_for_event(body.event_id, &data.db, &data.redis).await;

    if odds.is_none() {
        return HttpResponse::BadRequest().body("Odds for event/selection not available");
    }

    let odds = odds.unwrap();
    let i = odds.teams.iter().position(|t| t == &body.selection);

    let odd_val = match i {
        Some(i) => odds.odds[i],
        None => return HttpResponse::BadRequest().body("Odds for event/selection not available"),
    };

    let amount_dec = match BigDecimal::try_from(body.amount) {
        Ok(a) => a,
        Err(_) => return HttpResponse::BadRequest().body("Invalid decimal amount"),
    };
    let bet_id = Uuid::new_v4();

    let query = {|| async {sqlx::query!(
        "INSERT INTO bets_schema.bets (id, user_id, event_id, selection, odds_at_placement, amount, status) VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')",
        bet_id,
        user_id,
        body.event_id,
        body.selection,
        BigDecimal::from_f64(odd_val),
        amount_dec
    )
    .execute(&data.db)
    .await}}.retry(ExponentialBuilder::default().with_jitter()).when(betting_common::sqlx_retry_when).await;

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
        // TODO handle SQL failure via dead letter queue
        let _ = {
            || async {
                sqlx::query!(
                    "UPDATE bets_schema.bets SET status = 'FAILED' WHERE id = $1",
                    bet_id
                )
                .execute(&data.db)
                .await
            }
        }
        .retry(ExponentialBuilder::default().with_jitter())
        .when(betting_common::sqlx_retry_when)
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
    let rmq_chan = connect_rmq(&rmq_url, "betting-service")
        .await
        .expect("Failed RMQ connection");

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
