use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use backon::{ExponentialBuilder, Retryable};
use betting_common::{
    BetCancelled, BetRequested, BetWon, EventSettled, NotificationPush, WalletStatus, connect_pg,
    connect_rmq, declare_queue_with_dlx, exchanges, get_odds_for_event, publish_event,
    publish_event_with_trace, req_get_request_id, req_get_user_id, req_get_user_role, setup_dlq,
};
use bigdecimal::{BigDecimal, FromPrimitive, RoundingMode, ToPrimitive};
use futures_util::stream::StreamExt;
use lapin::{options::*, types::FieldTable};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
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

// Handle wallet lock funds outcome (SUCCESS / FAILED)
async fn handle_wallet_status(
    pool: &PgPool,
    rmq: &lapin::Channel,
    event: WalletStatus,
    _delivery: &lapin::message::Delivery,
) {
    let bet_status = {
        || async {
            sqlx::query!(
                "SELECT status, user_id, amount, odds_at_placement, selection FROM bets_schema.bets WHERE id = $1",
                event.bet_id
            )
            .fetch_optional(pool)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    let bet_row = match bet_status {
        Ok(Some(row)) => row,
        Ok(None) => {
            log::warn!(
                "Received wallet status for non-existent bet_id: {}",
                event.bet_id
            );
            if event.status == "SUCCESS" {
                // Bet record was missing; issue automatic refund
                let refund_ev = BetCancelled {
                    bet_id: event.bet_id,
                    user_id: Uuid::nil(),
                };
                let _ = publish_event(
                    rmq,
                    exchanges::BETTING,
                    "bet.cancel.request_refund",
                    refund_ev,
                )
                .await;
            }
            return;
        }
        Err(e) => {
            log::error!(
                "Database failure in handle_wallet_status (bet_id: {}): {:?}",
                event.bet_id,
                e
            );
            return;
        }
    };

    let s = bet_row.status;
    let status = if event.status == "SUCCESS" {
        match s.as_str() {
            "PENDING-LOST" => "LOST",
            "PENDING-WON" => "WON",
            "PENDING" => "CONFIRMED",
            _ => {
                log::info!("Bet {} already in final state {}", event.bet_id, s);
                return;
            }
        }
    } else {
        "FAILED"
    };

    let update_res = {
        || async {
            sqlx::query!(
                "UPDATE bets_schema.bets SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING user_id, amount, selection",
                status,
                event.bet_id
            )
            .fetch_optional(pool)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if let Ok(Some(row)) = update_res {
        let user_id: Uuid = row.user_id;
        let selection: String = row.selection;

        if status == "CONFIRMED" {
            let notif = NotificationPush {
                user_id,
                notification_type: "bet_confirmed".into(),
                title: "Bet Confirmed".into(),
                payload: serde_json::json!({
                    "content": [{"type": "text", "text": format!("Your bet on '{}' has been placed successfully.", selection)}],
                    "metadata": {"bet_id": event.bet_id}
                }),
            };
            let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
        } else if status == "FAILED" {
            let notif = NotificationPush {
                user_id,
                notification_type: "bet_failed".into(),
                title: "Bet Placement Failed".into(),
                payload: serde_json::json!({
                    "content": [{"type": "text", "text": format!("Your bet on '{}' could not be placed due to insufficient wallet funds.", selection)}],
                    "metadata": {"bet_id": event.bet_id}
                }),
            };
            let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
        } else if status == "WON" {
            // Settle bet payout
            let amount: BigDecimal = bet_row.amount;
            let odds: BigDecimal = bet_row.odds_at_placement;
            let payout = amount * odds;
            let payout_f = payout
                .with_scale_round(4, RoundingMode::HalfEven)
                .to_f64()
                .unwrap_or(0.0);

            let event_msg = BetWon {
                bet_id: event.bet_id,
                user_id,
                payout_amount: payout_f,
                payout_amount_full: payout.to_plain_string(),
            };
            let _ = publish_event(rmq, exchanges::BETTING, "bet.won", event_msg).await;

            let notif = NotificationPush {
                user_id,
                notification_type: "bet_won".into(),
                title: "Congratulations! You Won!".into(),
                payload: serde_json::json!({
                    "content": [{"type": "text", "text": format!("Your bet on '{}' won! Payout of ${:.2} is being credited to your wallet.", selection, payout_f)}],
                    "metadata": {"bet_id": event.bet_id, "payout": payout_f}
                }),
            };
            let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
        } else if status == "LOST" {
            let notif = NotificationPush {
                user_id,
                notification_type: "bet_lost".into(),
                title: "Bet Lost".into(),
                payload: serde_json::json!({
                    "content": [{"type": "text", "text": format!("Your bet on '{}' lost. The amount has been deducted from your wallet.", selection)}],
                    "metadata": {"bet_id": event.bet_id}
                }),
            };
            let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
        }
    }
}

// Handle event settlement and cascade payouts / notifications
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
                RETURNING id, user_id, amount, odds_at_placement, status, selection
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
        log::error!(
            "Database update failed during handle_event_settled (event_id: {})",
            event.event_id
        );
        return;
    }

    let updated_bets = res.unwrap();

    for bet in updated_bets {
        let bet_id: Uuid = bet.id;
        let user_id: Uuid = bet.user_id;
        let selection: String = bet.selection;

        if bet.status == "WON" {
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

            let notif = NotificationPush {
                user_id,
                notification_type: "bet_won".into(),
                title: "Bet Won!".into(),
                payload: serde_json::json!({
                    "content": [{"type": "text", "text": format!("Your bet on '{}' won! Payout of ${:.2} has been credited to your wallet.", selection, payout_f)}],
                    "metadata": {"bet_id": bet_id, "payout": payout_f}
                }),
            };
            let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
        } else if bet.status == "LOST" || bet.status == "PENDING-LOST" {
            let notif = NotificationPush {
                user_id,
                notification_type: "bet_lost".into(),
                title: "Bet Outcome Settled".into(),
                payload: serde_json::json!({
                    "content": [{"type": "text", "text": format!("Match settled. Your bet on '{}' was not successful this time.", selection)}],
                    "metadata": {"bet_id": bet_id}
                }),
            };
            let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
        }
    }
}

// Handle bet cancellation refund completion notification
async fn handle_bet_cancel_refunded(pool: &PgPool, rmq: &lapin::Channel, event: WalletStatus) {
    if event.status == "SUCCESS" {
        let update_res = {
            || async {
                sqlx::query!(
                    "UPDATE bets_schema.bets SET status = 'CANCELLED', updated_at = NOW() WHERE id = $1 RETURNING user_id, amount, selection",
                    event.bet_id
                )
                .fetch_optional(pool)
                .await
            }
        }
        .retry(ExponentialBuilder::default().with_jitter())
        .when(betting_common::sqlx_retry_when)
        .await;

        if let Ok(Some(row)) = update_res {
            let user_id: Uuid = row.user_id;
            let selection: String = row.selection;
            let amount: f64 = row.amount.to_f64().unwrap_or(0.0);

            let notif = NotificationPush {
                user_id,
                notification_type: "bet_cancelled".into(),
                title: "Bet Cancelled & Refunded".into(),
                payload: serde_json::json!({
                    "content": [{"type": "text", "text": format!("Your bet on '{}' was cancelled. ${:.2} has been refunded to your wallet.", selection, amount)}],
                    "metadata": {"bet_id": event.bet_id, "amount": amount}
                }),
            };
            let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
        }
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

    let query = {
        || async {
            sqlx::query!(
                "INSERT INTO bets_schema.bets (id, user_id, event_id, selection, odds_at_placement, amount, status) VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')",
                bet_id,
                user_id,
                body.event_id,
                body.selection,
                BigDecimal::from_f64(odd_val),
                amount_dec
            )
            .execute(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
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

    let bet = {
        || async {
            sqlx::query!(
                "SELECT user_id, status FROM bets_schema.bets WHERE id = $1",
                bet_id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if bet.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    let bet = bet.unwrap();
    if bet.is_none() {
        return HttpResponse::NotFound().body("Bet not found");
    }

    let b = bet.unwrap();
    if b.user_id != user_id {
        return HttpResponse::NotFound().body("Bet not found");
    }

    if b.status != "CONFIRMED" && !b.status.starts_with("PENDING") {
        return HttpResponse::BadRequest().body("Bet cannot be cancelled in its current state");
    }

    let trace_id = req_get_request_id(&req);
    let event = BetCancelled { bet_id, user_id };

    if publish_event_with_trace(
        &data.rmq,
        exchanges::BETTING,
        "bet.cancel.request_refund",
        event,
        &trace_id,
    )
    .await
    .is_err()
    {
        return HttpResponse::InternalServerError().body("Failed to process cancellation request");
    }

    HttpResponse::Accepted().json(serde_json::json!({
        "bet_id": bet_id,
        "status": "CANCELLING"
    }))
}

async fn get_bets_by_event(path: web::Path<Uuid>, data: web::Data<AppState>) -> impl Responder {
    let event_id = path.into_inner();

    let rows_req = {
        || async {
            sqlx::query!(
                "SELECT selection, amount FROM bets_schema.bets WHERE event_id = $1 AND status != 'FAILED' AND status != 'CANCELLED'",
                event_id
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

    let mut total_bets = 0;
    let mut total_volume = 0.0;
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut volumes: HashMap<String, f64> = HashMap::new();

    for row in rows {
        total_bets += 1;
        let amount_f = row.amount.to_f64().unwrap_or(0.0);
        total_volume += amount_f;

        *counts.entry(row.selection.clone()).or_insert(0) += 1;
        *volumes.entry(row.selection).or_insert(0.0) += amount_f;
    }

    let mut bets_by_selection = Vec::new();
    for (selection, count) in counts {
        let volume = volumes.get(&selection).copied().unwrap_or(0.0);
        bets_by_selection.push(SelectionMetrics {
            selection,
            bet_count: count,
            volume,
        });
    }

    HttpResponse::Ok().json(EventBetsMetricsResponse {
        event_id,
        total_bets,
        total_volume,
        bets_by_selection,
    })
}

// TODO pagination
async fn get_user_bets(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let user_id = path.into_inner();
    let auth_id = req_get_user_id(&req);
    let auth_role = req_get_user_role(&req);
    if auth_role.is_none() || auth_id.is_none() {
        return HttpResponse::Unauthorized().finish();
    }
    let auth_role = auth_role.unwrap();
    let auth_id = auth_id.unwrap();
    if auth_role != "admin" && auth_id != user_id {
        return HttpResponse::Forbidden().finish();
    }

    let rows_req = {
        || async {
            sqlx::query!(
                "SELECT id, user_id, event_id, selection, odds_at_placement, amount, status, created_at FROM bets_schema.bets WHERE user_id = $1 ORDER BY created_at DESC",
                user_id
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

    let res: Vec<BetResponse> = rows
        .into_iter()
        .map(|r| BetResponse {
            id: r.id,
            user_id: r.user_id,
            event_id: r.event_id,
            selection: r.selection,
            odds_at_placement: r.odds_at_placement.to_f64().unwrap_or(1.0),
            amount: r.amount.to_f64().unwrap_or(0.0),
            status: r.status,
            created_at: r.created_at,
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

    // Declare Exchanges and Dead-Letter Queue
    rmq_chan
        .exchange_declare(
            exchanges::BETTING.into(),
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

    // Consumer 1: Wallet status events (funds locked / insufficient)
    let pool_clone = pool.clone();
    let chan_clone = rmq_chan.clone();
    tokio::spawn(async move {
        let q = declare_queue_with_dlx(
            &chan_clone,
            "betting_wallet_status",
            "betting.wallet_status_dead_letter",
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
                match serde_json::from_slice::<WalletStatus>(&delivery.data) {
                    Ok(ev) => {
                        handle_wallet_status(&pool_clone, &chan_clone, ev, &delivery).await;
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                    }
                    Err(e) => {
                        log::error!("Malformed WalletStatus payload, routing to DLQ: {:?}", e);
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
    });

    // Consumer 2: Event settled
    let pool_clone2 = pool.clone();
    let chan_clone2 = rmq_chan.clone();
    tokio::spawn(async move {
        let q = declare_queue_with_dlx(
            &chan_clone2,
            "betting_event_settled",
            "betting.event_settled_dead_letter",
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
                match serde_json::from_slice::<EventSettled>(&delivery.data) {
                    Ok(ev) => {
                        handle_event_settled(&pool_clone2, &chan_clone2, ev).await;
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                    }
                    Err(e) => {
                        log::error!("Malformed EventSettled payload, routing to DLQ: {:?}", e);
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
    });

    // Consumer 3: Bet cancel refunded confirmation
    let pool_clone3 = pool.clone();
    let chan_clone3 = rmq_chan.clone();
    tokio::spawn(async move {
        let q = declare_queue_with_dlx(
            &chan_clone3,
            "betting_refund_responses",
            "betting.refund_dead_letter",
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
                match serde_json::from_slice::<WalletStatus>(&delivery.data) {
                    Ok(ev) => {
                        handle_bet_cancel_refunded(&pool_clone3, &chan_clone3, ev).await;
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                    }
                    Err(e) => {
                        log::error!(
                            "Malformed refund confirmation payload, routing to DLQ: {:?}",
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
