use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
use bigdecimal::ToPrimitive;
use futures_util::{lock::Mutex, stream::StreamExt};
use lapin::{
    BasicProperties, Connection, ConnectionProperties, options::*,
    publisher_confirm::PublisherConfirm, types::FieldTable,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions, types::BigDecimal};
use std::{collections::HashMap, env, sync::Arc};
use tokio::sync::oneshot;
use uuid::Uuid;

struct AppState {
    db: PgPool,
    rmq: lapin::Channel,
    redis_client: redis::Client,
    wallet_checkpoints: Arc<Mutex<HashMap<String, oneshot::Sender<WalletStatus>>>>,
}

#[derive(Deserialize)]
struct PlaceBetReq {
    event_id: Uuid,
    selection: String,
    amount: f64,
}

#[derive(Serialize)]
struct BetRequested {
    bet_id: Uuid,
    user_id: Uuid,
    amount: f64,
}

#[derive(Deserialize)]
struct WalletStatus {
    bet_id: Uuid,
    status: String,
}

#[derive(Deserialize)]
struct EventSettled {
    event_id: Uuid,
    winning_selection: String,
}

#[derive(Serialize)]
struct BetWon {
    bet_id: Uuid,
    user_id: Uuid,
    payout_amount: f64,
}

async fn publish_event_props(
    channel: &lapin::Channel,
    exchange: &str,
    routing_key: &str,
    payload: impl Serialize,
    properties: BasicProperties,
) -> Result<PublisherConfirm, lapin::Error> {
    let payload = serde_json::to_vec(&payload).unwrap();
    channel
        .basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions::default(),
            &payload,
            properties,
        )
        .await
}

async fn publish_event(
    channel: &lapin::Channel,
    exchange: &str,
    routing_key: &str,
    payload: impl Serialize,
) -> Result<PublisherConfirm, lapin::Error> {
    publish_event_props(
        channel,
        exchange,
        routing_key,
        payload,
        BasicProperties::default(),
    )
    .await
}

async fn place_bet(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<PlaceBetReq>,
) -> impl Responder {
    let user_id_str = req.headers().get("X-User-ID").unwrap().to_str().unwrap();
    let user_id = Uuid::parse_str(user_id_str).unwrap();

    // Read odds from Redis
    let mut redis_conn = match data.redis_client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let odds_json: Option<String> = redis_conn.get(format!("odds:{}", body.event_id)).await.ok();
    let odds_value = if let Some(j) = odds_json {
        let v: serde_json::Value = serde_json::from_str(&j).unwrap_or_default();
        v.get(&body.selection)
            .and_then(|val| val.as_f64())
            .unwrap_or(2.0) // Default odds 2.0 for mock if missing
    } else {
        2.0
    };

    let bet_id = Uuid::new_v4();
    let amount = BigDecimal::try_from(body.amount).unwrap();
    let odds_bd = BigDecimal::try_from(odds_value).unwrap();

    if let Err(_) = sqlx::query!(
        "INSERT INTO bets_schema.bets (id, user_id, event_id, selection, odds_at_placement, amount, status) VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')",
        bet_id, user_id, body.event_id, body.selection, odds_bd, amount
    ).execute(&data.db).await {
        return HttpResponse::InternalServerError().finish();
    }

    let ev = BetRequested {
        bet_id,
        user_id,
        amount: body.amount,
    };

    let corr_id = Uuid::new_v4().to_string();
    let (tx, rx) = oneshot::channel::<WalletStatus>();
    data.wallet_checkpoints
        .lock()
        .await
        .insert(corr_id.clone(), tx);

    if let Err(_) = publish_event_props(
        &data.rmq,
        "betting_topic",
        "bet.requested",
        &ev,
        BasicProperties::default().with_correlation_id(corr_id.into()),
    )
    .await
    {
        // Rollback if publish_event failed
        let _ = sqlx::query!(
            "UPDATE bets_schema.bets SET status = 'FAILED' WHERE id = $1",
            bet_id
        )
        .execute(&data.db)
        .await;
        return HttpResponse::InternalServerError().finish();
    }

    let status = match rx.await {
        Ok(s) => s,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    #[derive(Serialize)]
    struct PlaceRes {
        bet_id: Uuid,
        status: String,
    }
    HttpResponse::Accepted().json(PlaceRes {
        bet_id,
        status: status.status,
    })
}

async fn cancel_bet(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    let bet_id = Uuid::parse_str(req.match_info().get("id").unwrap()).unwrap();
    let _ = sqlx::query!(
        "UPDATE bets_schema.bets SET status = 'CANCELLED' WHERE id = $1 AND status = 'PENDING'",
        bet_id
    )
    .execute(&data.db)
    .await;
    HttpResponse::Ok().finish()
}

async fn handle_wallet_status(pool: &PgPool, rmq: &lapin::Channel, status: WalletStatus) {
    let current_status = sqlx::query!(
        "SELECT status FROM bets_schema.bets WHERE id = $1",
        status.bet_id
    )
    .fetch_one(pool)
    .await
    .unwrap()
    .status;

    let new_status = if status.status == "SUCCESS" {
        if current_status == "PENDING-WON" {
            let bet = sqlx::query!(
                "SELECT id, user_id, amount, odds_at_placement FROM bets_schema.bets WHERE id = $1",
                status.bet_id
            )
            .fetch_one(pool)
            .await
            .unwrap();
            let amount_f = bet.amount.to_f64().unwrap_or(0.0);
            let odds_f = bet.odds_at_placement.to_f64().unwrap_or(1.0);
            let payout = amount_f * odds_f;
            let _ = publish_event(
                rmq,
                "betting_topic",
                "bet.won",
                BetWon {
                    bet_id: bet.id,
                    user_id: bet.user_id,
                    payout_amount: payout,
                },
            )
            .await;
            "WON"
        } else if current_status == "PENDING-LOST" {
            "LOST"
        } else {
            "CONFIRMED"
        }
    } else {
        "FAILED"
    };
    let _ = sqlx::query!(
        "UPDATE bets_schema.bets SET status = $1 WHERE id = $2",
        new_status,
        status.bet_id
    )
    .execute(pool)
    .await;
}

async fn handle_event_settled(pool: &PgPool, rmq: &lapin::Channel, event: EventSettled) {
    const BATCH_SIZE: i64 = 1000;
    let mut last_id = Uuid::nil();

    // Settle Confirmed/Pending Bets in batches to prevent out-of-memory errors
    loop {
        let bets = sqlx::query!(
            "SELECT id, user_id, selection, amount, odds_at_placement, status \
             FROM bets_schema.bets \
             WHERE event_id = $1 AND (status = 'CONFIRMED' OR status = 'PENDING') AND id > $2 \
             ORDER BY id LIMIT $3",
            event.event_id,
            last_id,
            BATCH_SIZE
        )
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        if bets.is_empty() {
            break;
        }

        last_id = bets.last().unwrap().id;

        for bet in bets {
            if bet.status == "PENDING" {
                // User has placed a bet but wallet service hadn't yet confirmed
                let new_status = match bet.selection == event.winning_selection {
                    true => "PENDING-WON",
                    false => "PENDING-LOST",
                };
                let _ = sqlx::query!(
                    "UPDATE bets_schema.bets SET status = $1 WHERE id = $2",
                    new_status,
                    bet.id
                )
                .execute(pool)
                .await;
                continue;
            }

            if bet.selection == event.winning_selection {
                let _ = sqlx::query!(
                    "UPDATE bets_schema.bets SET status = 'WON' WHERE id = $1",
                    bet.id
                )
                .execute(pool)
                .await;
                let amount_f = bet.amount.to_f64().unwrap_or(0.0);
                let odds_f = bet.odds_at_placement.to_f64().unwrap_or(1.0);
                let payout = amount_f * odds_f;
                let _ = publish_event(
                    rmq,
                    "betting_topic",
                    "bet.won",
                    BetWon {
                        bet_id: bet.id,
                        user_id: bet.user_id,
                        payout_amount: payout,
                    },
                )
                .await;
            } else {
                let _ = sqlx::query!(
                    "UPDATE bets_schema.bets SET status = 'LOST' WHERE id = $1",
                    bet.id
                )
                .execute(pool)
                .await;
            }
        }
    }

    // Delete FAILED bets
    let _ = sqlx::query!(
        "DELETE FROM bets_schema.bets WHERE event_id = $1 AND status = 'FAILED'",
        event.event_id
    )
    .execute(pool)
    .await;
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db_url = env::var("DATABASE_URL").unwrap();
    let pool = PgPoolOptions::new().connect(&db_url).await.unwrap();

    let redis_url = env::var("REDIS_URL").unwrap();
    let redis_client = redis::Client::open(redis_url).unwrap();

    let rmq_url = env::var("RABBITMQ_URL").unwrap();
    let rmq_conn = Connection::connect(&rmq_url, ConnectionProperties::default())
        .await
        .unwrap();
    let rmq_chan = rmq_conn.create_channel().await.unwrap();

    rmq_chan
        .exchange_declare(
            "betting_topic",
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    rmq_chan
        .basic_qos(10, BasicQosOptions::default()) //? 10 or 100 prefetch
        .await
        .unwrap();

    let wallet_checkpoints = Arc::new(Mutex::new(
        HashMap::<String, oneshot::Sender<WalletStatus>>::new(),
    ));

    // Consumers
    let pool_clone = pool.clone();
    let chan_clone = rmq_chan.clone();
    let wallet_checkpoints_cloned = wallet_checkpoints.clone();
    tokio::spawn(async move {
        let q = chan_clone
            .queue_declare(
                "betting_wallet_responses",
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
                q.name().as_str(),
                "wallet_topic",
                "wallet.funds_locked",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone
            .queue_bind(
                q.name().as_str(),
                "wallet_topic",
                "wallet.funds_insufficient",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let mut consumer = chan_clone
            .basic_consume(
                q.name().as_str(),
                "bet_c1",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        while let Some(delivery) = consumer.next().await {
            if let Ok(delivery) = delivery {
                if let Ok(ev) = serde_json::from_slice::<WalletStatus>(&delivery.data) {
                    if let Some(id) = delivery.properties.correlation_id()
                        && let Some(tx) = wallet_checkpoints_cloned.lock().await.remove(id.as_str())
                    {
                        let _ = tx.send(ev);
                        continue;
                    }

                    handle_wallet_status(&pool_clone, &chan_clone, ev).await;
                }
                let _ = delivery
                    .ack(lapin::options::BasicAckOptions::default())
                    .await;
            }
        }
    });

    let pool_clone2 = pool.clone();
    let chan_clone2 = rmq_chan.clone();
    tokio::spawn(async move {
        let q = chan_clone2
            .queue_declare(
                "betting_event_settlements",
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
                q.name().as_str(),
                "event_topic",
                "event.settled",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let mut consumer = chan_clone2
            .basic_consume(
                q.name().as_str(),
                "bet_c2",
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
                let _ = delivery
                    .ack(lapin::options::BasicAckOptions::default())
                    .await;
            }
        }
    });

    let state = web::Data::new(AppState {
        db: pool,
        rmq: rmq_chan,
        redis_client,
        wallet_checkpoints,
    });
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/api/v1/bets", web::post().to(place_bet))
            .route("/api/v1/bets/{id}", web::delete().to(cancel_bet))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
