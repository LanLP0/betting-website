use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
use backon::ExponentialBuilder;
use backon::Retryable;
use bigdecimal::ToPrimitive;
use futures_util::stream::StreamExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties, options::*,
    publisher_confirm::PublisherConfirm, types::FieldTable,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions, types::BigDecimal};
use std::env;
use uuid::Uuid;

struct AppState {
    db: PgPool,
    #[allow(dead_code)]
    rmq: lapin::Channel,
}

#[derive(Deserialize)]
struct FundReq {
    amount: f64,
}

#[derive(Serialize, Deserialize)]
struct UserEvent {
    id: Uuid,
    username: String,
}

#[derive(Serialize, Deserialize)]
struct BetRequested {
    bet_id: Uuid,
    user_id: Uuid,
    amount: f64,
}

#[derive(Serialize, Deserialize)]
struct BetWon {
    bet_id: Uuid,
    user_id: Uuid,
    payout_amount: f64,
}

#[derive(Serialize, Deserialize)]
struct BetCancelled {
    user_id: Uuid,
    bet_id: Uuid,
}

#[derive(Serialize)]
struct WalletStatus {
    bet_id: Uuid,
    status: String,
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

async fn handle_user_create_wallet(pool: &PgPool, event: UserEvent) {
    let _ = sqlx::query!(
        "INSERT INTO wallet_schema.wallets (user_id, balance) VALUES ($1, 0.00) ON CONFLICT DO NOTHING",
        event.id
    )
    .execute(pool)
    .await;
}

async fn handle_bet_requested(
    pool: &PgPool,
    rmq: &lapin::Channel,
    event: BetRequested,
    delivery: &lapin::message::Delivery,
) {
    let mut tx = match pool.begin().await {
        Ok(t) => t,
        Err(_) => return,
    };

    let wallet = sqlx::query!(
        "SELECT balance FROM wallet_schema.wallets WHERE user_id = $1 FOR UPDATE",
        event.user_id
    )
    .fetch_optional(&mut *tx)
    .await
    .ok()
    .flatten();

    let mut props = BasicProperties::default();
    if let Some(w) = wallet {
        let amount = BigDecimal::try_from(event.amount).unwrap();
        if w.balance >= amount {
            let _ = sqlx::query!(
                "UPDATE wallet_schema.wallets SET balance = balance - $1 WHERE user_id = $2",
                amount,
                event.user_id
            )
            .execute(&mut *tx)
            .await;

            let _ = sqlx::query!(
                "INSERT INTO wallet_schema.transactions (user_id, amount, type, reference_id) VALUES ($1, $2, 'BET_PLACED', $3)",
                event.user_id,
                amount,
                event.bet_id
            )
            .execute(&mut *tx)
            .await;

            let _ = tx.commit().await;
            if let Some(corr_id) = delivery.properties.correlation_id() {
                props = props.with_correlation_id(corr_id.to_owned());
            }
            publish_event_props(
                rmq,
                "wallet_topic",
                "wallet.funds_locked",
                WalletStatus {
                    bet_id: event.bet_id,
                    status: "SUCCESS".into(),
                },
                props,
            )
            .await;
            return;
        }
    }

    let _ = tx.rollback().await;

    if let Some(corr_id) = delivery.properties.correlation_id() {
        props = props.with_correlation_id(corr_id.to_owned());
    }
    publish_event_props(
        rmq,
        "wallet_topic",
        "wallet.funds_insufficient",
        WalletStatus {
            bet_id: event.bet_id,
            status: "FAILED".into(),
        },
        props,
    )
    .await;
}

async fn handle_bet_won(pool: &PgPool, event: BetWon) {
    let mut tx = match pool.begin().await {
        Ok(t) => t,
        Err(_) => return,
    };

    let amount = BigDecimal::try_from(event.payout_amount).unwrap();
    let _ = sqlx::query!(
        "UPDATE wallet_schema.wallets SET balance = balance + $1 WHERE user_id = $2",
        amount,
        event.user_id
    )
    .execute(&mut *tx)
    .await;

    let _ = sqlx::query!(
        "INSERT INTO wallet_schema.transactions (user_id, amount, type, reference_id) VALUES ($1, $2, 'BET_WON', $3)",
        event.user_id,
        amount,
        event.bet_id
    ).execute(&mut *tx).await;

    let _ = tx.commit().await;
}

async fn handle_bet_cancel_request_refund(
    pool: &PgPool,
    rmq: &lapin::Channel,
    event: BetCancelled,
) {
    let trans = sqlx::query!("SELECT * FROM wallet_schema.transactions WHERE user_id = $1 AND type = 'BET_PLACED' AND reference_id = $2", event.user_id, event.bet_id).fetch_optional(pool).await;
    if let Some(t) = trans.unwrap() {
        let mut tx = pool.begin().await.unwrap();
        let _ = sqlx::query!(
            "UPDATE wallet_schema.wallets SET balance = balance + $1 WHERE user_id = $2",
            t.amount,
            event.user_id
        )
        .execute(&mut *tx)
        .await;
        let _ = sqlx::query!(
            "INSERT INTO wallet_schema.transactions (user_id, amount, type, reference_id) VALUES ($1, $2, 'REFUND', $3)",
            event.user_id,
            t.amount,
            event.bet_id
        ).execute(&mut *tx).await;
        let _ = tx.commit().await;
        publish_event(
            rmq,
            "betting_topic",
            "bet.cancel.refunded",
            WalletStatus {
                bet_id: event.bet_id,
                status: "SUCCESS".into(),
            },
        )
        .await;
    }
}

async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

async fn get_balance(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    let user_id_str = req.headers().get("X-User-ID").unwrap().to_str().unwrap();
    let user_id = Uuid::parse_str(user_id_str).unwrap();

    if let Ok(Some(wallet)) = sqlx::query!(
        "SELECT balance FROM wallet_schema.wallets WHERE user_id = $1",
        user_id
    )
    .fetch_optional(&data.db)
    .await
    {
        #[derive(Serialize)]
        struct Bal {
            balance: f64,
        }
        let val: f64 = wallet.balance.to_f64().unwrap_or(0.0);
        HttpResponse::Ok().json(Bal { balance: val })
    } else {
        HttpResponse::NotFound().finish()
    }
}

async fn deposit(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<FundReq>,
) -> impl Responder {
    let user_id_str = req.headers().get("X-User-ID").unwrap().to_str().unwrap();
    let user_id = Uuid::parse_str(user_id_str).unwrap();

    let mut tx = data.db.begin().await.unwrap();

    let amount = BigDecimal::try_from(body.amount).unwrap();
    let _ = sqlx::query!(
        "UPDATE wallet_schema.wallets SET balance = balance + $1 WHERE user_id = $2",
        amount,
        user_id
    )
    .execute(&mut *tx)
    .await;
    let _ = sqlx::query!(
        "INSERT INTO wallet_schema.transactions (user_id, amount, type) VALUES ($1, $2, 'DEPOSIT')",
        user_id,
        amount
    )
    .execute(&mut *tx)
    .await;
    if let Err(_) = tx.commit().await {
        return HttpResponse::InternalServerError().finish();
    }

    HttpResponse::Ok().finish()
}

async fn withdraw(
    req: HttpRequest,
    data: web::Data<AppState>,
    body: web::Json<FundReq>,
) -> impl Responder {
    let user_id_str = req.headers().get("X-User-ID").unwrap().to_str().unwrap();
    let user_id = Uuid::parse_str(user_id_str).unwrap();

    let mut tx = data.db.begin().await.unwrap();
    let wallet = sqlx::query!(
        "SELECT balance FROM wallet_schema.wallets WHERE user_id = $1 FOR UPDATE",
        user_id
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();

    let amount = BigDecimal::try_from(body.amount).unwrap();
    if wallet.balance >= amount {
        let _ = sqlx::query!(
            "UPDATE wallet_schema.wallets SET balance = balance - $1 WHERE user_id = $2",
            amount,
            user_id
        )
        .execute(&mut *tx)
        .await;
        let _ = sqlx::query!("INSERT INTO wallet_schema.transactions (user_id, amount, type) VALUES ($1, $2, 'WITHDRAW')", user_id, amount).execute(&mut *tx).await;
        let _ = tx.commit().await;
        HttpResponse::Ok().finish()
    } else {
        HttpResponse::BadRequest().body("Insufficient funds")
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL required");
    let pool = {
        || async {
            PgPoolOptions::new()
                .max_connections(5)
                .connect(&db_url)
                .await
        }
    }
    .retry(ExponentialBuilder::default().with_max_times(4))
    .await
    .unwrap();

    let rmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL required");
    let rmq_conn =
        { || async { Connection::connect(&rmq_url, ConnectionProperties::default()).await } }
            .retry(ExponentialBuilder::default().with_max_times(4))
            .await
            .unwrap();
    let rmq_chan = rmq_conn
        .create_channel()
        .await
        .expect("Failed to create channel");

    rmq_chan
        .exchange_declare(
            "wallet_topic",
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    rmq_chan
        .basic_qos(1, BasicQosOptions::default())
        .await
        .unwrap();

    // Consumers
    let pool_clone = pool.clone();
    let chan_clone = rmq_chan.clone();
    tokio::spawn(async move {
        let q1 = chan_clone
            .queue_declare(
                "wallet_user_creates",
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
                q1.name().as_str(),
                "wallet_topic",
                "user.create_wallet",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let mut consumer = chan_clone
            .basic_consume(
                q1.name().as_str(),
                "wallet_c1",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        while let Some(delivery) = consumer.next().await {
            if let Ok(delivery) = delivery {
                if let Ok(ev) = serde_json::from_slice::<UserEvent>(&delivery.data) {
                    handle_user_create_wallet(&pool_clone, ev).await;
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
                "wallet_bet_requests",
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
                "betting_topic",
                "bet.requested",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone2
            .queue_bind(
                q.name().as_str(),
                "betting_topic",
                "bet.won",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone2
            .queue_bind(
                q.name().as_str(),
                "betting_topic",
                "bet.cancel.request_refund",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let mut consumer = chan_clone2
            .basic_consume(
                q.name().as_str(),
                "wallet_c2",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        while let Some(delivery) = consumer.next().await {
            if let Ok(delivery) = delivery {
                let routing_key = delivery.routing_key.as_str();
                if routing_key == "bet.requested" {
                    if let Ok(ev) = serde_json::from_slice::<BetRequested>(&delivery.data) {
                        handle_bet_requested(&pool_clone2, &chan_clone2, ev, &delivery).await;
                    }
                } else if routing_key == "bet.won" {
                    if let Ok(ev) = serde_json::from_slice::<BetWon>(&delivery.data) {
                        handle_bet_won(&pool_clone2, ev).await;
                    }
                } else if routing_key == "bet.cancel.request_refund" {
                    if let Ok(ev) = serde_json::from_slice::<BetCancelled>(&delivery.data) {
                        handle_bet_cancel_request_refund(&pool_clone2, &chan_clone2, ev).await;
                    }
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
    });
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/api/v1/wallet/health", web::get().to(get_health))
            .route("/api/v1/wallet/{id}", web::get().to(get_balance))
            .route("/api/v1/wallet/{id}/deposit", web::post().to(deposit))
            .route("/api/v1/wallet/{id}/withdraw", web::post().to(withdraw))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
