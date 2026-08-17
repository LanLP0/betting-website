use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use betting_common::{
    BetCancelled, BetRequested, BetWon, DepositRequest, DepositResponse, NotificationPush,
    PaymentGateway, RegisterRequest, RegisterResponse, UserEvent, WalletStatus, WithdrawRequest,
    WithdrawResponse, connect_pg, connect_rmq, exchanges, publish_event, publish_event_props,
    publish_event_with_trace, req_get_request_id, req_user_match_id, verify_hmac_signature,
};
use bigdecimal::ToPrimitive;
use futures_util::stream::StreamExt;
use lapin::{BasicProperties, options::*, types::FieldTable};
use serde::Deserialize;
use sqlx::{PgPool, types::BigDecimal};
use std::env;
use std::sync::Arc;
use uuid::Uuid;

pub struct HttpPaymentGateway {
    client: reqwest::Client,
    mock_service_url: String,
}

impl HttpPaymentGateway {
    pub fn new(mock_service_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            mock_service_url,
        }
    }
}

impl PaymentGateway for HttpPaymentGateway {
    fn request_deposit<'a>(
        &'a self,
        req: DepositRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<DepositResponse, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let url = format!("{}/mock/api/v1/deposit/request", self.mock_service_url);
            let res = self
                .client
                .post(&url)
                .json(&req)
                .send()
                .await
                .map_err(|e| e.to_string())?;

            if res.status().is_success() {
                res.json::<DepositResponse>()
                    .await
                    .map_err(|e| e.to_string())
            } else {
                Err("Failed to request deposit from payment gateway".into())
            }
        })
    }

    fn request_registration<'a>(
        &'a self,
        req: RegisterRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<RegisterResponse, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let url = format!("{}/mock/api/v1/register/request", self.mock_service_url);
            let res = self
                .client
                .post(&url)
                .json(&req)
                .send()
                .await
                .map_err(|e| e.to_string())?;

            if res.status().is_success() {
                res.json::<RegisterResponse>()
                    .await
                    .map_err(|e| e.to_string())
            } else {
                Err("Failed to request registration from payment gateway".into())
            }
        })
    }

    fn withdraw<'a>(
        &'a self,
        req: WithdrawRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<WithdrawResponse, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let url = format!("{}/mock/api/v1/withdraw", self.mock_service_url);
            let res = self
                .client
                .post(&url)
                .json(&req)
                .send()
                .await
                .map_err(|e| e.to_string())?;

            if res.status().is_success() {
                res.json::<WithdrawResponse>()
                    .await
                    .map_err(|e| e.to_string())
            } else {
                Err("Withdrawal rejected by payment gateway".into())
            }
        })
    }
}

struct AppState {
    db: PgPool,
    rmq: lapin::Channel,
    webhook_secret: String,
    gateway: Arc<dyn PaymentGateway>,
}

#[derive(Deserialize)]
struct FundReq {
    amount: f64,
}

#[derive(Deserialize)]
struct PaymentCallbackReq {
    transaction_id: Uuid,
    user_id: Uuid,
    amount: f64,
    status: String,
}

#[derive(Deserialize)]
struct RegisterCallbackReq {
    user_id: Uuid,
    payment_token: String,
    status: String,
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
        Err(e) => {
            eprintln!("Failed to begin transaction for bet_requested: {:?}", e);
            return;
        }
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
    if let Some(corr_id) = delivery.properties.correlation_id() {
        props = props.with_correlation_id(corr_id.to_owned());
    }

    if let Some(w) = wallet {
        let balance: BigDecimal = w.balance;
        if let Ok(amount) = BigDecimal::try_from(event.amount) {
            if balance >= amount {
                let deduct = sqlx::query!(
                    "UPDATE wallet_schema.wallets SET balance = balance - $1 WHERE user_id = $2",
                    amount,
                    event.user_id
                )
                .execute(&mut *tx)
                .await;

                let record = sqlx::query!(
                    "INSERT INTO wallet_schema.transactions (user_id, amount, type, reference_id) VALUES ($1, $2, 'BET_PLACED', $3)",
                    event.user_id,
                    &amount,
                    event.bet_id
                )
                .execute(&mut *tx)
                .await;

                if deduct.is_ok() && record.is_ok() {
                    if tx.commit().await.is_ok() {
                        let _ = publish_event_props(
                            rmq,
                            exchanges::WALLET,
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
                } else {
                    let _ = tx.rollback().await;
                }
            } else {
                let _ = tx.rollback().await;
            }
        } else {
            let _ = tx.rollback().await;
        }
    } else {
        let _ = tx.rollback().await;
    }

    let _ = publish_event_props(
        rmq,
        exchanges::WALLET,
        "wallet.funds_insufficient",
        WalletStatus {
            bet_id: event.bet_id,
            status: "FAILED".into(),
        },
        props,
    )
    .await;
}

async fn handle_bet_won(pool: &PgPool, rmq: &lapin::Channel, event: BetWon) {
    let mut tx = match pool.begin().await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Failed to begin tx for bet_won: {:?}", e);
            return;
        }
    };

    let amount = match BigDecimal::try_from(event.payout_amount) {
        Ok(a) => a,
        Err(_) => return,
    };

    let update = sqlx::query!(
        "UPDATE wallet_schema.wallets SET balance = balance + $1 WHERE user_id = $2",
        amount,
        event.user_id
    )
    .execute(&mut *tx)
    .await;

    let record = sqlx::query!(
        "INSERT INTO wallet_schema.transactions (user_id, amount, type, reference_id) VALUES ($1, $2, 'BET_WON', $3)",
        event.user_id,
        &amount,
        event.bet_id
    )
    .execute(&mut *tx)
    .await;

    if update.is_ok() && record.is_ok() && tx.commit().await.is_ok() {
        let notif = NotificationPush {
            user_id: event.user_id,
            notification_type: "payout_credited".into(),
            title: "Winnings Credited!".into(),
            message: format!(
                "Congratulations! You won ${:.2} on your bet.",
                event.payout_amount
            ),
            payload: serde_json::json!({ "bet_id": event.bet_id, "amount": event.payout_amount }),
        };
        let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
    }
}

async fn handle_bet_cancel_request_refund(
    pool: &PgPool,
    rmq: &lapin::Channel,
    event: BetCancelled,
) {
    let trans = sqlx::query!(
        "SELECT amount FROM wallet_schema.transactions WHERE user_id = $1 AND type = 'BET_PLACED' AND reference_id = $2",
        event.user_id,
        event.bet_id
    )
    .fetch_optional(pool)
    .await;

    if let Ok(Some(t)) = trans {
        let amount: BigDecimal = t.amount;
        if let Ok(mut tx) = pool.begin().await {
            let update = sqlx::query!(
                "UPDATE wallet_schema.wallets SET balance = balance + $1 WHERE user_id = $2",
                amount,
                event.user_id
            )
            .execute(&mut *tx)
            .await;

            let record = sqlx::query!(
                "INSERT INTO wallet_schema.transactions (user_id, amount, type, reference_id) VALUES ($1, $2, 'REFUND', $3)",
                event.user_id,
                &amount,
                event.bet_id
            )
            .execute(&mut *tx)
            .await;

            if update.is_ok() && record.is_ok() && tx.commit().await.is_ok() {
                let _ = publish_event(
                    rmq,
                    exchanges::BETTING,
                    "bet.cancel.refunded",
                    WalletStatus {
                        bet_id: event.bet_id,
                        status: "SUCCESS".into(),
                    },
                )
                .await;
            }
        }
    }
}

async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

async fn get_balance(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_user_id = path.into_inner();
    if !req_user_match_id(&req, &target_user_id) {
        return HttpResponse::Forbidden().finish();
    }

    if let Ok(Some(wallet)) = sqlx::query!(
        "SELECT balance FROM wallet_schema.wallets WHERE user_id = $1",
        target_user_id
    )
    .fetch_optional(&data.db)
    .await
    {
        let balance: BigDecimal = wallet.balance;
        let val: f64 = balance.to_f64().unwrap_or(0.0);
        HttpResponse::Ok().json(serde_json::json!({ "balance": val }))
    } else {
        HttpResponse::NotFound().finish()
    }
}

async fn deposit(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    body: web::Json<FundReq>,
) -> impl Responder {
    let target_user_id = path.into_inner();
    if !req_user_match_id(&req, &target_user_id) {
        return HttpResponse::Forbidden().finish();
    }

    if body.amount <= 0.0 || body.amount.is_nan() || body.amount.is_infinite() {
        return HttpResponse::BadRequest().body("Invalid amount");
    }

    let callback_url = format!(
        "http://wallet-service:8080/api/v1/wallet/{}/callback/payment",
        target_user_id
    );

    let dep_req = DepositRequest {
        user_id: Some(target_user_id),
        amount: body.amount,
        response_webhook: callback_url,
    };

    match data.gateway.request_deposit(dep_req).await {
        Ok(res_body) => HttpResponse::Ok().json(res_body),
        Err(err) => HttpResponse::InternalServerError().body(err),
    }
}

async fn withdraw(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    body: web::Json<FundReq>,
) -> impl Responder {
    let target_user_id = path.into_inner();
    if !req_user_match_id(&req, &target_user_id) {
        return HttpResponse::Forbidden().finish();
    }

    if body.amount <= 0.0 || body.amount.is_nan() || body.amount.is_infinite() {
        return HttpResponse::BadRequest().body("Invalid amount");
    }

    let mut tx = match data.db.begin().await {
        Ok(t) => t,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let wallet = match sqlx::query!(
        "SELECT balance, payment_gateway_token FROM wallet_schema.wallets WHERE user_id = $1 FOR UPDATE",
        target_user_id
    )
    .fetch_one(&mut *tx)
    .await
    {
        Ok(w) => w,
        Err(_) => return HttpResponse::NotFound().finish(),
    };

    let balance: BigDecimal = wallet.balance;
    let amount = match BigDecimal::try_from(body.amount) {
        Ok(a) => a,
        Err(_) => return HttpResponse::BadRequest().body("Invalid decimal amount"),
    };

    if balance < amount {
        return HttpResponse::BadRequest().body("Insufficient funds");
    }

    let gateway_token = match wallet.payment_gateway_token {
        Some(token) => token,
        None => return HttpResponse::BadRequest().body("No payment method registered"),
    };

    let deduct = sqlx::query!(
        "UPDATE wallet_schema.wallets SET balance = balance - $1 WHERE user_id = $2",
        amount,
        target_user_id
    )
    .execute(&mut *tx)
    .await;

    if deduct.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    let idempotency_key = Uuid::new_v4().to_string();
    let record = sqlx::query!(
        "INSERT INTO wallet_schema.transactions (user_id, amount, type, reference_id) VALUES ($1, $2, 'WITHDRAW', $3)",
        target_user_id,
        amount,
        Uuid::parse_str(&idempotency_key).unwrap_or_else(|_| Uuid::new_v4())
    )
    .execute(&mut *tx)
    .await;

    if record.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    let withdraw_req = WithdrawRequest {
        user_id: Some(target_user_id),
        amount: body.amount,
        gateway_token,
        idempotency_key,
    };

    match data.gateway.withdraw(withdraw_req).await {
        Ok(_) => {
            if tx.commit().await.is_ok() {
                HttpResponse::Ok().finish()
            } else {
                HttpResponse::InternalServerError().finish()
            }
        }
        Err(err) => {
            let _ = tx.rollback().await;
            HttpResponse::InternalServerError().body(err)
        }
    }
}

async fn payment_callback(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    bytes: web::Bytes,
) -> impl Responder {
    let path_user_id = path.into_inner();

    if !verify_hmac_signature(req.headers(), &bytes, &data.webhook_secret) {
        return HttpResponse::Unauthorized().body("Invalid signature");
    }

    let payload: PaymentCallbackReq = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(_) => return HttpResponse::BadRequest().body("Invalid JSON payload"),
    };

    if payload.user_id != path_user_id {
        return HttpResponse::Forbidden().body("User ID mismatch with URL path");
    }

    if payload.status == "SUCCESS" {
        // SEC-3 Idempotency check: skip duplicate processing
        let existing = sqlx::query!(
            "SELECT id FROM wallet_schema.transactions WHERE user_id = $1 AND type = 'DEPOSIT' AND reference_id = $2",
            payload.user_id,
            payload.transaction_id
        )
        .fetch_optional(&data.db)
        .await;

        if let Ok(Some(_)) = existing {
            return HttpResponse::Ok().body("Already processed");
        }

        let mut tx = match data.db.begin().await {
            Ok(t) => t,
            Err(_) => return HttpResponse::InternalServerError().finish(),
        };

        let amount = match BigDecimal::try_from(payload.amount) {
            Ok(a) => a,
            Err(_) => return HttpResponse::BadRequest().body("Invalid decimal amount"),
        };

        let update = sqlx::query!(
            "UPDATE wallet_schema.wallets SET balance = balance + $1 WHERE user_id = $2",
            amount,
            payload.user_id
        )
        .execute(&mut *tx)
        .await;

        let record = sqlx::query!(
            "INSERT INTO wallet_schema.transactions (user_id, amount, type, reference_id) VALUES ($1, $2, 'DEPOSIT', $3)",
            payload.user_id,
            &amount,
            payload.transaction_id
        )
        .execute(&mut *tx)
        .await;

        if update.is_err() || record.is_err() || tx.commit().await.is_err() {
            return HttpResponse::InternalServerError().finish();
        }

        let trace_id = req_get_request_id(&req);
        let notif = NotificationPush {
            user_id: payload.user_id,
            notification_type: "deposit_complete".into(),
            title: "Deposit Successful".into(),
            message: format!("${:.2} has been added to your wallet.", payload.amount),
            payload: serde_json::json!({ "transaction_id": payload.transaction_id, "amount": payload.amount }),
        };
        let _ = publish_event_with_trace(
            &data.rmq,
            exchanges::NOTIFICATION,
            "notification.push",
            &notif,
            &trace_id,
        )
        .await;
    }

    HttpResponse::Ok().finish()
}

async fn register_callback(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    bytes: web::Bytes,
) -> impl Responder {
    let path_user_id = path.into_inner();

    if !verify_hmac_signature(req.headers(), &bytes, &data.webhook_secret) {
        return HttpResponse::Unauthorized().body("Invalid signature");
    }

    let payload: RegisterCallbackReq = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(_) => return HttpResponse::BadRequest().body("Invalid JSON payload"),
    };

    if payload.user_id != path_user_id {
        return HttpResponse::Forbidden().body("User ID mismatch with URL path");
    }

    if payload.status == "SUCCESS" {
        let _ = sqlx::query!(
            "UPDATE wallet_schema.wallets SET payment_gateway_token = $1 WHERE user_id = $2",
            &payload.payment_token,
            payload.user_id
        )
        .execute(&data.db)
        .await;
    }

    HttpResponse::Ok().finish()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL required");
    let pool = connect_pg(&db_url, 5).await.expect("Failed DB connection");

    let rmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL required");
    let rmq_chan = connect_rmq(&rmq_url).await.expect("Failed RMQ connection");

    let webhook_secret = env::var("WEBHOOK_SECRET").expect("WEBHOOK_SECRET env var required");
    let mock_service_url =
        env::var("MOCK_SERVICE_URL").unwrap_or_else(|_| "http://mock-service:8080".into());

    let gateway: Arc<dyn PaymentGateway> = Arc::new(HttpPaymentGateway::new(mock_service_url));

    rmq_chan
        .exchange_declare(
            exchanges::WALLET.into(),
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

    let pool_clone = pool.clone();
    let chan_clone = rmq_chan.clone();
    tokio::spawn(async move {
        let q1 = chan_clone
            .queue_declare(
                "wallet_user_creates".into(),
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
                q1.name().to_owned(),
                exchanges::WALLET.into(),
                "user.create_wallet".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let mut consumer = chan_clone
            .basic_consume(
                q1.name().to_owned(),
                "wallet_c1".into(),
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
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
        }
    });

    let pool_clone2 = pool.clone();
    let chan_clone2 = rmq_chan.clone();
    tokio::spawn(async move {
        let q = chan_clone2
            .queue_declare(
                "wallet_bet_requests".into(),
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
                exchanges::BETTING.into(),
                "bet.requested".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone2
            .queue_bind(
                q.name().to_owned(),
                exchanges::BETTING.into(),
                "bet.won".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        chan_clone2
            .queue_bind(
                q.name().to_owned(),
                exchanges::BETTING.into(),
                "bet.cancel.request_refund".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let mut consumer = chan_clone2
            .basic_consume(
                q.name().to_owned(),
                "wallet_c2".into(),
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
                        handle_bet_won(&pool_clone2, &chan_clone2, ev).await;
                    }
                } else if routing_key == "bet.cancel.request_refund" {
                    if let Ok(ev) = serde_json::from_slice::<BetCancelled>(&delivery.data) {
                        handle_bet_cancel_request_refund(&pool_clone2, &chan_clone2, ev).await;
                    }
                }

                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
        }
    });

    let state = web::Data::new(AppState {
        db: pool,
        rmq: rmq_chan,
        webhook_secret,
        gateway,
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .route("/api/v1/wallet/health", web::get().to(get_health))
            .route("/api/v1/wallet/{id}", web::get().to(get_balance))
            .route("/api/v1/wallet/{id}/deposit", web::post().to(deposit))
            .route("/api/v1/wallet/{id}/withdraw", web::post().to(withdraw))
            .route(
                "/api/v1/wallet/{id}/callback/payment",
                web::post().to(payment_callback),
            )
            .route(
                "/api/v1/wallet/{id}/callback/register",
                web::post().to(register_callback),
            )
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
