use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use backon::{ExponentialBuilder, Retryable};
use betting_common::http::BadRequestResponse;
use betting_common::{
    BetCancelled, BetRequested, BetWon, DepositRequest, DepositResponse, DepositWebhookResponse,
    NotificationPush, PaymentGateway, RegisterPaymentInfoRequest, RegisterPaymentInfoResponse,
    RegisterPaymentInfoWebhookResponse, UserCreated, WalletStatus, WithdrawRequest,
    WithdrawResponse, connect_pg, connect_rmq, exchanges, publish_event, publish_event_props,
    publish_event_with_trace, req_get_request_id, req_get_user_id, req_get_user_role,
    verify_hmac_signature,
};
use bigdecimal::ToPrimitive;
use futures_util::stream::StreamExt;
use lapin::{BasicProperties, options::*, types::FieldTable};
use serde::Deserialize;
use sqlx::{PgPool, types::BigDecimal};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

// In real world scenario, this token would be registered with payment gateway,
// and used to look up service informations (display to user), and lookup webhook_secret
const DEFAULT_SERVICE_TOKEN: &str = "WALLETSERVICEXYZ";

pub struct MockServicePaymentGateway {
    client: reqwest::Client,
    mock_service_url: String,
}

impl MockServicePaymentGateway {
    pub fn new(mock_service_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            mock_service_url,
        }
    }
}

impl PaymentGateway for MockServicePaymentGateway {
    fn request_deposit<'a>(
        &'a self,
        req: DepositRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<DepositResponse, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let url = format!("{}/mock/api/v1/deposit/request", self.mock_service_url);
            let res = { || async { self.client.post(&url).json(&req).send().await } }
                .retry(ExponentialBuilder::default().with_jitter())
                .when(betting_common::reqwest_http_retry_when)
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

    fn request_payment_info_registration<'a>(
        &'a self,
        req: RegisterPaymentInfoRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<RegisterPaymentInfoResponse, String>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let url = format!("{}/mock/api/v1/register/request", self.mock_service_url);
            let res = { || async { self.client.post(&url).json(&req).send().await } }
                .retry(ExponentialBuilder::default().with_jitter())
                .when(betting_common::reqwest_http_retry_when)
                .await
                .map_err(|e| e.to_string())?;

            if res.status().is_success() {
                res.json::<RegisterPaymentInfoResponse>()
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
            let res = { || async { self.client.post(&url).json(&req).send().await } }
                .retry(ExponentialBuilder::default().with_jitter())
                .when(betting_common::reqwest_http_retry_when)
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

/// Consumer: Create initial wallet record upon user registration
async fn handle_user_create_wallet(pool: &PgPool, event: UserCreated) {
    let insert_res = {
        || async {
            sqlx::query!(
                "INSERT INTO wallet_schema.wallets (user_id, balance) VALUES ($1, 0.00) ON CONFLICT DO NOTHING",
                event.id
            )
            .execute(pool)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if let Err(e) = insert_res {
        log::error!(
            "Failed to create initial wallet for user {}: {:?}",
            event.id,
            e
        );
    }
}

/*
 * ARCHITECTURAL NOTICE: BET PLACEMENT SAGA & FINANCIAL LOCK IDEMPOTENCY
 *
 * Context & Scenario:
 * Triggered by `bet.requested` event from Betting Service.
 * 1. Pessimistic Row Locking:
 *    - Acquires `SELECT balance FROM wallet_schema.wallets WHERE user_id = $1 FOR UPDATE` to
 *      strictly prevent concurrent balance deduction race conditions.
 * 2. Idempotency & Deduplication Guard:
 *    - In distributed messaging (RabbitMQ), network retries can redeliver `bet.requested`.
 *    - Check if a `BET_PLACED` transaction for `reference_id = bet_id` already exists.
 *      If it exists, re-publish `wallet.funds_locked` (idempotent success) and return immediately
 *      without deducting the user balance twice.
 * 3. Atomic State & Audit Recording:
 *    - Deducts amount from wallet balance and inserts a `BET_PLACED` transaction record.
 * 4. Saga Response:
 *    - Emits `wallet.funds_locked` on success, or `wallet.funds_insufficient` on failure,
 *      preserving correlation / trace IDs.
 */
async fn handle_bet_requested(
    pool: &PgPool,
    rmq: &lapin::Channel,
    event: BetRequested,
    delivery: &lapin::message::Delivery,
) {
    let mut props = BasicProperties::default();
    if let Some(corr_id) = delivery.properties.correlation_id() {
        props = props.with_correlation_id(corr_id.to_owned());
    }

    // 1. Idempotency Check: if this bet has already locked funds, re-publish success and exit
    let existing_tx = sqlx::query!(
        "SELECT id FROM wallet_schema.transactions WHERE user_id = $1 AND type = 'BET_PLACED' AND reference_id = $2",
        event.user_id,
        event.bet_id
    )
    .fetch_optional(pool)
    .await;

    if let Ok(Some(_)) = existing_tx {
        log::warn!(
            "Duplicate bet.requested received for bet_id: {}. Re-publishing funds_locked idempotently.",
            event.bet_id
        );
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

    // 2. Lock wallet row and evaluate balance
    let wallet = sqlx::query!(
        "SELECT balance FROM wallet_schema.wallets WHERE user_id = $1 FOR UPDATE",
        event.user_id
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    if let Some(w) = wallet {
        let balance: BigDecimal = w.balance;
        if let Ok(amount) = BigDecimal::try_from(event.amount) {
            if amount > BigDecimal::from(0) && balance >= amount {
                let mut tx = match pool.begin().await {
                    Ok(t) => t,
                    Err(e) => {
                        log::error!(
                            "Failed to begin transaction for bet_requested (bet_id: {}): {:?}",
                            event.bet_id,
                            e
                        );
                        return;
                    }
                };

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
            }
        }
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

/*
 * ARCHITECTURAL NOTICE: BET SETTLEMENT PAYOUT IDEMPOTENCY
 *
 * Context & Scenario:
 * Triggered when a bet wins via `bet.won` event from Betting Service.
 *
 * Duplicate Protection:
 * In the event of network retries or RMQ re-deliveries, we must ensure winnings are not credited
 * multiple times. A check for existing `BET_WON` transaction with `reference_id = bet_id` ensures
 * absolute idempotency.
 */
async fn handle_bet_won(pool: &PgPool, rmq: &lapin::Channel, event: BetWon) {
    // Idempotency check: prevent duplicate payouts
    let existing_payout = sqlx::query!(
        "SELECT id FROM wallet_schema.transactions WHERE user_id = $1 AND type = 'BET_WON' AND reference_id = $2",
        event.user_id,
        event.bet_id
    )
    .fetch_optional(pool)
    .await;

    if let Ok(Some(_)) = existing_payout {
        log::warn!(
            "Payout already processed for bet_id: {}, skipping duplicate.",
            event.bet_id
        );
        return;
    }

    let amount = match BigDecimal::try_from(event.payout_amount) {
        Ok(a) => a,
        Err(e) => {
            log::error!(
                "Payout amount parse error for user {} (bet_id: {}): {:?}",
                event.user_id,
                event.bet_id,
                e
            );
            return;
        }
    };

    let mut tx = match pool.begin().await {
        Ok(t) => t,
        Err(e) => {
            log::error!(
                "Failed to begin tx for bet_won (bet_id: {}): {:?}",
                event.bet_id,
                e
            );
            return;
        }
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
            payload: serde_json::json!({
                "content": [{
                    "type": "text",
                    "text": format!("Congratulations! You won ${:.2} on your bet.", event.payout_amount)
                }],
                "metadata": {
                    "bet_id": event.bet_id,
                    "amount": event.payout_amount
                }
            }),
        };
        let _ = publish_event(rmq, exchanges::NOTIFICATION, "notification.push", &notif).await;
    }
}

/*
 * ARCHITECTURAL NOTICE: BET CANCELLATION & REFUND SAGA IDEMPOTENCY
 *
 * Context & Scenario:
 * Triggered when a user cancels an eligible bet via `bet.cancel.request_refund`.
 *
 * Duplicate Protection:
 * Checks if a `REFUND` transaction for `reference_id = bet_id` was already applied.
 * If already refunded, immediately re-publishes `bet.cancel.refunded` without double-crediting.
 */
async fn handle_bet_cancel_request_refund(
    pool: &PgPool,
    rmq: &lapin::Channel,
    event: BetCancelled,
) {
    // Idempotency check: prevent duplicate refunds
    let existing_refund = sqlx::query!(
        "SELECT id FROM wallet_schema.transactions WHERE user_id = $1 AND type = 'REFUND' AND reference_id = $2",
        event.user_id,
        event.bet_id
    )
    .fetch_optional(pool)
    .await;

    if let Ok(Some(_)) = existing_refund {
        log::warn!(
            "Refund already processed for bet_id: {}. Re-publishing confirmation.",
            event.bet_id
        );
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
        return;
    }

    let trans = sqlx::query!(
        "SELECT amount FROM wallet_schema.transactions WHERE user_id = $1 AND type = 'BET_PLACED' AND reference_id = $2",
        event.user_id,
        event.bet_id
    )
    .fetch_optional(pool)
    .await;

    if let Ok(Some(t)) = trans {
        let mut tx = match pool.begin().await {
            Ok(t) => t,
            Err(e) => {
                log::error!(
                    "Failed to begin tx for bet_cancel_request_refund (bet_id: {}): {:?}",
                    event.bet_id,
                    e
                );
                return;
            }
        };
        let amount: BigDecimal = t.amount;
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

        if update.is_ok() && record.is_ok() {
            if tx.commit().await.is_ok() {
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
                return;
            }
        } else {
            let _ = tx.rollback().await;
        }
    }
}

async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

/// Get wallet balance for a user with RBAC and retry resilience
async fn get_balance(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_user_id = path.into_inner();
    let auth_user_id = req_get_user_id(&req);
    let auth_user_role = req_get_user_role(&req);

    // RBAC: User can only inspect their own wallet balance unless admin
    match (auth_user_id, auth_user_role) {
        (_, "admin") => {}
        (Some(uid), _) if uid == target_user_id => {}
        (Some(_), _) => return HttpResponse::Forbidden().finish(),
        (None, _) => return HttpResponse::Unauthorized().finish(),
    }

    let query_res = {
        || async {
            sqlx::query!(
                "SELECT balance FROM wallet_schema.wallets WHERE user_id = $1",
                target_user_id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    match query_res {
        Ok(Some(wallet)) => {
            let balance: BigDecimal = wallet.balance;
            let val: f64 = balance.to_f64().unwrap_or(0.0);
            HttpResponse::Ok().json(serde_json::json!({ "balance": val }))
        }
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(e) => {
            log::error!("Database query failed in get_balance: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// Request deposit from payment gateway
async fn deposit(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    body: web::Json<FundReq>,
) -> impl Responder {
    let target_user_id = path.into_inner();
    let auth_user_id = req_get_user_id(&req);
    let auth_user_role = req_get_user_role(&req);

    match (auth_user_id, auth_user_role) {
        (_, "admin") => {}
        (Some(uid), _) if uid == target_user_id => {}
        (Some(_), _) => return HttpResponse::Forbidden().finish(),
        (None, _) => return HttpResponse::Unauthorized().finish(),
    }

    if body.amount <= 0.0 || body.amount.is_nan() || body.amount.is_infinite() {
        return HttpResponse::BadRequest().body("Invalid amount");
    }

    // Verify wallet exists before initiating deposit intent
    let wallet_exists = {
        || async {
            sqlx::query!(
                "SELECT 1 as exists FROM wallet_schema.wallets WHERE user_id = $1",
                target_user_id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    match wallet_exists {
        Ok(Some(_)) => {}
        Ok(None) => return HttpResponse::NotFound().body("Wallet not found"),
        Err(e) => {
            log::error!(
                "Database error checking wallet existence (user_id: {}): {:?}",
                target_user_id,
                e
            );
            return HttpResponse::InternalServerError().finish();
        }
    }

    let callback_url = format!(
        "http://wallet-service:8080/api/v1/wallet/{}/callback/payment",
        target_user_id
    );

    let dep_req = DepositRequest {
        service_token: DEFAULT_SERVICE_TOKEN.into(),
        user_id: Some(target_user_id),
        amount: body.amount,
        response_webhook: callback_url,
    };

    match data.gateway.request_deposit(dep_req).await {
        Ok(res_body) => HttpResponse::Ok().json(res_body),
        Err(err) => HttpResponse::InternalServerError().body(err),
    }
}

/// Request payment method registration from payment gateway to obtain a client secret
async fn register_payment_method(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_user_id = path.into_inner();
    let auth_user_id = req_get_user_id(&req);
    let auth_user_role = req_get_user_role(&req);

    // RBAC: User can only register payment methods for their own wallet unless admin
    match (auth_user_id, auth_user_role) {
        (_, "admin") => {}
        (Some(uid), _) if uid == target_user_id => {}
        (Some(_), _) => return HttpResponse::Forbidden().finish(),
        (None, _) => return HttpResponse::Unauthorized().finish(),
    }

    // Verify wallet exists before requesting registration from payment gateway
    let wallet_exists = {
        || async {
            sqlx::query!(
                "SELECT 1 as exists FROM wallet_schema.wallets WHERE user_id = $1",
                target_user_id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    match wallet_exists {
        Ok(Some(_)) => {}
        Ok(None) => return HttpResponse::NotFound().body("Wallet not found"),
        Err(e) => {
            log::error!(
                "Database error checking wallet existence for registration (user_id: {}): {:?}",
                target_user_id,
                e
            );
            return HttpResponse::InternalServerError().finish();
        }
    }

    let callback_url = format!(
        "http://wallet-service:8080/api/v1/wallet/{}/callback/register",
        target_user_id
    );

    let reg_req = RegisterPaymentInfoRequest {
        service_token: DEFAULT_SERVICE_TOKEN.into(),
        response_webhook: callback_url,
    };

    match data
        .gateway
        .request_payment_info_registration(reg_req)
        .await
    {
        Ok(res_body) => HttpResponse::Ok().json(res_body),
        Err(err) => HttpResponse::InternalServerError().body(err),
    }
}

/*
 * =========================================================================================
 * ARCHITECTURAL NOTICE: WITHDRAWAL SAGA & TRANSACTION HOLD-TIME MITIGATION
 * =========================================================================================
 * Context & Scenario:
 * Withdrawal involves checking local ledger balance, locking funds, and calling an external
 * payment gateway API synchronously.
 *
 * Critical Distributed Anti-Pattern Mitigated:
 * Holding a PostgreSQL transaction/row-lock open across a remote HTTP call (`gateway.withdraw()`)
 * risks thread starvation, database connection exhaustion, and cascading lock contention under
 * latency spikes or network partitions.
 *
 * Recommended 2-Phase Reservation Blueprint:
 * 1. Phase 1 (Local Reservation):
 *    - Begin TX $\rightarrow$ Verify balance $\rightarrow$ Deduct balance $\rightarrow$ Insert `transactions`
 *      record with status `PENDING_WITHDRAWAL` $\rightarrow$ Commit TX immediately (release DB lock).
 * 2. Phase 2 (External Execution):
 *    - Call external Payment Gateway with unique Idempotency Key.
 * 3. Phase 3 (Settlement / Compensation):
 *    - On Success: Update transaction status to `SETTLED` $\rightarrow$ Emit `withdraw_complete` notification.
 *    - On Failure: Execute compensating TX (restore balance $\rightarrow$ Mark transaction `FAILED`).
 * =========================================================================================
 */
async fn withdraw(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    body: web::Json<FundReq>,
) -> impl Responder {
    let target_user_id = path.into_inner();
    let auth_user_id = req_get_user_id(&req);
    let auth_user_role = req_get_user_role(&req);

    match (auth_user_id, auth_user_role) {
        (_, "admin") => {}
        (Some(uid), _) if uid == target_user_id => {}
        (Some(_), _) => return HttpResponse::Forbidden().finish(),
        (None, _) => return HttpResponse::Unauthorized().finish(),
    }

    if body.amount <= 0.0 || body.amount.is_nan() || body.amount.is_infinite() {
        return HttpResponse::BadRequest().body("Invalid amount");
    }

    let wallet = match sqlx::query!(
        "SELECT balance, payment_gateway_token FROM wallet_schema.wallets WHERE user_id = $1 FOR UPDATE",
        target_user_id
    )
    .fetch_one(&data.db)
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

    if amount < 0 {
        return HttpResponse::BadRequest().body("Amount cannot be negative");
    }

    if balance < amount {
        return HttpResponse::BadRequest().body("Insufficient funds");
    }

    let gateway_token = match wallet.payment_gateway_token {
        Some(token) => token,
        None => return HttpResponse::BadRequest().body("No payment method registered"),
    };

    let mut tx = match data.db.begin().await {
        Ok(t) => t,
        Err(e) => {
            log::error!("Failed to begin tx for withdraw: {:?}", e);
            return HttpResponse::InternalServerError().finish();
        }
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

    // Extract client-provided Idempotency-Key or fall back to request trace ID
    let idempotency_key = req
        .headers()
        .get("Idempotency-Key")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| req_get_request_id(&req));

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
        service_token: DEFAULT_SERVICE_TOKEN.into(),
        user_id: Some(target_user_id),
        amount: body.amount,
        gateway_token,
        idempotency_key,
    };

    match data.gateway.withdraw(withdraw_req).await {
        Ok(_) => {
            if tx.commit().await.is_ok() {
                // Emit withdrawal notification
                let trace_id = req_get_request_id(&req);
                let notif = NotificationPush {
                    user_id: target_user_id,
                    notification_type: "withdraw_complete".into(),
                    title: "Withdrawal Processed".into(),
                    payload: serde_json::json!({
                        "content": [{
                            "type": "text",
                            "text": format!("Your withdrawal of ${:.2} has been processed successfully.", body.amount)
                        }],
                        "metadata": {
                            "amount": body.amount
                        }
                    }),
                };
                let _ = publish_event_with_trace(
                    &data.rmq,
                    exchanges::NOTIFICATION,
                    "notification.push",
                    &notif,
                    &trace_id,
                )
                .await;

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

/// Webhook callback from payment gateway to confirm deposit completion
async fn payment_callback(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    bytes: web::Bytes,
) -> impl Responder {
    let path_user_id = path.into_inner();

    // 1. Webhook Signature Verification (HMAC-SHA256)
    if !verify_hmac_signature(req.headers(), &bytes, &data.webhook_secret) {
        return HttpResponse::Unauthorized().json(BadRequestResponse {
            status: "failed".to_string(),
            err_code: "invalid_params".to_string(),
            should_retry: false,
            msg: Some("Invalid signature".to_string()),
        });
    }

    let payload: DepositWebhookResponse = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(_) => {
            return HttpResponse::BadRequest().json(BadRequestResponse {
                status: "failed".to_string(),
                err_code: "invalid_params".to_string(),
                should_retry: false,
                msg: Some("Invalid JSON payload".to_string()),
            });
        }
    };

    if payload.user_id != path_user_id {
        return HttpResponse::Forbidden().json(BadRequestResponse {
            status: "failed".to_string(),
            err_code: "invalid_params".to_string(),
            should_retry: false,
            msg: Some("User ID mismatch with URL path".to_string()),
        });
    }

    if payload.status == "SUCCESS" {
        // 2. Idempotency Check inside TX: prevent double crediting
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

        let amount = match BigDecimal::from_str(&payload.amount_full)
            .or_else(|_| BigDecimal::try_from(payload.amount))
        {
            Ok(a) => a,
            Err(_) => {
                return HttpResponse::BadRequest().json(BadRequestResponse {
                    status: "failed".to_string(),
                    err_code: "invalid_params".to_string(),
                    should_retry: false,
                    msg: Some("Invalid decimal amount".to_string()),
                });
            }
        };

        let mut tx = match data.db.begin().await {
            Ok(t) => t,
            Err(e) => {
                log::error!(
                    "Failed to begin tx for payment_callback (trans_id: {}): {:?}",
                    payload.transaction_id,
                    e
                );
                return HttpResponse::InternalServerError().finish();
            }
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
            payload: serde_json::json!({
                "content": [{
                    "type": "text",
                    "text": format!("${:.2} has been added to your wallet.", payload.amount)
                }],
                "metadata": {
                    "transaction_id": payload.transaction_id,
                    "amount": payload.amount
                }
            }),
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

/// Webhook callback from payment gateway to register a payment method
async fn register_payment_information_callback(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    bytes: web::Bytes,
) -> impl Responder {
    let user_id = path.into_inner();

    if !verify_hmac_signature(req.headers(), &bytes, &data.webhook_secret) {
        return HttpResponse::Unauthorized().json(BadRequestResponse {
            status: "failed".to_string(),
            err_code: "invalid_params".to_string(),
            should_retry: false,
            msg: Some("Invalid signature".to_string()),
        });
    }

    let payload: RegisterPaymentInfoWebhookResponse = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(_) => {
            return HttpResponse::BadRequest().json(BadRequestResponse {
                status: "failed".to_string(),
                err_code: "invalid_params".to_string(),
                should_retry: false,
                msg: Some("Invalid JSON payload".to_string()),
            });
        }
    };

    if payload.status == "SUCCESS" {
        let update_res = {
            || async {
                sqlx::query!(
                    "UPDATE wallet_schema.wallets SET payment_gateway_token = $1 WHERE user_id = $2",
                    &payload.payment_token,
                    &user_id
                )
                .execute(&data.db)
                .await
            }
        }
        .retry(ExponentialBuilder::default().with_jitter())
        .when(betting_common::sqlx_retry_when)
        .await;

        if let Err(e) = update_res {
            log::error!(
                "Failed to update payment_gateway_token for user {}: {:?}",
                user_id,
                e
            );
            return HttpResponse::InternalServerError().finish();
        }
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
    let rmq_chan = connect_rmq(&rmq_url, "wallet-service")
        .await
        .expect("Failed RMQ connection");

    let webhook_secret = env::var("WEBHOOK_SECRET").expect("WEBHOOK_SECRET env var required");
    let mock_service_url =
        env::var("MOCK_SERVICE_URL").unwrap_or_else(|_| "http://mock-service:8080".into());

    let gateway: Arc<dyn PaymentGateway> =
        Arc::new(MockServicePaymentGateway::new(mock_service_url));

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

    // Consumer 1: User creation events
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
                if let Ok(ev) = serde_json::from_slice::<UserCreated>(&delivery.data) {
                    handle_user_create_wallet(&pool_clone, ev).await;
                }
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
        }
    });

    // Consumer 2: Betting events (BetRequested, BetWon, BetCancelled)
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
                "/api/v1/wallet/{id}/register",
                web::post().to(register_payment_method),
            )
            .route(
                "/api/v1/wallet/{id}/callback/payment",
                web::post().to(payment_callback),
            )
            .route(
                "/api/v1/wallet/{id}/callback/register",
                web::post().to(register_payment_information_callback),
            )
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
