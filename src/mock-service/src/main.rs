use actix_web::{App, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use backon::{ExponentialBuilder, Retryable};
use betting_common::{
    DepositRequest, DepositResponse, DepositWebhookResponse, EventOdds, RegisterPaymentInfoRequest,
    RegisterPaymentInfoResponse, RegisterPaymentInfoWebhookResponse, WithdrawRequest,
    WithdrawResponse, http::BadRequestResponse,
};
use bigdecimal::{RoundingMode, ToPrimitive};
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use rand::RngExt;
use serde::Deserialize;
use sha2::Sha256;
use sqlx::{PgPool, types::BigDecimal};
use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

struct AppState {
    db: PgPool,
    http_client: reqwest::Client,
    failure_rate: f64,
    latency_min_ms: u64,
    latency_max_ms: u64,
    webhook_timeout_rate: f64,
    default_webhook_secret: String,
}

impl AppState {
    async fn apply_chaos(&self) -> Option<HttpResponse> {
        let failure_rate = self.failure_rate;
        let latency_min = self.latency_min_ms;
        let latency_max = self.latency_max_ms;

        let delay_ms = if latency_max > 0 && latency_max >= latency_min {
            let mut rng = rand::rng();
            rng.random_range(latency_min..=latency_max)
        } else {
            0
        };

        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        if failure_rate > 0.0 {
            let should_fail = {
                let mut rng = rand::rng();
                rng.random_bool(failure_rate.clamp(0.0, 1.0))
            };
            if should_fail {
                return Some(HttpResponse::InternalServerError().body("Chaos injected failure"));
            }
        }

        None
    }
}

// Request & Response DTOs
#[derive(Deserialize)]
struct ConfirmDepositReq {
    client_secret: String,
}

#[derive(Deserialize)]
struct ConfirmRegisterReq {
    client_secret: String,
}

#[derive(Deserialize)]
struct EventSubscribeReq {
    webhook_url: String,
    service_name: String,
}

// Checked
// HMAC-SHA256 Helper
fn compute_hmac_signature(payload: &[u8], secret: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let signed_content = format!("{}.{}", timestamp, String::from_utf8_lossy(payload));
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signed_content.as_bytes());
    let sig = hex::encode(mac.finalize().into_bytes());
    format!("t={},v1={}", timestamp, sig)
}

// Checked
// Asynchronously dispatch signed webhook with retry capability (1s -> 5s -> 25s exponential backoff)
async fn dispatch_webhook(
    http_client: reqwest::Client,
    pool: PgPool,
    webhook_url: String,
    secret: String,
    event_type: String,
    payload: serde_json::Value,
    webhook_timeout_rate: f64,
) {
    // Chaos simulation: artificial hang / timeout
    let should_hang = if webhook_timeout_rate > 0.0 {
        let mut rng = rand::rng();
        rng.random_bool(webhook_timeout_rate.clamp(0.0, 1.0))
    } else {
        false
    };

    if should_hang {
        tokio::time::sleep(Duration::from_secs(6)).await;
    }

    let body = serde_json::to_vec(&payload).unwrap_or_default();
    let signature = compute_hmac_signature(&body, &secret);

    let result = {
        || async {
            http_client
                .post(&webhook_url)
                .header("Content-Type", "application/json")
                .header("X-Webhook-Signature", &signature)
                .body(body.clone())
                .timeout(Duration::from_secs(5))
                .send()
                .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::reqwest_http_retry_when)
    .await;

    let status = if result.is_ok() {
        "delivered".to_string()
    } else {
        // TODO: send failed webhook to consumer
        "failed".to_string()
    };

    let _ = {
        || async {
            sqlx::query!(
            "INSERT INTO mock_schema.webhook_deliveries (webhook_secret_id, event_type, payload, status, last_attempt_at) VALUES ((SELECT id FROM mock_schema.webhook_secrets WHERE secret = $1 LIMIT 1), $2, $3, $4, NOW())",
            &secret, &event_type, &payload, status
        )
        .execute(&pool).await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;
}

// Checked
// Handlers
async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

// Checked
async fn deposit_request(
    data: web::Data<AppState>,
    body: web::Json<DepositRequest>,
) -> impl Responder {
    if let Some(res) = data.apply_chaos().await {
        return res;
    }

    let client_secret = format!("sec_{}", Uuid::new_v4());
    let transaction_id = Uuid::new_v4();
    let user_id = body.user_id.unwrap_or_else(Uuid::new_v4);
    let amount = match BigDecimal::try_from(body.amount) {
        Ok(a) => a,
        Err(_) => return HttpResponse::BadRequest().body("Invalid amount"),
    };
    let secret = data.default_webhook_secret.clone();

    let res = {|| async{sqlx::query!(
            "INSERT INTO mock_schema.deposit_requests (id, user_id, amount, client_secret, webhook_url, webhook_secret, expire_at, status) VALUES ($1, $2, $3, $4, $5, $6, NOW() + INTERVAL '10 minute', 'pending')",
            transaction_id, user_id, amount, client_secret, body.response_webhook, secret
        )
        .execute(&data.db)
        .await}}
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if res.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    HttpResponse::Ok().json(DepositResponse {
        status: "pending".into(),
        client_secret,
        transaction_id,
    })
}

// Checked
async fn register_payment_info_request(
    data: web::Data<AppState>,
    body: web::Json<RegisterPaymentInfoRequest>,
) -> impl Responder {
    if let Some(res) = data.apply_chaos().await {
        return res;
    }

    let client_secret = format!("reg_sec_{}", Uuid::new_v4());
    let secret = data.default_webhook_secret.clone();

    let res = {
        || async {
            sqlx::query!(
                "INSERT INTO mock_schema.payment_info_requests (client_secret, webhook_url, webhook_secret, expire_at) VALUES ($1, $2, $3, NOW() + INTERVAL '10 minute')",
                &client_secret, &body.response_webhook, &secret
            )
            .execute(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if res.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    HttpResponse::Ok().json(RegisterPaymentInfoResponse {
        status: "pending".into(),
        client_secret,
    })
}

// Checked
async fn confirm_deposit(
    data: web::Data<AppState>,
    body: web::Json<ConfirmDepositReq>,
) -> impl Responder {
    if let Some(res) = data.apply_chaos().await {
        return res;
    }

    let dep_req = {|| async {
        sqlx::query!(
            "SELECT id, user_id, amount, status, webhook_url, webhook_secret, expire_at FROM mock_schema.deposit_requests WHERE client_secret = $1",
            &body.client_secret
        )
        .fetch_optional(&data.db)
        .await
    }}
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when).await;

    let dep = match dep_req {
        Ok(Some(d)) => d,
        _ => {
            return HttpResponse::BadRequest()
                .body("Deposit request not found or already processed");
        }
    };

    if dep.expire_at < Utc::now() {
        let res = {|| async {
            sqlx::query!(
                "UPDATE mock_schema.deposit_requests SET status = 'failed' WHERE client_secret = $1",
                &body.client_secret
            )
            .execute(&data.db)
            .await
        }}
        .retry(ExponentialBuilder::default().with_jitter())
        .when(betting_common::sqlx_retry_when).await;

        if res.is_err() {
            log::info!(
                "Failed to update deposit request status to failed (id: {})",
                &dep.id
            );
        }

        return HttpResponse::BadRequest().body("Reqest expired");
    }

    let res = {
        || async {
            sqlx::query!(
                "UPDATE mock_schema.deposit_requests SET status = 'confirmed' WHERE client_secret = $1",
                &body.client_secret
            )
            .execute(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if res.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    let id: Uuid = dep.id;
    let user_id: Uuid = dep.user_id;
    let amount: BigDecimal = dep.amount.with_scale_round(4, RoundingMode::HalfEven);

    let webhook_url: String = dep.webhook_url;
    let secret: String = dep.webhook_secret;

    let payload = DepositWebhookResponse {
        transaction_id: id,
        user_id,
        amount: amount.to_f64().unwrap_or(0.0),
        amount_full: amount.to_plain_string(),
        status: "SUCCESS".into(),
    };

    tokio::spawn(dispatch_webhook(
        data.http_client.clone(),
        data.db.clone(),
        webhook_url,
        secret,
        "deposit.confirmed".into(),
        serde_json::to_value(payload).unwrap(),
        data.webhook_timeout_rate,
    ));

    HttpResponse::Ok().json(serde_json::json!({
        "status": "SUCCESS",
        "transaction_id": id
    }))
}

async fn confirm_register(
    data: web::Data<AppState>,
    body: web::Json<ConfirmRegisterReq>,
) -> impl Responder {
    if let Some(res) = data.apply_chaos().await {
        return res;
    }

    let token = format!("pm_tok_{}", Uuid::new_v4());
    let res = {|| async {sqlx::query!(
        "INSERT INTO mock_schema.payment_information (token, account_number, account_name, bank_name, bank_code) VALUES ($1, '1234567890', 'Mock Account', 'Test Bank', 'TEST01')",
        &token
    )
    .execute(&data.db)
    .await}}
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if res.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    // Send callback to subscriber if client_secret registered
    let secret_row = {|| async {
        sqlx::query!(
            "SELECT webhook_url, webhook_secret, expire_at FROM mock_schema.payment_info_requests WHERE client_secret = $1",
            &body.client_secret
        )
        .fetch_optional(&data.db)
        .await
    }}
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if let Ok(Some(s)) = secret_row {
        if s.expire_at < Utc::now() {
            return HttpResponse::BadRequest().body("Reqest expired");
        }

        let payload = serde_json::to_value(RegisterPaymentInfoWebhookResponse {
            payment_token: token,
            status: "SUCCESS".into(),
        })
        .unwrap();

        tokio::spawn(dispatch_webhook(
            data.http_client.clone(),
            data.db.clone(),
            s.webhook_url,
            s.webhook_secret,
            "payment.registered".into(),
            payload.clone(),
            data.webhook_timeout_rate,
        ));

        return HttpResponse::Ok().json(payload);
    }

    return HttpResponse::BadRequest().body("Payment information registration not found");
}

// Checked
async fn withdraw(data: web::Data<AppState>, body: web::Json<WithdrawRequest>) -> impl Responder {
    if let Some(res) = data.apply_chaos().await {
        return res;
    }

    if body.amount <= 0.0 || body.gateway_token.is_empty() {
        return HttpResponse::BadRequest().json(BadRequestResponse {
            status: "failed".to_string(),
            err_code: "invalid_params".to_string(),
            should_retry: false,
            msg: Some("Invalid withdrawal parameters".to_string()),
        });
    }

    // Check idempotency store first
    let cached_res = {|| async {sqlx::query!(
        "SELECT response_status_code, response_body FROM mock_schema.idempotency_keys WHERE idempotency_key = $1",
        &body.idempotency_key
    )
    .fetch_optional(&data.db)
    .await}}
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if let Ok(Some(cached)) = cached_res {
        let code: i32 = cached.response_status_code;
        let body_val: serde_json::Value = cached.response_body;
        return HttpResponse::build(actix_web::http::StatusCode::from_u16(code as u16).unwrap())
            .json(body_val);
    }

    let transaction_id = Uuid::new_v4();
    let res_body = serde_json::to_value(&WithdrawResponse {
        transaction_id,
        status: "SUCCESS".into(),
    })
    .unwrap();

    let res = {
        || async {
            sqlx::query!(
                "INSERT INTO mock_schema.idempotency_keys (idempotency_key, response_status_code, response_body) VALUES ($1, 200, $2)",
                &body.idempotency_key,
                &res_body
            )
            .execute(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when).await;

    if res.is_err() {
        HttpResponse::InternalServerError().finish();
    }

    // Real world transaction with Bank API happens here

    HttpResponse::Ok().json(res_body)
}

// Checked
async fn get_events(data: web::Data<AppState>) -> impl Responder {
    if let Some(res) = data.apply_chaos().await {
        return res;
    }

    // TODO pagination
    let rows_req = {
        || async {
            sqlx::query!(
                "SELECT id, name, description, status, teams, odds FROM mock_schema.events"
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
    let list: Vec<_> = rows
        .into_iter()
        .map(|r| {
            let id: Uuid = r.id;
            let name: String = r.name;
            let description: String = r.description;
            let status: String = r.status;
            let teams: Vec<String> = r.teams;
            let odds: Vec<BigDecimal> = r.odds;
            let odds_f: Vec<f64> = odds
                .into_iter()
                .map(|o| o.to_f64().unwrap_or(2.0))
                .collect();

            serde_json::json!({
                "id": id,
                "name": name,
                "description": description,
                "status": status,
                "teams": teams,
                "odds": odds_f
            })
        })
        .collect();

    HttpResponse::Ok().json(serde_json::json!({ "events": list }))
}

// Checked
async fn get_event(path: web::Path<Uuid>, data: web::Data<AppState>) -> impl Responder {
    if let Some(res) = data.apply_chaos().await {
        return res;
    }

    let id = path.into_inner();
    let req = {
        || async {
            sqlx::query!(
                "SELECT id, name, description, status, teams, odds FROM mock_schema.events WHERE id = $1",
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

    let r = req.unwrap();
    match r {
        None => HttpResponse::NotFound().finish(),
        Some(r) => {
            let ev_id: Uuid = r.id;
            let name: String = r.name;
            let description: String = r.description;
            let status: String = r.status;
            let teams: Vec<String> = r.teams;
            let odds: Vec<BigDecimal> = r.odds;
            let odds_f: Vec<f64> = odds
                .into_iter()
                .map(|o| o.to_f64().unwrap_or(f64::NAN))
                .collect();

            HttpResponse::Ok().json(serde_json::json!({
                "id": ev_id,
                "name": name,
                "description": description,
                "status": status,
                "teams": teams,
                "odds": odds_f
            }))
        }
    }
}

// Checked
async fn get_event_odds(path: web::Path<Uuid>, data: web::Data<AppState>) -> impl Responder {
    if let Some(res) = data.apply_chaos().await {
        return res;
    }

    let id = path.into_inner();
    let req = {
        || async {
            sqlx::query!(
                "SELECT teams, odds FROM mock_schema.events WHERE id = $1",
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

    let r = req.unwrap();
    match r {
        None => HttpResponse::NotFound().finish(),
        Some(r) => {
            let teams: Vec<String> = r.teams;
            let odds: Vec<BigDecimal> = r.odds;
            let mut odds_list = Vec::new();
            for (team, o) in teams.into_iter().zip(odds.into_iter()) {
                odds_list.push(serde_json::json!({
                    "team": team,
                    "value": o.to_f64().unwrap_or(2.0)
                }));
            }
            HttpResponse::Ok().json(serde_json::json!({ "odds": odds_list }))
        }
    }
}

// Checked
async fn subscribe_events(
    data: web::Data<AppState>,
    body: web::Json<EventSubscribeReq>,
) -> impl Responder {
    if let Some(res) = data.apply_chaos().await {
        return res;
    }

    let secret = data.default_webhook_secret.clone();

    let req = {|| async {sqlx::query!(
        "INSERT INTO mock_schema.webhook_secrets (service_name, webhook_url, secret) VALUES ($1, $2, $3) ON CONFLICT (service_name) DO UPDATE SET webhook_url = EXCLUDED.webhook_url, secret = EXCLUDED.secret",
        body.service_name,
        body.webhook_url,
        secret
    )
    .execute(&data.db)
    .await}}.retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    if req.is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    HttpResponse::Ok().json(serde_json::json!({ "status": "subscribed", "secret": secret }))
}

// Checked
// Background Odds Generator / Drifter / Event Creator & Settlement Worker
async fn odds_drift_worker(pool: PgPool, http_client: reqwest::Client, webhook_timeout_rate: f64) {
    let mut interval = tokio::time::interval(Duration::from_secs(3));
    let mut tick_counter: u64 = 0;

    loop {
        interval.tick().await;
        tick_counter += 1;

        // 1. Periodically create new event if open count < 10
        if tick_counter % 10 == 0 {
            let count = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM mock_schema.events WHERE status = 'open'"
            )
            .fetch_one(&pool)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);

            if count < 10 {
                let id = Uuid::new_v4();
                let name = format!("Match {}", id.to_string()[..8].to_uppercase());
                let teams = vec!["Home Team".to_string(), "Away Team".to_string()];
                let default_odds = vec![
                    BigDecimal::try_from(2.0).unwrap(),
                    BigDecimal::try_from(1.9).unwrap(),
                ];

                let _ = sqlx::query!(
                    "INSERT INTO mock_schema.events (id, name, description, status, teams, odds) VALUES ($1, $2, 'Simulated Match', 'open', $3, $4)",
                    id, name, &teams, &default_odds
                )
                .execute(&pool)
                .await;
            }
        }

        // 2. Periodically settle long-running open matches (GAP-7)
        if tick_counter % 25 == 0 {
            let open_to_settle = sqlx::query!(
                "SELECT id, teams FROM mock_schema.events WHERE status = 'open' LIMIT 1"
            )
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();

            if let Some(ev) = open_to_settle {
                if let Some(winning) = ev.teams.first().cloned() {
                    let _ = sqlx::query!(
                    "UPDATE mock_schema.events SET status = 'settled', winning_selection = $1, settled_at = NOW() WHERE id = $2",
                    &winning,
                    ev.id
                )
                .execute(&pool)
                .await;
                }
            }
        }

        // 3. Drift odds for existing open events
        let open_events = match sqlx::query!(
            "SELECT id, teams, odds FROM mock_schema.events WHERE status = 'open' ORDER BY RANDOM() LIMIT 20"
        )
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(_) => continue,
        };

        for ev in open_events {
            let ev_id: Uuid = ev.id;
            let teams: Vec<String> = ev.teams;
            let odds: Vec<BigDecimal> = ev.odds;

            let updated_odds: Vec<BigDecimal> = odds
                .iter()
                .map(|o| {
                    let current = o.to_f64().unwrap_or(2.0);
                    let shift = {
                        let mut rng = rand::rng();
                        rng.random_range(-0.10..0.10)
                    };
                    let new_val = (current + shift).clamp(1.05, 15.00);
                    BigDecimal::try_from(new_val).unwrap()
                })
                .collect();

            // TODO Batching
            let _ = sqlx::query!(
                "UPDATE mock_schema.events SET odds = $1 WHERE id = $2",
                &updated_odds,
                ev_id
            )
            .execute(&pool)
            .await;

            // TODO Batching
            // Notify event subscribers via webhook
            if let Ok(subs) =
                sqlx::query!("SELECT webhook_url, secret FROM mock_schema.webhook_secrets")
                    .fetch_all(&pool)
                    .await
            {
                let odds_f: Vec<f64> = updated_odds
                    .iter()
                    .map(|o| o.to_f64().unwrap_or(2.0))
                    .collect();
                let payload = serde_json::json!(EventOdds {
                    event_id: ev_id,
                    status: "open".into(),
                    winning_selection: None,
                    teams: teams.clone(),
                    odds: odds_f,
                });

                for sub in subs {
                    let webhook_url: String = sub.webhook_url;
                    let secret: String = sub.secret;

                    tokio::spawn(dispatch_webhook(
                        http_client.clone(),
                        pool.clone(),
                        webhook_url,
                        secret,
                        "odds.updated".into(),
                        payload.clone(),
                        webhook_timeout_rate,
                    ));
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

    let failure_rate: f64 = env::var("FAILURE_RATE")
        .unwrap_or_else(|_| "0.0".into())
        .parse()
        .unwrap_or(0.0);
    let latency_min_ms: u64 = env::var("LATENCY_MIN_MS")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap_or(0);
    let latency_max_ms: u64 = env::var("LATENCY_MAX_MS")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap_or(0);
    let webhook_timeout_rate: f64 = env::var("WEBHOOK_TIMEOUT_RATE")
        .unwrap_or_else(|_| "0.0".into())
        .parse()
        .unwrap_or(0.0);

    let default_webhook_secret = env::var("WEBHOOK_SECRET")
        .unwrap_or_else(|_| "shared_production_webhook_secret_key_12345".into());

    let http_client = reqwest::Client::new();
    tokio::spawn(odds_drift_worker(
        pool.clone(),
        http_client.clone(),
        webhook_timeout_rate,
    ));

    let state = web::Data::new(AppState {
        db: pool,
        http_client,
        failure_rate,
        latency_min_ms,
        latency_max_ms,
        webhook_timeout_rate,
        default_webhook_secret,
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .route("/mock/health", web::get().to(health_check))
            .route(
                "/mock/api/v1/deposit/request",
                web::post().to(deposit_request),
            )
            .route(
                "/mock/api/v1/register/request",
                web::post().to(register_payment_info_request),
            )
            .route("/mock/deposit", web::post().to(confirm_deposit))
            .route("/mock/register", web::post().to(confirm_register))
            .route("/mock/api/v1/withdraw", web::post().to(withdraw))
            .route("/mock/api/v1/events", web::get().to(get_events))
            .route("/mock/api/v1/events/{id}", web::get().to(get_event))
            .route(
                "/mock/api/v1/events/{id}/odds",
                web::get().to(get_event_odds),
            )
            .route(
                "/mock/api/v1/events/subscribe",
                web::post().to(subscribe_events),
            )
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
