use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use backon::{ExponentialBuilder, Retryable};
use betting_common::{
    Claims, PaginationQuery, UserCreated, connect_pg, connect_rmq, decode_jwt_rs256,
    encode_jwt_rs256, exchanges, http::req_get_user_id, publish_event_with_trace,
    req_get_request_id, req_get_user_role, setup_dlq, validate_email, validate_password,
    validate_username,
};
use chrono::{Duration, Utc};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::env;
use uuid::Uuid;

// Static precomputed dummy Argon2 hash for constant-time comparison on non-existent usernames (timing attack mitigation)
const DUMMY_ARGON2_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$ZHVtbXlzYWx0MTIzNDU2Nw$qK0zZ29/X0zJ4Jv0r5g8H4w2E8u1N6y5Q4w3E2r1T0y";

#[derive(Deserialize)]
struct RegisterReq {
    username: String,
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct LoginReq {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct UpdateProfileReq {
    username: Option<String>,
    email: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct UserResponse {
    id: Uuid,
    username: String,
    email: String,
    role: String,
}

struct AppState {
    db: PgPool,
    rmq: lapin::Channel,
    jwt_private_key: Vec<u8>,
    jwt_public_key: Vec<u8>,
}

async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

// TODO create unified functions for name, password, email check with detailed error description

/*
 * =========================================================================================
 * ARCHITECTURAL NOTICE: USER REGISTRATION & TRANSACTIONAL CONSISTENCY
 * =========================================================================================
 * Context & Scenario:
 * During user registration, the system creates a DB record in `users_schema.users` and then
 * publishes two asynchronous domain events:
 * 1. `user.created` on exchange `user_topic` (consumed by Analytics / Management)
 * 2. `user.create_wallet` on exchange `wallet_topic` (consumed by Wallet Service to create 0.00 ledger)
 *
 * Failure Scenario (Dual-Write Anti-Pattern):
 * If the PostgreSQL commit succeeds but RabbitMQ broker connection drops before `user.create_wallet`
 * is published, the user account exists in an orphaned state without a corresponding wallet.
 * Subsequent deposit, withdrawal, or betting operations for this user will fail indefinitely.
 *
 * Target Blueprint (Transactional Outbox Pattern):
 * To guarantee 100% distributed consistency:
 * - Persist the `user.create_wallet` payload into an `outbox_events` table inside the SAME
 *   PostgreSQL transaction as the user insert.
 * - An Outbox Relay (or CDC via Debezium/pg_logical) reads uncommitted outbox rows and publishes
 *   them to RabbitMQ with publisher confirms and at-least-once delivery guarantees.
 * =========================================================================================
 */
async fn register(
    req_http: HttpRequest,
    data: web::Data<AppState>,
    req: web::Json<RegisterReq>,
) -> impl Responder {
    let username = req.username.trim();
    let email = req.email.trim();
    let password = &req.password;

    if let Err(msg) = validate_username(username) {
        return HttpResponse::BadRequest().body(msg);
    }

    if let Err(msg) = validate_email(email) {
        return HttpResponse::BadRequest().body(msg);
    }

    if let Err(msg) = validate_password(password) {
        return HttpResponse::BadRequest().body(msg);
    }

    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = match argon2.hash_password(password.as_bytes(), &salt) {
        Ok(h) => h.to_string(),
        Err(e) => {
            log::error!("Argon2 password hashing failed: {:?}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let id = Uuid::new_v4();
    let query_res = sqlx::query!(
        "INSERT INTO users_schema.users (id, username, email, password_hash) VALUES ($1, $2, $3, $4)",
        id,
        username,
        email,
        &hash
    )
    .execute(&data.db)
    .await;

    match query_res {
        Ok(_) => {
            let trace_id = req_get_request_id(&req_http);
            let ev = UserCreated {
                id,
                username: username.to_string(),
            };

            let pub_user = publish_event_with_trace(
                &data.rmq,
                exchanges::USER,
                "user.created",
                &ev,
                &trace_id,
            )
            .await;
            let pub_wallet = betting_common::publish_event_with_trace(
                &data.rmq,
                exchanges::WALLET,
                "user.create_wallet",
                &ev,
                &trace_id,
            )
            .await;

            if pub_user.is_err() || pub_wallet.is_err() {
                log::error!(
                    "Failed to publish user creation events for user_id {} (trace_id: {})",
                    id,
                    trace_id
                );
            }

            HttpResponse::Created().json(UserResponse {
                id,
                username: username.to_string(),
                email: email.to_string(),
                role: "user".to_string(),
            })
        }
        Err(sqlx::Error::Database(ref dbe)) if dbe.is_unique_violation() => {
            HttpResponse::Conflict().body("Username or email already exists")
        }
        Err(e) => {
            log::error!("Database error during registration: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

async fn login(data: web::Data<AppState>, req: web::Json<LoginReq>) -> impl Responder {
    let username = req.username.trim();
    if username.is_empty() || req.password.is_empty() {
        return HttpResponse::Unauthorized().finish();
    }

    let user_row_res = {
        || async {
            sqlx::query!(
                "SELECT id, username, password_hash, role FROM users_schema.users WHERE username = $1",
                username
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    let user_row = match user_row_res {
        Ok(Some(u)) => Some(u),
        Ok(None) => None,
        Err(e) => {
            log::error!("Database query failed during login: {:?}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    if let Some(user) = user_row {
        let parsed_hash = match PasswordHash::new(&user.password_hash) {
            Ok(h) => h,
            Err(e) => {
                log::error!(
                    "Stored password hash is corrupted: {:?} (user_id: {})",
                    e,
                    user.id
                );
                return HttpResponse::InternalServerError().finish();
            }
        };

        if Argon2::default()
            .verify_password(req.password.as_bytes(), &parsed_hash)
            .is_err()
        {
            return HttpResponse::Unauthorized().finish();
        }

        let now = Utc::now();
        let iat = now.timestamp() as usize;
        let exp = now
            .checked_add_signed(Duration::hours(24))
            .expect("valid timestamp")
            .timestamp() as usize;

        let claims = Claims {
            sub: user.id.to_string(),
            username: user.username,
            role: user.role,
            iat,
            exp,
        };

        let token = match encode_jwt_rs256(&claims, &data.jwt_private_key) {
            Ok(t) => t,
            Err(e) => {
                log::error!("JWT RS256 token generation failed: {:?}", e);
                return HttpResponse::InternalServerError().finish();
            }
        };

        #[derive(Serialize)]
        struct TokenRes {
            token: String,
        }

        HttpResponse::Ok().json(TokenRes { token })
    } else {
        // Run dummy verification to equalize execution time and prevent user enumeration
        if let Ok(dummy_hash) = PasswordHash::new(DUMMY_ARGON2_HASH) {
            let _ = Argon2::default().verify_password(req.password.as_bytes(), &dummy_hash);
        }
        HttpResponse::Unauthorized().finish()
    }
}

/// Nginx auth_request subrequest endpoint: verifies RS256 JWT and injects identity headers
async fn auth_verify(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            let trimmed = auth_str.trim();
            // Handle case-insensitive "Bearer " prefix
            if trimmed.len() > 7 && trimmed[..7].eq_ignore_ascii_case("bearer ") {
                let token = trimmed[7..].trim();
                if let Ok(claims) = decode_jwt_rs256(token, &data.jwt_public_key) {
                    return HttpResponse::Ok()
                        .insert_header(("X-User-ID", claims.sub))
                        .insert_header(("X-User-Role", claims.role))
                        .finish();
                }
            }
        }
    }
    HttpResponse::Unauthorized().finish()
}

/// Get user profile with strict authorization check
async fn get_user_profile(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_id = path.into_inner();
    let auth_user_role = req_get_user_role(&req);
    let auth_user_id = req_get_user_id(&req);

    match (auth_user_id, auth_user_role) {
        (_, Some("admin")) => {}
        (Some(uid), _) if uid == target_id => {}
        (Some(_), _) => return HttpResponse::Forbidden().finish(),
        (None, _) => return HttpResponse::Unauthorized().finish(),
    }

    let user_query = {
        || async {
            sqlx::query!(
                "SELECT id, username, email, role FROM users_schema.users WHERE id = $1",
                target_id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    match user_query {
        Ok(Some(u)) => HttpResponse::Ok().json(UserResponse {
            id: u.id,
            username: u.username,
            email: u.email,
            role: u.role,
        }),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(e) => {
            log::error!("Database query failed in get_user_profile: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// Update user profile with strict validation and conflict handling
async fn update_user_profile(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    body: web::Json<UpdateProfileReq>,
) -> impl Responder {
    let target_id = path.into_inner();
    let auth_user_role = req_get_user_role(&req);
    let auth_user_id = req_get_user_id(&req);

    // Strict Authorization check
    match (auth_user_id, auth_user_role) {
        (_, Some("admin")) => {}
        (Some(uid), _) if uid == target_id => {}
        (Some(_), _) => return HttpResponse::Forbidden().finish(),
        (None, _) => return HttpResponse::Unauthorized().finish(),
    }

    let new_username = body.username.as_deref().map(str::trim);
    let new_email = body.email.as_deref().map(str::trim);

    if new_username.is_none() && new_email.is_none() {
        return HttpResponse::BadRequest().body("No fields provided for update");
    }

    if let Some(uname) = new_username {
        if let Err(msg) = validate_username(uname) {
            return HttpResponse::BadRequest().body(msg);
        }
    }

    if let Some(em) = new_email {
        if let Err(msg) = validate_email(em) {
            return HttpResponse::BadRequest().body(msg);
        }
    }

    let update_res = {
        || async {
            sqlx::query!(
                r#"
                UPDATE users_schema.users 
                SET 
                    username = COALESCE($1, username),
                    email = COALESCE($2, email)
                WHERE id = $3
                RETURNING id, username, email, role
                "#,
                new_username,
                new_email,
                target_id
            )
            .fetch_optional(&data.db)
            .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(betting_common::sqlx_retry_when)
    .await;

    match update_res {
        Ok(Some(updated)) => HttpResponse::Ok().json(UserResponse {
            id: updated.id,
            username: updated.username,
            email: updated.email,
            role: updated.role,
        }),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(sqlx::Error::Database(ref dbe)) if dbe.is_unique_violation() => {
            HttpResponse::Conflict().body("Username or email already taken")
        }
        Err(e) => {
            log::error!("Database error in update_user_profile: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/*
 * =========================================================================================
 * ARCHITECTURAL NOTICE: USER DELETION & SAGA COMPENSATION / CASCADES
 * =========================================================================================
 * Context & Scenario:
 * Deleting a user account involves cross-schema / cross-service dependencies:
 * 1. `wallet_schema.wallets` & `transactions` (Financial ledger)
 * 2. `bets_schema.bets` (Active & historical bets)
 * 3. `notification_schema.user_notifications` (Push history)
 *
 * Target Distributed Architecture:
 * - Soft-delete flag (`deleted_at TIMESTAMP`) should be favored over hard-delete to maintain
 *   immutable audit compliance for financial regulators.
 * - If hard-deleted, emit a `user.deleted` event across RabbitMQ to allow Wallet, Betting,
 *   and Notification services to settle active transactions, cancel open bids, and clean up.
 * =========================================================================================
 */
async fn delete_user(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_id = path.into_inner();
    let auth_user_role = req_get_user_role(&req);
    let auth_user_id = req_get_user_id(&req);

    match (auth_user_id, auth_user_role) {
        (_, Some("admin")) => {}
        (Some(uid), _) if uid == target_id => {}
        (Some(_), _) => return HttpResponse::Forbidden().finish(),
        (None, _) => return HttpResponse::Unauthorized().finish(),
    }

    let delete_res = {
        || async {
            sqlx::query!(
                "DELETE FROM users_schema.users WHERE id = $1 RETURNING id",
                target_id
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
            let ev = serde_json::json!({ "user_id": target_id });
            let _ = publish_event_with_trace(
                &data.rmq,
                exchanges::USER,
                "user.deleted",
                &ev,
                &trace_id,
            )
            .await;
            HttpResponse::Ok().finish()
        }
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(e) => {
            log::error!(
                "Database error in delete_user: {:?} (user_id: {})",
                e,
                target_id
            );
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// Admin-only: list all platform users with pagination support
async fn get_all_users(
    req: HttpRequest,
    query: web::Query<PaginationQuery>,
    data: web::Data<AppState>,
) -> impl Responder {
    let auth_user_role = req_get_user_role(&req);
    if auth_user_role != Some("admin") {
        return HttpResponse::Forbidden().finish();
    }

    let limit = query.get_limit(50, 100);
    let offset = query.get_offset();

    let rows_res = {
        || async {
            sqlx::query!(
                "SELECT id, username, email, role, COUNT(*) OVER() AS total_count FROM users_schema.users ORDER BY created_at DESC LIMIT $1 OFFSET $2",
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

    match rows_res {
        Ok(rows) => {
            let count: i64 = if rows.is_empty() {
                0
            } else {
                rows[0].total_count.unwrap_or(0)
            };
            let users: Vec<_> = rows
                .into_iter()
                .map(|r| UserResponse {
                    id: r.id,
                    username: r.username,
                    email: r.email,
                    role: r.role,
                })
                .collect();
            HttpResponse::Ok().json((count, users))
        }
        Err(e) => {
            log::error!("Database error in get_all_users: {:?}", e);
            HttpResponse::InternalServerError().finish()
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
    let rmq_chan = connect_rmq(&rmq_url, "user-service")
        .await
        .expect("Failed RMQ connection");

    rmq_chan
        .exchange_declare(
            exchanges::USER.into(),
            lapin::ExchangeKind::Topic,
            lapin::options::ExchangeDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();

    rmq_chan
        .exchange_declare(
            exchanges::WALLET.into(),
            lapin::ExchangeKind::Topic,
            lapin::options::ExchangeDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();

    let _ = setup_dlq(&rmq_chan).await;

    let jwt_private_key_path =
        env::var("JWT_PRIVATE_KEY_FILE").expect("JWT_PRIVATE_KEY_FILE env var required");
    let jwt_private_key =
        std::fs::read(&jwt_private_key_path).expect("Failed to read JWT_PRIVATE_KEY_FILE");

    let jwt_public_key_path =
        env::var("JWT_PUBLIC_KEY_FILE").expect("JWT_PUBLIC_KEY_FILE env var required");
    let jwt_public_key =
        std::fs::read(&jwt_public_key_path).expect("Failed to read JWT_PUBLIC_KEY_FILE");

    let state = web::Data::new(AppState {
        db: pool,
        rmq: rmq_chan,
        jwt_private_key,
        jwt_public_key,
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .route("/api/v1/auth/health", web::get().to(get_health))
            .route("/api/v1/auth/register", web::post().to(register))
            .route("/api/v1/auth/login", web::post().to(login))
            .route("/api/v1/auth/verify", web::get().to(auth_verify))
            .route("/api/v1/auth/verify", web::post().to(auth_verify))
            .route("/api/v1/users", web::get().to(get_all_users))
            .route("/api/v1/users/{id}", web::get().to(get_user_profile))
            .route("/api/v1/users/{id}", web::put().to(update_user_profile))
            .route("/api/v1/users/{id}", web::delete().to(delete_user))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
