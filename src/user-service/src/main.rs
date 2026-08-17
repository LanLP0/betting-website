use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use betting_common::{
    Claims, UserCreated, connect_pg, connect_rmq, decode_jwt_rs256, encode_jwt_rs256, exchanges,
};
use chrono::{Duration, Utc};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::env;
use uuid::Uuid;

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

async fn register(
    req_http: HttpRequest,
    data: web::Data<AppState>,
    req: web::Json<RegisterReq>,
) -> impl Responder {
    let username = req.username.trim();
    let email = req.email.trim();
    let password = &req.password;

    if username.len() < 3
        || username.len() > 32
        || !username.chars().all(|c| c.is_alphanumeric() || c == '_')
    {
        return HttpResponse::BadRequest()
            .body("Username must be 3-32 alphanumeric characters or underscores");
    }

    if email.len() < 5 || email.len() > 255 || !email.contains('@') || !email.contains('.') {
        return HttpResponse::BadRequest().body("Invalid email format");
    }

    if password.len() < 8 || password.len() > 128 {
        return HttpResponse::BadRequest().body("Password must be between 8 and 128 characters");
    }

    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = match argon2.hash_password(password.as_bytes(), &salt) {
        Ok(h) => h.to_string(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let id = Uuid::new_v4();
    let query = sqlx::query!(
        "INSERT INTO users_schema.users (id, username, email, password_hash) VALUES ($1, $2, $3, $4)",
        id,
        username,
        email,
        &hash
    )
    .execute(&data.db)
    .await;

    match query {
        Ok(_) => {
            let trace_id = betting_common::req_get_request_id(&req_http);
            let ev = UserCreated {
                id,
                username: username.to_string(),
            };
            let _ = betting_common::publish_event_with_trace(
                &data.rmq,
                exchanges::USER,
                "user.created",
                &ev,
                &trace_id,
            )
            .await;
            let _ = betting_common::publish_event_with_trace(
                &data.rmq,
                exchanges::WALLET,
                "user.create_wallet",
                &ev,
                &trace_id,
            )
            .await;

            HttpResponse::Created().json(UserResponse {
                id,
                username: username.to_string(),
                email: email.to_string(),
                role: "user".to_string(),
            })
        }
        Err(_) => HttpResponse::Conflict().body("Username or email already exists"),
    }
}

async fn login(data: web::Data<AppState>, req: web::Json<LoginReq>) -> impl Responder {
    let user_row = match sqlx::query!(
        "SELECT id, username, password_hash, role FROM users_schema.users WHERE username = $1",
        &req.username
    )
    .fetch_optional(&data.db)
    .await
    {
        Ok(Some(u)) => u,
        _ => return HttpResponse::Unauthorized().finish(),
    };

    let user_id: Uuid = user_row.id;
    let username: String = user_row.username;
    let password_hash: String = user_row.password_hash;
    let role: String = user_row.role;

    let parsed_hash = match PasswordHash::new(&password_hash) {
        Ok(h) => h,
        Err(_) => return HttpResponse::InternalServerError().finish(),
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
        sub: user_id.to_string(),
        username,
        role,
        iat,
        exp,
    };

    let token = match encode_jwt_rs256(&claims, &data.jwt_private_key) {
        Ok(t) => t,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    #[derive(Serialize)]
    struct TokenRes {
        token: String,
    }

    HttpResponse::Ok().json(TokenRes { token })
}

async fn auth_verify(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with("Bearer ") {
                let token = &auth_str[7..];
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

async fn get_user_profile(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_id = path.into_inner();
    let auth_user_id = req.headers().get("X-User-ID").and_then(|h| h.to_str().ok());
    let auth_user_role = req
        .headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");

    if let Some(uid_str) = auth_user_id {
        if let Ok(uid) = Uuid::parse_str(uid_str) {
            if uid != target_id && auth_user_role != "admin" {
                return HttpResponse::Forbidden().finish();
            }
        }
    }

    if let Ok(Some(u)) = sqlx::query!(
        "SELECT id, username, email, role FROM users_schema.users WHERE id = $1",
        target_id
    )
    .fetch_optional(&data.db)
    .await
    {
        HttpResponse::Ok().json(UserResponse {
            id: u.id,
            username: u.username,
            email: u.email,
            role: u.role,
        })
    } else {
        HttpResponse::NotFound().finish()
    }
}

async fn update_user_profile(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
    body: web::Json<UpdateProfileReq>,
) -> impl Responder {
    let target_id = path.into_inner();
    let auth_user_id = req.headers().get("X-User-ID").and_then(|h| h.to_str().ok());
    let auth_user_role = betting_common::req_get_user_role(&req);

    if let Some(uid_str) = auth_user_id {
        if let Ok(uid) = Uuid::parse_str(uid_str) {
            if uid != target_id && auth_user_role != "admin" {
                return HttpResponse::Forbidden().finish();
            }
        }
    } else if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    if let Some(ref new_username) = body.username {
        let trimmed = new_username.trim();
        if trimmed.len() >= 3 && trimmed.len() <= 32 {
            let _ = sqlx::query!(
                "UPDATE users_schema.users SET username = $1 WHERE id = $2",
                trimmed,
                target_id
            )
            .execute(&data.db)
            .await;
        }
    }

    if let Some(ref new_email) = body.email {
        let trimmed = new_email.trim();
        if trimmed.contains('@') && trimmed.contains('.') {
            let _ = sqlx::query!(
                "UPDATE users_schema.users SET email = $1 WHERE id = $2",
                trimmed,
                target_id
            )
            .execute(&data.db)
            .await;
        }
    }

    HttpResponse::Ok().finish()
}

async fn delete_user(
    req: HttpRequest,
    path: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let target_id = path.into_inner();
    let auth_user_id = req.headers().get("X-User-ID").and_then(|h| h.to_str().ok());
    let auth_user_role = req
        .headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");

    if let Some(uid_str) = auth_user_id {
        if let Ok(uid) = Uuid::parse_str(uid_str) {
            if uid != target_id && auth_user_role != "admin" {
                return HttpResponse::Forbidden().finish();
            }
        }
    }

    let _ = sqlx::query!("DELETE FROM users_schema.users WHERE id = $1", target_id)
        .execute(&data.db)
        .await;

    HttpResponse::Ok().finish()
}

async fn get_all_users(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    let auth_user_role = req
        .headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let rows = sqlx::query!("SELECT id, username, email, role FROM users_schema.users LIMIT 100")
        .fetch_all(&data.db)
        .await
        .unwrap_or_default();

    let users: Vec<_> = rows
        .into_iter()
        .map(|r| UserResponse {
            id: r.id,
            username: r.username,
            email: r.email,
            role: r.role,
        })
        .collect();

    HttpResponse::Ok().json(users)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();

    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL required");
    let pool = connect_pg(&db_url, 5).await.expect("Failed DB connection");

    let rmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL required");
    let rmq_chan = connect_rmq(&rmq_url).await.expect("Failed RMQ connection");

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
