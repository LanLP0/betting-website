use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use chrono::{Duration, Utc};
use jsonwebtoken::{EncodingKey, Header, encode};
use lapin::{BasicProperties, Connection, ConnectionProperties, options::*, types::FieldTable};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::env;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    role: String,
    exp: usize,
}

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

#[derive(Serialize, Deserialize)]
struct UserResponse {
    id: Uuid,
    username: String,
    role: String,
}

struct AppState {
    db: PgPool,
    rmq: lapin::Channel,
    jwt_secret: Vec<u8>,
}

async fn publish_event(
    channel: &lapin::Channel,
    exchange: &str,
    routing_key: &str,
    payload: impl Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(&payload)?;
    channel
        .basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default(),
        )
        .await?;
    Ok(())
}

async fn register(data: web::Data<AppState>, req: web::Json<RegisterReq>) -> impl Responder {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = match argon2.hash_password(req.password.as_bytes(), &salt) {
        Ok(h) => h.to_string(),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let id = Uuid::new_v4();
    let query = sqlx::query!(
        "INSERT INTO users_schema.users (id, username, email, password_hash) VALUES ($1, $2, $3, $4)",
        id,
        req.username,
        req.email,
        hash
    )
    .execute(&data.db)
    .await;

    match query {
        Ok(_) => {
            #[derive(Serialize)]
            struct UserEvent {
                id: Uuid,
                username: String,
            }
            let ev = UserEvent {
                id,
                username: req.username.clone(),
            };
            let _ = publish_event(&data.rmq, "user_topic", "user.created", &ev).await;
            let _ = publish_event(&data.rmq, "wallet_topic", "user.create_wallet", &ev).await;

            HttpResponse::Created().json(UserResponse {
                id,
                username: req.username.clone(),
                role: "user".to_string(),
            })
        }
        Err(_) => HttpResponse::Conflict().body("Username or email already exists"),
    }
}

async fn login(data: web::Data<AppState>, req: web::Json<LoginReq>) -> impl Responder {
    let user = match sqlx::query!(
        "SELECT id, username, password_hash, role FROM users_schema.users WHERE username = $1",
        req.username
    )
    .fetch_optional(&data.db)
    .await
    {
        Ok(Some(u)) => u,
        _ => return HttpResponse::Unauthorized().finish(),
    };

    let parsed_hash = match PasswordHash::new(&user.password_hash) {
        Ok(h) => h,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    if Argon2::default()
        .verify_password(req.password.as_bytes(), &parsed_hash)
        .is_err()
    {
        return HttpResponse::Unauthorized().finish();
    }

    let exp = Utc::now()
        .checked_add_signed(Duration::hours(24))
        .expect("valid timestamp")
        .timestamp() as usize;

    let claims = Claims {
        sub: user.id.to_string(),
        role: user.role.clone(),
        exp,
    };

    let token = match encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(&data.jwt_secret),
    ) {
        Ok(t) => t,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    #[derive(Serialize)]
    struct TokenRes {
        token: String,
    }

    HttpResponse::Ok().json(TokenRes { token })
}

async fn auth_verify(req: actix_web::HttpRequest, data: web::Data<AppState>) -> impl Responder {
    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with("Bearer ") {
                let token = &auth_str[7..];
                use jsonwebtoken::{DecodingKey, Validation, decode};
                let mut validation = Validation::default();
                validation.validate_exp = true;

                if let Ok(token_data) = decode::<Claims>(
                    token,
                    &DecodingKey::from_secret(&data.jwt_secret),
                    &validation,
                ) {
                    return HttpResponse::Ok()
                        .insert_header(("X-User-ID", token_data.claims.sub))
                        .insert_header(("X-User-Role", token_data.claims.role))
                        .finish();
                }
            }
        }
    }
    HttpResponse::Unauthorized().finish()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    let rmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL required");
    let rmq_conn = Connection::connect(&rmq_url, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");
    let rmq_chan = rmq_conn
        .create_channel()
        .await
        .expect("Failed to create channel");

    // Declare exchanges
    rmq_chan
        .exchange_declare(
            "user_topic",
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    rmq_chan
        .exchange_declare(
            "wallet_topic",
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let jwt_secret = match env::var("JWT_SECRET_FILE") {
        Ok(path) => std::fs::read(&path).expect("Failed to read JWT secret"),
        Err(_) => panic!("JWT_SECRET_FILE not set"),
    };

    let state = web::Data::new(AppState {
        db: pool,
        rmq: rmq_chan,
        jwt_secret,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/api/v1/auth/register", web::post().to(register))
            .route("/api/v1/auth/login", web::post().to(login))
            .route("/api/v1/auth/verify", web::get().to(auth_verify))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
