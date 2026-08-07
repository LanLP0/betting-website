use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
use actix_ws::Message;
use futures_util::StreamExt;
use rand::RngExt;
use redis::AsyncCommands;
use serde::Serialize;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{env, time::Duration};
use uuid::Uuid;

struct AppState {
    db: PgPool,
    redis: redis::Client,
}

#[derive(Serialize)]
struct Event {
    id: Uuid,
    name: String,
    status: String,
}

async fn get_events(data: web::Data<AppState>) -> impl Responder {
    let evs = sqlx::query!("SELECT id, name, status FROM events_schema.events")
        .fetch_all(&data.db)
        .await
        .unwrap_or_default();

    let res: Vec<Event> = evs
        .into_iter()
        .map(|e| Event {
            id: e.id,
            name: e.name,
            status: e.status,
        })
        .collect();
    HttpResponse::Ok().json(res)
}

async fn get_event(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    let id = Uuid::parse_str(req.match_info().get("id").unwrap()).unwrap();
    if let Ok(Some(ev)) = sqlx::query!(
        "SELECT id, name, status FROM events_schema.events WHERE id = $1",
        id
    )
    .fetch_optional(&data.db)
    .await
    {
        HttpResponse::Ok().json(Event {
            id: ev.id,
            name: ev.name,
            status: ev.status,
        })
    } else {
        HttpResponse::NotFound().finish()
    }
}

async fn mock_odds_worker(pool: PgPool, redis_client: redis::Client) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        let mut r_conn = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => continue,
        };

        let events = match sqlx::query!("SELECT id FROM events_schema.events WHERE status = 'OPEN'")
            .fetch_all(&pool)
            .await
        {
            Ok(e) => e,
            Err(_) => continue,
        };

        for ev in events {
            let (odds1, odds2) = {
                let mut rng = rand::rng();
                (rng.random_range(1.1..3.5), rng.random_range(1.1..3.5))
            };

            let odds_json = serde_json::json!({
                "team1": odds1,
                "team2": odds2
            })
            .to_string();

            let _: () = r_conn
                .set(format!("odds:{}", ev.id), odds_json)
                .await
                .unwrap_or(());
        }
    }
}

async fn ws_route(
    req: HttpRequest,
    stream: web::Payload,
    data: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;

    let redis_client = data.redis.clone();
    let pool = data.db.clone();

    // Spawn task to send periodic updates
    let mut session_clone = session.clone();
    actix_web::rt::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            if let Ok(mut r_conn) = redis_client.get_multiplexed_async_connection().await {
                // TODO send events user subcribed to, not all events
                if let Ok(events) =
                    sqlx::query!("SELECT id FROM events_schema.events WHERE status = 'OPEN'")
                        .fetch_all(&pool)
                        .await
                {
                    for ev in events {
                        let odds_json: String = r_conn
                            .get(format!("odds:{}", ev.id))
                            .await
                            .unwrap_or_else(|_| "{}".to_string());
                        let msg =
                            format!("{{\"event_id\": \"{}\", \"odds\": {}}}", ev.id, odds_json);
                        if session_clone.text(msg).await.is_err() {
                            return; // Client disconnected
                        }
                    }
                }
            }
        }
    });

    actix_web::rt::spawn(async move {
        while let Some(Ok(msg)) = msg_stream.next().await {
            match msg {
                Message::Ping(bytes) => {
                    if session.pong(&bytes).await.is_err() {
                        return;
                    }
                }
                Message::Close(_) => break,
                _ => (),
            }
        }
    });

    Ok(res)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db_url = env::var("DATABASE_URL").unwrap();
    let pool = PgPoolOptions::new().connect(&db_url).await.unwrap();

    let redis_url = env::var("REDIS_URL").unwrap();
    let redis_client = redis::Client::open(redis_url).unwrap();

    tokio::spawn(mock_odds_worker(pool.clone(), redis_client.clone()));

    let state = web::Data::new(AppState {
        db: pool,
        redis: redis_client,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/api/v1/events", web::get().to(get_events))
            .route("/api/v1/events/{id}", web::get().to(get_event))
            .route("/api/v1/events/ws", web::get().to(ws_route))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
