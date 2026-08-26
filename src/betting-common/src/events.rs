use backon::{ExponentialBuilder, Retryable};
use bigdecimal::ToPrimitive;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCreated {
    pub id: Uuid,
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BetRequested {
    pub bet_id: Uuid,
    pub user_id: Uuid,
    pub amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletStatus {
    pub bet_id: Uuid,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BetWon {
    pub bet_id: Uuid,
    pub user_id: Uuid,
    pub payout_amount: f64,
    pub payout_amount_full: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BetCancelled {
    pub user_id: Uuid,
    pub bet_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSettled {
    pub event_id: Uuid,
    pub winning_selection: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPush {
    pub user_id: Uuid,
    pub notification_type: String,
    pub title: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OddsRpcRequest {
    pub event_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OddsRpcResponse {
    pub event_id: Uuid,
    pub success: bool,
    pub event_odds: Option<EventOdds>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub status: String,
    pub winning_selection: Option<String>,
    pub teams: Vec<String>,
    pub odds: Vec<f64>,
    pub settled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventOdds {
    pub event_id: Uuid,
    pub status: String,
    pub winning_selection: Option<String>,
    pub teams: Vec<String>,
    pub odds: Vec<f64>,
}

// Checked
/// Get odds for an event_id, and populate redis if needed
pub async fn get_odds_for_event(
    event_id: Uuid,
    pool: &PgPool,
    redis_client: &redis::Client,
) -> Option<EventOdds> {
    let mut event_odds: Option<EventOdds> = None;

    // Check Redis
    let redis_key = format!("odds:{}", event_id);
    if let Ok(mut conn) = redis_client.get_multiplexed_async_connection().await {
        if let Ok(val) = conn.get::<_, String>(&redis_key).await {
            if let Ok(evo) = serde_json::from_str::<EventOdds>(&val) {
                event_odds = Some(evo);
            }
        }
    }

    // Check Database
    if event_odds.is_none() {
        let evo = {|| async {
            sqlx::query!("SELECT status, winning_selection, teams, odds FROM events_schema.events WHERE id = $1", event_id).fetch_optional(pool).await
        }}
        .retry(ExponentialBuilder::default().with_jitter()).when(crate::sqlx_retry_when).await;
        if let Ok(Some(ev)) = evo {
            let odds_f = ev
                .odds
                .iter()
                .map(|o| o.to_f64().unwrap_or_default())
                .collect::<Vec<_>>();
            event_odds = Some(EventOdds {
                event_id: event_id,
                status: ev.status,
                winning_selection: ev.winning_selection,
                teams: ev.teams,
                odds: odds_f,
            });

            // Write back to Redis
            if let Ok(mut conn) = redis_client.get_multiplexed_async_connection().await {
                let payload_str =
                    serde_json::to_string(&event_odds.as_ref().unwrap()).unwrap_or_default();
                let _: () = conn
                    .set_ex(redis_key, payload_str, 3000)
                    .await
                    .unwrap_or(());
            }
        }
    }

    event_odds
}
