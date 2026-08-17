use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCreated {
    pub id: Uuid,
    pub username: String,
}

// UserEvent is an alias commonly used in wallet/user services
pub type UserEvent = UserCreated;

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
    pub message: String,
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
    pub teams: Option<Vec<String>>,
    pub odds: Option<Vec<f64>>,
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
