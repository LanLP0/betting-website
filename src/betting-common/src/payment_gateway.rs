use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositRequest {
    pub user_id: Option<Uuid>,
    pub amount: f64,
    pub response_webhook: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositResponse {
    pub status: String,
    pub client_secret: String,
    pub transaction_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub service_name: String,
    pub response_webhook: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub status: String,
    pub client_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawRequest {
    pub user_id: Option<Uuid>,
    pub amount: f64,
    pub gateway_token: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawResponse {
    pub status: String,
    pub transaction_id: Uuid,
}

/// Abstract Payment Gateway Trait for decoupling financial transactions from specific providers/mocks
pub trait PaymentGateway: Send + Sync {
    fn request_deposit<'a>(
        &'a self,
        req: DepositRequest,
    ) -> Pin<Box<dyn Future<Output = Result<DepositResponse, String>> + Send + 'a>>;

    fn request_registration<'a>(
        &'a self,
        req: RegisterRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RegisterResponse, String>> + Send + 'a>>;

    fn withdraw<'a>(
        &'a self,
        req: WithdrawRequest,
    ) -> Pin<Box<dyn Future<Output = Result<WithdrawResponse, String>> + Send + 'a>>;
}
