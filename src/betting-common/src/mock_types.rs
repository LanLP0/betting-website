use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositRequest {
    pub service_token: String,
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
pub struct DepositWebhookResponse {
    pub transaction_id: Uuid,
    pub user_id: Uuid,
    pub amount: f64,
    pub amount_full: String, // BigDecimal
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterPaymentInfoRequest {
    pub service_token: String,
    pub response_webhook: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterPaymentInfoResponse {
    pub status: String,
    pub client_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterPaymentInfoWebhookResponse {
    pub payment_token: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawRequest {
    pub service_token: String,
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

pub trait PaymentGateway: Send + Sync {
    fn request_deposit<'a>(
        &'a self,
        req: DepositRequest,
    ) -> Pin<Box<dyn Future<Output = Result<DepositResponse, String>> + Send + 'a>>;

    fn request_payment_info_registration<'a>(
        &'a self,
        req: RegisterPaymentInfoRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RegisterPaymentInfoResponse, String>> + Send + 'a>>;

    fn withdraw<'a>(
        &'a self,
        req: WithdrawRequest,
    ) -> Pin<Box<dyn Future<Output = Result<WithdrawResponse, String>> + Send + 'a>>;
}

// Event Service
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EventSubscribeRequest {
    pub webhook_url: String,
    pub service_name: String,
}
