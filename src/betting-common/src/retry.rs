use reqwest::{Error as ReqwestError, StatusCode};
use sqlx::Error as SqlxError;

pub fn sqlx_retry_when(e: &SqlxError) -> bool {
    match e {
        SqlxError::Io(_) => true,
        SqlxError::Tls(_) => true,
        SqlxError::PoolTimedOut => true,
        SqlxError::PoolClosed => true,
        SqlxError::WorkerCrashed => true,
        SqlxError::BeginFailed => true,
        _ => false,
    }
}

pub fn reqwest_http_retry_when(e: &ReqwestError) -> bool {
    if e.is_timeout() || e.is_connect() {
        return true;
    }

    if let Some(status) = e.status() {
        if status == StatusCode::SERVICE_UNAVAILABLE || status == StatusCode::TOO_MANY_REQUESTS {
            return true;
        }
    }

    return false;
}
