use actix_web::HttpRequest;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BadRequestResponse {
    pub status: String,
    pub should_retry: bool,
    pub err_code: String,
    pub msg: Option<String>,
}

/// Extract X-Request-ID header from incoming request or generate a fallback
pub fn req_get_request_id(req: &HttpRequest) -> String {
    if let Some(header_val) = req.headers().get("X-Request-ID") {
        if let Ok(header_str) = header_val.to_str() {
            if !header_str.is_empty() {
                return header_str.to_string();
            }
        }
    }
    Uuid::new_v4().to_string()
}

/// Extract X-User-Role header
pub fn req_get_user_role(req: &HttpRequest) -> &str {
    req.headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
}

/// Extract X-User-ID header
pub fn req_get_user_id(req: &HttpRequest) -> Option<Uuid> {
    req.headers()
        .get("X-User-ID")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
}

/// Check if a request's X-User-ID header matches the given ID
pub fn req_user_match_id(req: &HttpRequest, id: &Uuid) -> bool {
    req_get_user_id(req).map_or(false, |uid| uid == *id)
}
