use std::str::FromStr;
use actix_web::HttpRequest;
use uuid::Uuid;

/// Check if a request's X-User-ID header matches the given ID
pub fn req_user_match_id(req: &HttpRequest, id: &Uuid) -> bool {
    if let Some(header_val) = req.headers().get("X-User-ID") {
        if let Ok(header_str) = header_val.to_str() {
            if let Ok(header_uuid) = Uuid::from_str(header_str) {
                return header_uuid == *id;
            }
        }
    }
    false
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

