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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PaginationQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

impl PaginationQuery {
    pub fn get_limit(&self, default: i64, max: i64) -> i64 {
        self.limit.unwrap_or(default).clamp(1, max)
    }

    pub fn get_offset(&self) -> i64 {
        self.offset.unwrap_or(0).max(0)
    }
}

// TODO enhance validations

/// Validate username: 3-32 chars, alphanumeric or underscores
pub fn validate_username(username: &str) -> Result<(), &'static str> {
    let s = username.trim();
    if s.len() < 3 || s.len() > 32 {
        return Err("Username must be between 3 and 32 characters");
    }
    if !s.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err("Username may only contain alphanumeric characters and underscores");
    }
    Ok(())
}

/// Validate email: 5-255 chars, contains '@' and '.', valid format
pub fn validate_email(email: &str) -> Result<(), &'static str> {
    let s = email.trim();
    if s.len() < 5 || s.len() > 255 {
        return Err("Email must be between 5 and 255 characters");
    }
    if !s.contains('@') || !s.contains('.') || s.starts_with('@') || s.ends_with('@') {
        return Err("Invalid email address format");
    }
    Ok(())
}

/// Validate password: 8-40 chars
pub fn validate_password(password: &str) -> Result<(), &'static str> {
    if password.len() < 8 || password.len() > 128 {
        return Err("Password must be between 8 and 40 characters");
    }
    Ok(())
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
pub fn req_get_user_role(req: &HttpRequest) -> Option<&str> {
    req.headers()
        .get("X-User-Role")
        .and_then(|h| h.to_str().ok())
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
