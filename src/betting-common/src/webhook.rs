use actix_web::http::header::HeaderMap;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

pub fn verify_hmac_signature(headers: &HeaderMap, body: &[u8], secret: &str) -> bool {
    if let Some(sig_header) = headers
        .get("X-Webhook-Signature")
        .and_then(|h| h.to_str().ok())
    {
        let parts: Vec<&str> = sig_header.split(',').collect();
        if parts.len() == 2 && parts[0].starts_with("t=") && parts[1].starts_with("v1=") {
            let timestamp_str = &parts[0][2..];
            let signature_hex = &parts[1][3..];

            // 1. Replay protection check (reject if older than 300s or in future > 60s)
            if let Ok(timestamp) = timestamp_str.parse::<u64>() {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                if now > timestamp + 300 || timestamp > now + 60 {
                    return false;
                }
            } else {
                return false;
            }

            // 2. Decode hex signature
            let signature_bytes = match hex::decode(signature_hex) {
                Ok(bytes) => bytes,
                Err(_) => return false,
            };

            // 3. Recompute HMAC over `timestamp.body`
            let signed_content = format!("{}.{}", timestamp_str, String::from_utf8_lossy(body));
            if let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) {
                mac.update(signed_content.as_bytes());
                // 4. Constant-time comparison
                return mac.verify_slice(&signature_bytes).is_ok();
            }
        }
    }
    false
}
