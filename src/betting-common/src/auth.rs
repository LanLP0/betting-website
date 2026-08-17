use jsonwebtoken::{DecodingKey, EncodingKey, Header, Algorithm, Validation, decode, encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub username: String,
    pub role: String,
    pub iat: usize,
    pub exp: usize,
}

pub fn encode_jwt_rs256(claims: &Claims, private_key_pem: &[u8]) -> Result<String, jsonwebtoken::errors::Error> {
    let key = EncodingKey::from_rsa_pem(private_key_pem)?;
    let header = Header::new(Algorithm::RS256);
    encode(&header, claims, &key)
}

pub fn decode_jwt_rs256(token: &str, public_key_pem: &[u8]) -> Result<Claims, jsonwebtoken::errors::Error> {
    let key = DecodingKey::from_rsa_pem(public_key_pem)?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.validate_exp = true;
    let token_data = decode::<Claims>(token, &key, &validation)?;
    Ok(token_data.claims)
}
