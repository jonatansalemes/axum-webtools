use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json, RequestPartsExt,
};

use chrono::TimeDelta;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};

use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt::Display;

pub trait JwtToken: Send + Sync {
    fn subject(&self) -> String;
}

fn get_jwt_secret() -> String {
    std::env::var("JWT_SECRET").expect("JWT_SECRET must be set")
}

fn get_jwt_issuer() -> String {
    std::env::var("JWT_ISSUER").expect("JWT_ISSUER must be set")
}

fn get_jwt_audience() -> String {
    std::env::var("JWT_AUDIENCE").expect("JWT_AUDIENCE must be set")
}

pub fn parse_jwt_token(token: impl Into<String>) -> Result<Claims, jsonwebtoken::errors::Error> {
    let token = token.into();
    let jwt_issuer = get_jwt_issuer();
    let jwt_audience = get_jwt_audience();
    let jwt_secret = get_jwt_secret();
    let decode_key = DecodingKey::from_secret(jwt_secret.as_bytes());

    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_audience(&[jwt_audience]);
    validation.set_issuer(&[jwt_issuer]);
    let token_data = decode::<Claims>(token.as_str(), &decode_key, &validation)?;
    Ok(token_data.claims)
}

pub fn create_jwt_token(subject: impl Into<String>) -> String {
    let now = chrono::Utc::now();
    let expires_at = TimeDelta::try_days(7)
        .map(|d| now + d)
        .expect("Failed to calculate expiration date");
    let issued_at = now.timestamp() as u64;
    let exp = expires_at.timestamp() as u64;
    let iss = get_jwt_issuer();
    let aud = get_jwt_audience();
    let sub = subject.into();

    let claims = Claims {
        iss,
        sub,
        issued_at,
        exp,
        aud,
    };

    let jwt_secret = get_jwt_secret();
    let encode_key = EncodingKey::from_secret(jwt_secret.as_bytes());
    encode(&Header::default(), &claims, &encode_key).unwrap()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub aud: String,
    pub iss: String,
    pub issued_at: u64,
    pub exp: u64,
}

impl Display for Claims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Email: {}", self.sub)
    }
}

#[cfg(not(any(test, feature = "jwt")))]
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| AuthError::InvalidToken)?;

        let claims = parse_jwt_token(bearer.token()).map_err(|_| AuthError::InvalidToken)?;

        Ok(claims)
    }
}

#[cfg(any(test, feature = "jwt"))]
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let sub = parts
            .headers
            .get("X-Claims-Subject")
            .unwrap()
            .to_str()
            .unwrap();
        let iss = parts
            .headers
            .get("X-Claims-Issuer")
            .unwrap()
            .to_str()
            .unwrap();
        let aud = parts
            .headers
            .get("X-Claims-Audience")
            .unwrap()
            .to_str()
            .unwrap();
        let issued_at = parts
            .headers
            .get("X-Claims-Issued-At")
            .unwrap()
            .to_str()
            .unwrap();
        let exp = parts
            .headers
            .get("X-Claims-Expiration")
            .unwrap()
            .to_str()
            .unwrap();

        let sub = sub.to_string();
        let iss = iss.to_string();
        let aud = aud.to_string();
        let issued_at = issued_at.parse().unwrap();
        let exp = exp.parse().unwrap();

        Ok(Claims {
            sub,
            aud,
            iss,
            issued_at,
            exp,
        })
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AuthError::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid token"),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}

#[derive(Debug)]
pub enum AuthError {
    InvalidToken,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fake::faker::internet::en::FreeEmail;
    use fake::Fake;

    fn setup() {
        std::env::set_var("JWT_SECRET", "secret");
        std::env::set_var("JWT_ISSUER", "issuer");
        std::env::set_var("JWT_AUDIENCE", "audience");
    }

    #[test]
    fn test_create_token() {
        setup();
        let email: String = FreeEmail().fake();
        let token = create_jwt_token(email.clone());
        let claims = parse_jwt_token(token).unwrap();
        assert_eq!(email, claims.sub);
    }

    #[test]
    fn test_invalid_token() {
        setup();
        let email: String = FreeEmail().fake();
        let mut token = create_jwt_token(email);
        token.push_str("a");
        let claims = parse_jwt_token(token);
        assert!(claims.is_err());
    }
}
