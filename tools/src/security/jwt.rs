use axum::{
    extract::Request, http::StatusCode, response::IntoResponse, response::Response, Json,
    RequestExt,
};
use chrono::TimeDelta;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt::Display;
use std::task::{Context, Poll};

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

pub fn parse_jwt_token(token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let jwt_issuer = get_jwt_issuer();
    let jwt_audience = get_jwt_audience();
    let jwt_secret = get_jwt_secret();
    let decode_key = DecodingKey::from_secret(jwt_secret.as_bytes());

    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_audience(&[jwt_audience]);
    validation.set_issuer(&[jwt_issuer]);
    let token_data = decode::<Claims>(token, &decode_key, &validation)?;
    Ok(token_data.claims)
}

pub struct CreateJwtResult {
    pub access_token: String,
    pub expires_in: u64,
    pub scopes: Vec<String>,
}

pub fn create_jwt_token(subject: impl Into<String>, scopes: Vec<String>) -> CreateJwtResult {
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
        scopes: scopes.clone(),
    };

    let jwt_secret = get_jwt_secret();
    let encode_key = EncodingKey::from_secret(jwt_secret.as_bytes());
    let access_token = encode(&Header::default(), &claims, &encode_key).unwrap();
    CreateJwtResult {
        access_token,
        expires_in: exp,
        scopes,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub aud: String,
    pub iss: String,
    pub issued_at: u64,
    pub exp: u64,
    pub scopes: Vec<String>,
}

impl Claims {
    pub fn has_scopes(&self, expected_scopes: &Vec<String>) -> bool {
        expected_scopes
            .iter()
            .all(|scope| self.scopes.contains(&scope.to_string()))
    }
}

impl Display for Claims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Email: {}", self.sub)
    }
}

#[derive(Clone)]
pub struct RequireScopeLayer {
    required_scopes: Vec<String>,
}

impl RequireScopeLayer {
    pub fn new() -> Self {
        Self {
            required_scopes: Vec::new(),
        }
    }

    pub fn with(mut self, require_scope: Vec<&str>) -> Self {
        self.required_scopes = require_scope.iter().map(|s| s.to_string()).collect();
        self
    }
}

impl<S> Layer<S> for RequireScopeLayer {
    type Service = RequireScopeMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequireScopeMiddleware {
            inner,
            required_scopes: self.required_scopes.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RequireScopeMiddleware<S> {
    inner: S,
    required_scopes: Vec<String>,
}

impl<S> Service<Request> for RequireScopeMiddleware<S>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let required_scopes = self.required_scopes.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            match request.extract_parts::<Claims>().await {
                Ok(claims) => {
                    if claims.has_scopes(&required_scopes) {
                        return inner.call(request).await;
                    }
                    let response = AuthError::NotSufficientScopes.into_response();
                    Ok(response)
                }
                Err(_) => {
                    let response = AuthError::InvalidToken.into_response();
                    Ok(response)
                }
            }
        })
    }
}

#[cfg(not(any(test, feature = "mock_jwt")))]
use axum::RequestPartsExt;
#[cfg(not(any(test, feature = "mock_jwt")))]
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use derive_more::Display;
use thiserror::Error;
use tower::{Layer, Service};

#[cfg(not(any(test, feature = "mock_jwt")))]
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

#[cfg(any(test, feature = "mock_jwt"))]
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
        let scopes = parts
            .headers
            .get("X-Claims-Scopes")
            .unwrap()
            .to_str()
            .unwrap()
            .split(',')
            .map(|s| s.to_string())
            .collect();

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
            scopes,
        })
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            AuthError::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid token"),
            AuthError::NotSufficientScopes => (StatusCode::FORBIDDEN, "Not sufficient scopes"),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}

#[derive(Debug, Error, Display)]
pub enum AuthError {
    InvalidToken,
    NotSufficientScopes,
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
        let jwt_token = create_jwt_token(email.clone(), vec!["customers:read".to_string()]);
        assert_eq!(jwt_token.scopes, vec!["customers:read"]);
        let now_plus_5_days =
            (chrono::Utc::now() + chrono::Duration::days(7)) - chrono::Duration::seconds(30);
        assert!(jwt_token.expires_in > now_plus_5_days.timestamp() as u64);

        let claims = parse_jwt_token(&jwt_token.access_token).unwrap();
        assert_eq!(vec!["customers:read".to_string()], claims.scopes);
        assert_eq!(email, claims.sub);
    }

    #[test]
    fn test_invalid_token() {
        setup();
        let email: String = FreeEmail().fake();
        let mut token = create_jwt_token(email, vec![]);
        token.access_token.push_str("a");
        let claims = parse_jwt_token(&token.access_token);
        assert!(claims.is_err());
    }
}
