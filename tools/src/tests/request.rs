use crate::security::jwt::Claims;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tower::ServiceExt;

pub struct Response<T: DeserializeOwned> {
    pub body: Option<T>,
    pub status_code: StatusCode,
    pub headers: HeaderMap,
}

pub async fn request<T: Serialize, R: DeserializeOwned>(
    router: Router,
    uri: &str,
    method: &str,
    body: T,
    claims: Option<Claims>,
) -> Response<R> {
    let request_body = Body::from(serde_json::to_vec(&body).unwrap());

    let claims = claims.unwrap_or(Claims {
        sub: "sub".to_string(),
        aud: "aud".to_string(),
        iss: "iss".to_string(),
        issued_at: chrono::Utc::now().timestamp() as u64,
        exp: (chrono::Utc::now() + chrono::Duration::days(7)).timestamp() as u64,
        scopes: vec![],
    });
    let request = Request::builder()
        .header("Content-Type", "application/json")
        .header("X-Claims-Subject", claims.sub)
        .header("X-Claims-Audience", claims.aud)
        .header("X-Claims-Issuer", claims.iss)
        .header("X-Claims-Issued-At", claims.issued_at.to_string())
        .header("X-Claims-Expiration", claims.exp.to_string())
        .header(
            "X-Claims-Scopes",
            claims.scopes.iter().fold(String::new(), |acc, s| {
                if acc.is_empty() {
                    s.to_string()
                } else {
                    format!("{},{}", acc, s)
                }
            }),
        )
        .uri(uri)
        .method(method)
        .body(request_body)
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    let status_code = response.status();
    let headers = response.headers().clone();

    let body = match status_code.as_u16() {
        200..=299 => {
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let r = serde_json::from_slice::<R>(&body).unwrap();
            Some(r)
        }
        _ => None,
    };
    Response {
        body,
        status_code,
        headers,
    }
}
