pub mod tests {
    use crate::db::sqlx::with_tx;
    use crate::security::jwt::{create_jwt_token, JwtToken};
    use axum::body::Body;
    use axum::http::{HeaderMap, Request, StatusCode};
    use axum::Router;
    use http_body_util::BodyExt;
    use scoped_futures::{ScopedBoxFuture, ScopedFutureExt};
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use sqlx::{Database, Error, Pool, Transaction};
    use tower::ServiceExt;

    pub struct Response<T: DeserializeOwned> {
        pub body: Option<T>,
        pub status_code: StatusCode,
        pub headers: HeaderMap,
    }

    pub async fn request<'a, T, R, F, X, P, DB>(
        pool: &Pool<DB>,
        router: Router,
        uri: &str,
        method: &str,
        body: T,
        user_generator: F,
    ) -> Response<R>
    where
        T: FnOnce(&Box<dyn JwtToken>) -> P,
        P: Serialize,
        R: DeserializeOwned,
        DB: Database,
        F: for<'r> FnOnce(&'r mut Transaction<DB>) -> ScopedBoxFuture<'a, 'r, Box<dyn JwtToken>>
            + Send
            + 'a + 'static,
    {
        let x = with_tx(&pool, |tx| {
            async move {
                let x = user_generator(tx).await;
                Ok::<_, Error>(x)
            }
            .scope_boxed()
        })
        .await
        .unwrap();

        let authorization = create_jwt_token(&x.subject());
        let request_body = Body::from(serde_json::to_vec(&body(&x)).unwrap());

        let request = Request::builder()
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", authorization))
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
}
