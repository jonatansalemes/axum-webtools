# axum-webtools

General purpose helpers for the [axum](https://github.com/tokio-rs/axum) web framework: SQLx transaction management, JWT authentication, and consistent HTTP responses.

> Part of the [axum-webtools](../README.md) workspace.

## Crate

- https://crates.io/crates/axum-webtools
- Companion macros crate: https://crates.io/crates/axum-webtools-macros

## Features

- `with_tx` — run SQLx transactions with automatic commit/rollback.
- `Claims` — extractor that requires and exposes the authenticated user from a JWT token.
- `HttpError` — struct to return consistent error responses.
- `ok` — helper to return successful responses.
- `#[endpoint]` — attribute macro (from `axum-webtools-macros`) for building handlers.

## Installation

```toml
[dependencies]
axum = { version = "xxx" }
axum-webtools = { version = "xxx" }
axum-webtools-macros = { version = "xxx" }
sqlx = { version = "xxxx" }
```

## Usage example

```rust
use axum::extract::State;
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use axum_webtools::db::sqlx::with_tx;
use axum_webtools::http::response::{ok, HttpError};
use axum_webtools::security::jwt::Claims;
use log::info;
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use axum_webtools_macros::endpoint;

pub type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

#[derive(Debug, Serialize)]
struct CreateNewUserResponse {
    id: i32,
    email: String,
}

struct User {
    id: i32,
    email: String,
    password: String,
}

async fn create_new_user<'a>(email: &str, password: &str, transaction: &mut Tx<'a>) -> sqlx::Result<User> {
    let user = sqlx::query_as!(
        User,
        r#"
        INSERT INTO users (email, password)
        VALUES ($1, $2)
        RETURNING *
        "#,
        email,
        password
    )
        .fetch_one(&mut **transaction)
        .await?;
    Ok(user)
}

async fn create_new_user_handler(
    State(pool): State<PgPool>,
) -> Result<Response, HttpError> {
    // with_tx is a helper function that wraps the transaction logic
    // if the closure returns an error, the transaction will be rolled back
    with_tx(&pool, async |tx| {
        let user = create_new_user("someemail", "somepassword", tx).await?;
        ok(CreateNewUserResponse {
            id: user.id,
            email: user.email,
        })
    })
        .await
}

async fn authenticated_handler(
    //inject claims into handler to require and get the authenticated user
    claims: Claims,
) -> Result<Response, HttpError> {
    let subject = claims.sub;
    info!("Authenticated user: {}", subject);
    ok(())
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {

    //jwt integration needs these environment variables
    std::env::set_var("JWT_SECRET", "yoursecret");
    std::env::set_var("JWT_ISSUER", "yourissuer");
    std::env::set_var("JWT_AUDIENCE", "youraudience");

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect("postgres://username:password@pgsql:5432/dbname")
        .await
        .expect("Failed to create pool");

    let router = Router::new()
        .route(
            "/api/v1/users",
            post(create_new_user_handler),
        )
        .route(
            "/api/v1/authenticated",
            get(authenticated_handler)
                    .layer(
                       RequireScopeLayer::new()
                               .with(vec!["some:scope"])
                    ),
        )
        .with_state(pool);

    let ip_addr = IpAddr::from_str("0.0.0.0").unwrap();
    let addr = SocketAddr::from((ip_addr, 8080));
    axum_server::bind(addr)
        .serve(router.into_make_service())
        .await
}
```

## JWT environment variables

The JWT integration (`Claims` extractor) requires the following environment variables:

- `JWT_SECRET` — signing secret
- `JWT_ISSUER` — expected token issuer
- `JWT_AUDIENCE` — expected token audience
