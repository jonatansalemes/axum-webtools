# Axum Web Tools

General purpose tools for axum web framework.

## Usage example with some features

* `with_tx` function to run SQLX transactions in Axum web framework.
* `Claims` struct to extract authenticated user from JWT token.
* `HttpError` struct to return error responses.
* `ok` function to return successful responses.
* `endpoint` macro to inject dependencies into handlers.

```toml

[dependencies]
axum = { version = "xxx" }
axum-webtools = { version = "xxx" }
axum-webtools-macros = { version = "xxx" }
sqlx = { version = "xxxx"}
```

```rust

use axum::extract::State;
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use axum_webtools::db::sqlx::with_tx;
use axum_webtools::http::response::{ok, HttpError};
use axum_webtools::security::jwt::Claims;
use log::info;
use scoped_futures::ScopedFutureExt;
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
    with_tx(&pool, |tx| async move {
        let user = create_new_user("someemail", "somepassword", tx).await?;
        ok(CreateNewUserResponse {
            id: user.id,
            email: user.email,
        })
    }.scope_boxed())
        .await
}

#[endpoint(transactional)]
//implicitly inject State(pool): State<PgPool> into the handler
//and wrap the handler with with_tx and inject tx: &mut Tx<'a> into the handler
async fn create_new_user_with_macro_handler() -> Result<Response, HttpError> {
    let user = create_new_user("someemail", "somepassword", tx).await?;
    ok(CreateNewUserResponse {
        id: user.id,
        email: user.email,
    })
}

async fn authenticated_handler(
    //inject claims into handler to require and get the authenticated user
    claims: Claims,
) -> Result<Response, HttpError> {
    let subject = claims.sub;
    info!("Authenticated user: {}", subject);
    ok(())
}

#[endpoint(private)]
//implicitly claims: Claims into the handler
async fn authenticated_with_macro_handler() -> Result<Response, HttpError> {
    let subject = claims.sub;
    info!("Authenticated user: {}", subject);
    ok(())
}

#[endpoint(transactional,private)]
//you can also combine multiple macros
async fn authenticated_with_macro_handler() -> Result<Response, HttpError> {
    let subject = claims.sub;
    // tx: &mut Tx<'a> to run transactions
    // claims:Claims is injected into the handler
    // pool is also injected into the handler
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
            get(authenticated_handler),
        )
        .with_state(pool);

    let ip_addr = IpAddr::from_str("0.0.0.0").unwrap();
    let addr = SocketAddr::from((ip_addr, 8080));
    axum_server::bind(addr)
        .serve(router.into_make_service())
        .await
}

```