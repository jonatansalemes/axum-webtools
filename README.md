# Axum Web Tools

General purpose tools for axum web framework.

## Usage example with some features

* `with_tx` function to run SQLX transactions in Axum web framework.
* `Claims` struct to extract authenticated user from JWT token.
* `HttpError` struct to return error responses.
* `ok` function to return successful responses.

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

## PgSQL Migrate

A powerful PostgreSQL migration tool included with axum-webtools that provides database schema management with advanced features for complex operations.

### Installation

Install the migration tool binary:

```bash
cargo install axum-webtools-pgsql-migrate
```

### Basic Usage

```bash
# Create a new migration
pgsql-migrate create -s "create_users_table"

# Run all pending migrations
pgsql-migrate up -d "postgres://user:pass@localhost/db"

# Run migrations with specific environment (default: prod)
pgsql-migrate up -d "postgres://user:pass@localhost/db" -e dev

# Rollback migrations (rollback 1 migration by default)
pgsql-migrate down -d "postgres://user:pass@localhost/db"

# Rollback specific number of migrations
pgsql-migrate down -d "postgres://user:pass@localhost/db" 3

# Rollback with specific environment
pgsql-migrate down -d "postgres://user:pass@localhost/db" -e dev 3

# Baseline existing migrations (mark as applied without running)
pgsql-migrate baseline -d "postgres://user:pass@localhost/db" -v 5
```

### Migration Files

Migrations are created as pairs of `.up.sql` and `.down.sql` files:

```
migrations/
├── 000001_create_users_table.up.sql
├── 000001_create_users_table.down.sql
├── 000002_add_indexes.up.sql
├── 000002_add_indexes.down.sql
└── 000003_create_materialized_views.up.sql
└── 000003_create_materialized_views.down.sql
```

### Advanced Features

#### 1. No Transaction Feature (`no-tx`)

Some PostgreSQL operations cannot run within transactions. Use the `no-tx` feature for operations like:
- `CREATE INDEX CONCURRENTLY`
- `CREATE MATERIALIZED VIEW`
- `ALTER TYPE ADD VALUE`

**Example:**

```sql
-- features: no-tx

-- This migration runs without a transaction wrapper
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);

-- Multiple materialized views in the same script
CREATE MATERIALIZED VIEW user_stats AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as user_count
FROM users 
GROUP BY DATE(created_at);

CREATE MATERIALIZED VIEW daily_activity AS
SELECT 
    DATE(last_login) as login_date,
    COUNT(*) as active_users
FROM users 
WHERE last_login IS NOT NULL
GROUP BY DATE(last_login);
```

#### 2. Split Statements Feature (`split-statements`)

When you need to execute multiple complex operations that require separate execution contexts, use the `split-statements` feature with markers:

**Example:**

```sql
-- features: split-statements

-- First block: Create base tables
-- split-start
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

INSERT INTO categories (name) VALUES 
    ('Electronics'),
    ('Books'),
    ('Clothing');
-- split-end

-- Second block: Create dependent materialized view
-- split-start
CREATE MATERIALIZED VIEW category_stats AS
SELECT 
    c.name,
    COUNT(p.id) as product_count
FROM categories c
LEFT JOIN products p ON p.category_id = c.id
GROUP BY c.id, c.name;

-- Create indexes on the materialized view
CREATE INDEX idx_category_stats_name ON category_stats(name);
-- split-end

-- Third block: Grant permissions
-- split-start
GRANT SELECT ON category_stats TO readonly_user;
GRANT ALL ON categories TO app_user;
-- split-end
```

#### 3. Skip On Environment Feature (`skip-on-env`)

Skip specific SQL blocks based on the current environment. This feature works at the **block level** within split statements, allowing fine-grained control over which blocks execute in different environments.

Use the `--env` or `-e` CLI parameter to specify the current environment (default: `prod`).

**Example: Skip seed data blocks in production**

```sql
-- features: split-statements

-- Block 1: Schema changes (runs in all environments)
-- split-start
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
-- split-end

-- Block 2: Seed data (skip in production)
-- split-start
-- skip-on-env prod
INSERT INTO users (email) VALUES
    ('dev@example.com'),
    ('test@example.com');
-- split-end

-- Block 3: More schema changes (runs in all environments)
-- split-start
CREATE INDEX idx_users_email ON users(email);
-- split-end
```

**Example: Skip performance optimizations in dev/homolog**

```sql
-- features: no-tx, split-statements

-- Block 1: Basic index (runs everywhere)
-- split-start
CREATE INDEX CONCURRENTLY idx_orders_user ON orders(user_id);
-- split-end

-- Block 2: Heavy index (skip in dev and homolog)
-- split-start
-- skip-on-env dev,homolog
CREATE INDEX CONCURRENTLY idx_orders_complex ON orders(created_at, status, total);
-- split-end
```

**Running with environment:**

```bash
# Run in dev environment - blocks with "-- skip-on-env dev" will be skipped
pgsql-migrate up -d "postgres://user:pass@localhost/db" -e dev

# Run in production (default) - blocks with "-- skip-on-env prod" will be skipped
pgsql-migrate up -d "postgres://user:pass@localhost/db"

# Run in homolog environment
pgsql-migrate up -d "postgres://user:pass@localhost/db" -e homolog
```

#### 4. Combined Features

You can combine features for complex scenarios:

**Example: Multiple materialized views without transactions**

```sql
-- features: no-tx, split-statements

-- First materialized view block
-- split-start
CREATE MATERIALIZED VIEW hourly_sales AS
SELECT 
    DATE_TRUNC('hour', created_at) as hour,
    SUM(total_amount) as total_sales,
    COUNT(*) as order_count
FROM orders
GROUP BY DATE_TRUNC('hour', created_at);
-- split-end

-- Second materialized view block
-- split-start
CREATE MATERIALIZED VIEW product_performance AS
SELECT 
    p.id,
    p.name,
    COUNT(oi.id) as times_sold,
    SUM(oi.quantity) as total_quantity
FROM products p
LEFT JOIN order_items oi ON oi.product_id = p.id
GROUP BY p.id, p.name;
-- split-end

-- Concurrent indexes block
-- split-start
CREATE INDEX CONCURRENTLY idx_hourly_sales_hour ON hourly_sales(hour);
CREATE INDEX CONCURRENTLY idx_product_performance_times_sold ON product_performance(times_sold DESC);
-- split-end
```

### Migration Tracking

The tool automatically:
- Creates a `pgsql_migrate_schema_migrations` table to track applied migrations
- Stores content hashes to detect changes in already-applied migrations
- Marks migrations as "dirty" during execution to handle failed migrations
- Validates migration integrity before execution

### Error Handling

- **Dirty migrations**: If a migration fails, it's marked as dirty and must be manually resolved
- **Content changes**: Warns when applied migration content has changed
- **Validation**: Ensures proper marker pairing in split-statements feature
- **Transaction safety**: Automatically handles transaction wrapping based on features

### Use Cases

**Perfect for:**
- **Database schema evolution** with complex dependencies
- **Creating multiple materialized views** that need separate execution contexts  
- **Concurrent index creation** without blocking operations
- **Data migrations** that require multi-step processing
- **Permission management** across multiple database objects
- **Performance optimizations** that need specific execution patterns

**Example: Complex E-commerce Migration**

```sql
-- features: no-tx, split-statements

-- Create core product tables
-- split-start
CREATE TABLE product_categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    parent_id INTEGER REFERENCES product_categories(id)
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    category_id INTEGER NOT NULL REFERENCES product_categories(id),
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
-- split-end

-- Create performance materialized views
-- split-start
CREATE MATERIALIZED VIEW category_hierarchy AS
WITH RECURSIVE cat_tree AS (
    SELECT id, name, parent_id, 0 as level, ARRAY[id] as path
    FROM product_categories WHERE parent_id IS NULL
    UNION ALL
    SELECT c.id, c.name, c.parent_id, t.level + 1, t.path || c.id
    FROM product_categories c
    JOIN cat_tree t ON c.parent_id = t.id
)
SELECT * FROM cat_tree;
-- split-end

-- Create concurrent indexes for performance
-- split-start
CREATE INDEX CONCURRENTLY idx_products_category_price ON products(category_id, price DESC);
CREATE INDEX CONCURRENTLY idx_products_created_at ON products(created_at DESC);
-- split-end
```

This comprehensive migration system ensures reliable, trackable, and flexible database schema management for complex applications.
