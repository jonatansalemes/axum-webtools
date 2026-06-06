use sqlx::{Executor, Row};

/// Ensures the schema migrations table exists in the database.
///
/// # Arguments
/// * `pool` - Database connection pool
///
/// # Returns
/// * `Ok(())` if table exists or was created successfully
pub async fn ensure_schema_migrations_table(
    pool: &sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS pgsql_migrate_schema_migrations (
            version BIGINT PRIMARY KEY,
            dirty BOOLEAN NOT NULL DEFAULT FALSE,
            content_hash VARCHAR(64),
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#,
    )
    .await?;
    Ok(())
}

/// Retrieves all applied migrations from the database.
///
/// # Arguments
/// * `pool` - Database connection pool
///
/// # Returns
/// * A vector of tuples containing (version, dirty flag, content hash)
pub async fn get_applied_migrations(
    pool: &sqlx::PgPool,
) -> Result<Vec<(i64, bool, Option<String>)>, Box<dyn std::error::Error>> {
    let rows = sqlx::query(
        "SELECT version, dirty, content_hash FROM pgsql_migrate_schema_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await?;

    let migrations: Vec<(i64, bool, Option<String>)> = rows
        .iter()
        .map(|row| {
            (
                row.get("version"),
                row.get("dirty"),
                row.get("content_hash"),
            )
        })
        .collect();

    Ok(migrations)
}

/// Checks for dirty migrations and returns an error if any are found.
///
/// # Arguments
/// * `pool` - Database connection pool
///
/// # Returns
/// * `Ok(())` if no dirty migrations exist, or an error if any are found
pub async fn check_dirty_migrations(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let applied = get_applied_migrations(pool).await?;
    for (version, dirty, _) in applied {
        if dirty {
            return Err(format!(
                "Migration {} is dirty. Please fix it manually and update the pgsql_migrate_schema_migrations table.",
                version
            )
            .into());
        }
    }
    Ok(())
}

/// Gets the current highest migration version from the database.
///
/// # Arguments
/// * `pool` - Database connection pool
///
/// # Returns
/// * `Some(version)` if migrations exist, `None` otherwise
pub async fn get_current_version(
    pool: &sqlx::PgPool,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    let result =
        sqlx::query("SELECT MAX(version) as max_version FROM pgsql_migrate_schema_migrations")
            .fetch_one(pool)
            .await?;

    let version: Option<i64> = result.get("max_version");
    Ok(version)
}

/// Prints the current migration version to stdout.
///
/// # Arguments
/// * `pool` - Database connection pool
pub async fn print_current_version(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    match get_current_version(pool).await? {
        Some(version) => println!("Current version: {}", version),
        None => println!("Current version: None (no migrations applied)"),
    }
    Ok(())
}
