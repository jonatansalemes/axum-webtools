use crate::client::{quote_string, ClickHouseClient};

pub const MIGRATIONS_TABLE: &str = "clickhouse_migrate_schema_migrations";

/// Ensures the schema migrations table exists in the database.
///
/// ClickHouse has no cheap in-place `UPDATE`/`DELETE` (they are asynchronous
/// mutations), so the table is an append-only log: every state change inserts
/// a new row, and the current state of a version is its row with the highest
/// `sequence`. A rollback appends a row with `is_deleted = 1`.
///
/// # Arguments
/// * `client` - ClickHouse client
///
/// # Returns
/// * `Ok(())` if table exists or was created successfully
pub async fn ensure_schema_migrations_table(
    client: &ClickHouseClient,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
                version Int64,
                dirty UInt8,
                content_hash String,
                is_deleted UInt8 DEFAULT 0,
                sequence UInt64,
                applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
            )
            ENGINE = MergeTree
            ORDER BY (version, sequence)
            "#
        ))
        .await
}

/// Appends a state row for `version`.
///
/// `sequence` is taken from the server clock and forced to be strictly greater
/// than any existing row, so the latest write always wins even if the clock
/// steps backwards between runs.
async fn record_state(
    client: &ClickHouseClient,
    version: i64,
    dirty: bool,
    content_hash: &str,
    is_deleted: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .execute(&format!(
            "INSERT INTO {MIGRATIONS_TABLE} (version, dirty, content_hash, is_deleted, sequence) \
             SELECT {version}, {dirty}, {hash}, {is_deleted}, \
             greatest(toUInt64(toUnixTimestamp64Nano(now64(9))), \
                      (SELECT max(sequence) + 1 FROM {MIGRATIONS_TABLE}))",
            dirty = u8::from(dirty),
            hash = quote_string(content_hash),
            is_deleted = u8::from(is_deleted),
        ))
        .await
}

/// Records `version` as applied (or being applied, when `dirty`).
pub async fn mark_applied(
    client: &ClickHouseClient,
    version: i64,
    dirty: bool,
    content_hash: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    record_state(client, version, dirty, content_hash, false).await
}

/// Removes `version` from the applied set.
pub async fn mark_removed(
    client: &ClickHouseClient,
    version: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    record_state(client, version, false, "", true).await
}

/// Retrieves all applied migrations from the database.
///
/// # Arguments
/// * `client` - ClickHouse client
///
/// # Returns
/// * A vector of tuples containing (version, dirty flag, content hash)
pub async fn get_applied_migrations(
    client: &ClickHouseClient,
) -> Result<Vec<(i64, bool, Option<String>)>, Box<dyn std::error::Error>> {
    // Aliases must differ from the column names: ClickHouse resolves aliases
    // everywhere in the query, so `argMax(dirty, ...) AS dirty` would recurse.
    let rows = client
        .query_rows(&format!(
            "SELECT version, argMax(dirty, sequence) AS last_dirty, \
                    argMax(content_hash, sequence) AS last_hash \
             FROM {MIGRATIONS_TABLE} \
             GROUP BY version \
             HAVING argMax(is_deleted, sequence) = 0 \
             ORDER BY version"
        ))
        .await?;

    rows.into_iter()
        .map(|row| {
            let [version, dirty, hash]: [String; 3] = row
                .try_into()
                .map_err(|r| format!("Unexpected row shape from {MIGRATIONS_TABLE}: {r:?}"))?;
            Ok((
                version.parse()?,
                dirty == "1",
                Some(hash).filter(|h| !h.is_empty()),
            ))
        })
        .collect()
}

/// Checks for dirty migrations and returns an error if any are found.
///
/// # Arguments
/// * `client` - ClickHouse client
///
/// # Returns
/// * `Ok(())` if no dirty migrations exist, or an error if any are found
pub async fn check_dirty_migrations(
    client: &ClickHouseClient,
) -> Result<(), Box<dyn std::error::Error>> {
    let applied = get_applied_migrations(client).await?;
    for (version, dirty, _) in applied {
        if dirty {
            return Err(format!(
                "Migration {0} is dirty. Fix the database manually, then run 'clickhouse-migrate redo' to re-apply it or 'clickhouse-migrate force -v {0}' to mark it clean.",
                version
            )
            .into());
        }
    }
    Ok(())
}

/// Prints the current migration version to stdout.
///
/// # Arguments
/// * `client` - ClickHouse client
pub async fn print_current_version(
    client: &ClickHouseClient,
) -> Result<(), Box<dyn std::error::Error>> {
    let applied = get_applied_migrations(client).await?;
    match applied.iter().map(|(v, _, _)| *v).max() {
        Some(version) => println!("Current version: {}", version),
        None => println!("Current version: None (no migrations applied)"),
    }
    Ok(())
}
