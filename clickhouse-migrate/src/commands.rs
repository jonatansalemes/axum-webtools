use std::fs;
use std::io::{self, Write};
use std::path::Path;

use crate::cli::SafeModeConfirm;
use crate::client::ClickHouseClient;
use crate::db::{
    check_dirty_migrations, ensure_schema_migrations_table, get_applied_migrations, mark_applied,
    mark_removed, print_current_version, MIGRATIONS_TABLE,
};
use crate::migration::{compute_hash, split_sql_by_markers, Migration, MigrationSpec};
use crate::safe_mode::SafeConfig;
use crate::sql::{content_references_table, split_statements, strip_sql_comments};

/// Parses all migration files from the specified directory.
///
/// # Arguments
/// * `dir` - Path to the migrations directory
///
/// # Returns
/// * A sorted vector of Migration instances
pub fn parse_migrations(dir: &Path) -> Result<Vec<Migration>, Box<dyn std::error::Error>> {
    let mut migrations: Vec<Migration> = Vec::new();

    if !dir.exists() {
        return Err(format!("Migrations directory '{}' does not exist", dir.display()).into());
    }

    let mut up_files: std::collections::HashMap<u32, (String, String)> =
        std::collections::HashMap::new();
    let mut down_files: std::collections::HashMap<u32, String> = std::collections::HashMap::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy().to_string();

        if name.ends_with(".up.sql") {
            if let Some(version_str) = name.split('_').next() {
                if let Ok(version) = version_str.parse::<u32>() {
                    let content = fs::read_to_string(entry.path())?;
                    up_files.insert(version, (name.clone(), content));
                }
            }
        } else if name.ends_with(".down.sql") {
            if let Some(version_str) = name.split('_').next() {
                if let Ok(version) = version_str.parse::<u32>() {
                    let content = fs::read_to_string(entry.path())?;
                    down_files.insert(version, content);
                }
            }
        }
    }

    for (version, (filename, up_content)) in up_files {
        let down_content = down_files.get(&version).cloned().unwrap_or_default();
        migrations.push(Migration {
            version,
            filename,
            up: MigrationSpec::new(up_content),
            down: if down_content.is_empty() {
                MigrationSpec::empty()
            } else {
                MigrationSpec::new(down_content)
            },
        });
    }

    migrations.sort_by_key(|m| m.version);

    Ok(migrations)
}

/// Returns a one-line preview of a statement for error messages.
fn statement_preview(statement: &str) -> String {
    const MAX: usize = 120;
    let one_line = statement.split_whitespace().collect::<Vec<_>>().join(" ");
    match one_line.char_indices().nth(MAX) {
        Some((idx, _)) => format!("{}...", &one_line[..idx]),
        None => one_line,
    }
}

/// Executes every statement in `sql`, one request per statement, stopping at
/// the first failure.
async fn execute_statements(
    client: &ClickHouseClient,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let statements = split_statements(sql);
    let total = statements.len();
    for (i, statement) in statements.iter().enumerate() {
        if let Err(e) = client.execute(statement).await {
            eprintln!(
                "  Error in statement {}/{}: {}",
                i + 1,
                total,
                statement_preview(statement)
            );
            return Err(e);
        }
    }
    Ok(())
}

/// Executes the body of a migration spec, honouring the `split-statements`
/// feature and its `skip-on-env` blocks.
///
/// ClickHouse has no transactions: statements that ran before a failure stay
/// applied, which is why callers mark the migration dirty around this call.
async fn execute_spec(
    client: &ClickHouseClient,
    spec: &MigrationSpec,
    env: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !spec.has_split_statements() {
        return execute_statements(client, &spec.content).await;
    }

    println!("  (splitting statements by markers due to split-statements feature)");
    let blocks = split_sql_by_markers(&spec.content)
        .map_err(|e| format!("Failed to parse split markers: {}", e))?;

    for (i, block) in blocks.iter().enumerate() {
        if block.should_skip(env) {
            println!(
                "  Skipping block {} (skip-on-env: {} matches current env: {})",
                i + 1,
                block.skip_on_env.join(","),
                env
            );
            continue;
        }

        if let Err(e) = execute_statements(client, &block.content).await {
            eprintln!("  Error in block {}: {}", i + 1, e);
            return Err(e);
        }
    }
    Ok(())
}

/// Executes the SQL files at the specified paths.
///
/// # Arguments
/// * `paths` - A slice of strings containing file paths.
/// * `client` - ClickHouse client
///
/// # Returns
/// * `Ok(())` if all SQL files executed successfully
async fn execute_hooks(
    paths: &[String],
    client: &ClickHouseClient,
) -> Result<(), Box<dyn std::error::Error>> {
    for path in paths {
        println!("Executing hook SQL: {}", path);
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read hook file '{}': {}", path, e))?;

        execute_statements(client, &content)
            .await
            .map_err(|e| format!("Failed to execute hook SQL from '{}': {}", path, e))?;
    }

    Ok(())
}

/// Runs pending up migrations against the database.
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - ClickHouse connection URL
/// * `env` - Environment name for conditional migration execution
/// * `safe_mode_tables` - Table names to watch for in pending migrations
/// * `safe_mode_confirm` - Action when unacknowledged safe-mode table found
/// * `pre_execute` - Paths to SQL files to run before migrations
/// * `post_execute` - Paths to SQL files to run after migrations
///
/// # Returns
/// * `Ok(())` if all migrations applied successfully
pub async fn run_up(
    path: &str,
    database: &str,
    env: &str,
    safe_mode_tables: &[String],
    safe_mode_confirm: &SafeModeConfirm,
    pre_execute: &[String],
    post_execute: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Running migrations in environment: {}", env);
    let client = ClickHouseClient::connect(database)?;

    if !pre_execute.is_empty() {
        execute_hooks(pre_execute, &client).await?;
    }

    ensure_schema_migrations_table(&client).await?;
    check_dirty_migrations(&client).await?;

    let applied = get_applied_migrations(&client).await?;
    let applied_map: std::collections::HashMap<i64, Option<String>> = applied
        .iter()
        .map(|(v, _, hash)| (*v, hash.clone()))
        .collect();

    let migrations = parse_migrations(Path::new(path))?;

    let safe_yml_path = Path::new(path).join("safe-mode.yml");
    let mut safe_config = SafeConfig::load(&safe_yml_path)?;

    let mut applied_count = 0;
    for migration in migrations {
        let version_i64 = migration.version as i64;
        let current_hash = compute_hash(&migration.up.content);

        if let Some(stored_hash) = applied_map.get(&version_i64) {
            if let Some(ref hash) = stored_hash {
                if hash != &current_hash {
                    eprintln!(
                        "  WARNING: Migration {} content has changed since it was applied!",
                        migration.filename
                    );
                    eprintln!("    Stored hash:  {}", hash);
                    eprintln!("    Current hash: {}", current_hash);
                }
            }
            continue;
        }

        if !safe_mode_tables.is_empty() {
            let content_lower = strip_sql_comments(&migration.up.content).to_lowercase();
            let found_tables: Vec<&str> = safe_mode_tables
                .iter()
                .filter(|t| content_references_table(&content_lower, t.as_str()))
                .map(|t| t.as_str())
                .collect();

            if !found_tables.is_empty() {
                eprintln!(
                    "  WARNING: Safe-mode table(s) [{}] found in migration {}. This migration may affect large or critical tables — review carefully before applying to production (e.g. ALTER TABLE mutations, schema changes on high-volume tables).",
                    found_tables.join(", "),
                    migration.filename
                );

                let script = &migration.filename;
                let unacknowledged: Vec<&str> = found_tables
                    .iter()
                    .filter(|&&t| !safe_config.is_acknowledged(script, t))
                    .copied()
                    .collect();

                if !unacknowledged.is_empty() {
                    match safe_mode_confirm {
                        SafeModeConfirm::ExitWithError => {
                            eprintln!(
                                "  ERROR: Unacknowledged table(s) [{}] in migration {}. Add them to safe-mode.yml or remove --safe-mode-confirm=exit-with-error to be prompted.",
                                unacknowledged.join(", "),
                                script
                            );
                            return Err(
                                "Migration aborted: unacknowledged safe-mode tables found.".into(),
                            );
                        }
                        SafeModeConfirm::Ask => {
                            eprint!("  Apply this migration? (y/N): ");
                            io::stderr().flush()?;
                            let mut input = String::new();
                            io::stdin().read_line(&mut input)?;
                            if input.trim().to_lowercase() != "y" {
                                eprintln!("  Aborting migration.");
                                return Err(
                                    "Migration aborted by user due to safe-mode table warning."
                                        .into(),
                                );
                            }
                        }
                    }
                    for t in &unacknowledged {
                        safe_config.acknowledge(script, t);
                    }
                    safe_config.save(&safe_yml_path)?;
                }
            }
        }

        println!("Applying migration: {}", migration.filename);

        mark_applied(&client, version_i64, true, &current_hash).await?;

        match execute_spec(&client, &migration.up, env).await {
            Ok(_) => {
                mark_applied(&client, version_i64, false, &current_hash).await?;
                println!("  Applied successfully");
                applied_count += 1;
            }
            Err(e) => {
                eprintln!("  Error applying migration {}: {}", migration.filename, e);
                eprintln!("  Migration {} is now marked as dirty.", migration.version);
                eprintln!(
                    "  ClickHouse has no transactions: statements before the failing one stay applied."
                );
                eprintln!(
                    "  Fix the issue, then run 'redo' to re-apply it or 'force -v {}' to mark it clean.",
                    migration.version
                );
                return Err(e);
            }
        }
    }

    if applied_count == 0 {
        println!("No new migrations to apply.");
    } else {
        println!("Applied {} migration(s).", applied_count);
    }

    if !post_execute.is_empty() {
        execute_hooks(post_execute, &client).await?;
    }

    print_current_version(&client).await?;

    Ok(())
}

/// Reports whether the database has any pending migrations, without applying
/// anything. Read-only apart from creating the tracking table if missing:
/// intended as a readiness/ordering gate (e.g. an init container that must
/// wait for migrations before running).
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - ClickHouse connection URL
/// * `env` - Environment name (informational; pending state is version-based)
///
/// # Returns
/// * `Ok(0)` if the database is up to date (every on-disk migration applied)
/// * `Ok(1)` if one or more migrations are pending
///
/// A dirty migration or an unreachable database is returned as `Err`, which the
/// caller maps to exit code 2.
pub async fn run_status(
    path: &str,
    database: &str,
    env: &str,
) -> Result<i32, Box<dyn std::error::Error>> {
    println!("Checking migration status in environment: {}", env);
    let client = ClickHouseClient::connect(database)?;

    ensure_schema_migrations_table(&client).await?;
    check_dirty_migrations(&client).await?; // dirty migration -> Err -> exit 2

    let applied = get_applied_migrations(&client).await?;
    let applied_versions: std::collections::HashSet<i64> =
        applied.iter().map(|(v, _, _)| *v).collect();

    let migrations = parse_migrations(Path::new(path))?;
    let pending: Vec<u32> = migrations
        .iter()
        .filter(|m| !applied_versions.contains(&(m.version as i64)))
        .map(|m| m.version)
        .collect();

    match applied_versions.iter().max() {
        Some(version) => println!("Applied version: {}", version),
        None => println!("Applied version: none (no migrations applied)"),
    }

    if pending.is_empty() {
        println!(
            "Status: up to date ({} migration(s) applied)",
            migrations.len()
        );
        Ok(0)
    } else {
        let pending_list = pending
            .iter()
            .map(|version| format!("{:06}", version))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "Status: {} migration(s) pending: {}",
            pending.len(),
            pending_list
        );
        Ok(1)
    }
}

/// Rolls back the specified number of migrations.
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - ClickHouse connection URL
/// * `env` - Environment name for conditional migration execution
/// * `count` - Number of migrations to roll back
/// * `safe_mode_skip_auto_remove` - Skip automatic removal of safe-mode.yml entries
/// * `pre_execute` - Paths to SQL files to run before rollback
/// * `post_execute` - Paths to SQL files to run after rollback
///
/// # Returns
/// * `Ok(())` if all rollbacks completed successfully
pub async fn run_down(
    path: &str,
    database: &str,
    env: &str,
    count: u32,
    safe_mode_skip_auto_remove: bool,
    pre_execute: &[String],
    post_execute: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Running rollback in environment: {}", env);
    let client = ClickHouseClient::connect(database)?;

    if !pre_execute.is_empty() {
        execute_hooks(pre_execute, &client).await?;
    }

    ensure_schema_migrations_table(&client).await?;
    check_dirty_migrations(&client).await?;

    let applied = get_applied_migrations(&client).await?;
    if applied.is_empty() {
        println!("No migrations to rollback.");
        return Ok(());
    }

    let migrations = parse_migrations(Path::new(path))?;
    let migration_map: std::collections::HashMap<u32, Migration> =
        migrations.into_iter().map(|m| (m.version, m)).collect();

    let safe_yml_path = Path::new(path).join("safe-mode.yml");
    let mut safe_config = SafeConfig::load(&safe_yml_path)?;

    let mut to_rollback: Vec<(i64, Option<String>)> =
        applied.into_iter().map(|(v, _, hash)| (v, hash)).collect();
    to_rollback.reverse();
    to_rollback.truncate(count as usize);

    let mut rolled_back_count = 0;
    for (version, stored_hash) in to_rollback {
        let version_u32 = version as u32;

        if let Some(migration) = migration_map.get(&version_u32) {
            println!("Rolling back migration: {}", migration.filename);

            if migration.down.is_empty() {
                eprintln!("  Warning: No down migration found for version {}", version);
                continue;
            }

            let hash = stored_hash.unwrap_or_default();
            mark_applied(&client, version, true, &hash).await?;

            match execute_spec(&client, &migration.down, env).await {
                Ok(_) => {
                    mark_removed(&client, version).await?;
                    println!("  Rolled back successfully");
                    rolled_back_count += 1;

                    if !safe_mode_skip_auto_remove {
                        safe_config.remove_migration(&migration.filename);
                        if safe_yml_path.exists() {
                            safe_config.save(&safe_yml_path)?;
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "  Error rolling back migration {}: {}",
                        migration.filename, e
                    );
                    eprintln!("  Migration {} is now marked as dirty.", version);
                    eprintln!(
                        "  Fix the issue, then run 'force -v {}' to mark it clean or re-run the rollback.",
                        version
                    );
                    return Err(e);
                }
            }
        } else {
            eprintln!("Warning: Migration file not found for version {}", version);
        }
    }

    if rolled_back_count == 0 {
        println!("No migrations rolled back.");
    } else {
        println!("Rolled back {} migration(s).", rolled_back_count);
    }

    if !post_execute.is_empty() {
        execute_hooks(post_execute, &client).await?;
    }

    print_current_version(&client).await?;

    Ok(())
}

/// Baselines the database by marking migrations as applied without executing them.
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - ClickHouse connection URL
/// * `target_version` - Version up to which migrations should be baselined
///
/// # Returns
/// * `Ok(())` if baseline completed successfully
pub async fn run_baseline(
    path: &str,
    database: &str,
    target_version: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = ClickHouseClient::connect(database)?;

    ensure_schema_migrations_table(&client).await?;

    let applied = get_applied_migrations(&client).await?;
    let applied_versions: std::collections::HashSet<i64> =
        applied.iter().map(|(v, _, _)| *v).collect();

    let migrations = parse_migrations(Path::new(path))?;

    let migrations_to_baseline: Vec<&Migration> = migrations
        .iter()
        .filter(|m| m.version <= target_version)
        .collect();

    if migrations_to_baseline.is_empty() {
        println!("No migrations found up to version {}", target_version);
        return Ok(());
    }

    let mut baselined_count = 0;
    for migration in migrations_to_baseline {
        let version_i64 = migration.version as i64;

        if applied_versions.contains(&version_i64) {
            println!("Skipping already applied migration: {}", migration.filename);
            continue;
        }

        let content_hash = compute_hash(&migration.up.content);
        mark_applied(&client, version_i64, false, &content_hash).await?;

        println!("Baselined migration: {}", migration.filename);
        baselined_count += 1;
    }

    if baselined_count == 0 {
        println!("No new migrations to baseline.");
    } else {
        println!(
            "Baselined {} migration(s) up to version {}.",
            baselined_count, target_version
        );
    }

    Ok(())
}

/// Marks a dirty migration as clean without executing anything.
///
/// This is the ClickHouse counterpart of manually editing the tracking table:
/// after fixing a partially applied migration by hand, `force` records it as
/// applied with the hash of the current file.
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - ClickHouse connection URL
/// * `version` - The dirty migration version to mark clean
///
/// # Returns
/// * `Ok(())` if the migration was marked clean
pub async fn run_force(
    path: &str,
    database: &str,
    version: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = ClickHouseClient::connect(database)?;

    ensure_schema_migrations_table(&client).await?;

    let applied = get_applied_migrations(&client).await?;
    let version_i64 = version as i64;
    match applied.iter().find(|(v, _, _)| *v == version_i64) {
        Some((_, true, _)) => {}
        Some((_, false, _)) => {
            println!("Migration {} is not dirty; nothing to do.", version);
            return Ok(());
        }
        None => {
            return Err(format!(
                "Migration {} is not recorded in {}. Use 'baseline' to mark unapplied migrations.",
                version, MIGRATIONS_TABLE
            )
            .into())
        }
    }

    let migrations = parse_migrations(Path::new(path))?;
    let migration = migrations
        .iter()
        .find(|m| m.version == version)
        .ok_or_else(|| format!("Migration file not found for version {}", version))?;

    mark_applied(
        &client,
        version_i64,
        false,
        &compute_hash(&migration.up.content),
    )
    .await?;
    println!("Marked migration {} as clean.", migration.filename);

    print_current_version(&client).await?;

    Ok(())
}

/// Redoes dirty migrations by removing them from the tracking table and re-applying.
///
/// ClickHouse has no transactions, so statements that succeeded before the
/// failure run again: prefer idempotent DDL (`IF NOT EXISTS` / `IF EXISTS`).
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - ClickHouse connection URL
/// * `env` - Environment name for conditional migration execution
/// * `safe_mode_tables` - Table names to watch for in pending migrations
/// * `safe_mode_confirm` - Action when unacknowledged safe-mode table found
/// * `pre_execute` - Paths to SQL files to run before redo
/// * `post_execute` - Paths to SQL files to run after redo
///
/// # Returns
/// * `Ok(())` if redo completed successfully
pub async fn run_redo(
    path: &str,
    database: &str,
    env: &str,
    safe_mode_tables: &[String],
    safe_mode_confirm: &SafeModeConfirm,
    pre_execute: &[String],
    post_execute: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let client = ClickHouseClient::connect(database)?;

    if !pre_execute.is_empty() {
        execute_hooks(pre_execute, &client).await?;
    }

    ensure_schema_migrations_table(&client).await?;

    let applied = get_applied_migrations(&client).await?;
    let dirty_migration = applied
        .iter()
        .filter(|(_, dirty, _)| *dirty)
        .max_by_key(|(version, _, _)| version);

    let (version, _, _) = match dirty_migration {
        Some(m) => m,
        None => {
            println!("No dirty migrations found.");
            return Ok(());
        }
    };

    println!("Redoing migration version: {}", version);

    mark_removed(&client, *version).await?;

    run_up(
        path,
        database,
        env,
        safe_mode_tables,
        safe_mode_confirm,
        &[], // Hooks already handled in run_redo or we don't want to run them twice
        &[],
    )
    .await?;

    if !post_execute.is_empty() {
        execute_hooks(post_execute, &client).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::SafeModeConfirm;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn get_database_url() -> String {
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| {
            "http://clickhousemigrate:clickhousemigrate@clickhouse:8123/clickhousemigrate"
                .to_string()
        })
    }

    static DB_SEQ: AtomicU32 = AtomicU32::new(0);

    /// Creates a fresh, uniquely-named ClickHouse database and returns a
    /// connection URL pointing at it. This isolates the tracking table so
    /// these tests can run in parallel without clobbering each other's state.
    async fn isolated_database_url() -> Result<String, Box<dyn std::error::Error>> {
        let base_url = get_database_url();
        let admin = ClickHouseClient::connect(&base_url)?;

        let seq = DB_SEQ.fetch_add(1, Ordering::SeqCst);
        let db = format!("chm_test_{}_{}", std::process::id(), seq);
        admin
            .execute(&format!("DROP DATABASE IF EXISTS {db}"))
            .await?;
        admin.execute(&format!("CREATE DATABASE {db}")).await?;

        let (before_query, query) = base_url.split_once('?').unwrap_or((&base_url, ""));
        let scheme_end = before_query.find("://").map(|i| i + 3).unwrap_or(0);
        let server = match before_query[scheme_end..].find('/') {
            Some(slash) => &before_query[..scheme_end + slash],
            None => before_query,
        };
        let sep = if query.is_empty() { "" } else { "?" };
        Ok(format!("{server}/{db}{sep}{query}"))
    }

    fn write_migration(dir: &Path, version: u32, name: &str, up: &str, down: &str) {
        let stem = format!("{version:06}_{name}");
        fs::write(dir.join(format!("{stem}.up.sql")), up).unwrap();
        fs::write(dir.join(format!("{stem}.down.sql")), down).unwrap();
    }

    async fn table_exists(
        client: &ClickHouseClient,
        table: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let rows = client
            .query_rows(&format!(
                "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '{table}'"
            ))
            .await?;
        Ok(rows[0][0] == "1")
    }

    async fn up(path: &Path, url: &str) -> Result<(), Box<dyn std::error::Error>> {
        run_up(
            path.to_str().unwrap(),
            url,
            "test",
            &[],
            &SafeModeConfirm::ExitWithError,
            &[],
            &[],
        )
        .await
    }

    #[test]
    fn test_statement_preview_truncates() {
        let long = format!("SELECT {}", "x, ".repeat(100));
        let preview = statement_preview(&long);
        assert!(preview.ends_with("..."));
        assert_eq!(preview.chars().count(), 123);
        assert_eq!(statement_preview("SELECT\n    1"), "SELECT 1");
    }

    #[tokio::test]
    async fn test_execute_hooks_success() -> Result<(), Box<dyn std::error::Error>> {
        let client = ClickHouseClient::connect(&get_database_url())?;
        let temp_dir = tempfile::tempdir()?;
        let hook_path = temp_dir.path().join("hook.sql");
        fs::write(&hook_path, "SELECT 1;\nSELECT 2;")?;

        execute_hooks(&[hook_path.to_str().unwrap().to_string()], &client).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_execute_hooks_failure() -> Result<(), Box<dyn std::error::Error>> {
        let client = ClickHouseClient::connect(&get_database_url())?;
        let temp_dir = tempfile::tempdir()?;
        let hook_path = temp_dir.path().join("hook.sql");
        fs::write(&hook_path, "INVALID SQL")?;

        let result = execute_hooks(&[hook_path.to_str().unwrap().to_string()], &client).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_execute_hooks_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let client = ClickHouseClient::connect(&get_database_url())?;
        let result = execute_hooks(&["/non/existent/path".to_string()], &client).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_up_down_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let url = isolated_database_url().await?;
        let client = ClickHouseClient::connect(&url)?;
        let temp_dir = tempfile::tempdir()?;
        write_migration(
            temp_dir.path(),
            1,
            "events",
            "-- Add migration script here\n\
             CREATE TABLE events (id UInt64, name String) ENGINE = MergeTree ORDER BY id;\n\
             INSERT INTO events VALUES (1, 'a;b');\n",
            "DROP TABLE IF EXISTS events;",
        );
        write_migration(
            temp_dir.path(),
            2,
            "events_daily",
            "CREATE TABLE events_daily (day Date) ENGINE = MergeTree ORDER BY day;",
            "DROP TABLE IF EXISTS events_daily;",
        );

        up(temp_dir.path(), &url).await?;
        assert!(table_exists(&client, "events").await?);
        assert!(table_exists(&client, "events_daily").await?);
        let applied = get_applied_migrations(&client).await?;
        assert_eq!(
            applied.iter().map(|(v, d, _)| (*v, *d)).collect::<Vec<_>>(),
            vec![(1, false), (2, false)]
        );

        // Re-running is a no-op.
        up(temp_dir.path(), &url).await?;

        run_down(
            temp_dir.path().to_str().unwrap(),
            &url,
            "test",
            1,
            true,
            &[],
            &[],
        )
        .await?;
        assert!(table_exists(&client, "events").await?);
        assert!(!table_exists(&client, "events_daily").await?);
        let applied = get_applied_migrations(&client).await?;
        assert_eq!(applied.len(), 1);

        // Rolled-back versions can be applied again.
        up(temp_dir.path(), &url).await?;
        assert!(table_exists(&client, "events_daily").await?);
        assert_eq!(get_applied_migrations(&client).await?.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_failed_migration_is_dirty_then_redo() -> Result<(), Box<dyn std::error::Error>> {
        let url = isolated_database_url().await?;
        let client = ClickHouseClient::connect(&url)?;
        let temp_dir = tempfile::tempdir()?;
        write_migration(
            temp_dir.path(),
            1,
            "broken",
            "CREATE TABLE IF NOT EXISTS t1 (id UInt8) ENGINE = Memory;\nSELECT * FROM missing_table;",
            "DROP TABLE IF EXISTS t1;",
        );

        assert!(up(temp_dir.path(), &url).await.is_err());
        let applied = get_applied_migrations(&client).await?;
        assert_eq!(
            applied.iter().map(|(v, d, _)| (*v, *d)).collect::<Vec<_>>(),
            vec![(1, true)]
        );
        // No transactions: the first statement stays applied.
        assert!(table_exists(&client, "t1").await?);
        // A dirty migration blocks further runs.
        assert!(up(temp_dir.path(), &url).await.is_err());

        write_migration(
            temp_dir.path(),
            1,
            "broken",
            "CREATE TABLE IF NOT EXISTS t1 (id UInt8) ENGINE = Memory;\nSELECT 1;",
            "DROP TABLE IF EXISTS t1;",
        );
        run_redo(
            temp_dir.path().to_str().unwrap(),
            &url,
            "test",
            &[],
            &SafeModeConfirm::ExitWithError,
            &[],
            &[],
        )
        .await?;
        let applied = get_applied_migrations(&client).await?;
        assert_eq!(
            applied.iter().map(|(v, d, _)| (*v, *d)).collect::<Vec<_>>(),
            vec![(1, false)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_force_marks_dirty_migration_clean() -> Result<(), Box<dyn std::error::Error>> {
        let url = isolated_database_url().await?;
        let client = ClickHouseClient::connect(&url)?;
        let temp_dir = tempfile::tempdir()?;
        write_migration(
            temp_dir.path(),
            1,
            "broken",
            "SELECT * FROM missing_table;",
            "",
        );

        assert!(up(temp_dir.path(), &url).await.is_err());
        run_force(temp_dir.path().to_str().unwrap(), &url, 1).await?;

        let applied = get_applied_migrations(&client).await?;
        assert_eq!(
            applied.iter().map(|(v, d, _)| (*v, *d)).collect::<Vec<_>>(),
            vec![(1, false)]
        );
        assert!(run_force(temp_dir.path().to_str().unwrap(), &url, 2)
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_split_statements_skip_on_env() -> Result<(), Box<dyn std::error::Error>> {
        let url = isolated_database_url().await?;
        let client = ClickHouseClient::connect(&url)?;
        let temp_dir = tempfile::tempdir()?;
        write_migration(
            temp_dir.path(),
            1,
            "blocks",
            "-- features: split-statements\n\
             -- split-start\n\
             CREATE TABLE a (id UInt8) ENGINE = Memory;\n\
             CREATE TABLE b (id UInt8) ENGINE = Memory;\n\
             -- split-end\n\
             -- split-start\n\
             -- skip-on-env test\n\
             CREATE TABLE skipped (id UInt8) ENGINE = Memory;\n\
             -- split-end\n",
            "",
        );

        up(temp_dir.path(), &url).await?;
        assert!(table_exists(&client, "a").await?);
        assert!(table_exists(&client, "b").await?);
        assert!(!table_exists(&client, "skipped").await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_baseline_marks_without_running() -> Result<(), Box<dyn std::error::Error>> {
        let url = isolated_database_url().await?;
        let client = ClickHouseClient::connect(&url)?;
        let temp_dir = tempfile::tempdir()?;
        write_migration(
            temp_dir.path(),
            1,
            "one",
            "CREATE TABLE one (id UInt8) ENGINE = Memory;",
            "",
        );
        write_migration(
            temp_dir.path(),
            2,
            "two",
            "CREATE TABLE two (id UInt8) ENGINE = Memory;",
            "",
        );

        run_baseline(temp_dir.path().to_str().unwrap(), &url, 1).await?;
        assert!(!table_exists(&client, "one").await?);

        up(temp_dir.path(), &url).await?;
        assert!(!table_exists(&client, "one").await?);
        assert!(table_exists(&client, "two").await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_run_status_reports_pending_and_up_to_date(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = isolated_database_url().await?;
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().to_str().unwrap();

        // Empty directory and fresh database: nothing pending.
        assert_eq!(run_status(path, &url, "test").await?, 0);

        write_migration(temp_dir.path(), 1, "init", "SELECT 1", "SELECT 1");
        assert_eq!(run_status(path, &url, "test").await?, 1);

        up(temp_dir.path(), &url).await?;
        assert_eq!(run_status(path, &url, "test").await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_run_status_dirty_migration_errors() -> Result<(), Box<dyn std::error::Error>> {
        let url = isolated_database_url().await?;
        let client = ClickHouseClient::connect(&url)?;
        ensure_schema_migrations_table(&client).await?;
        mark_applied(&client, 1, true, "deadbeef").await?;

        let temp_dir = tempfile::tempdir()?;
        write_migration(temp_dir.path(), 1, "init", "SELECT 1", "SELECT 1");

        let result = run_status(temp_dir.path().to_str().unwrap(), &url, "test").await;
        assert!(
            result.is_err(),
            "a dirty migration must surface as an error, not a status code"
        );
        Ok(())
    }
}
