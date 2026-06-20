use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, Executor};
use std::fs;
use std::io::{self, Write};
use std::path::Path;

use crate::cli::SafeModeConfirm;
use crate::db::{
    check_dirty_migrations, ensure_schema_migrations_table, get_applied_migrations,
    print_current_version,
};
use crate::migration::{compute_hash, split_sql_by_markers, Migration, MigrationSpec};
use crate::safe_mode::SafeConfig;

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

/// Executes the SQL files at the specified paths.
///
/// # Arguments
/// * `paths` - A slice of strings containing file paths.
/// * `pool` - Database connection pool
///
/// # Returns
/// * `Ok(())` if all SQL files executed successfully
async fn execute_hooks(
    paths: &[String],
    pool: &sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    for path in paths {
        println!("Executing hook SQL: {}", path);
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read hook file '{}': {}", path, e))?;

        pool.execute(AssertSqlSafe(content.as_str()))
            .await
            .map_err(|e| format!("Failed to execute hook SQL from '{}': {}", path, e))?;
    }

    Ok(())
}

/// Runs pending up migrations against the database.
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - Database connection URL
/// * `env` - Environment name for conditional migration execution
/// * `safe_mode_tables` - Table names to watch for in pending migrations
/// * `safe_mode_confirm` - Action when unacknowledged safe-mode table found
/// * `pre_execute` - Space-separated paths to scripts/programs to run before migrations
/// * `post_execute` - Space-separated paths to scripts/programs to run after migrations
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
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database)
        .await?;

    if !pre_execute.is_empty() {
        execute_hooks(pre_execute, &pool).await?;
    }

    ensure_schema_migrations_table(&pool).await?;
    check_dirty_migrations(&pool).await?;

    let applied = get_applied_migrations(&pool).await?;
    let applied_map: std::collections::HashMap<i64, Option<String>> = applied
        .iter()
        .map(|(v, _, hash)| (*v, hash.clone()))
        .collect();

    let migrations = parse_migrations(Path::new(path))?;

    let safe_yml_path = Path::new(path).join("safe-mode.yml");
    let mut safe_config = SafeConfig::load(&safe_yml_path);

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
            let content_lower = migration.up.content.to_lowercase();
            let found_tables: Vec<&str> = safe_mode_tables
                .iter()
                .filter(|t| content_lower.contains(t.as_str()))
                .map(|t| t.as_str())
                .collect();

            if !found_tables.is_empty() {
                eprintln!(
                    "  WARNING: Safe-mode table(s) [{}] found in migration {}. This migration may affect large or critical tables — review carefully before applying to production (e.g. ALTER TABLE, schema changes on high-traffic tables).",
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

        sqlx::query("INSERT INTO pgsql_migrate_schema_migrations (version, dirty, content_hash) VALUES ($1, TRUE, $2)")
            .bind(version_i64)
            .bind(&current_hash)
            .execute(&pool)
            .await?;

        let use_transaction = !migration.up.has_no_tx();
        let use_split = migration.up.has_split_statements();

        if !use_transaction {
            println!("  (running without transaction due to no-tx feature)");
        }
        if use_split {
            println!("  (splitting statements by markers due to split-statements feature)");
        }

        let result: Result<(), Box<dyn std::error::Error>> = if use_split {
            match split_sql_by_markers(&migration.up.content) {
                Ok(blocks) => {
                    let mut exec_result: Result<(), Box<dyn std::error::Error>> = Ok(());
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

                        if use_transaction {
                            let mut tx = pool.begin().await?;
                            match tx.execute(AssertSqlSafe(block.content.as_str())).await {
                                Ok(_) => {
                                    tx.commit().await?;
                                }
                                Err(e) => {
                                    eprintln!("  Error in block {}: {}", i + 1, e);
                                    exec_result = Err(e.into());
                                    break;
                                }
                            }
                        } else {
                            match pool.execute(AssertSqlSafe(block.content.as_str())).await {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("  Error in block {}: {}", i + 1, e);
                                    exec_result = Err(e.into());
                                    break;
                                }
                            }
                        }
                    }
                    exec_result
                }
                Err(e) => Err(format!("Failed to parse split markers: {}", e).into()),
            }
        } else if use_transaction {
            let mut tx = pool.begin().await?;
            match tx
                .execute(AssertSqlSafe(migration.up.content.as_str()))
                .await
            {
                Ok(_) => {
                    tx.commit().await?;
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        } else {
            pool.execute(AssertSqlSafe(migration.up.content.as_str()))
                .await
                .map(|_| ())
                .map_err(|e| e.into())
        };

        match result {
            Ok(_) => {
                sqlx::query(
                    "UPDATE pgsql_migrate_schema_migrations SET dirty = FALSE WHERE version = $1",
                )
                .bind(version_i64)
                .execute(&pool)
                .await?;
                println!("  Applied successfully");
                applied_count += 1;
            }
            Err(e) => {
                eprintln!("  Error applying migration {}: {}", migration.filename, e);
                eprintln!("  Migration {} is now marked as dirty.", migration.version);
                eprintln!("  Please fix the issue and update pgsql_migrate_schema_migrations table manually.");
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
        execute_hooks(post_execute, &pool).await?;
    }

    print_current_version(&pool).await?;

    Ok(())
}

/// Rolls back the specified number of migrations.
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - Database connection URL
/// * `env` - Environment name for conditional migration execution
/// * `count` - Number of migrations to roll back
/// * `safe_mode_skip_auto_remove` - Skip automatic removal of safe-mode.yml entries
/// * `pre_execute` - Space-separated paths to scripts/programs to run before rollback
/// * `post_execute` - Space-separated paths to scripts/programs to run after rollback
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
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database)
        .await?;

    if !pre_execute.is_empty() {
        execute_hooks(pre_execute, &pool).await?;
    }

    ensure_schema_migrations_table(&pool).await?;
    check_dirty_migrations(&pool).await?;

    let applied = get_applied_migrations(&pool).await?;
    if applied.is_empty() {
        println!("No migrations to rollback.");
        return Ok(());
    }

    let migrations = parse_migrations(Path::new(path))?;
    let migration_map: std::collections::HashMap<u32, Migration> =
        migrations.into_iter().map(|m| (m.version, m)).collect();

    let safe_yml_path = Path::new(path).join("safe-mode.yml");
    let mut safe_config = SafeConfig::load(&safe_yml_path);

    let mut versions_to_rollback: Vec<i64> = applied.iter().map(|(v, _, _)| *v).collect();
    versions_to_rollback.reverse();
    versions_to_rollback.truncate(count as usize);

    let mut rolled_back_count = 0;
    for version in versions_to_rollback {
        let version_u32 = version as u32;

        if let Some(migration) = migration_map.get(&version_u32) {
            println!("Rolling back migration: {}", migration.filename);

            if migration.down.is_empty() {
                eprintln!("  Warning: No down migration found for version {}", version);
                continue;
            }

            sqlx::query(
                "UPDATE pgsql_migrate_schema_migrations SET dirty = TRUE WHERE version = $1",
            )
            .bind(version)
            .execute(&pool)
            .await?;

            let use_transaction = !migration.down.has_no_tx();
            let use_split = migration.down.has_split_statements();

            if !use_transaction {
                println!("  (running without transaction due to no-tx feature)");
            }
            if use_split {
                println!("  (splitting statements by markers due to split-statements feature)");
            }

            let result: Result<(), Box<dyn std::error::Error>> = if use_split {
                match split_sql_by_markers(&migration.down.content) {
                    Ok(blocks) => {
                        let mut exec_result: Result<(), Box<dyn std::error::Error>> = Ok(());
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

                            if use_transaction {
                                let mut tx = pool.begin().await?;
                                match tx.execute(AssertSqlSafe(block.content.as_str())).await {
                                    Ok(_) => {
                                        tx.commit().await?;
                                    }
                                    Err(e) => {
                                        eprintln!("  Error in block {}: {}", i + 1, e);
                                        exec_result = Err(e.into());
                                        break;
                                    }
                                }
                            } else {
                                match pool.execute(AssertSqlSafe(block.content.as_str())).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        eprintln!("  Error in block {}: {}", i + 1, e);
                                        exec_result = Err(e.into());
                                        break;
                                    }
                                }
                            }
                        }
                        exec_result
                    }
                    Err(e) => Err(format!("Failed to parse split markers: {}", e).into()),
                }
            } else if use_transaction {
                let mut tx = pool.begin().await?;
                match tx
                    .execute(AssertSqlSafe(migration.down.content.as_str()))
                    .await
                {
                    Ok(_) => {
                        tx.commit().await?;
                        Ok(())
                    }
                    Err(e) => Err(e.into()),
                }
            } else {
                pool.execute(AssertSqlSafe(migration.down.content.as_str()))
                    .await
                    .map(|_| ())
                    .map_err(|e| e.into())
            };

            match result {
                Ok(_) => {
                    sqlx::query("DELETE FROM pgsql_migrate_schema_migrations WHERE version = $1")
                        .bind(version)
                        .execute(&pool)
                        .await?;
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
                        "  Please fix the issue and update pgsql_migrate_schema_migrations table manually."
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
        execute_hooks(post_execute, &pool).await?;
    }

    print_current_version(&pool).await?;

    Ok(())
}

/// Baselines the database by marking migrations as applied without executing them.
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - Database connection URL
/// * `target_version` - Version up to which migrations should be baselined
///
/// # Returns
/// * `Ok(())` if baseline completed successfully
pub async fn run_baseline(
    path: &str,
    database: &str,
    target_version: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database)
        .await?;

    ensure_schema_migrations_table(&pool).await?;

    let applied = get_applied_migrations(&pool).await?;
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

        sqlx::query(
            "INSERT INTO pgsql_migrate_schema_migrations (version, dirty, content_hash, applied_at) VALUES ($1, FALSE, $2, NOW())",
        )
        .bind(version_i64)
        .bind(&content_hash)
        .execute(&pool)
        .await?;

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

/// Redoes dirty migrations by removing them from the tracking table and re-applying.
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - Database connection URL
/// * `env` - Environment name for conditional migration execution
/// * `safe_mode_tables` - Table names to watch for in pending migrations
/// * `safe_mode_confirm` - Action when unacknowledged safe-mode table found
/// * `pre_execute` - Space-separated paths to scripts/programs to run before redo
/// * `post_execute` - Space-separated paths to scripts/programs to run after redo
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
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database)
        .await?;

    if !pre_execute.is_empty() {
        execute_hooks(pre_execute, &pool).await?;
    }

    ensure_schema_migrations_table(&pool).await?;

    let applied = get_applied_migrations(&pool).await?;
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

    sqlx::query("DELETE FROM pgsql_migrate_schema_migrations WHERE version = $1")
        .bind(version)
        .execute(&pool)
        .await?;

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
        execute_hooks(post_execute, &pool).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_database_url() -> String {
        std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://pgsqlmigrate:pgsqlmigrate@pgsql:5432/pgsqlmigrate".to_string()
        })
    }

    #[tokio::test]
    async fn test_execute_hooks_success() -> Result<(), Box<dyn std::error::Error>> {
        let database_url = get_database_url();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;

        let temp_dir = tempfile::tempdir()?;
        let hook_path = temp_dir.path().join("hook.sql");
        fs::write(&hook_path, "SELECT 1")?;

        execute_hooks(&[hook_path.to_str().unwrap().to_string()], &pool).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_execute_hooks_multiple_success() -> Result<(), Box<dyn std::error::Error>> {
        let database_url = get_database_url();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;

        let temp_dir = tempfile::tempdir()?;
        let hook1_path = temp_dir.path().join("hook1.sql");
        let hook2_path = temp_dir.path().join("hook2.sql");
        fs::write(&hook1_path, "SELECT 1")?;
        fs::write(&hook2_path, "SELECT 2")?;

        execute_hooks(
            &[
                hook1_path.to_str().unwrap().to_string(),
                hook2_path.to_str().unwrap().to_string(),
            ],
            &pool,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_execute_hooks_failure() -> Result<(), Box<dyn std::error::Error>> {
        let database_url = get_database_url();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;

        let temp_dir = tempfile::tempdir()?;
        let hook_path = temp_dir.path().join("hook.sql");
        fs::write(&hook_path, "INVALID SQL")?;

        let result = execute_hooks(&[hook_path.to_str().unwrap().to_string()], &pool).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_execute_hooks_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let database_url = get_database_url();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;

        let result = execute_hooks(&["/non/existent/path".to_string()], &pool).await;
        assert!(result.is_err());
        Ok(())
    }
}
