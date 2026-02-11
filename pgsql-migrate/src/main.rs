use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, Row};
use std::fs;
use std::path::Path;

#[derive(Parser)]
#[command(name = "pgsql-migrate")]
#[command(about = "A simple PostgreSQL migration tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run migrations up
    #[command(name = "up")]
    Up {
        /// Path to migrations directory
        #[arg(short = 'p', long = "path", default_value = "migrations")]
        path: String,

        /// Database connection URL
        #[arg(short = 'd', long = "database")]
        database: String,

        /// Environment name (used for skip-on-env feature)
        #[arg(short = 'e', long = "env", default_value = "prod")]
        env: String,
    },

    /// Run migrations down
    #[command(name = "down")]
    Down {
        /// Path to migrations directory
        #[arg(short = 'p', long = "path", default_value = "migrations")]
        path: String,

        /// Database connection URL
        #[arg(short = 'd', long = "database")]
        database: String,

        /// Environment name (used for skip-on-env feature)
        #[arg(short = 'e', long = "env", default_value = "prod")]
        env: String,

        /// Number of migrations to rollback
        #[arg(default_value = "1")]
        count: u32,
    },

    /// Create a new migration
    #[command(name = "create")]
    Create {
        /// Directory where migrations are stored
        #[arg(short = 'd', long = "dir", default_value = "migrations")]
        dir: String,

        /// Name of the migration
        #[arg(short = 's', long = "seq")]
        name: String,
    },

    /// Baseline migrations up to a version (mark as applied without running)
    #[command(name = "baseline")]
    Baseline {
        /// Path to migrations directory
        #[arg(short = 'p', long = "path", default_value = "migrations")]
        path: String,

        /// Database connection URL
        #[arg(short = 'd', long = "database")]
        database: String,

        /// Version to baseline up to (inclusive)
        #[arg(short = 'v', long = "version")]
        version: u32,
    },

    /// Redo the last dirty migration (mark as clean and reapply)
    #[command(name = "redo")]
    Redo {
        /// Path to migrations directory
        #[arg(short = 'p', long = "path", default_value = "migrations")]
        path: String,

        /// Database connection URL
        #[arg(short = 'd', long = "database")]
        database: String,

        /// Environment name (used for skip-on-env feature)
        #[arg(short = 'e', long = "env", default_value = "prod")]
        env: String,
    },

    /// Create a database backup using pg_dump
    #[command(name = "backup")]
    Backup {
        /// Database connection URL
        #[arg(short = 'd', long = "database")]
        database: String,

        /// Output file path for the backup
        #[arg(short = 'o', long = "output")]
        output: String,

        /// Backup format: plain (SQL), custom, directory, or tar
        #[arg(short = 'f', long = "format", default_value = "custom")]
        format: String,

        /// Compress the backup (level 0-9, only for custom/directory format)
        #[arg(short = 'c', long = "compress")]
        compress: Option<u8>,

        /// Do not output commands to set ownership of objects
        #[arg(long = "no-owner")]
        no_owner: bool,

        /// Prevent dumping of access privileges (grant/revoke commands)
        #[arg(long = "no-acl")]
        no_acl: bool,
    },

    /// Restore a database backup using pg_restore or psql
    #[command(name = "restore")]
    Restore {
        /// Database connection URL
        #[arg(short = 'd', long = "database")]
        database: String,

        /// Input file path for the backup
        #[arg(short = 'i', long = "input")]
        input: String,

        /// Drop database objects before recreating them
        #[arg(long = "clean")]
        clean: bool,

        /// Create the database before restoring
        #[arg(long = "create")]
        create: bool,

        /// Do not output commands to set ownership of objects
        #[arg(long = "no-owner")]
        no_owner: bool,

        /// Skip restoration of access privileges (grant/revoke commands)
        #[arg(long = "no-acl")]
        no_acl: bool,
    },
}

/// Migration features that control execution behavior
#[derive(Debug, Clone, PartialEq, Eq)]
enum MigrationFeature {
    /// Run migration without wrapping in a transaction
    /// Useful for: CREATE INDEX CONCURRENTLY, CREATE MATERIALIZED VIEW, ALTER TYPE ADD VALUE
    NoTransaction,
    /// Split statements into individual executions using -- split-start and -- split-end markers
    SplitStatements,
}

impl MigrationFeature {
    /// Parse a feature from string
    fn from_str(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "no-tx" => Some(MigrationFeature::NoTransaction),
            "split-statements" => Some(MigrationFeature::SplitStatements),
            _ => None,
        }
    }
}

/// Encapsulates the SQL content and features for a migration direction (up or down)
#[derive(Debug, Clone)]
struct MigrationSpec {
    content: String,
    features: Vec<MigrationFeature>,
}

impl MigrationSpec {
    /// Create a new MigrationSpec, parsing features from the content
    fn new(content: String) -> Self {
        let features = Self::parse_features(&content);
        Self { content, features }
    }

    /// Create an empty MigrationSpec
    fn empty() -> Self {
        Self {
            content: String::new(),
            features: Vec::new(),
        }
    }

    /// Check if this spec has the NoTransaction feature
    fn has_no_tx(&self) -> bool {
        self.features.contains(&MigrationFeature::NoTransaction)
    }

    /// Check if this spec has the SplitStatements feature
    fn has_split_statements(&self) -> bool {
        self.features.contains(&MigrationFeature::SplitStatements)
    }

    /// Check if content is empty
    fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    /// Parse features from migration file content
    /// Looks for: -- features: no-tx, other-feature
    fn parse_features(content: &str) -> Vec<MigrationFeature> {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("-- features:") {
                let features_str = trimmed.trim_start_matches("-- features:").trim();
                return features_str
                    .split(',')
                    .filter_map(MigrationFeature::from_str)
                    .collect();
            }
            // Stop looking after first non-comment, non-empty line
            if !trimmed.is_empty() && !trimmed.starts_with("--") {
                break;
            }
        }
        Vec::new()
    }
}

/// Represents a parsed migration with its version, filename, and SQL content
struct Migration {
    version: u32,
    filename: String,
    up: MigrationSpec,
    down: MigrationSpec,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Up {
            path,
            database,
            env,
        } => {
            run_up(&path, &database, &env).await?;
        }
        Commands::Down {
            path,
            database,
            env,
            count,
        } => {
            run_down(&path, &database, &env, count).await?;
        }
        Commands::Create { dir, name } => {
            create_migration(&dir, &name)?;
        }
        Commands::Baseline {
            path,
            database,
            version,
        } => {
            run_baseline(&path, &database, version).await?;
        }
        Commands::Redo {
            path,
            database,
            env,
        } => {
            run_redo(&path, &database, &env).await?;
        }
        Commands::Backup {
            database,
            output,
            format,
            compress,
            no_owner,
            no_acl,
        } => {
            run_backup(&database, &output, &format, compress, no_owner, no_acl).await?;
        }
        Commands::Restore {
            database,
            input,
            clean,
            create,
            no_owner,
            no_acl,
        } => {
            run_restore(&database, &input, clean, create, no_owner, no_acl).await?;
        }
    }

    Ok(())
}

/// Normalize name: lowercase, replace spaces with underscores, keep only alphanumeric and underscores
fn normalize_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c == ' ' { '_' } else { c })
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// Get the next migration version based on existing files
fn get_next_version(dir: &Path) -> Result<u32, Box<dyn std::error::Error>> {
    if !dir.exists() {
        return Ok(1);
    }

    let mut max_version: u32 = 0;

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();

        // Parse version from filename like "000001_name.up.sql"
        if let Some(version_str) = name.split('_').next() {
            if let Ok(version) = version_str.parse::<u32>() {
                max_version = max_version.max(version);
            }
        }
    }

    Ok(max_version + 1)
}

/// Create a new migration file pair
fn create_migration(dir: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let dir_path = Path::new(dir);

    // Create directory if it doesn't exist
    if !dir_path.exists() {
        fs::create_dir_all(dir_path)?;
        println!("Created migrations directory: {}", dir);
    }

    let version = get_next_version(dir_path)?;
    let normalized_name = normalize_name(name);

    let up_filename = format!("{:06}_{}.up.sql", version, normalized_name);
    let down_filename = format!("{:06}_{}.down.sql", version, normalized_name);

    let up_path = dir_path.join(&up_filename);
    let down_path = dir_path.join(&down_filename);

    fs::write(&up_path, "-- Add migration script here\n")?;
    fs::write(&down_path, "-- Add rollback script here\n")?;

    println!("Created migration files:");
    println!("  {}", up_path.display());
    println!("  {}", down_path.display());

    Ok(())
}

/// Compute SHA256 hash of content
fn compute_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Represents a SQL block with optional skip-on-env environments
#[derive(Debug, Clone)]
struct SqlBlock {
    content: String,
    skip_on_env: Vec<String>,
}

impl SqlBlock {
    /// Check if this block should be skipped based on the current environment
    fn should_skip(&self, current_env: &str) -> bool {
        self.skip_on_env
            .iter()
            .any(|e| e.eq_ignore_ascii_case(current_env))
    }
}

/// Split SQL content into blocks using -- split-start and -- split-end markers
/// Each block can optionally have a -- skip-on-env directive to skip it on certain environments
/// Returns an error if markers are mismatched (start without end, end without start, or unclosed block)
fn split_sql_by_markers(content: &str) -> Result<Vec<SqlBlock>, String> {
    let mut blocks = Vec::new();
    let mut current_block = String::new();
    let mut current_skip_envs: Vec<String> = Vec::new();
    let mut in_block = false;
    let mut block_start_line = 0;

    for (line_num, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        let line_number = line_num + 1; // 1-based line numbers

        if trimmed == "-- split-start" {
            if in_block {
                return Err(format!(
                    "Line {}: Found '-- split-start' but previous block starting at line {} was not closed with '-- split-end'",
                    line_number, block_start_line
                ));
            }
            in_block = true;
            block_start_line = line_number;
            current_block.clear();
            current_skip_envs.clear();
            continue;
        }

        if trimmed == "-- split-end" {
            if !in_block {
                return Err(format!(
                    "Line {}: Found '-- split-end' without a matching '-- split-start'",
                    line_number
                ));
            }
            // Save the block
            let block_content = current_block.trim().to_string();
            if !block_content.is_empty() {
                blocks.push(SqlBlock {
                    content: block_content,
                    skip_on_env: current_skip_envs.clone(),
                });
            }
            in_block = false;
            current_block.clear();
            current_skip_envs.clear();
            continue;
        }

        if in_block {
            // Check for skip-on-env directive inside the block
            if trimmed.starts_with("-- skip-on-env") {
                let envs_str = trimmed.trim_start_matches("-- skip-on-env").trim();
                current_skip_envs = envs_str
                    .split(',')
                    .map(|e| e.trim().to_lowercase())
                    .filter(|e| !e.is_empty())
                    .collect();
                // Don't add this line to the block content
                continue;
            }

            if !current_block.is_empty() {
                current_block.push('\n');
            }
            current_block.push_str(line);
        }
    }

    // Check for unclosed block
    if in_block {
        return Err(format!(
            "Block starting at line {} was not closed with '-- split-end'",
            block_start_line
        ));
    }

    if blocks.is_empty() {
        return Err(
            "split-statements feature requires at least one block delimited by '-- split-start' and '-- split-end'".to_string()
        );
    }

    Ok(blocks)
}

/// Ensure schema_migrations table exists
async fn ensure_schema_migrations_table(
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

/// Get applied migrations
async fn get_applied_migrations(
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

/// Check if there are dirty migrations
async fn check_dirty_migrations(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
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

/// Get current migration version
async fn get_current_version(
    pool: &sqlx::PgPool,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    let result =
        sqlx::query("SELECT MAX(version) as max_version FROM pgsql_migrate_schema_migrations")
            .fetch_one(pool)
            .await?;

    let version: Option<i64> = result.get("max_version");
    Ok(version)
}

/// Print current version
async fn print_current_version(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    match get_current_version(pool).await? {
        Some(version) => println!("Current version: {}", version),
        None => println!("Current version: None (no migrations applied)"),
    }
    Ok(())
}

/// Parse migration files from directory
fn parse_migrations(dir: &Path) -> Result<Vec<Migration>, Box<dyn std::error::Error>> {
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

/// Run migrations up
async fn run_up(path: &str, database: &str, env: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Running migrations in environment: {}", env);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database)
        .await?;

    ensure_schema_migrations_table(&pool).await?;
    check_dirty_migrations(&pool).await?;

    let applied = get_applied_migrations(&pool).await?;
    let applied_map: std::collections::HashMap<i64, Option<String>> = applied
        .iter()
        .map(|(v, _, hash)| (*v, hash.clone()))
        .collect();

    let migrations = parse_migrations(Path::new(path))?;

    let mut applied_count = 0;
    for migration in migrations {
        let version_i64 = migration.version as i64;
        let current_hash = compute_hash(&migration.up.content);

        // Check if migration was already applied
        if let Some(stored_hash) = applied_map.get(&version_i64) {
            // Check if content hash differs
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

        println!("Applying migration: {}", migration.filename);

        // Mark as dirty before applying
        sqlx::query("INSERT INTO pgsql_migrate_schema_migrations (version, dirty, content_hash) VALUES ($1, TRUE, $2)")
            .bind(version_i64)
            .bind(&current_hash)
            .execute(&pool)
            .await?;

        // Check if migration should run without transaction
        let use_transaction = !migration.up.has_no_tx();
        let use_split = migration.up.has_split_statements();

        // Print feature info
        if !use_transaction {
            println!("  (running without transaction due to no-tx feature)");
        }
        if use_split {
            println!("  (splitting statements by markers due to split-statements feature)");
        }

        let result: Result<(), Box<dyn std::error::Error>> = if use_split {
            // Split by markers and execute each block
            match split_sql_by_markers(&migration.up.content) {
                Ok(blocks) => {
                    let mut exec_result: Result<(), Box<dyn std::error::Error>> = Ok(());
                    for (i, block) in blocks.iter().enumerate() {
                        // Check if block should be skipped based on environment
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
                            // Each block in its own transaction
                            let mut tx = pool.begin().await?;
                            match tx.execute(block.content.as_str()).await {
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
                            // Execute without transaction
                            match pool.execute(block.content.as_str()).await {
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
            // Apply migration inside a transaction
            let mut tx = pool.begin().await?;
            match tx.execute(migration.up.content.as_str()).await {
                Ok(_) => {
                    tx.commit().await?;
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        } else {
            // Apply migration without transaction (single execution)
            pool.execute(migration.up.content.as_str())
                .await
                .map(|_| ())
                .map_err(|e| e.into())
        };

        match result {
            Ok(_) => {
                // Mark as clean
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

    print_current_version(&pool).await?;

    Ok(())
}

/// Run migrations down
async fn run_down(
    path: &str,
    database: &str,
    env: &str,
    count: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Running rollback in environment: {}", env);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database)
        .await?;

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

    // Get versions to rollback (from newest to oldest)
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

            // Mark as dirty before rollback
            sqlx::query(
                "UPDATE pgsql_migrate_schema_migrations SET dirty = TRUE WHERE version = $1",
            )
            .bind(version)
            .execute(&pool)
            .await?;

            // Check if migration should run without transaction
            let use_transaction = !migration.down.has_no_tx();
            let use_split = migration.down.has_split_statements();

            // Print feature info
            if !use_transaction {
                println!("  (running without transaction due to no-tx feature)");
            }
            if use_split {
                println!("  (splitting statements by markers due to split-statements feature)");
            }

            let result: Result<(), Box<dyn std::error::Error>> = if use_split {
                // Split by markers and execute each block
                match split_sql_by_markers(&migration.down.content) {
                    Ok(blocks) => {
                        let mut exec_result: Result<(), Box<dyn std::error::Error>> = Ok(());
                        for (i, block) in blocks.iter().enumerate() {
                            // Check if block should be skipped based on environment
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
                                // Each block in its own transaction
                                let mut tx = pool.begin().await?;
                                match tx.execute(block.content.as_str()).await {
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
                                // Execute without transaction
                                match pool.execute(block.content.as_str()).await {
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
                // Apply down migration inside a transaction
                let mut tx = pool.begin().await?;
                match tx.execute(migration.down.content.as_str()).await {
                    Ok(_) => {
                        tx.commit().await?;
                        Ok(())
                    }
                    Err(e) => Err(e.into()),
                }
            } else {
                // Apply migration without transaction (single execution)
                pool.execute(migration.down.content.as_str())
                    .await
                    .map(|_| ())
                    .map_err(|e| e.into())
            };

            match result {
                Ok(_) => {
                    // Remove from pgsql_migrate_schema_migrations
                    sqlx::query("DELETE FROM pgsql_migrate_schema_migrations WHERE version = $1")
                        .bind(version)
                        .execute(&pool)
                        .await?;
                    println!("  Rolled back successfully");
                    rolled_back_count += 1;
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

    print_current_version(&pool).await?;

    Ok(())
}

/// Run baseline up to a specific version (mark all migrations as applied without running them)
async fn run_baseline(
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

        // Skip if already applied
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

/// Redo the last dirty migration (mark as clean and reapply)
async fn run_redo(path: &str, database: &str, env: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database)
        .await?;

    ensure_schema_migrations_table(&pool).await?;

    // Find the last dirty migration
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

    // Mark as clean (not dirty) and delete from schema to allow run_up to reapply it
    sqlx::query("DELETE FROM pgsql_migrate_schema_migrations WHERE version = $1")
        .bind(version)
        .execute(&pool)
        .await?;

    // Call run_up to reapply the migration
    run_up(path, database, env).await?;

    Ok(())
}

/// Parse PostgreSQL connection URL to extract components
fn parse_pg_url(url: &str) -> Result<PgConnectionInfo, Box<dyn std::error::Error>> {
    // Expected format: postgresql://[user[:password]@][host][:port][/dbname][?params]
    let url = url
        .strip_prefix("postgresql://")
        .or_else(|| url.strip_prefix("postgres://"))
        .ok_or("Invalid PostgreSQL URL: must start with postgresql:// or postgres://")?;

    let (auth_host, db_params) = url.split_once('/').unwrap_or((url, ""));
    let (db_name, _params) = db_params.split_once('?').unwrap_or((db_params, ""));

    let (auth, host_port) = if let Some((a, h)) = auth_host.rsplit_once('@') {
        (Some(a), h)
    } else {
        (None, auth_host)
    };

    let (user, password) = if let Some(auth_str) = auth {
        let (u, p) = auth_str.split_once(':').unwrap_or((auth_str, ""));
        (
            Some(u.to_string()),
            if p.is_empty() {
                None
            } else {
                Some(p.to_string())
            },
        )
    } else {
        (None, None)
    };

    let (host, port) = if let Some((h, p)) = host_port.rsplit_once(':') {
        (h.to_string(), Some(p.to_string()))
    } else {
        (host_port.to_string(), None)
    };

    Ok(PgConnectionInfo {
        host: if host.is_empty() {
            "localhost".to_string()
        } else {
            host
        },
        port: port.unwrap_or_else(|| "5432".to_string()),
        user: user.unwrap_or_else(|| "postgres".to_string()),
        password,
        database: if db_name.is_empty() {
            "postgres".to_string()
        } else {
            db_name.to_string()
        },
    })
}

struct PgConnectionInfo {
    host: String,
    port: String,
    user: String,
    password: Option<String>,
    database: String,
}

/// Check if a command exists in PATH
fn command_exists(cmd: &str) -> bool {
    std::process::Command::new("which")
        .arg(cmd)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Create a database backup using pg_dump
async fn run_backup(
    database: &str,
    output: &str,
    format: &str,
    compress: Option<u8>,
    no_owner: bool,
    no_acl: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if pg_dump exists
    if !command_exists("pg_dump") {
        return Err("pg_dump command not found. Please install PostgreSQL client tools. Example: sudo apt update && sudo apt install postgresql-client-16".into());
    }

    println!("Creating backup of database...");

    let conn_info = parse_pg_url(database)?;

    // Validate format
    let format_flag = match format.to_lowercase().as_str() {
        "plain" | "p" => "p",
        "custom" | "c" => "c",
        "directory" | "d" => "d",
        "tar" | "t" => "t",
        _ => {
            return Err(format!(
                "Invalid format '{}'. Use: plain, custom, directory, or tar",
                format
            )
            .into())
        }
    };

    // Validate compression level
    if let Some(level) = compress {
        if level > 9 {
            return Err("Compression level must be between 0 and 9".into());
        }
        if format_flag == "p" {
            return Err("Compression is not supported for plain format".into());
        }
    }

    // Build pg_dump command
    let mut cmd = std::process::Command::new("pg_dump");

    cmd.arg("--host")
        .arg(&conn_info.host)
        .arg("--port")
        .arg(&conn_info.port)
        .arg("--username")
        .arg(&conn_info.user)
        .arg("--format")
        .arg(format_flag)
        .arg("--file")
        .arg(output)
        .arg("--verbose");

    if let Some(level) = compress {
        cmd.arg("--compress").arg(level.to_string());
    }

    if no_owner {
        cmd.arg("--no-owner");
    }

    if no_acl {
        cmd.arg("--no-acl");
    }

    cmd.arg(&conn_info.database);

    // Set password via environment variable if provided
    if let Some(ref password) = conn_info.password {
        cmd.env("PGPASSWORD", password);
    }

    // Log command being run (without sensitive information)
    let mut cmd_str = format!(
        "Running: pg_dump --host {} --port {} --username {} --dbname {} --format {} --file {}",
        conn_info.host, conn_info.port, conn_info.user, conn_info.database, format, output
    );
    if let Some(level) = compress {
        cmd_str.push_str(&format!(" --compress {}", level));
    }
    if no_owner {
        cmd_str.push_str(" --no-owner");
    }
    if no_acl {
        cmd_str.push_str(" --no-acl");
    }
    println!("{}", cmd_str);

    let output_result = cmd.output()?;

    if output_result.status.success() {
        println!("✓ Backup created successfully: {}", output);
        println!("  Format: {}", format);
        if let Some(level) = compress {
            println!("  Compression: level {}", level);
        }
        if no_owner {
            println!("  No ownership information");
        }
        if no_acl {
            println!("  No ACL information");
        }
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output_result.stderr);
        Err(format!("pg_dump failed:\n{}", stderr).into())
    }
}

/// Restore a database backup using pg_restore or psql
async fn run_restore(
    database: &str,
    input: &str,
    clean: bool,
    create: bool,
    no_owner: bool,
    no_acl: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Restoring database from backup...");

    let conn_info = parse_pg_url(database)?;

    // Check if file exists
    if !Path::new(input).exists() {
        return Err(format!("Backup file not found: {}", input).into());
    }

    // Detect format by trying to read the file
    let is_plain_sql = input.ends_with(".sql") || is_plain_sql_file(input)?;

    let output_result = if is_plain_sql {
        println!("Detected plain SQL format, using psql...");

        // Check if psql exists
        if !command_exists("psql") {
            return Err("psql command not found. Please install PostgreSQL client tools. Example: sudo apt update && sudo apt install postgresql-client-16".into());
        }

        if no_owner || no_acl {
            println!(
                "  Warning: --no-owner and --no-acl flags are not supported for plain SQL format"
            );
        }

        let mut cmd = std::process::Command::new("psql");

        cmd.arg("--host")
            .arg(&conn_info.host)
            .arg("--port")
            .arg(&conn_info.port)
            .arg("--username")
            .arg(&conn_info.user)
            .arg("--dbname")
            .arg(&conn_info.database)
            .arg("--file")
            .arg(input);

        if let Some(ref password) = conn_info.password {
            cmd.env("PGPASSWORD", password);
        }

        println!(
            "Running: psql --host {} --port {} --username {} --dbname {} --file {}",
            conn_info.host, conn_info.port, conn_info.user, conn_info.database, input
        );

        cmd.output()?
    } else {
        println!("Detected custom/directory/tar format, using pg_restore...");

        // Check if pg_restore exists
        if !command_exists("pg_restore") {
            return Err("pg_restore command not found. Please install PostgreSQL client tools. Example: sudo apt update && sudo apt install postgresql-client-16".into());
        }

        let mut cmd = std::process::Command::new("pg_restore");

        cmd.arg("--host")
            .arg(&conn_info.host)
            .arg("--port")
            .arg(&conn_info.port)
            .arg("--username")
            .arg(&conn_info.user)
            .arg("--dbname")
            .arg(&conn_info.database)
            .arg("--verbose");

        if clean {
            cmd.arg("--clean");
        }

        if create {
            cmd.arg("--create");
        }

        if no_owner {
            cmd.arg("--no-owner");
        }

        if no_acl {
            cmd.arg("--no-acl");
        }

        cmd.arg(input);

        if let Some(ref password) = conn_info.password {
            cmd.env("PGPASSWORD", password);
        }

        println!(
            "Running: pg_restore --host {} --port {} --username {} --dbname {} {}{}{}{}{}",
            conn_info.host,
            conn_info.port,
            conn_info.user,
            conn_info.database,
            if clean { "--clean " } else { "" },
            if create { "--create " } else { "" },
            if no_owner { "--no-owner " } else { "" },
            if no_acl { "--no-acl " } else { "" },
            input
        );

        cmd.output()?
    };

    if output_result.status.success() {
        println!("✓ Database restored successfully from: {}", input);
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output_result.stderr);
        let stdout = String::from_utf8_lossy(&output_result.stdout);

        // pg_restore sometimes outputs warnings to stderr even on success
        // Check if there are actual errors or just warnings
        if stderr.contains("ERROR") || stderr.contains("FATAL") {
            Err(format!("Restore failed:\nSTDERR:\n{}\nSTDOUT:\n{}", stderr, stdout).into())
        } else {
            println!("✓ Database restored successfully from: {}", input);
            if !stderr.is_empty() {
                println!("Warnings:\n{}", stderr);
            }
            Ok(())
        }
    }
}

/// Check if a file is plain SQL by reading the first few bytes
fn is_plain_sql_file(path: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let mut file = std::fs::File::open(path)?;
    let mut buffer = [0u8; 8];
    use std::io::Read;
    let bytes_read = file.read(&mut buffer)?;

    if bytes_read == 0 {
        return Ok(true); // Empty file, treat as plain SQL
    }

    // Custom format starts with "PGDMP"
    // Directory format is a directory
    // Tar format starts with tar magic bytes
    // Plain SQL is text-based

    let is_custom = bytes_read >= 5 && &buffer[0..5] == b"PGDMP";
    let is_text = buffer[0..bytes_read]
        .iter()
        .all(|&b| b.is_ascii() || b == b'\n' || b == b'\r' || b == b'\t');

    Ok(!is_custom && is_text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distr::{Alphanumeric, SampleString};

    fn random_string(prefix: &str) -> String {
        let random_suffix = Alphanumeric.sample_string(&mut rand::rng(), 8);
        format!("{}-{}", prefix, random_suffix)
    }

    fn get_database_url() -> String {
        std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://pgsqlmigrate:pgsqlmigrate@pgsql:5432/pgsqlmigrate".to_string()
        })
    }

    #[tokio::test]
    async fn test_backup() -> Result<(), Box<dyn std::error::Error>> {
        let database_url = get_database_url();
        let backup_file = random_string("backup") + ".sql";
        run_backup(&database_url, &backup_file, "custom", Some(9), true, true).await?;
        assert!(
            Path::new(&backup_file).exists(),
            "Backup file was not created"
        );
        fs::remove_file(&backup_file)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_create_migration() -> Result<(), Box<dyn std::error::Error>> {
        let migration_name = random_string("migration_");
        let dir = "migrations";
        let version = get_next_version(Path::new(dir))?;
        let normalized_name = normalize_name(&migration_name);
        create_migration(dir, &migration_name)?;
        let expected_up = format!("{}/{:06}_{}.up.sql", dir, version, normalized_name);
        let expected_down = format!("{}/{:06}_{}.down.sql", dir, version, normalized_name);
        assert!(
            Path::new(&expected_up).exists(),
            "Up migration file was not created"
        );
        assert!(
            Path::new(&expected_down).exists(),
            "Down migration file was not created"
        );
        fs::remove_file(&expected_up)?;
        fs::remove_file(&expected_down)?;
        Ok(())
    }
}
