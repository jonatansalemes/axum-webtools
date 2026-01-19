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
        Commands::Up { path, database } => {
            run_up(&path, &database).await?;
        }
        Commands::Down {
            path,
            database,
            count,
        } => {
            run_down(&path, &database, count).await?;
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

/// Split SQL content into blocks using -- split-start and -- split-end markers
/// Returns an error if markers are mismatched (start without end, end without start, or unclosed block)
fn split_sql_by_markers(content: &str) -> Result<Vec<String>, String> {
    let mut blocks = Vec::new();
    let mut current_block = String::new();
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
                blocks.push(block_content);
            }
            in_block = false;
            current_block.clear();
            continue;
        }

        if in_block {
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
async fn run_up(path: &str, database: &str) -> Result<(), Box<dyn std::error::Error>> {
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
                        if use_transaction {
                            // Each block in its own transaction
                            let mut tx = pool.begin().await?;
                            match tx.execute(block.as_str()).await {
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
                            match pool.execute(block.as_str()).await {
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

    Ok(())
}

/// Run migrations down
async fn run_down(
    path: &str,
    database: &str,
    count: u32,
) -> Result<(), Box<dyn std::error::Error>> {
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
                            if use_transaction {
                                // Each block in its own transaction
                                let mut tx = pool.begin().await?;
                                match tx.execute(block.as_str()).await {
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
                                match pool.execute(block.as_str()).await {
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
