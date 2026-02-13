use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, Row};
use std::fs;
use std::path::Path;
use urlencoding::decode;

#[derive(Parser)]
#[command(name = "pgsql-migrate")]
#[command(about = "A simple PostgreSQL migration tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(name = "up")]
    Up {
        #[arg(short = 'p', long = "path")]
        path: Option<String>,

        #[arg(short = 'd', long = "database")]
        database: Option<String>,

        #[arg(short = 'e', long = "env")]
        env: Option<String>,
    },

    #[command(name = "down")]
    Down {
        #[arg(short = 'p', long = "path")]
        path: Option<String>,

        #[arg(short = 'd', long = "database")]
        database: Option<String>,

        #[arg(short = 'e', long = "env")]
        env: Option<String>,

        #[arg(default_value = "1")]
        count: u32,
    },

    #[command(name = "create")]
    Create {
        #[arg(short = 'd', long = "dir", default_value = "migrations")]
        dir: String,

        #[arg(short = 's', long = "seq")]
        name: String,
    },

    #[command(name = "baseline")]
    Baseline {
        #[arg(short = 'p', long = "path", default_value = "migrations")]
        path: String,

        #[arg(short = 'd', long = "database")]
        database: String,

        #[arg(short = 'v', long = "version")]
        version: u32,
    },

    #[command(name = "redo")]
    Redo {
        #[arg(short = 'p', long = "path", default_value = "migrations")]
        path: String,

        #[arg(short = 'd', long = "database")]
        database: String,

        #[arg(short = 'e', long = "env", default_value = "prod")]
        env: String,
    },
    #[command(name = "backup")]
    Backup {
        #[arg(short = 'd', long = "database")]
        database: String,

        #[arg(short = 'o', long = "output")]
        output: String,

        #[arg(short = 'f', long = "format", default_value = "custom")]
        format: String,

        #[arg(short = 'c', long = "compress")]
        compress: Option<u8>,

        #[arg(short = 'j', long = "jobs")]
        jobs: Option<u8>,

        #[arg(long = "no-owner")]
        no_owner: bool,

        #[arg(long = "no-acl")]
        no_acl: bool,
    },

    #[command(name = "restore")]
    Restore {
        #[arg(short = 'd', long = "database")]
        database: String,

        #[arg(short = 'i', long = "input")]
        input: String,

        #[arg(long = "clean")]
        clean: bool,

        #[arg(long = "create")]
        create: bool,

        #[arg(long = "no-owner")]
        no_owner: bool,

        #[arg(long = "no-acl")]
        no_acl: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationFeature {
    NoTransaction,
    SplitStatements,
}

impl MigrationFeature {
    /// Parses a string into a MigrationFeature variant.
    ///
    /// # Arguments
    /// * `s` - String representation of the feature ("no-tx" or "split-statements")
    ///
    /// # Returns
    /// * `Some(MigrationFeature)` if the string matches a valid feature
    /// * `None` if the string doesn't match any known feature
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "no-tx" => Some(MigrationFeature::NoTransaction),
            "split-statements" => Some(MigrationFeature::SplitStatements),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MigrationSpec {
    pub content: String,
    pub features: Vec<MigrationFeature>,
}

impl MigrationSpec {
    /// Creates a new MigrationSpec by parsing content and extracting features.
    ///
    /// # Arguments
    /// * `content` - The SQL migration content to parse
    pub fn new(content: String) -> Self {
        let features = Self::parse_features(&content);
        Self { content, features }
    }

    /// Creates an empty MigrationSpec with no content or features.
    pub fn empty() -> Self {
        Self {
            content: String::new(),
            features: Vec::new(),
        }
    }

    /// Checks if the no-transaction feature is enabled.
    ///
    /// # Returns
    /// * `true` if the NoTransaction feature is present
    pub fn has_no_tx(&self) -> bool {
        self.features.contains(&MigrationFeature::NoTransaction)
    }

    /// Checks if the split-statements feature is enabled.
    ///
    /// # Returns
    /// * `true` if the SplitStatements feature is present
    pub fn has_split_statements(&self) -> bool {
        self.features.contains(&MigrationFeature::SplitStatements)
    }

    /// Checks if the migration content is empty.
    ///
    /// # Returns
    /// * `true` if content string is empty
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    fn parse_features(content: &str) -> Vec<MigrationFeature> {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("-- features:") {
                let features_str = trimmed.trim_start_matches("-- features:").trim();
                return features_str
                    .split(',')
                    .filter_map(MigrationFeature::parse)
                    .collect();
            }
            if !trimmed.is_empty() && !trimmed.starts_with("--") {
                break;
            }
        }
        Vec::new()
    }
}

pub struct Migration {
    pub version: u32,
    pub filename: String,
    pub up: MigrationSpec,
    pub down: MigrationSpec,
}

/// Entry point for the pgsql-migrate CLI tool.
///
/// Parses command-line arguments and dispatches to the appropriate subcommand handler.
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Up {
            path,
            database,
            env,
        } => {
            let resolved_path =
                resolve_config_value(path, "MIGRATIONS_DIR", Some("migrations"), "path")?;

            let resolved_database =
                resolve_config_value(database, "DATABASE_URL", None, "database")?;

            let resolved_env = resolve_config_value(env, "ENV", Some("prod"), "env")?;

            println!("Running migrations with:");
            println!("  Path:     {}", resolved_path);
            println!("  Database: {}", mask_database_url(&resolved_database));
            println!("  Env:      {}", resolved_env);
            println!();

            run_up(&resolved_path, &resolved_database, &resolved_env).await?;
        }
        Commands::Down {
            path,
            database,
            env,
            count,
        } => {
            let resolved_path =
                resolve_config_value(path, "MIGRATIONS_DIR", Some("migrations"), "path")?;

            let resolved_database =
                resolve_config_value(database, "DATABASE_URL", None, "database")?;

            let resolved_env = resolve_config_value(env, "ENV", Some("prod"), "env")?;

            println!("Rolling back migrations with:");
            println!("  Path:     {}", resolved_path);
            println!("  Database: {}", mask_database_url(&resolved_database));
            println!("  Env:      {}", resolved_env);
            println!("  Count:    {}", count);
            println!();

            run_down(&resolved_path, &resolved_database, &resolved_env, count).await?;
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
            jobs,
            no_owner,
            no_acl,
        } => {
            run_backup(
                &database, &output, &format, compress, jobs, no_owner, no_acl,
            )
            .await?;
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

/// Resolves a configuration value with priority: CLI argument > environment variable > default.
///
/// # Arguments
/// * `cli_value` - Optional value provided via CLI argument
/// * `env_var_name` - Name of the environment variable to check
/// * `default_value` - Optional default value if both CLI and env var are missing
/// * `config_name` - Human-readable name of the configuration for error messages
///
/// # Returns
/// * The resolved value or an error if no value could be determined
fn resolve_config_value(
    cli_value: Option<String>,
    env_var_name: &str,
    default_value: Option<&str>,
    config_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(value) = cli_value {
        if !value.trim().is_empty() {
            return Ok(value);
        }
    }

    if let Ok(value) = std::env::var(env_var_name) {
        if !value.trim().is_empty() {
            return Ok(value);
        }
    }

    if let Some(default) = default_value {
        return Ok(default.to_string());
    }

    Err(format!(
        "Missing required configuration '{}'. Provide it via:\n  \
         - CLI argument: --{}\n  \
         - Environment variable: {}",
        config_name, config_name, env_var_name
    )
    .into())
}

/// Masks sensitive parts of a database URL for safe display.
///
/// # Arguments
/// * `url` - The database connection URL
///
/// # Returns
/// * A masked version of the URL with password hidden
fn mask_database_url(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(proto_end) = url.find("://") {
            let after_proto = &url[proto_end + 3..at_pos];
            if let Some(colon_pos) = after_proto.find(':') {
                let user = &after_proto[..colon_pos];
                let before_auth = &url[..proto_end + 3];
                let after_auth = &url[at_pos..];
                return format!("{}{}:****{}", before_auth, user, after_auth);
            }
        }
    }
    url.to_string()
}

/// Normalizes a migration name by converting to lowercase and replacing invalid characters.
///
/// # Arguments
/// * `name` - The migration name to normalize
///
/// # Returns
/// * A normalized string containing only lowercase alphanumeric characters and underscores
pub fn normalize_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c == ' ' { '_' } else { c })
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// Determines the next migration version number by scanning existing migrations.
///
/// # Arguments
/// * `dir` - Path to the migrations directory
///
/// # Returns
/// * The next sequential version number (highest existing version + 1)
pub fn get_next_version(dir: &Path) -> Result<u32, Box<dyn std::error::Error>> {
    if !dir.exists() {
        return Ok(1);
    }

    let mut max_version: u32 = 0;

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();

        if let Some(version_str) = name.split('_').next() {
            if let Ok(version) = version_str.parse::<u32>() {
                max_version = max_version.max(version);
            }
        }
    }

    Ok(max_version + 1)
}

/// Creates a new migration pair (up and down files) in the specified directory.
///
/// # Arguments
/// * `dir` - Directory path where migration files will be created
/// * `name` - Name of the migration (will be normalized)
///
/// # Returns
/// * `Ok(())` on success, or an error if file creation fails
pub fn create_migration(dir: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let dir_path = Path::new(dir);

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

/// Computes a SHA-256 hash of the given content.
///
/// # Arguments
/// * `content` - The content to hash
///
/// # Returns
/// * A hexadecimal string representation of the SHA-256 hash
pub fn compute_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Clone)]
pub struct SqlBlock {
    pub content: String,
    pub skip_on_env: Vec<String>,
}

impl SqlBlock {
    /// Determines if this SQL block should be skipped for the given environment.
    ///
    /// # Arguments
    /// * `current_env` - The current environment name to check against
    ///
    /// # Returns
    /// * `true` if the block should be skipped for the current environment
    pub fn should_skip(&self, current_env: &str) -> bool {
        self.skip_on_env
            .iter()
            .any(|e| e.eq_ignore_ascii_case(current_env))
    }
}

/// Splits SQL content into blocks delimited by split-start and split-end markers.
///
/// # Arguments
/// * `content` - The SQL content to split
///
/// # Returns
/// * A vector of SqlBlock instances, or an error if markers are mismatched
pub fn split_sql_by_markers(content: &str) -> Result<Vec<SqlBlock>, String> {
    let mut blocks = Vec::new();
    let mut current_block = String::new();
    let mut current_skip_envs: Vec<String> = Vec::new();
    let mut in_block = false;
    let mut block_start_line = 0;

    for (line_num, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        let line_number = line_num + 1;

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
            if trimmed.starts_with("-- skip-on-env") {
                let envs_str = trimmed.trim_start_matches("-- skip-on-env").trim();
                current_skip_envs = envs_str
                    .split(',')
                    .map(|e| e.trim().to_lowercase())
                    .filter(|e| !e.is_empty())
                    .collect();
                continue;
            }

            if !current_block.is_empty() {
                current_block.push('\n');
            }
            current_block.push_str(line);
        }
    }

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

/// Runs pending up migrations against the database.
///
/// # Arguments
/// * `path` - Path to the migrations directory
/// * `database` - Database connection URL
/// * `env` - Environment name for conditional migration execution
///
/// # Returns
/// * `Ok(())` if all migrations applied successfully
pub async fn run_up(
    path: &str,
    database: &str,
    env: &str,
) -> Result<(), Box<dyn std::error::Error>> {
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
            let mut tx = pool.begin().await?;
            match tx.execute(migration.up.content.as_str()).await {
                Ok(_) => {
                    tx.commit().await?;
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        } else {
            pool.execute(migration.up.content.as_str())
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
///
/// # Returns
/// * `Ok(())` if all rollbacks completed successfully
pub async fn run_down(
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
                let mut tx = pool.begin().await?;
                match tx.execute(migration.down.content.as_str()).await {
                    Ok(_) => {
                        tx.commit().await?;
                        Ok(())
                    }
                    Err(e) => Err(e.into()),
                }
            } else {
                pool.execute(migration.down.content.as_str())
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
///
/// # Returns
/// * `Ok(())` if redo completed successfully
pub async fn run_redo(
    path: &str,
    database: &str,
    env: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database)
        .await?;

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

    run_up(path, database, env).await?;

    Ok(())
}

/// Parses a PostgreSQL connection URL into its components.
///
/// # Arguments
/// * `url` - PostgreSQL connection URL (postgres:// or postgresql://)
///
/// # Returns
/// * A PgConnectionInfo struct with parsed connection details
pub fn parse_pg_url(url: &str) -> Result<PgConnectionInfo, Box<dyn std::error::Error>> {
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
            Some(
                decode(u)
                    .map_err(|e| format!("Invalid UTF-8 in username after URL decoding: {}", e))?
                    .into_owned(),
            ),
            if p.is_empty() {
                None
            } else {
                Some(
                    decode(p)
                        .map_err(|e| {
                            format!("Invalid UTF-8 in password after URL decoding: {}", e)
                        })?
                        .into_owned(),
                )
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
            decode(db_name)
                .map_err(|e| format!("Invalid UTF-8 in database name after URL decoding: {}", e))?
                .into_owned()
        },
    })
}

#[derive(Debug)]
pub struct PgConnectionInfo {
    pub host: String,
    pub port: String,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

const DEFAULT_PG_VERSION: u32 = 15;

/// Checks if a command exists in the system PATH.
///
/// # Arguments
/// * `cmd` - Command name to check
///
/// # Returns
/// * `true` if the command exists, `false` otherwise
pub fn command_exists(cmd: &str) -> bool {
    std::process::Command::new("which")
        .arg(cmd)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Retrieves the major version number of the installed pg_dump tool.
///
/// # Returns
/// * The major version number (e.g., 15 for PostgreSQL 15.x)
pub fn get_pg_dump_version() -> Result<u32, Box<dyn std::error::Error>> {
    let output = std::process::Command::new("pg_dump")
        .arg("--version")
        .output()?;

    let version_str = String::from_utf8(output.stdout)?;

    for token in version_str.split_whitespace() {
        if token.chars().next().is_some_and(|c| c.is_ascii_digit()) && token.contains('.') {
            if let Some(major_version_str) = token.split('.').next() {
                if let Ok(version) = major_version_str.parse::<u32>() {
                    if (9..=99).contains(&version) {
                        return Ok(version);
                    }
                }
            }
        }
    }

    Err(format!(
        "Could not parse pg_dump version from output: {}",
        version_str.trim()
    )
    .into())
}

/// Creates a database backup using pg_dump.
///
/// # Arguments
/// * `database` - Database connection URL
/// * `output` - Output file path for the backup
/// * `format` - Backup format (plain, custom, directory, or tar)
/// * `compress` - Optional compression level (0-9)
/// * `jobs` - Optional number of parallel jobs for backup (directory format only)
/// * `no_owner` - Exclude ownership information
/// * `no_acl` - Exclude ACL information
///
/// # Returns
/// * `Ok(())` if backup completed successfully
pub async fn run_backup(
    database: &str,
    output: &str,
    format: &str,
    compress: Option<u8>,
    jobs: Option<u8>,
    no_owner: bool,
    no_acl: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !command_exists("pg_dump") {
        return Err("pg_dump command not found. Please install PostgreSQL client tools. Example: sudo apt update && sudo apt install postgresql-client-16".into());
    }

    println!("Creating backup of database...");

    let conn_info = parse_pg_url(database)?;

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

    if let Some(level) = compress {
        if level > 9 {
            return Err("Compression level must be between 0 and 9".into());
        }
        if format_flag == "p" {
            return Err("Compression is not supported for plain format".into());
        }
    }

    if let Some(num_jobs) = jobs {
        if num_jobs == 0 {
            return Err("Number of jobs must be at least 1".into());
        }
        if format_flag != "d" {
            return Err("Parallel backup (--jobs) is only supported for directory format. Use --format directory".into());
        }
    }

    let mut cmd = tokio::process::Command::new("pg_dump");

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
        let pg_version = get_pg_dump_version().unwrap_or_else(|e| {
            eprintln!("Warning: Could not detect the PostgreSQL version ({}). Defaulting to PostgreSQL {}.", e, DEFAULT_PG_VERSION);
            DEFAULT_PG_VERSION
        });

        if pg_version >= 16 {
            cmd.arg(format!("--compress=gzip:{}", level));
        } else {
            cmd.arg("--compress").arg(level.to_string());
        }
    }

    if let Some(num_jobs) = jobs {
        cmd.arg("--jobs").arg(num_jobs.to_string());
    }

    if no_owner {
        cmd.arg("--no-owner");
    }

    if no_acl {
        cmd.arg("--no-acl");
    }

    cmd.arg(&conn_info.database);

    if let Some(ref password) = conn_info.password {
        cmd.env("PGPASSWORD", password);
    }

    let mut cmd_str = format!(
        "Running: pg_dump --host {} --port {} --username {} --dbname {} --format {} --file {}",
        conn_info.host, conn_info.port, conn_info.user, conn_info.database, format, output
    );
    if let Some(level) = compress {
        cmd_str.push_str(&format!(" --compress {}", level));
    }
    if let Some(num_jobs) = jobs {
        cmd_str.push_str(&format!(" --jobs {}", num_jobs));
    }
    if no_owner {
        cmd_str.push_str(" --no-owner");
    }
    if no_acl {
        cmd_str.push_str(" --no-acl");
    }
    println!("{}", cmd_str);

    let output_result = cmd.output().await?;

    if output_result.status.success() {
        println!("✓ Backup created successfully: {}", output);
        println!("  Format: {}", format);
        if let Some(level) = compress {
            println!("  Compression: level {}", level);
        }
        if let Some(num_jobs) = jobs {
            println!("  Parallel jobs: {}", num_jobs);
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

/// Restores a database from a backup file using pg_restore or psql.
///
/// # Arguments
/// * `database` - Database connection URL
/// * `input` - Input backup file path
/// * `clean` - Drop database objects before recreating them
/// * `create` - Create the database before restoring
/// * `no_owner` - Skip restoration of ownership
/// * `no_acl` - Skip restoration of access privileges
///
/// # Returns
/// * `Ok(())` if restore completed successfully
pub async fn run_restore(
    database: &str,
    input: &str,
    clean: bool,
    create: bool,
    no_owner: bool,
    no_acl: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Restoring database from backup...");

    let conn_info = parse_pg_url(database)?;

    if !Path::new(input).exists() {
        return Err(format!("Backup file not found: {}", input).into());
    }

    let is_plain_sql = input.ends_with(".sql") || is_plain_sql_file(input)?;

    let output_result = if is_plain_sql {
        println!("Detected plain SQL format, using psql...");

        if !command_exists("psql") {
            return Err("psql command not found. Please install PostgreSQL client tools. Example: sudo apt update && sudo apt install postgresql-client-16".into());
        }

        if no_owner || no_acl {
            println!(
                "  Warning: --no-owner and --no-acl flags are not supported for plain SQL format"
            );
        }

        let mut cmd = tokio::process::Command::new("psql");

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

        cmd.output().await?
    } else {
        println!("Detected custom/directory/tar format, using pg_restore...");

        if !command_exists("pg_restore") {
            return Err("pg_restore command not found. Please install PostgreSQL client tools. Example: sudo apt update && sudo apt install postgresql-client-16".into());
        }

        let mut cmd = tokio::process::Command::new("pg_restore");

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

        cmd.output().await?
    };

    if output_result.status.success() {
        println!("✓ Database restored successfully from: {}", input);
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output_result.stderr);
        let stdout = String::from_utf8_lossy(&output_result.stdout);

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

/// Checks if a file is plain SQL by reading the first few bytes.
///
/// # Arguments
/// * `path` - Path to the file to check
///
/// # Returns
/// * `true` if the file appears to be plain SQL, `false` otherwise
pub fn is_plain_sql_file(path: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let mut file = std::fs::File::open(path)?;
    let mut buffer = [0u8; 8];
    use std::io::Read;
    let bytes_read = file.read(&mut buffer)?;

    if bytes_read == 0 {
        return Ok(true);
    }

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
        let backup_file = random_string("backup") + ".backup";
        run_backup(
            &database_url,
            &backup_file,
            "custom",
            Some(9),
            None,
            true,
            true,
        )
        .await?;
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

    #[test]
    fn test_parse_pg_url_basic() -> Result<(), Box<dyn std::error::Error>> {
        let url = "postgresql://user:pass@localhost:5432/mydb";
        let info = parse_pg_url(url)?;
        assert_eq!(info.user, "user");
        assert_eq!(info.password, Some("pass".to_string()));
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, "5432");
        assert_eq!(info.database, "mydb");
        Ok(())
    }

    #[test]
    fn test_parse_pg_url_encoded_password() -> Result<(), Box<dyn std::error::Error>> {
        let url = "postgresql://user:p%40ss@localhost:5432/mydb";
        let info = parse_pg_url(url)?;
        assert_eq!(info.user, "user");
        assert_eq!(info.password, Some("p@ss".to_string()));
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, "5432");
        assert_eq!(info.database, "mydb");
        Ok(())
    }

    #[test]
    fn test_parse_pg_url_encoded_username() -> Result<(), Box<dyn std::error::Error>> {
        let url = "postgresql://us%40er:pass@localhost:5432/mydb";
        let info = parse_pg_url(url)?;
        assert_eq!(info.user, "us@er");
        assert_eq!(info.password, Some("pass".to_string()));
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, "5432");
        assert_eq!(info.database, "mydb");
        Ok(())
    }

    #[test]
    fn test_parse_pg_url_special_chars() -> Result<(), Box<dyn std::error::Error>> {
        let url = "postgresql://user:p%40ss%3Aword%2Ftest%3Fquery@localhost:5432/mydb";
        let info = parse_pg_url(url)?;
        assert_eq!(info.user, "user");
        assert_eq!(info.password, Some("p@ss:word/test?query".to_string()));
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, "5432");
        assert_eq!(info.database, "mydb");
        Ok(())
    }

    #[test]
    fn test_parse_pg_url_defaults() -> Result<(), Box<dyn std::error::Error>> {
        let url = "postgresql://localhost/mydb";
        let info = parse_pg_url(url)?;
        assert_eq!(info.user, "postgres");
        assert_eq!(info.password, None);
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, "5432");
        assert_eq!(info.database, "mydb");
        Ok(())
    }

    #[test]
    fn test_parse_pg_url_encoded_database() -> Result<(), Box<dyn std::error::Error>> {
        let url = "postgresql://user:pass@localhost:5432/my%2Ddb";
        let info = parse_pg_url(url)?;
        assert_eq!(info.user, "user");
        assert_eq!(info.password, Some("pass".to_string()));
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, "5432");
        assert_eq!(info.database, "my-db");
        Ok(())
    }
}
