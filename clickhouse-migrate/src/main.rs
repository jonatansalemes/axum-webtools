mod cli;
mod client;
mod commands;
mod config;
mod db;
mod migration;
mod safe_mode;
mod sql;

use clap::Parser;

use cli::*;
use commands::*;
use config::*;

/// Environment variable holding the ClickHouse URL. Deliberately not
/// `DATABASE_URL`, which applications usually point at their OLTP database.
const DATABASE_ENV_VAR: &str = "CLICKHOUSE_URL";

/// Entry point for the clickhouse-migrate CLI tool.
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
            safe_mode,
            safe_mode_confirm,
            pre_execute,
            post_execute,
        } => {
            let resolved_path = resolve_config_value(
                path,
                "CLICKHOUSE_MIGRATIONS_DIR",
                Some("ch-migrations"),
                "path",
            )?;

            let resolved_database =
                resolve_config_value(database, DATABASE_ENV_VAR, None, "database")?;

            let resolved_env = resolve_config_value(env, "ENV", Some("prod"), "env")?;

            let safe_mode_tables = normalize_comma_separated_args(safe_mode.as_deref(), true);

            let pre_execute_hooks = normalize_comma_separated_args(pre_execute.as_deref(), false);

            let post_execute_hooks = normalize_comma_separated_args(post_execute.as_deref(), false);

            println!("Running migrations with:");
            println!("  Path:     {}", resolved_path);
            println!("  Database: {}", mask_database_url(&resolved_database));
            println!("  Env:      {}", resolved_env);
            if !safe_mode_tables.is_empty() {
                println!("  Safe mode tables: {}", safe_mode_tables.join(", "));
            }
            println!();

            run_up(
                &resolved_path,
                &resolved_database,
                &resolved_env,
                &safe_mode_tables,
                &safe_mode_confirm,
                &pre_execute_hooks,
                &post_execute_hooks,
            )
            .await?;
        }
        Commands::Down {
            path,
            database,
            env,
            count,
            safe_mode_skip_auto_remove,
            pre_execute,
            post_execute,
        } => {
            let resolved_path = resolve_config_value(
                path,
                "CLICKHOUSE_MIGRATIONS_DIR",
                Some("ch-migrations"),
                "path",
            )?;

            let resolved_database =
                resolve_config_value(database, DATABASE_ENV_VAR, None, "database")?;

            let resolved_env = resolve_config_value(env, "ENV", Some("prod"), "env")?;

            let pre_execute_hooks = normalize_comma_separated_args(pre_execute.as_deref(), false);

            let post_execute_hooks = normalize_comma_separated_args(post_execute.as_deref(), false);

            println!("Rolling back migrations with:");
            println!("  Path:     {}", resolved_path);
            println!("  Database: {}", mask_database_url(&resolved_database));
            println!("  Env:      {}", resolved_env);
            println!("  Count:    {}", count);
            println!();

            run_down(
                &resolved_path,
                &resolved_database,
                &resolved_env,
                count,
                safe_mode_skip_auto_remove,
                &pre_execute_hooks,
                &post_execute_hooks,
            )
            .await?;
        }
        Commands::Status {
            path,
            database,
            env,
        } => {
            let resolved_path = resolve_config_value(
                path,
                "CLICKHOUSE_MIGRATIONS_DIR",
                Some("ch-migrations"),
                "path",
            )?;

            let resolved_database =
                resolve_config_value(database, DATABASE_ENV_VAR, None, "database")?;

            let resolved_env = resolve_config_value(env, "ENV", Some("prod"), "env")?;

            // Distinct exit codes let callers gate on status without parsing stdout:
            //   0 = up to date, 1 = pending, 2 = dirty/unreachable.
            let code = match run_status(&resolved_path, &resolved_database, &resolved_env).await {
                Ok(code) => code,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    2
                }
            };
            std::process::exit(code);
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
            safe_mode,
            safe_mode_confirm,
            pre_execute,
            post_execute,
        } => {
            let safe_mode_tables = normalize_comma_separated_args(safe_mode.as_deref(), true);

            let pre_execute_hooks = normalize_comma_separated_args(pre_execute.as_deref(), false);

            let post_execute_hooks = normalize_comma_separated_args(post_execute.as_deref(), false);

            run_redo(
                &path,
                &database,
                &env,
                &safe_mode_tables,
                &safe_mode_confirm,
                &pre_execute_hooks,
                &post_execute_hooks,
            )
            .await?;
        }
        Commands::Force {
            path,
            database,
            version,
        } => {
            run_force(&path, &database, version).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::config::*;
    use super::migration::*;

    use rand::distr::{Alphanumeric, SampleString};
    use std::fs;
    use std::path::Path;

    fn random_string(prefix: &str) -> String {
        let random_suffix = Alphanumeric.sample_string(&mut rand::rng(), 8);
        format!("{}-{}", prefix, random_suffix)
    }

    #[test]
    fn test_create_migration() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let dir = temp_dir.path().join("migrations");
        let dir_str = dir.to_str().unwrap();
        let migration_name = random_string("migration_");
        let normalized_name = normalize_name(&migration_name);

        create_migration(dir_str, &migration_name)?;
        create_migration(dir_str, &migration_name)?;

        for version in [1, 2] {
            let up = format!("{}/{:06}_{}.up.sql", dir_str, version, normalized_name);
            let down = format!("{}/{:06}_{}.down.sql", dir_str, version, normalized_name);
            assert!(Path::new(&up).exists(), "Up migration file was not created");
            assert!(
                Path::new(&down).exists(),
                "Down migration file was not created"
            );
        }
        assert_eq!(fs::read_dir(&dir)?.count(), 4);
        Ok(())
    }

    #[test]
    fn test_mask_database_url() {
        assert_eq!(
            mask_database_url("http://user:secret@localhost:8123/db"),
            "http://user:****@localhost:8123/db"
        );
        assert_eq!(
            mask_database_url("http://localhost:8123/db"),
            "http://localhost:8123/db"
        );
    }

    #[test]
    fn test_normalize_comma_separated_args() {
        let arg = Some(" a, B , , c ");
        let result = normalize_comma_separated_args(arg, true);
        assert_eq!(result, vec!["a", "b", "c"]);

        let result = normalize_comma_separated_args(arg, false);
        assert_eq!(result, vec!["a", "B", "c"]);

        let result = normalize_comma_separated_args(None, true);
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_features() {
        let spec = MigrationSpec::new("-- features: split-statements, unknown\nSELECT 1".into());
        assert!(spec.has_split_statements());
        assert_eq!(spec.features, vec![MigrationFeature::SplitStatements]);
    }

    #[test]
    fn test_compute_hash() {
        assert_eq!(
            compute_hash("123456"),
            "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92"
        );
    }
}
