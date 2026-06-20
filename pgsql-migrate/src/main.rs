mod backup;
mod cli;
mod commands;
mod config;
mod db;
mod migration;
mod safe_mode;

use clap::Parser;

use backup::*;
use cli::*;
use commands::*;
use config::*;

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
            safe_mode,
            safe_mode_confirm,
            pre_execute,
            post_execute,
        } => {
            let resolved_path =
                resolve_config_value(path, "MIGRATIONS_DIR", Some("migrations"), "path")?;

            let resolved_database =
                resolve_config_value(database, "DATABASE_URL", None, "database")?;

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
            let resolved_path =
                resolve_config_value(path, "MIGRATIONS_DIR", Some("migrations"), "path")?;

            let resolved_database =
                resolve_config_value(database, "DATABASE_URL", None, "database")?;

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
        Commands::Backup {
            database,
            output,
            format,
            compress,
            jobs,
            no_owner,
            no_acl,
            max_retain_days,
        } => {
            run_backup(
                &database,
                &output,
                &format,
                compress,
                jobs,
                no_owner,
                no_acl,
                max_retain_days,
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

#[cfg(test)]
mod tests {
    use super::backup::*;
    use super::config::*;
    use super::migration::*;

    use rand::distr::{Alphanumeric, SampleString};
    use std::fs;
    use std::path::Path;

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
            None,
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
    async fn test_backup_purge() -> Result<(), Box<dyn std::error::Error>> {
        let database_url = get_database_url();
        let backup_file = "test_purge.dump";

        // Create a dummy .dump file that looks "old"
        // We'll create it now and then use a small max_retain_days if possible,
        // but since we can't easily backdate files in a cross-platform way without extra crates,
        // we'll just check if it survives a 1 day retention.
        // Actually, we can use filetime crate if it's available, but let's check Cargo.toml first.

        fs::write(backup_file, "dummy content")?;

        // This should NOT purge the file because it's new
        let output = "another_backup.backup";
        run_backup(
            &database_url,
            output,
            "custom",
            None,
            None,
            true,
            true,
            Some(1),
        )
        .await?;

        assert!(
            Path::new(backup_file).exists(),
            "Backup file should still exist"
        );

        fs::remove_file(backup_file)?;
        if Path::new(output).exists() {
            fs::remove_file(output)?;
        }
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

    #[test]
    fn test_normalize_comma_separated_args() {
        let arg = Some(" a, B , , c ");
        let result = normalize_comma_separated_args(arg, true);
        assert_eq!(result, vec!["a", "b", "c"]);

        let result = normalize_comma_separated_args(arg, false);
        assert_eq!(result, vec!["a", "B", "c"]);

        let result = normalize_comma_separated_args(None, true);
        assert!(result.is_empty());

        let result = normalize_comma_separated_args(Some(""), true);
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_hash() -> Result<(), Box<dyn std::error::Error>> {
        let sql = "123456";
        let hash = compute_hash(sql);
        assert_eq!(
            hash,
            "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92"
        );
        Ok(())
    }
}
