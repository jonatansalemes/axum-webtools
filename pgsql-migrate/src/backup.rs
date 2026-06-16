use std::path::Path;
use urlencoding::decode;

#[derive(Debug)]
pub struct PgConnectionInfo {
    pub host: String,
    pub port: String,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

const DEFAULT_PG_VERSION: u32 = 15;

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
/// * `compress` - Optional compression level (1-9)
/// * `jobs` - Optional number of parallel jobs for backup (directory format only)
/// * `no_owner` - Exclude ownership information
/// * `no_acl` - Exclude ACL information
///
/// # Returns
/// * `Ok(())` if backup completed successfully
#[allow(clippy::too_many_arguments)]
pub async fn run_backup(
    database: &str,
    output: &str,
    format: &str,
    compress: Option<u8>,
    jobs: Option<u8>,
    no_owner: bool,
    no_acl: bool,
    max_retain_days: Option<u64>,
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
        if !(1..=9).contains(&level) {
            return Err("Compression level must be between 1 and 9".into());
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

        if let Some(days) = max_retain_days {
            purge_old_backups(output, days)?;
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

fn purge_old_backups(
    output_path: &str,
    max_retain_days: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(output_path);
    let parent_dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };

    println!(
        "Scanning for old backups in {} (max retain days: {})...",
        parent_dir.display(),
        max_retain_days
    );

    let now = std::time::SystemTime::now();
    let max_duration = std::time::Duration::from_secs(max_retain_days * 24 * 60 * 60);

    for entry in std::fs::read_dir(parent_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() && path.extension().is_some_and(|ext| ext == "dump") {
            let metadata = entry.metadata()?;

            let modified = metadata.modified()?;
            let created = metadata.created().unwrap_or(modified);

            let created_elapsed = now
                .duration_since(created)
                .unwrap_or(std::time::Duration::ZERO);

            if created_elapsed > max_duration {
                println!("Purging old backup: {}", path.display());
                std::fs::remove_file(path)?;
            }
        }
    }

    Ok(())
}
