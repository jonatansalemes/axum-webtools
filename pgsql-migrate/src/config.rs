use std::fs;
use std::path::Path;

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
pub fn resolve_config_value(
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
pub fn mask_database_url(url: &str) -> String {
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
