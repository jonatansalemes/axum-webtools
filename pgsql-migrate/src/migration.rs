use sha2::{Digest, Sha256};

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

/// Computes a SHA-256 hash of the given content.
///
/// # Arguments
/// * `content` - The content to hash
///
/// # Returns
/// * A hexadecimal string representation of the SHA-256 hash
pub fn compute_hash(content: &str) -> String {
    let bytes = Sha256::digest(content.as_bytes());
    hex::encode(bytes)
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
