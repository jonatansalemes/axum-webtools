use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

// Maps script filename -> list of acknowledged table names.
// Example safe-mode.yml:
//   migrations:
//     000001_initial_schema.up.sql:
//       - users
//       - orders
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SafeConfig {
    #[serde(default)]
    pub migrations: HashMap<String, Vec<String>>,
}

impl SafeConfig {
    pub fn load(path: &Path) -> Self {
        if path.exists() {
            if let Ok(content) = fs::read_to_string(path) {
                if let Ok(config) = serde_yaml::from_str(&content) {
                    return config;
                }
            }
        }
        Self::default()
    }

    pub fn save(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let content = serde_yaml::to_string(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    pub fn is_acknowledged(&self, script: &str, table: &str) -> bool {
        self.migrations
            .get(script)
            .map(|tables| tables.iter().any(|t| t.eq_ignore_ascii_case(table)))
            .unwrap_or(false)
    }

    pub fn acknowledge(&mut self, script: &str, table: &str) {
        let tables = self.migrations.entry(script.to_string()).or_default();
        if !tables.iter().any(|t| t.eq_ignore_ascii_case(table)) {
            tables.push(table.to_lowercase());
        }
    }

    pub fn remove_migration(&mut self, script: &str) {
        self.migrations.remove(script);
    }
}
