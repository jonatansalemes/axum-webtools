use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
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
    #[serde(default, serialize_with = "serialize_migrations")]
    pub migrations: HashMap<String, Vec<String>>,
}

/// Parses the leading version number of a migration script filename,
/// e.g. `000012_add_users.up.sql` -> `Some(12)`.
fn version_of(script: &str) -> Option<u32> {
    script.split('_').next()?.parse().ok()
}

/// Emits migrations ordered by version number so the generated YAML is stable
/// across runs and produces clean git diffs. A plain `HashMap` would serialize
/// in randomized hash order and reshuffle the file on every save.
///
/// `load` rejects keys without a version, so `version_of` is `Some` for every
/// key here; sorting on the `Option` keeps that assumption from panicking.
fn serialize_migrations<S>(
    migrations: &HashMap<String, Vec<String>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut entries: Vec<_> = migrations.iter().collect();
    entries.sort_by(|(a, _), (b, _)| version_of(a).cmp(&version_of(b)).then_with(|| a.cmp(b)));

    let mut map = serializer.serialize_map(Some(entries.len()))?;
    for (script, tables) in entries {
        map.serialize_entry(script, tables)?;
    }
    map.end()
}

impl SafeConfig {
    /// Loads the safe-mode config. A missing file is an empty config (first run);
    /// anything unreadable, malformed, or containing a key that is not a versioned
    /// migration filename is an error. Callers save back to the same path, so a
    /// silent default would overwrite every existing acknowledgement.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read '{}': {}", path.display(), e))?;
        let config: Self = serde_yaml::from_str(&content)
            .map_err(|e| format!("Failed to parse '{}': {}", path.display(), e))?;

        let mut unversioned: Vec<&str> = config
            .migrations
            .keys()
            .filter(|script| version_of(script).is_none())
            .map(String::as_str)
            .collect();
        unversioned.sort_unstable();

        if !unversioned.is_empty() {
            return Err(format!(
                "Invalid migration key(s) in '{}': {}. Keys must be migration filenames with a version prefix (e.g. 000001_initial_schema.up.sql). Fix or remove them.",
                path.display(),
                unversioned.join(", ")
            )
            .into());
        }

        Ok(config)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config_of(scripts: &[&str]) -> SafeConfig {
        let mut config = SafeConfig::default();
        for script in scripts {
            config.acknowledge(script, "users");
        }
        config
    }

    fn keys_in_order(yaml: &str) -> Vec<&str> {
        yaml.lines()
            .filter(|l| l.ends_with(".sql:"))
            .map(|l| l.trim().trim_end_matches(':'))
            .collect()
    }

    #[test]
    fn serializes_in_version_order() {
        let scripts = [
            "000010_ten.up.sql",
            "000002_two.up.sql",
            "000001_one.up.sql",
        ];
        let yaml = serde_yaml::to_string(&config_of(&scripts)).unwrap();

        assert_eq!(
            keys_in_order(&yaml),
            [
                "000001_one.up.sql",
                "000002_two.up.sql",
                "000010_ten.up.sql"
            ]
        );
    }

    #[test]
    fn orders_numerically_not_lexicographically() {
        // "9" > "10" as strings, but migration 9 must come first.
        let yaml = serde_yaml::to_string(&config_of(&["10_ten.up.sql", "9_nine.up.sql"])).unwrap();

        assert_eq!(keys_in_order(&yaml), ["9_nine.up.sql", "10_ten.up.sql"]);
    }

    #[test]
    fn output_is_stable_across_independently_built_maps() {
        // HashMap iteration order varies per instance, so rebuilding the same
        // config must still yield byte-identical YAML.
        let forward = config_of(&[
            "000001_one.up.sql",
            "000002_two.up.sql",
            "000003_three.up.sql",
            "000004_four.up.sql",
        ]);
        let reverse = config_of(&[
            "000004_four.up.sql",
            "000003_three.up.sql",
            "000002_two.up.sql",
            "000001_one.up.sql",
        ]);

        assert_eq!(
            serde_yaml::to_string(&forward).unwrap(),
            serde_yaml::to_string(&reverse).unwrap()
        );
    }

    #[test]
    fn round_trips_through_yaml() {
        let yaml = serde_yaml::to_string(&config_of(&["000001_one.up.sql"])).unwrap();
        let parsed: SafeConfig = serde_yaml::from_str(&yaml).unwrap();

        assert!(parsed.is_acknowledged("000001_one.up.sql", "users"));
    }

    fn load_from(contents: &str) -> (tempfile::TempDir, Result<SafeConfig, String>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("safe-mode.yml");
        fs::write(&path, contents).unwrap();
        let result = SafeConfig::load(&path).map_err(|e| e.to_string());
        (dir, result)
    }

    #[test]
    fn missing_file_loads_as_empty_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = SafeConfig::load(&dir.path().join("safe-mode.yml")).unwrap();

        assert!(config.migrations.is_empty());
    }

    #[test]
    fn malformed_file_errors_instead_of_silently_emptying() {
        // Defaulting here would make the next save() overwrite real acknowledgements.
        let (_dir, result) = load_from("migrations: [this is not a map]");

        let err = result.expect_err("malformed yaml must not load as an empty config");
        assert!(err.contains("Failed to parse"), "unexpected error: {err}");
    }

    #[test]
    fn unversioned_key_is_rejected() {
        let (_dir, result) = load_from(
            "migrations:\n  hotfix.up.sql:\n    - users\n  000001_one.up.sql:\n    - orders\n",
        );

        let err = result.expect_err("an unversioned key must fail the load");
        assert!(err.contains("hotfix.up.sql"), "unexpected error: {err}");
        assert!(
            !err.contains("000001_one.up.sql"),
            "valid key must not be reported: {err}"
        );
    }

    #[test]
    fn every_unversioned_key_is_reported_at_once() {
        // One run should surface all offenders, not fail on an arbitrary first.
        let (_dir, result) = load_from("migrations:\n  zebra.up.sql: []\n  aaa.up.sql: []\n");

        let err = result.expect_err("unversioned keys must fail the load");
        assert!(
            err.contains("aaa.up.sql, zebra.up.sql"),
            "expected both keys, sorted: {err}"
        );
    }

    #[test]
    fn versioned_keys_load_cleanly() {
        let (_dir, result) =
            load_from("migrations:\n  9_nine.up.sql:\n    - users\n  000010_ten.up.sql: []\n");

        let config = result.expect("versioned keys must load");
        assert!(config.is_acknowledged("9_nine.up.sql", "users"));
    }
}
