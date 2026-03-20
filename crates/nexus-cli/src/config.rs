//! Configuration file support for the CLI.
//!
//! Loads and saves CLI configuration from TOML files.

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// CLI configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    /// Server hostname.
    #[serde(default = "default_host")]
    pub host: String,

    /// Server port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Database name.
    #[serde(default)]
    pub database: Option<String>,

    /// Username.
    #[serde(default)]
    pub username: Option<String>,

    /// Password (stored only in config file, not logged).
    #[serde(default, skip_serializing)]
    pub password: Option<String>,

    /// Default output format.
    #[serde(default = "default_format")]
    pub output_format: String,

    /// Enable timing by default.
    #[serde(default)]
    pub timing: bool,

    /// History file path.
    #[serde(default)]
    pub history_file: Option<PathBuf>,

    /// Maximum history size.
    #[serde(default = "default_history_size")]
    pub history_size: usize,

    /// Editor to use for \e command.
    #[serde(default)]
    pub editor: Option<String>,

    /// Pager to use for long output.
    #[serde(default)]
    pub pager: Option<String>,

    /// Whether to use mock mode (for testing).
    #[serde(skip)]
    pub mock_mode: bool,
}

fn default_host() -> String {
    "localhost".to_string()
}

fn default_port() -> u16 {
    5432
}

fn default_format() -> String {
    "table".to_string()
}

fn default_history_size() -> usize {
    1000
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            database: None,
            username: None,
            password: None,
            output_format: default_format(),
            timing: false,
            history_file: None,
            history_size: default_history_size(),
            editor: None,
            pager: None,
            mock_mode: false,
        }
    }
}

/// Display-safe connection information. Excludes all sensitive fields
/// (password, username) to satisfy CodeQL's `rust/cleartext-logging` rule.
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub database: String,
}

impl std::fmt::Display for ConnectionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{} (database: {})", self.host, self.port, self.database)
    }
}

impl CliConfig {
    /// Creates a new default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Extracts display-safe connection info, excluding sensitive fields.
    /// The returned struct never contained a password, breaking any
    /// taint chain that static analysis tools might track.
    pub fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone().unwrap_or_else(|| "nexusdb".into()),
        }
    }

    /// Loads configuration from a file (async, non-blocking).
    pub async fn from_file(path: &Path) -> Result<Self> {
        let content = tokio::fs::read_to_string(path).await?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// Saves configuration to a file (async, non-blocking).
    pub async fn save(&self, path: &Path) -> Result<()> {
        let content = toml::to_string_pretty(self)?;

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(path, content).await?;
        Ok(())
    }

    /// Loads the default configuration file (async, non-blocking).
    ///
    /// Looks in the following locations:
    /// 1. ~/.config/nexusdb/config.toml
    /// 2. ~/.nexusdb/config.toml
    /// 3. Returns default if not found
    pub async fn load_default() -> Result<Self> {
        // Try XDG config first
        if let Some(config_dir) = dirs::config_dir() {
            let path = config_dir.join("nexusdb").join("config.toml");
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                return Self::from_file(&path).await;
            }
        }

        // Try home directory
        if let Some(home) = dirs::home_dir() {
            let path = home.join(".nexusdb").join("config.toml");
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                return Self::from_file(&path).await;
            }
        }

        // Return default configuration
        Ok(Self::default())
    }

    /// Returns the default configuration file path.
    pub fn default_config_path() -> Option<PathBuf> {
        dirs::config_dir().map(|d| d.join("nexusdb").join("config.toml"))
    }

    /// Returns a builder for configuration.
    pub fn builder() -> CliConfigBuilder {
        CliConfigBuilder::new()
    }
}

/// Builder for CLI configuration.
#[derive(Default)]
pub struct CliConfigBuilder {
    config: CliConfig,
}

impl CliConfigBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the host.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.config.host = host.into();
        self
    }

    /// Sets the port.
    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    /// Sets the database.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.config.database = Some(database.into());
        self
    }

    /// Sets the username.
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.config.username = Some(username.into());
        self
    }

    /// Sets the password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.config.password = Some(password.into());
        self
    }

    /// Sets the output format.
    pub fn output_format(mut self, format: impl Into<String>) -> Self {
        self.config.output_format = format.into();
        self
    }

    /// Enables timing.
    pub fn timing(mut self, enabled: bool) -> Self {
        self.config.timing = enabled;
        self
    }

    /// Builds the configuration.
    pub fn build(self) -> CliConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = CliConfig::default();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert!(config.database.is_none());
    }

    #[test]
    fn test_builder() {
        let config = CliConfig::builder()
            .host("remote.host")
            .port(5433)
            .database("mydb")
            .username("user")
            .build();

        assert_eq!(config.host, "remote.host");
        assert_eq!(config.port, 5433);
        assert_eq!(config.database, Some("mydb".to_string()));
        assert_eq!(config.username, Some("user".to_string()));
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("config.toml");

        let config = CliConfig::builder()
            .host("test.host")
            .port(9999)
            .database("testdb")
            .build();

        config.save(&path).await.unwrap();

        let loaded = CliConfig::from_file(&path).await.unwrap();
        assert_eq!(loaded.host, "test.host");
        assert_eq!(loaded.port, 9999);
        assert_eq!(loaded.database, Some("testdb".to_string()));
    }

    #[test]
    fn test_parse_toml() {
        let toml = r#"
            host = "db.example.com"
            port = 5433
            database = "production"
            output_format = "json"
            timing = true
        "#;

        let config: CliConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 5433);
        assert_eq!(config.database, Some("production".to_string()));
        assert_eq!(config.output_format, "json");
        assert!(config.timing);
    }
}
