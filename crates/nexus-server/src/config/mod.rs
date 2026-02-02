//! Server configuration.
//!
//! This module provides configuration management for the NexusDB server.

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Host address to bind to.
    #[serde(default = "default_host")]
    pub host: String,

    /// Port to listen on.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Data directory for persistent storage.
    #[serde(default)]
    pub data_dir: Option<PathBuf>,

    /// WAL directory (defaults to data_dir/wal).
    #[serde(default)]
    pub wal_dir: Option<PathBuf>,

    /// Run in memory-only mode.
    #[serde(default)]
    pub memory_mode: bool,

    /// Maximum number of client connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Buffer pool size in MB.
    #[serde(default = "default_buffer_pool_mb")]
    pub buffer_pool_mb: usize,

    /// Query timeout in seconds.
    #[serde(default = "default_query_timeout")]
    pub query_timeout_secs: u64,

    /// Enable query logging.
    #[serde(default)]
    pub query_logging: bool,

    /// Enable slow query logging (queries taking longer than this threshold).
    #[serde(default = "default_slow_query_threshold")]
    pub slow_query_threshold_ms: u64,

    /// TLS certificate file path.
    #[serde(default)]
    pub tls_cert: Option<PathBuf>,

    /// TLS key file path.
    #[serde(default)]
    pub tls_key: Option<PathBuf>,

    /// Enable Raft consensus for distributed mode.
    #[serde(default)]
    pub enable_raft: bool,

    /// Raft node ID.
    #[serde(default = "default_node_id")]
    pub node_id: u64,

    /// Raft peer addresses.
    #[serde(default)]
    pub raft_peers: Vec<String>,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    5432
}

fn default_max_connections() -> usize {
    100
}

fn default_buffer_pool_mb() -> usize {
    128
}

fn default_query_timeout() -> u64 {
    300
}

fn default_slow_query_threshold() -> u64 {
    1000
}

fn default_node_id() -> u64 {
    1
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            data_dir: None,
            wal_dir: None,
            memory_mode: false,
            max_connections: default_max_connections(),
            buffer_pool_mb: default_buffer_pool_mb(),
            query_timeout_secs: default_query_timeout(),
            query_logging: false,
            slow_query_threshold_ms: default_slow_query_threshold(),
            tls_cert: None,
            tls_key: None,
            enable_raft: false,
            node_id: default_node_id(),
            raft_peers: Vec::new(),
        }
    }
}

impl ServerConfig {
    /// Creates a new default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads configuration from a file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// Saves configuration to a file.
    pub fn save(&self, path: &Path) -> Result<()> {
        let content = self.to_toml()?;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(path, content)?;
        Ok(())
    }

    /// Converts configuration to TOML string.
    pub fn to_toml(&self) -> Result<String> {
        Ok(toml::to_string_pretty(self)?)
    }

    /// Returns the effective WAL directory.
    pub fn effective_wal_dir(&self) -> Option<PathBuf> {
        self.wal_dir
            .clone()
            .or_else(|| self.data_dir.as_ref().map(|d| d.join("wal")))
    }

    /// Returns the socket address.
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Creates a builder for configuration.
    pub fn builder() -> ServerConfigBuilder {
        ServerConfigBuilder::new()
    }
}

/// Builder for server configuration.
#[derive(Default)]
pub struct ServerConfigBuilder {
    config: ServerConfig,
}

impl ServerConfigBuilder {
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

    /// Sets the data directory.
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.data_dir = Some(dir.into());
        self
    }

    /// Enables memory mode.
    pub fn memory_mode(mut self, enabled: bool) -> Self {
        self.config.memory_mode = enabled;
        self
    }

    /// Sets maximum connections.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.config.max_connections = max;
        self
    }

    /// Sets buffer pool size in MB.
    pub fn buffer_pool_mb(mut self, size: usize) -> Self {
        self.config.buffer_pool_mb = size;
        self
    }

    /// Enables Raft consensus.
    pub fn enable_raft(mut self, enabled: bool) -> Self {
        self.config.enable_raft = enabled;
        self
    }

    /// Sets the node ID.
    pub fn node_id(mut self, id: u64) -> Self {
        self.config.node_id = id;
        self
    }

    /// Adds Raft peers.
    pub fn raft_peers(mut self, peers: Vec<String>) -> Self {
        self.config.raft_peers = peers;
        self
    }

    /// Builds the configuration.
    pub fn build(self) -> ServerConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 5432);
        assert_eq!(config.max_connections, 100);
        assert!(!config.memory_mode);
    }

    #[test]
    fn test_builder() {
        let config = ServerConfig::builder()
            .host("localhost")
            .port(5433)
            .memory_mode(true)
            .max_connections(50)
            .build();

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5433);
        assert!(config.memory_mode);
        assert_eq!(config.max_connections, 50);
    }

    #[test]
    fn test_to_toml() {
        let config = ServerConfig::default();
        let toml = config.to_toml().unwrap();
        assert!(toml.contains("host"));
        assert!(toml.contains("port"));
    }

    #[test]
    fn test_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("config.toml");

        let config = ServerConfig::builder().host("testhost").port(9999).build();

        config.save(&path).unwrap();

        let loaded = ServerConfig::from_file(&path).unwrap();
        assert_eq!(loaded.host, "testhost");
        assert_eq!(loaded.port, 9999);
    }

    #[test]
    fn test_effective_wal_dir() {
        let config = ServerConfig::builder().data_dir("/data/nexus").build();

        assert_eq!(
            config.effective_wal_dir(),
            Some(PathBuf::from("/data/nexus/wal"))
        );

        let config2 = ServerConfig::builder().data_dir("/data/nexus").build();
        let mut config2 = config2;
        config2.wal_dir = Some(PathBuf::from("/wal/custom"));

        assert_eq!(
            config2.effective_wal_dir(),
            Some(PathBuf::from("/wal/custom"))
        );
    }
}
