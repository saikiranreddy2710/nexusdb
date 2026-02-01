//! Database configuration structures.
//!
//! These structures define all configurable aspects of a NexusDB instance.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

use crate::types::NodeId;

/// Main database configuration.
///
/// This is the top-level configuration structure that contains all
/// component configurations.
///
/// # Example
///
/// ```rust
/// use nexus_common::config::DatabaseConfig;
///
/// let config = DatabaseConfig::default();
/// assert_eq!(config.storage.page_size, 8192);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Unique identifier for this node in the cluster.
    pub node_id: NodeId,

    /// Data directory for persistent storage.
    pub data_dir: PathBuf,

    /// Storage engine configuration.
    pub storage: StorageConfig,

    /// Buffer pool configuration.
    pub buffer_pool: BufferPoolConfig,

    /// Write-ahead log configuration.
    pub wal: WalConfig,

    /// Raft consensus configuration.
    pub raft: RaftConfig,

    /// Cluster configuration.
    pub cluster: ClusterConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId::new(1),
            data_dir: PathBuf::from("./data"),
            storage: StorageConfig::default(),
            buffer_pool: BufferPoolConfig::default(),
            wal: WalConfig::default(),
            raft: RaftConfig::default(),
            cluster: ClusterConfig::default(),
        }
    }
}

impl DatabaseConfig {
    /// Creates a new configuration with the specified data directory.
    #[must_use]
    pub fn with_data_dir(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            ..Default::default()
        }
    }

    /// Creates a minimal configuration for testing.
    #[must_use]
    pub fn for_testing() -> Self {
        Self {
            node_id: NodeId::new(1),
            data_dir: PathBuf::from("/tmp/nexusdb_test"),
            storage: StorageConfig::default(),
            buffer_pool: BufferPoolConfig {
                size_bytes: 16 * 1024 * 1024, // 16 MB for tests
                ..Default::default()
            },
            wal: WalConfig {
                sync_on_commit: false, // Faster tests
                ..Default::default()
            },
            raft: RaftConfig::default(),
            cluster: ClusterConfig::default(),
        }
    }

    /// Validates the configuration and returns an error if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.storage.page_size < 4096 {
            return Err("page_size must be at least 4096 bytes".to_string());
        }

        if !self.storage.page_size.is_power_of_two() {
            return Err("page_size must be a power of 2".to_string());
        }

        if self.buffer_pool.size_bytes < self.storage.page_size * 16 {
            return Err("buffer_pool.size_bytes must be at least 16 pages".to_string());
        }

        Ok(())
    }
}

/// Storage engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Size of each page in bytes. Must be a power of 2.
    /// Default: 8192 (8 KB)
    pub page_size: usize,

    /// Maximum key size in bytes.
    /// Default: 16384 (16 KB)
    pub max_key_size: usize,

    /// Maximum value size in bytes.
    /// Default: 1048576 (1 MB)
    pub max_value_size: usize,

    /// Use direct I/O (O_DIRECT on Linux).
    /// Default: true
    pub use_direct_io: bool,

    /// Enable data compression.
    /// Default: false
    pub enable_compression: bool,

    /// Compression algorithm (if enabled).
    /// Options: "lz4", "zstd", "snappy"
    pub compression_algorithm: String,

    /// SageTree specific: maximum delta chain length before merge.
    /// Default: 8
    pub max_delta_chain_length: usize,

    /// SageTree specific: target fill factor for pages (0.0 - 1.0).
    /// Default: 0.7
    pub page_fill_factor: f64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            page_size: 8192,
            max_key_size: 16 * 1024,
            max_value_size: 1024 * 1024,
            use_direct_io: true,
            enable_compression: false,
            compression_algorithm: "lz4".to_string(),
            max_delta_chain_length: 8,
            page_fill_factor: 0.7,
        }
    }
}

/// Buffer pool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferPoolConfig {
    /// Total buffer pool size in bytes.
    /// Default: 1073741824 (1 GB)
    pub size_bytes: usize,

    /// Number of buffer pool partitions for reducing contention.
    /// Default: 16
    pub num_partitions: usize,

    /// Eviction batch size (number of pages to evict at once).
    /// Default: 64
    pub eviction_batch_size: usize,

    /// Low watermark for free pages (trigger eviction).
    /// Default: 0.1 (10%)
    pub low_watermark: f64,

    /// High watermark for free pages (stop eviction).
    /// Default: 0.2 (20%)
    pub high_watermark: f64,

    /// Enable NUMA-aware memory allocation.
    /// Default: true
    pub numa_aware: bool,

    /// Enable huge pages (2MB) for buffer pool.
    /// Default: false
    pub use_huge_pages: bool,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            size_bytes: 1024 * 1024 * 1024, // 1 GB
            num_partitions: 16,
            eviction_batch_size: 64,
            low_watermark: 0.1,
            high_watermark: 0.2,
            numa_aware: true,
            use_huge_pages: false,
        }
    }
}

impl BufferPoolConfig {
    /// Returns the number of pages that can be held in the buffer pool.
    #[must_use]
    pub const fn max_pages(&self, page_size: usize) -> usize {
        self.size_bytes / page_size
    }
}

/// Write-ahead log configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// WAL directory (relative to data_dir if not absolute).
    pub dir: PathBuf,

    /// Maximum WAL segment size in bytes.
    /// Default: 67108864 (64 MB)
    pub segment_size: usize,

    /// Sync WAL to disk on every commit.
    /// Default: true
    pub sync_on_commit: bool,

    /// Use group commit for better throughput.
    /// Default: true
    pub group_commit: bool,

    /// Group commit delay (wait for more transactions).
    /// Default: 1ms
    #[serde(with = "humantime_serde")]
    pub group_commit_delay: Duration,

    /// Maximum number of transactions in a group commit.
    /// Default: 100
    pub group_commit_max_size: usize,

    /// Checkpoint interval.
    /// Default: 60s
    #[serde(with = "humantime_serde")]
    pub checkpoint_interval: Duration,

    /// Number of WAL segments to retain after checkpoint.
    /// Default: 3
    pub segments_to_retain: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("wal"),
            segment_size: 64 * 1024 * 1024, // 64 MB
            sync_on_commit: true,
            group_commit: true,
            group_commit_delay: Duration::from_millis(1),
            group_commit_max_size: 100,
            checkpoint_interval: Duration::from_secs(60),
            segments_to_retain: 3,
        }
    }
}

/// Raft consensus configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Election timeout range (min).
    /// Default: 150ms
    #[serde(with = "humantime_serde")]
    pub election_timeout_min: Duration,

    /// Election timeout range (max).
    /// Default: 300ms
    #[serde(with = "humantime_serde")]
    pub election_timeout_max: Duration,

    /// Heartbeat interval.
    /// Default: 50ms
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,

    /// Maximum entries per AppendEntries RPC.
    /// Default: 100
    pub max_entries_per_request: usize,

    /// Maximum size of entries per AppendEntries RPC.
    /// Default: 1048576 (1 MB)
    pub max_size_per_request: usize,

    /// Enable leader lease for local reads.
    /// Default: true
    pub enable_leader_lease: bool,

    /// Leader lease duration.
    /// Default: 100ms
    #[serde(with = "humantime_serde")]
    pub leader_lease_duration: Duration,

    /// Enable pipelining for log replication.
    /// Default: true
    pub enable_pipelining: bool,

    /// Pipeline window size (number of in-flight requests).
    /// Default: 10
    pub pipeline_window: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            max_entries_per_request: 100,
            max_size_per_request: 1024 * 1024,
            enable_leader_lease: true,
            leader_lease_duration: Duration::from_millis(100),
            enable_pipelining: true,
            pipeline_window: 10,
        }
    }
}

impl RaftConfig {
    /// Validates that election timeout is greater than heartbeat interval.
    pub fn validate(&self) -> Result<(), String> {
        if self.election_timeout_min <= self.heartbeat_interval {
            return Err("election_timeout_min must be greater than heartbeat_interval".to_string());
        }

        if self.election_timeout_max <= self.election_timeout_min {
            return Err(
                "election_timeout_max must be greater than election_timeout_min".to_string(),
            );
        }

        Ok(())
    }
}

/// Cluster configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// List of cluster peer addresses.
    /// Format: "node_id=host:port"
    pub peers: Vec<String>,

    /// Address to listen on for peer communication.
    pub listen_addr: String,

    /// Address to listen on for client connections.
    pub client_addr: String,

    /// Enable TLS for peer communication.
    /// Default: false
    pub enable_tls: bool,

    /// TLS certificate file.
    pub tls_cert_file: Option<PathBuf>,

    /// TLS key file.
    pub tls_key_file: Option<PathBuf>,

    /// Connection timeout.
    /// Default: 5s
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,

    /// Request timeout.
    /// Default: 10s
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            listen_addr: "0.0.0.0:9700".to_string(),
            client_addr: "0.0.0.0:9701".to_string(),
            enable_tls: false,
            tls_cert_file: None,
            tls_key_file: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// Serde helper for Duration using humantime format.
mod humantime_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = humantime::format_duration(*duration).to_string();
        s.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        humantime::parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DatabaseConfig::default();
        assert_eq!(config.storage.page_size, 8192);
        assert_eq!(config.buffer_pool.size_bytes, 1024 * 1024 * 1024);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation() {
        let mut config = DatabaseConfig::default();
        config.storage.page_size = 1024; // Too small
        assert!(config.validate().is_err());

        config.storage.page_size = 8192;
        config.buffer_pool.size_bytes = 1024; // Too small
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_buffer_pool_max_pages() {
        let config = BufferPoolConfig::default();
        let max_pages = config.max_pages(8192);
        assert_eq!(max_pages, 131072); // 1GB / 8KB
    }

    #[test]
    fn test_raft_config_validation() {
        let config = RaftConfig::default();
        assert!(config.validate().is_ok());

        let mut bad_config = config.clone();
        bad_config.election_timeout_min = Duration::from_millis(10);
        bad_config.heartbeat_interval = Duration::from_millis(100);
        assert!(bad_config.validate().is_err());
    }

    #[test]
    fn test_testing_config() {
        let config = DatabaseConfig::for_testing();
        assert_eq!(config.buffer_pool.size_bytes, 16 * 1024 * 1024);
        assert!(!config.wal.sync_on_commit);
    }
}
