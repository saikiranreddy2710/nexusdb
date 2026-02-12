//! Configuration for the LSM-tree key-value storage engine.
//!
//! Provides tunable parameters for all components: memtable sizing,
//! SSTable block sizes, compaction strategies, bloom filter settings,
//! and write throttling.

use std::path::PathBuf;
use std::time::Duration;

/// Top-level configuration for the LSM-tree engine.
#[derive(Debug, Clone)]
pub struct LsmConfig {
    /// Directory for storing data files (SSTables, manifest, etc.).
    pub data_dir: PathBuf,

    /// ── MemTable Settings ──────────────────────────────────────
    /// Maximum size of a single memtable before it becomes immutable
    /// and is scheduled for flush to disk.
    pub memtable_size: usize,

    /// Maximum number of immutable memtables waiting to be flushed.
    /// When exceeded, writes will stall.
    pub max_immutable_memtables: usize,

    /// ── SSTable Settings ───────────────────────────────────────
    /// Target size for data blocks within an SSTable.
    /// Smaller blocks give better point-lookup latency;
    /// larger blocks give better sequential scan throughput.
    pub block_size: usize,

    /// Number of keys between restart points in a block.
    /// Lower values speed up seeks within a block at the cost of space.
    pub block_restart_interval: usize,

    /// Target SSTable file size for L1+ levels.
    pub target_file_size: usize,

    /// ── Compaction Settings ────────────────────────────────────
    /// Compaction strategy to use.
    pub compaction_strategy: CompactionStrategy,

    /// Number of L0 files that triggers a compaction to L1.
    pub l0_compaction_trigger: usize,

    /// Number of L0 files that triggers a write slowdown.
    pub l0_slowdown_trigger: usize,

    /// Number of L0 files that triggers a write stop.
    pub l0_stop_trigger: usize,

    /// Maximum number of levels in the LSM tree.
    pub max_levels: usize,

    /// Size ratio between adjacent levels (level_size * ratio = next_level_size).
    pub level_size_multiplier: usize,

    /// Target size for L1 in bytes.
    pub l1_max_bytes: u64,

    /// Maximum number of concurrent compactions.
    pub max_background_compactions: usize,

    /// Rate limit for compaction I/O in bytes per second (0 = unlimited).
    pub compaction_rate_limit: u64,

    /// ── Bloom Filter Settings ──────────────────────────────────
    /// Enable bloom filters on SSTable blocks.
    pub enable_bloom_filter: bool,

    /// Bits per key for bloom filters. Higher values reduce false positives
    /// but use more memory. 10 bits/key gives ~1% false positive rate.
    pub bloom_bits_per_key: usize,

    /// ── Write Settings ─────────────────────────────────────────
    /// Sync writes to the OS on every put (durability vs. performance).
    pub sync_writes: bool,

    /// Enable write-ahead logging for crash recovery.
    pub enable_wal: bool,

    /// ── Read Settings ──────────────────────────────────────────
    /// Enable block cache for frequently accessed data blocks.
    pub enable_block_cache: bool,

    /// Size of the block cache in bytes.
    pub block_cache_size: usize,

    /// Verify checksums on every read.
    pub verify_checksums: bool,

    /// ── Compression ────────────────────────────────────────────
    /// Compression algorithm for SSTable blocks.
    pub compression: CompressionType,

    /// ── Recovery ───────────────────────────────────────────────
    /// Maximum time to wait for recovery on startup.
    pub recovery_timeout: Duration,
}

/// Compaction strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStrategy {
    /// Leveled compaction (RocksDB default). Good for read-heavy workloads.
    /// Each level has non-overlapping key ranges (except L0).
    Leveled,
    /// Size-tiered compaction (Cassandra default). Good for write-heavy workloads.
    /// SSTables at each level can have overlapping key ranges.
    SizeTiered,
    /// FIFO compaction. Simply drops the oldest SSTable when space is needed.
    /// Good for time-series data with TTL.
    Fifo,
}

/// Compression algorithm for SSTable blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression.
    None,
    /// LZ4 compression (fast, moderate ratio).
    Lz4,
    /// Snappy compression (fast, low ratio).
    Snappy,
    /// Zstd compression (slower, high ratio).
    Zstd,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data/kv"),

            // MemTable: 64 MB default, max 3 immutable waiting
            memtable_size: 64 * 1024 * 1024,
            max_immutable_memtables: 3,

            // SSTable: 4 KB blocks, restart every 16 keys, 64 MB target files
            block_size: 4096,
            block_restart_interval: 16,
            target_file_size: 64 * 1024 * 1024,

            // Compaction: leveled, trigger at 4 L0 files, 7 levels, 10x multiplier
            compaction_strategy: CompactionStrategy::Leveled,
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
            max_levels: 7,
            level_size_multiplier: 10,
            l1_max_bytes: 256 * 1024 * 1024, // 256 MB
            max_background_compactions: 4,
            compaction_rate_limit: 0,

            // Bloom filter: 10 bits/key (~1% false positive rate)
            enable_bloom_filter: true,
            bloom_bits_per_key: 10,

            // Write settings: WAL enabled, no sync per write (group commit)
            sync_writes: false,
            enable_wal: true,

            // Read settings: block cache enabled, 512 MB
            enable_block_cache: true,
            block_cache_size: 512 * 1024 * 1024,
            verify_checksums: true,

            // Compression: none by default (LZ4 if feature enabled)
            compression: CompressionType::None,

            // Recovery: 5 minute timeout
            recovery_timeout: Duration::from_secs(300),
        }
    }
}

impl LsmConfig {
    /// Create a new configuration with the specified data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            ..Default::default()
        }
    }

    /// Configuration optimized for testing (small sizes, fast operations).
    pub fn for_testing(data_dir: impl Into<PathBuf>) -> Self {
        let mut cfg = Self::default();
        cfg.data_dir = data_dir.into();
        cfg.memtable_size = 4 * 1024; // 4 KB
        cfg.max_immutable_memtables = 2;
        cfg.block_size = 256;
        cfg.block_restart_interval = 4;
        cfg.target_file_size = 4 * 1024; // 4 KB
        cfg.l0_compaction_trigger = 2;
        cfg.l0_slowdown_trigger = 4;
        cfg.l0_stop_trigger = 6;
        cfg.max_levels = 4;
        cfg.level_size_multiplier = 4;
        cfg.l1_max_bytes = 16 * 1024; // 16 KB
        cfg.max_background_compactions = 1;
        cfg.compaction_rate_limit = 0;
        cfg.enable_bloom_filter = true;
        cfg.bloom_bits_per_key = 10;
        cfg.sync_writes = false;
        cfg.enable_wal = false;
        cfg.enable_block_cache = false;
        cfg.block_cache_size = 0;
        cfg.verify_checksums = true;
        cfg.compression = CompressionType::None;
        cfg.recovery_timeout = Duration::from_secs(10);
        cfg
    }

    /// Configuration optimized for write-heavy workloads.
    pub fn write_optimized(data_dir: impl Into<PathBuf>) -> Self {
        let mut cfg = Self::default();
        cfg.data_dir = data_dir.into();
        cfg.memtable_size = 128 * 1024 * 1024; // 128 MB
        cfg.max_immutable_memtables = 4;
        cfg.block_size = 16 * 1024; // 16 KB blocks
        cfg.target_file_size = 256 * 1024 * 1024; // 256 MB files
        cfg.compaction_strategy = CompactionStrategy::SizeTiered;
        cfg.max_background_compactions = 8;
        cfg.l0_compaction_trigger = 8;
        cfg.l0_slowdown_trigger = 16;
        cfg.l0_stop_trigger = 24;
        cfg
    }

    /// Configuration optimized for read-heavy workloads.
    pub fn read_optimized(data_dir: impl Into<PathBuf>) -> Self {
        let mut cfg = Self::default();
        cfg.data_dir = data_dir.into();
        cfg.block_size = 4096; // 4 KB blocks (smaller = faster seeks)
        cfg.block_restart_interval = 8; // More restart points
        cfg.enable_bloom_filter = true;
        cfg.bloom_bits_per_key = 14; // Lower false positive rate
        cfg.block_cache_size = 1024 * 1024 * 1024; // 1 GB cache
        cfg
    }

    /// Validate the configuration and return any errors.
    pub fn validate(&self) -> Result<(), String> {
        if self.memtable_size < 1024 {
            return Err("memtable_size must be at least 1024 bytes".into());
        }
        if self.block_size < 64 {
            return Err("block_size must be at least 64 bytes".into());
        }
        if self.block_restart_interval == 0 {
            return Err("block_restart_interval must be > 0".into());
        }
        if self.max_levels == 0 || self.max_levels > 20 {
            return Err("max_levels must be between 1 and 20".into());
        }
        if self.level_size_multiplier < 2 {
            return Err("level_size_multiplier must be at least 2".into());
        }
        if self.l0_compaction_trigger == 0 {
            return Err("l0_compaction_trigger must be > 0".into());
        }
        if self.l0_slowdown_trigger <= self.l0_compaction_trigger {
            return Err("l0_slowdown_trigger must be > l0_compaction_trigger".into());
        }
        if self.l0_stop_trigger <= self.l0_slowdown_trigger {
            return Err("l0_stop_trigger must be > l0_slowdown_trigger".into());
        }
        if self.bloom_bits_per_key == 0 && self.enable_bloom_filter {
            return Err("bloom_bits_per_key must be > 0 when bloom filter is enabled".into());
        }
        Ok(())
    }

    /// Calculate the maximum size for a given level.
    pub fn max_bytes_for_level(&self, level: usize) -> u64 {
        if level == 0 {
            // L0 is bounded by file count, not size
            return u64::MAX;
        }
        let mut size = self.l1_max_bytes;
        for _ in 1..level {
            size = size.saturating_mul(self.level_size_multiplier as u64);
        }
        size
    }
}
