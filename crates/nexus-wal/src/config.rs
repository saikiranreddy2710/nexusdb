//! WAL configuration.
//!
//! This module provides configuration options for the Write-Ahead Log.

use std::path::PathBuf;
use std::time::Duration;

use nexus_common::constants::{
    DEFAULT_WAL_SEGMENT_SIZE, GROUP_COMMIT_BATCH_SIZE, MAX_WAL_RECORD_SIZE,
};

/// Sync policy for WAL writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Sync after every write (safest, slowest).
    EveryWrite,
    /// Sync after group commit batches.
    GroupCommit,
    /// Sync at fixed intervals.
    Periodic {
        /// Interval between syncs.
        interval: Duration,
    },
    /// Never sync (fastest, unsafe - for testing only).
    Never,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        Self::GroupCommit
    }
}

/// Configuration for the Write-Ahead Log.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory where WAL segments are stored.
    pub dir: PathBuf,

    /// Size of each WAL segment file in bytes.
    pub segment_size: usize,

    /// Maximum size of a single WAL record.
    pub max_record_size: usize,

    /// Sync policy for durability.
    pub sync_policy: SyncPolicy,

    /// Maximum number of records to batch in group commit.
    pub group_commit_batch_size: usize,

    /// Maximum time to wait for group commit batch to fill.
    pub group_commit_timeout: Duration,

    /// Whether to pre-allocate segment files.
    pub preallocate_segments: bool,

    /// Number of segments to keep after checkpoint.
    pub retain_segments: usize,

    /// Whether to use direct I/O (O_DIRECT on Linux).
    pub direct_io: bool,

    /// Buffer size for WAL writes.
    pub write_buffer_size: usize,

    /// Whether to verify checksums on read.
    pub verify_checksums: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data/wal"),
            segment_size: DEFAULT_WAL_SEGMENT_SIZE,
            max_record_size: MAX_WAL_RECORD_SIZE,
            sync_policy: SyncPolicy::default(),
            group_commit_batch_size: GROUP_COMMIT_BATCH_SIZE,
            group_commit_timeout: Duration::from_millis(10),
            preallocate_segments: true,
            retain_segments: 10,
            direct_io: false,             // Disabled by default for compatibility
            write_buffer_size: 64 * 1024, // 64 KB
            verify_checksums: true,
        }
    }
}

impl WalConfig {
    /// Creates a new WAL configuration with the specified directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ..Default::default()
        }
    }

    /// Sets the segment size.
    #[must_use]
    pub fn with_segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    /// Sets the maximum record size.
    #[must_use]
    pub fn with_max_record_size(mut self, size: usize) -> Self {
        self.max_record_size = size;
        self
    }

    /// Sets the sync policy.
    #[must_use]
    pub fn with_sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.sync_policy = policy;
        self
    }

    /// Sets the group commit batch size.
    #[must_use]
    pub fn with_group_commit_batch_size(mut self, size: usize) -> Self {
        self.group_commit_batch_size = size;
        self
    }

    /// Sets the group commit timeout.
    #[must_use]
    pub fn with_group_commit_timeout(mut self, timeout: Duration) -> Self {
        self.group_commit_timeout = timeout;
        self
    }

    /// Sets whether to preallocate segments.
    #[must_use]
    pub fn with_preallocate_segments(mut self, preallocate: bool) -> Self {
        self.preallocate_segments = preallocate;
        self
    }

    /// Sets the number of segments to retain after checkpoint.
    #[must_use]
    pub fn with_retain_segments(mut self, count: usize) -> Self {
        self.retain_segments = count;
        self
    }

    /// Sets whether to use direct I/O.
    #[must_use]
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }

    /// Sets the write buffer size.
    #[must_use]
    pub fn with_write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = size;
        self
    }

    /// Sets whether to verify checksums on read.
    #[must_use]
    pub fn with_verify_checksums(mut self, verify: bool) -> Self {
        self.verify_checksums = verify;
        self
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.segment_size < 1024 * 1024 {
            return Err("Segment size must be at least 1 MB".to_string());
        }

        if self.max_record_size > self.segment_size / 2 {
            return Err("Max record size must be at most half the segment size".to_string());
        }

        if self.group_commit_batch_size == 0 {
            return Err("Group commit batch size must be positive".to_string());
        }

        if self.write_buffer_size < 4096 {
            return Err("Write buffer size must be at least 4 KB".to_string());
        }

        Ok(())
    }

    /// Returns the segment file path for a given segment ID.
    pub fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.dir.join(format!("wal_{:016x}.log", segment_id))
    }

    /// Calculates the segment ID for a given LSN.
    pub fn segment_id_for_lsn(&self, lsn: u64) -> u64 {
        lsn / self.segment_size as u64
    }

    /// Calculates the offset within a segment for a given LSN.
    pub fn segment_offset_for_lsn(&self, lsn: u64) -> usize {
        (lsn % self.segment_size as u64) as usize
    }

    /// Calculates the starting LSN for a given segment ID.
    pub fn start_lsn_for_segment(&self, segment_id: u64) -> u64 {
        segment_id * self.segment_size as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = WalConfig::default();
        assert_eq!(config.segment_size, DEFAULT_WAL_SEGMENT_SIZE);
        assert_eq!(config.max_record_size, MAX_WAL_RECORD_SIZE);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_builder() {
        let config = WalConfig::new("/tmp/wal")
            .with_segment_size(128 * 1024 * 1024)
            .with_sync_policy(SyncPolicy::EveryWrite)
            .with_direct_io(true);

        assert_eq!(config.dir, PathBuf::from("/tmp/wal"));
        assert_eq!(config.segment_size, 128 * 1024 * 1024);
        assert_eq!(config.sync_policy, SyncPolicy::EveryWrite);
        assert!(config.direct_io);
    }

    #[test]
    fn test_config_validation() {
        // Segment too small
        let config = WalConfig::default().with_segment_size(1024);
        assert!(config.validate().is_err());

        // Record too large
        let config = WalConfig::default()
            .with_segment_size(1024 * 1024)
            .with_max_record_size(1024 * 1024);
        assert!(config.validate().is_err());

        // Zero batch size
        let config = WalConfig::default().with_group_commit_batch_size(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_segment_path() {
        let config = WalConfig::new("/data/wal");
        let path = config.segment_path(0);
        assert_eq!(path, PathBuf::from("/data/wal/wal_0000000000000000.log"));

        let path = config.segment_path(42);
        assert_eq!(path, PathBuf::from("/data/wal/wal_000000000000002a.log"));
    }

    #[test]
    fn test_lsn_calculations() {
        let config = WalConfig::default().with_segment_size(64 * 1024 * 1024);

        // LSN 0 -> segment 0, offset 0
        assert_eq!(config.segment_id_for_lsn(0), 0);
        assert_eq!(config.segment_offset_for_lsn(0), 0);

        // LSN at segment boundary
        let boundary = 64 * 1024 * 1024;
        assert_eq!(config.segment_id_for_lsn(boundary as u64), 1);
        assert_eq!(config.segment_offset_for_lsn(boundary as u64), 0);

        // LSN in middle of second segment
        let mid = boundary + boundary / 2;
        assert_eq!(config.segment_id_for_lsn(mid as u64), 1);
        assert_eq!(config.segment_offset_for_lsn(mid as u64), boundary / 2);

        // Starting LSN for segment
        assert_eq!(config.start_lsn_for_segment(0), 0);
        assert_eq!(config.start_lsn_for_segment(1), boundary as u64);
    }

    #[test]
    fn test_sync_policy() {
        assert_eq!(SyncPolicy::default(), SyncPolicy::GroupCommit);

        let periodic = SyncPolicy::Periodic {
            interval: Duration::from_millis(100),
        };
        if let SyncPolicy::Periodic { interval } = periodic {
            assert_eq!(interval, Duration::from_millis(100));
        } else {
            panic!("Expected Periodic");
        }
    }
}
