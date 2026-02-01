//! System-wide constants for NexusDB.
//!
//! This module defines constants used across the database.
//! These values are carefully chosen for optimal performance.

// =============================================================================
// Page and Memory Constants
// =============================================================================

/// Default page size in bytes (8 KB).
///
/// This is a common page size that balances I/O efficiency with memory usage.
/// It matches the typical SSD block size and OS page size.
pub const DEFAULT_PAGE_SIZE: usize = 8 * 1024;

/// Minimum page size in bytes (4 KB).
pub const MIN_PAGE_SIZE: usize = 4 * 1024;

/// Maximum page size in bytes (64 KB).
pub const MAX_PAGE_SIZE: usize = 64 * 1024;

/// Page header size in bytes.
///
/// The header contains: page_id (8), lsn (8), checksum (4), flags (2),
/// slot_count (2), free_space_offset (2), free_space_end (2) = 28 bytes.
/// We round up to 32 for alignment.
pub const PAGE_HEADER_SIZE: usize = 32;

/// Slot size in the page slot array.
///
/// Each slot contains: offset (2), length (2) = 4 bytes.
pub const SLOT_SIZE: usize = 4;

// =============================================================================
// Key and Value Limits
// =============================================================================

/// Maximum key size in bytes (16 KB).
///
/// Keys larger than half a page would cause issues with B+ tree balancing.
pub const MAX_KEY_SIZE: usize = 16 * 1024;

/// Maximum inline value size in bytes (1 MB).
///
/// Larger values are stored in overflow pages.
pub const MAX_VALUE_SIZE: usize = 1024 * 1024;

/// Maximum inline value size before using overflow pages.
///
/// Values larger than this are stored separately to avoid page fragmentation.
pub const OVERFLOW_THRESHOLD: usize = DEFAULT_PAGE_SIZE / 4;

// =============================================================================
// Buffer Pool Constants
// =============================================================================

/// Default buffer pool size (1 GB).
pub const DEFAULT_BUFFER_POOL_SIZE: usize = 1024 * 1024 * 1024;

/// Minimum buffer pool size (16 MB).
pub const MIN_BUFFER_POOL_SIZE: usize = 16 * 1024 * 1024;

/// Number of buffer pool partitions for reducing lock contention.
pub const BUFFER_POOL_PARTITIONS: usize = 16;

/// Clock hand sweep batch size for eviction.
pub const EVICTION_BATCH_SIZE: usize = 64;

// =============================================================================
// WAL Constants
// =============================================================================

/// Default WAL segment size (64 MB).
pub const DEFAULT_WAL_SEGMENT_SIZE: usize = 64 * 1024 * 1024;

/// Maximum WAL record size (10 MB).
pub const MAX_WAL_RECORD_SIZE: usize = 10 * 1024 * 1024;

/// WAL record header size.
///
/// Contains: lsn (8), txn_id (8), type (1), flags (1), length (4),
/// prev_lsn (8), checksum (4) = 34 bytes, rounded to 40.
pub const WAL_RECORD_HEADER_SIZE: usize = 40;

/// Magic number for WAL segment header.
pub const WAL_MAGIC: u32 = 0x4E58_5741; // "NXWA" in ASCII

// =============================================================================
// Transaction Constants
// =============================================================================

/// Maximum number of concurrent transactions.
pub const MAX_CONCURRENT_TRANSACTIONS: usize = 65536;

/// Maximum transaction size (total bytes of changes).
pub const MAX_TRANSACTION_SIZE: usize = 256 * 1024 * 1024; // 256 MB

/// Lock acquisition timeout (default).
pub const DEFAULT_LOCK_TIMEOUT_MS: u64 = 10_000; // 10 seconds

// =============================================================================
// Raft Constants
// =============================================================================

/// Maximum number of nodes in a Raft cluster.
pub const MAX_CLUSTER_SIZE: usize = 50;

/// Maximum Raft log entries per AppendEntries RPC.
pub const MAX_RAFT_ENTRIES_PER_REQUEST: usize = 100;

/// Maximum size of Raft entries per AppendEntries RPC (1 MB).
pub const MAX_RAFT_SIZE_PER_REQUEST: usize = 1024 * 1024;

// =============================================================================
// SageTree Constants
// =============================================================================

/// Maximum delta chain length before triggering a merge.
pub const MAX_DELTA_CHAIN_LENGTH: usize = 8;

/// Default page fill factor (70%).
pub const DEFAULT_PAGE_FILL_FACTOR: f64 = 0.7;

/// Minimum page fill factor before merge (40%).
pub const MIN_PAGE_FILL_FACTOR: f64 = 0.4;

/// Maximum branching factor for internal nodes.
pub const MAX_BRANCHING_FACTOR: usize = 256;

// =============================================================================
// Checksum and Magic Numbers
// =============================================================================

/// Magic number for data file header.
pub const DATA_FILE_MAGIC: u32 = 0x4E58_4442; // "NXDB" in ASCII

/// Version number for data file format.
pub const DATA_FILE_VERSION: u32 = 1;

/// Magic number for page header validation.
pub const PAGE_MAGIC: u16 = 0x4E58; // "NX" in ASCII

// =============================================================================
// Performance Tuning
// =============================================================================

/// Number of I/O threads for async operations.
pub const IO_THREAD_COUNT: usize = 4;

/// io_uring submission queue size.
pub const IO_URING_SQ_SIZE: u32 = 256;

/// Prefetch distance (number of pages to read ahead).
pub const PREFETCH_DISTANCE: usize = 8;

/// Batch size for group commit.
pub const GROUP_COMMIT_BATCH_SIZE: usize = 100;

// =============================================================================
// Timeouts and Intervals
// =============================================================================

/// Default operation timeout in milliseconds.
pub const DEFAULT_TIMEOUT_MS: u64 = 30_000; // 30 seconds

/// Checkpoint interval in seconds.
pub const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 60;

/// Stats collection interval in seconds.
pub const STATS_COLLECTION_INTERVAL_SECS: u64 = 10;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_constants() {
        // Page size should be power of 2
        assert!(DEFAULT_PAGE_SIZE.is_power_of_two());
        assert!(MIN_PAGE_SIZE.is_power_of_two());
        assert!(MAX_PAGE_SIZE.is_power_of_two());

        // Header should fit in page
        assert!(PAGE_HEADER_SIZE < DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn test_key_value_limits() {
        // Key should fit in half a page
        assert!(MAX_KEY_SIZE <= DEFAULT_PAGE_SIZE * 2);

        // Overflow threshold should be less than page size
        assert!(OVERFLOW_THRESHOLD < DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn test_buffer_pool_constants() {
        // Buffer pool should hold at least some pages
        assert!(MIN_BUFFER_POOL_SIZE / DEFAULT_PAGE_SIZE >= 16);
    }
}
