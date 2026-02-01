//! Database error types.
//!
//! Provides comprehensive error types for all database operations.

use std::fmt;
use thiserror::Error;

use crate::types::{Lsn, NodeId, PageId, TxnId};

/// Error codes for categorizing errors.
///
/// These codes can be used for programmatic error handling and
/// are stable across versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    // General errors (0x0000 - 0x00FF)
    /// Unknown or unspecified error.
    Unknown = 0x0000,
    /// Internal error (bug).
    Internal = 0x0001,
    /// Operation not supported.
    NotSupported = 0x0002,
    /// Invalid argument provided.
    InvalidArgument = 0x0003,
    /// Operation timed out.
    Timeout = 0x0004,
    /// Operation was cancelled.
    Cancelled = 0x0005,

    // I/O errors (0x0100 - 0x01FF)
    /// General I/O error.
    Io = 0x0100,
    /// File not found.
    FileNotFound = 0x0101,
    /// Permission denied.
    PermissionDenied = 0x0102,
    /// Disk full.
    DiskFull = 0x0103,
    /// Data corruption detected.
    Corruption = 0x0104,

    // Storage errors (0x0200 - 0x02FF)
    /// Page not found in storage.
    PageNotFound = 0x0200,
    /// Page is corrupted.
    PageCorrupted = 0x0201,
    /// Buffer pool is full.
    BufferPoolFull = 0x0202,
    /// Key not found.
    KeyNotFound = 0x0203,
    /// Key already exists.
    KeyExists = 0x0204,
    /// Key too large.
    KeyTooLarge = 0x0205,
    /// Value too large.
    ValueTooLarge = 0x0206,

    // Transaction errors (0x0300 - 0x03FF)
    /// Transaction was aborted.
    TransactionAborted = 0x0300,
    /// Transaction conflict detected.
    TransactionConflict = 0x0301,
    /// Deadlock detected.
    Deadlock = 0x0302,
    /// Lock acquisition failed.
    LockFailed = 0x0303,
    /// Transaction not found.
    TransactionNotFound = 0x0304,
    /// Transaction already committed.
    TransactionCommitted = 0x0305,

    // WAL errors (0x0400 - 0x04FF)
    /// WAL is corrupted.
    WalCorrupted = 0x0400,
    /// WAL write failed.
    WalWriteFailed = 0x0401,
    /// WAL is full.
    WalFull = 0x0402,
    /// Invalid LSN.
    InvalidLsn = 0x0403,

    // Cluster errors (0x0500 - 0x05FF)
    /// Node not found in cluster.
    NodeNotFound = 0x0500,
    /// Not the leader.
    NotLeader = 0x0501,
    /// Leader unknown.
    LeaderUnknown = 0x0502,
    /// Replication failed.
    ReplicationFailed = 0x0503,
    /// Quorum not reached.
    QuorumNotReached = 0x0504,
    /// Node is not ready.
    NodeNotReady = 0x0505,

    // Query errors (0x0600 - 0x06FF)
    /// SQL syntax error.
    SyntaxError = 0x0600,
    /// Table not found.
    TableNotFound = 0x0601,
    /// Column not found.
    ColumnNotFound = 0x0602,
    /// Type mismatch.
    TypeMismatch = 0x0603,
    /// Query planning failed.
    PlanningFailed = 0x0604,
    /// Query execution failed.
    ExecutionFailed = 0x0605,
}

impl ErrorCode {
    /// Returns the numeric code.
    #[inline]
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Returns the error category name.
    #[must_use]
    pub const fn category(&self) -> &'static str {
        match (*self as u16) >> 8 {
            0x00 => "General",
            0x01 => "I/O",
            0x02 => "Storage",
            0x03 => "Transaction",
            0x04 => "WAL",
            0x05 => "Cluster",
            0x06 => "Query",
            _ => "Unknown",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The main error type for NexusDB.
///
/// This enum covers all possible errors that can occur during database
/// operations. Each variant includes relevant context for debugging.
///
/// # Example
///
/// ```rust
/// use nexus_common::error::{NexusError, NexusResult};
/// use nexus_common::types::PageId;
///
/// fn read_page(page_id: PageId) -> NexusResult<Vec<u8>> {
///     Err(NexusError::PageNotFound { page_id })
/// }
/// ```
#[derive(Debug, Error)]
pub enum NexusError {
    // ==========================================================================
    // General Errors
    // ==========================================================================
    /// Internal error - this indicates a bug.
    #[error("internal error: {message}")]
    Internal {
        /// Error message.
        message: String,
    },

    /// Operation not supported.
    #[error("operation not supported: {operation}")]
    NotSupported {
        /// The unsupported operation.
        operation: String,
    },

    /// Invalid argument provided.
    #[error("invalid argument: {message}")]
    InvalidArgument {
        /// Error message.
        message: String,
    },

    /// Operation timed out.
    #[error("operation timed out after {duration_ms}ms")]
    Timeout {
        /// Timeout duration in milliseconds.
        duration_ms: u64,
    },

    /// Operation was cancelled.
    #[error("operation was cancelled")]
    Cancelled,

    // ==========================================================================
    // I/O Errors
    // ==========================================================================
    /// I/O error from the underlying system.
    #[error("I/O error: {source}")]
    Io {
        /// The underlying I/O error.
        #[from]
        source: std::io::Error,
    },

    /// Data corruption detected.
    #[error("data corruption detected: {message}")]
    Corruption {
        /// Description of the corruption.
        message: String,
    },

    /// Checksum mismatch.
    #[error("checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        /// Expected checksum.
        expected: u32,
        /// Actual checksum.
        actual: u32,
    },

    // ==========================================================================
    // Storage Errors
    // ==========================================================================
    /// Page not found.
    #[error("page {page_id} not found")]
    PageNotFound {
        /// The missing page ID.
        page_id: PageId,
    },

    /// Page is corrupted.
    #[error("page {page_id} is corrupted: {reason}")]
    PageCorrupted {
        /// The corrupted page ID.
        page_id: PageId,
        /// Reason for corruption.
        reason: String,
    },

    /// Buffer pool is full.
    #[error("buffer pool is full, cannot allocate new page")]
    BufferPoolFull,

    /// Key not found.
    #[error("key not found")]
    KeyNotFound,

    /// Key already exists.
    #[error("key already exists")]
    KeyExists,

    /// Key is too large.
    #[error("key size {size} exceeds maximum {max_size}")]
    KeyTooLarge {
        /// Actual key size.
        size: usize,
        /// Maximum allowed size.
        max_size: usize,
    },

    /// Value is too large.
    #[error("value size {size} exceeds maximum {max_size}")]
    ValueTooLarge {
        /// Actual value size.
        size: usize,
        /// Maximum allowed size.
        max_size: usize,
    },

    /// Page is full.
    #[error("page {page_id} is full, cannot insert record of size {record_size}")]
    PageFull {
        /// The full page.
        page_id: PageId,
        /// Size of the record that couldn't be inserted.
        record_size: usize,
    },

    // ==========================================================================
    // Transaction Errors
    // ==========================================================================
    /// Transaction was aborted.
    #[error("transaction {txn_id} aborted: {reason}")]
    TransactionAborted {
        /// The aborted transaction.
        txn_id: TxnId,
        /// Reason for abort.
        reason: String,
    },

    /// Transaction conflict detected (serialization failure).
    #[error("transaction {txn_id} conflict with {conflicting_txn_id}")]
    TransactionConflict {
        /// The conflicting transaction.
        txn_id: TxnId,
        /// The transaction it conflicts with.
        conflicting_txn_id: TxnId,
    },

    /// Deadlock detected.
    #[error("deadlock detected, transaction {txn_id} was chosen as victim")]
    Deadlock {
        /// The transaction that was aborted.
        txn_id: TxnId,
    },

    /// Lock acquisition failed.
    #[error("failed to acquire lock for transaction {txn_id}: {reason}")]
    LockFailed {
        /// The transaction that couldn't acquire the lock.
        txn_id: TxnId,
        /// Reason for failure.
        reason: String,
    },

    /// Transaction not found.
    #[error("transaction {txn_id} not found")]
    TransactionNotFound {
        /// The missing transaction.
        txn_id: TxnId,
    },

    // ==========================================================================
    // WAL Errors
    // ==========================================================================
    /// WAL is corrupted.
    #[error("WAL corrupted at LSN {lsn}: {reason}")]
    WalCorrupted {
        /// The LSN where corruption was detected.
        lsn: Lsn,
        /// Reason for corruption.
        reason: String,
    },

    /// WAL write failed.
    #[error("WAL write failed: {reason}")]
    WalWriteFailed {
        /// Reason for failure.
        reason: String,
    },

    /// Invalid LSN.
    #[error("invalid LSN {lsn}")]
    InvalidLsn {
        /// The invalid LSN.
        lsn: Lsn,
    },

    // ==========================================================================
    // Cluster Errors
    // ==========================================================================
    /// Node not found.
    #[error("node {node_id} not found")]
    NodeNotFound {
        /// The missing node.
        node_id: NodeId,
    },

    /// This node is not the leader.
    #[error("not the leader, leader is {leader_id:?}")]
    NotLeader {
        /// The current leader, if known.
        leader_id: Option<NodeId>,
    },

    /// Leader is unknown.
    #[error("leader is unknown")]
    LeaderUnknown,

    /// Replication failed.
    #[error("replication failed: {reason}")]
    ReplicationFailed {
        /// Reason for failure.
        reason: String,
    },

    /// Quorum not reached.
    #[error("quorum not reached: got {received} of {required} votes")]
    QuorumNotReached {
        /// Votes received.
        received: usize,
        /// Votes required.
        required: usize,
    },

    // ==========================================================================
    // Query Errors
    // ==========================================================================
    /// SQL syntax error.
    #[error("syntax error at position {position}: {message}")]
    SyntaxError {
        /// Position in the query.
        position: usize,
        /// Error message.
        message: String,
    },

    /// Table not found.
    #[error("table '{table}' not found")]
    TableNotFound {
        /// The missing table.
        table: String,
    },

    /// Column not found.
    #[error("column '{column}' not found in table '{table}'")]
    ColumnNotFound {
        /// The missing column.
        column: String,
        /// The table name.
        table: String,
    },

    /// Type mismatch.
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        /// Expected type.
        expected: String,
        /// Actual type.
        actual: String,
    },

    /// Query execution failed.
    #[error("query execution failed: {reason}")]
    ExecutionFailed {
        /// Reason for failure.
        reason: String,
    },

    // ==========================================================================
    // Configuration Errors
    // ==========================================================================
    /// Invalid configuration.
    #[error("invalid configuration: {message}")]
    InvalidConfig {
        /// Error message.
        message: String,
    },
}

impl NexusError {
    /// Returns the error code for this error.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::Internal { .. } => ErrorCode::Internal,
            Self::NotSupported { .. } => ErrorCode::NotSupported,
            Self::InvalidArgument { .. } => ErrorCode::InvalidArgument,
            Self::Timeout { .. } => ErrorCode::Timeout,
            Self::Cancelled => ErrorCode::Cancelled,
            Self::Io { .. } => ErrorCode::Io,
            Self::Corruption { .. } => ErrorCode::Corruption,
            Self::ChecksumMismatch { .. } => ErrorCode::Corruption,
            Self::PageNotFound { .. } => ErrorCode::PageNotFound,
            Self::PageCorrupted { .. } => ErrorCode::PageCorrupted,
            Self::BufferPoolFull => ErrorCode::BufferPoolFull,
            Self::KeyNotFound => ErrorCode::KeyNotFound,
            Self::KeyExists => ErrorCode::KeyExists,
            Self::KeyTooLarge { .. } => ErrorCode::KeyTooLarge,
            Self::ValueTooLarge { .. } => ErrorCode::ValueTooLarge,
            Self::PageFull { .. } => ErrorCode::BufferPoolFull,
            Self::TransactionAborted { .. } => ErrorCode::TransactionAborted,
            Self::TransactionConflict { .. } => ErrorCode::TransactionConflict,
            Self::Deadlock { .. } => ErrorCode::Deadlock,
            Self::LockFailed { .. } => ErrorCode::LockFailed,
            Self::TransactionNotFound { .. } => ErrorCode::TransactionNotFound,
            Self::WalCorrupted { .. } => ErrorCode::WalCorrupted,
            Self::WalWriteFailed { .. } => ErrorCode::WalWriteFailed,
            Self::InvalidLsn { .. } => ErrorCode::InvalidLsn,
            Self::NodeNotFound { .. } => ErrorCode::NodeNotFound,
            Self::NotLeader { .. } => ErrorCode::NotLeader,
            Self::LeaderUnknown => ErrorCode::LeaderUnknown,
            Self::ReplicationFailed { .. } => ErrorCode::ReplicationFailed,
            Self::QuorumNotReached { .. } => ErrorCode::QuorumNotReached,
            Self::SyntaxError { .. } => ErrorCode::SyntaxError,
            Self::TableNotFound { .. } => ErrorCode::TableNotFound,
            Self::ColumnNotFound { .. } => ErrorCode::ColumnNotFound,
            Self::TypeMismatch { .. } => ErrorCode::TypeMismatch,
            Self::ExecutionFailed { .. } => ErrorCode::ExecutionFailed,
            Self::InvalidConfig { .. } => ErrorCode::InvalidArgument,
        }
    }

    /// Returns true if this error is retryable.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout { .. }
                | Self::BufferPoolFull
                | Self::LockFailed { .. }
                | Self::NotLeader { .. }
                | Self::LeaderUnknown
                | Self::QuorumNotReached { .. }
        )
    }

    /// Returns true if this error represents a transaction conflict.
    #[must_use]
    pub const fn is_conflict(&self) -> bool {
        matches!(
            self,
            Self::TransactionConflict { .. }
                | Self::Deadlock { .. }
                | Self::TransactionAborted { .. }
        )
    }

    /// Creates an internal error.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Creates an invalid argument error.
    #[must_use]
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            message: message.into(),
        }
    }

    /// Creates a corruption error.
    #[must_use]
    pub fn corruption(message: impl Into<String>) -> Self {
        Self::Corruption {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code() {
        let err = NexusError::PageNotFound {
            page_id: PageId::new(42),
        };
        assert_eq!(err.code(), ErrorCode::PageNotFound);
        assert_eq!(err.code().category(), "Storage");
    }

    #[test]
    fn test_error_display() {
        let err = NexusError::PageNotFound {
            page_id: PageId::new(42),
        };
        assert_eq!(err.to_string(), "page 42 not found");
    }

    #[test]
    fn test_retryable() {
        assert!(NexusError::BufferPoolFull.is_retryable());
        assert!(NexusError::LeaderUnknown.is_retryable());
        assert!(!NexusError::KeyNotFound.is_retryable());
    }

    #[test]
    fn test_conflict() {
        let err = NexusError::TransactionConflict {
            txn_id: TxnId::new(1),
            conflicting_txn_id: TxnId::new(2),
        };
        assert!(err.is_conflict());
        assert!(!NexusError::KeyNotFound.is_conflict());
    }

    #[test]
    fn test_io_error_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let nexus_err: NexusError = io_err.into();
        assert_eq!(nexus_err.code(), ErrorCode::Io);
    }
}
