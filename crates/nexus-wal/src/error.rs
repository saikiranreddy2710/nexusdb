//! WAL error types.
//!
//! This module defines all error types for the Write-Ahead Log.

use std::io;
use std::path::PathBuf;
use thiserror::Error;

use nexus_common::types::Lsn;

/// Result type for WAL operations.
pub type WalResult<T> = Result<T, WalError>;

/// Errors that can occur during WAL operations.
#[derive(Debug, Error)]
pub enum WalError {
    /// I/O error during WAL operations.
    #[error("WAL I/O error: {source}")]
    Io {
        #[from]
        source: io::Error,
    },

    /// WAL directory does not exist.
    #[error("WAL directory does not exist: {path}")]
    DirectoryNotFound { path: PathBuf },

    /// WAL segment file is corrupted.
    #[error("WAL segment corrupted at LSN {lsn}: {reason}")]
    SegmentCorrupted { lsn: Lsn, reason: String },

    /// Invalid WAL segment magic number.
    #[error("Invalid WAL segment magic: expected {expected:#010x}, found {found:#010x}")]
    InvalidMagic { expected: u32, found: u32 },

    /// Invalid WAL segment version.
    #[error("Unsupported WAL version: expected {expected}, found {found}")]
    UnsupportedVersion { expected: u32, found: u32 },

    /// WAL record checksum mismatch.
    #[error("WAL record checksum mismatch at LSN {lsn}: expected {expected:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        lsn: Lsn,
        expected: u32,
        computed: u32,
    },

    /// WAL record too large.
    #[error("WAL record too large: {size} bytes exceeds maximum {max} bytes")]
    RecordTooLarge { size: usize, max: usize },

    /// WAL segment is full.
    #[error("WAL segment is full: {segment_id}")]
    SegmentFull { segment_id: u64 },

    /// WAL segment not found.
    #[error("WAL segment not found: {segment_id}")]
    SegmentNotFound { segment_id: u64 },

    /// Invalid LSN requested.
    #[error("Invalid LSN: {lsn} (segment {segment_id} contains LSN range {start} to {end})")]
    InvalidLsn {
        lsn: Lsn,
        segment_id: u64,
        start: Lsn,
        end: Lsn,
    },

    /// LSN not found in any segment.
    #[error("LSN {lsn} not found in any WAL segment")]
    LsnNotFound { lsn: Lsn },

    /// WAL is closed.
    #[error("WAL is closed")]
    Closed,

    /// WAL record deserialization error.
    #[error("Failed to deserialize WAL record: {reason}")]
    DeserializationError { reason: String },

    /// WAL record serialization error.
    #[error("Failed to serialize WAL record: {reason}")]
    SerializationError { reason: String },

    /// End of segment reached.
    #[error("End of WAL segment reached")]
    EndOfSegment,

    /// No active segment.
    #[error("No active WAL segment")]
    NoActiveSegment,

    /// Group commit timeout.
    #[error("Group commit timed out after {timeout_ms} ms")]
    GroupCommitTimeout { timeout_ms: u64 },

    /// Configuration error.
    #[error("WAL configuration error: {reason}")]
    ConfigError { reason: String },
}

impl WalError {
    /// Creates a segment corrupted error.
    pub fn segment_corrupted(lsn: Lsn, reason: impl Into<String>) -> Self {
        Self::SegmentCorrupted {
            lsn,
            reason: reason.into(),
        }
    }

    /// Creates a checksum mismatch error.
    pub fn checksum_mismatch(lsn: Lsn, expected: u32, computed: u32) -> Self {
        Self::ChecksumMismatch {
            lsn,
            expected,
            computed,
        }
    }

    /// Creates a record too large error.
    pub fn record_too_large(size: usize, max: usize) -> Self {
        Self::RecordTooLarge { size, max }
    }

    /// Creates a deserialization error.
    pub fn deserialization_error(reason: impl Into<String>) -> Self {
        Self::DeserializationError {
            reason: reason.into(),
        }
    }

    /// Creates a serialization error.
    pub fn serialization_error(reason: impl Into<String>) -> Self {
        Self::SerializationError {
            reason: reason.into(),
        }
    }

    /// Creates a config error.
    pub fn config_error(reason: impl Into<String>) -> Self {
        Self::ConfigError {
            reason: reason.into(),
        }
    }

    /// Returns true if this is a recoverable error.
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::SegmentFull { .. } | Self::GroupCommitTimeout { .. } | Self::EndOfSegment
        )
    }

    /// Returns true if this is a corruption error.
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            Self::SegmentCorrupted { .. }
                | Self::ChecksumMismatch { .. }
                | Self::InvalidMagic { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = WalError::segment_corrupted(Lsn::new(100), "bad data");
        assert!(err.is_corruption());
        assert!(!err.is_recoverable());

        let err = WalError::checksum_mismatch(Lsn::new(100), 0x1234, 0x5678);
        assert!(err.is_corruption());

        let err = WalError::SegmentFull { segment_id: 1 };
        assert!(err.is_recoverable());
        assert!(!err.is_corruption());
    }

    #[test]
    fn test_error_display() {
        let err = WalError::record_too_large(100_000_000, 10_000_000);
        let msg = format!("{}", err);
        assert!(msg.contains("100000000"));
        assert!(msg.contains("10000000"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let wal_err: WalError = io_err.into();
        assert!(matches!(wal_err, WalError::Io { .. }));
    }
}
