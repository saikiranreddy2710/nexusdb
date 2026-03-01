//! Error types for the LSM-tree key-value storage engine.
//!
//! Provides a comprehensive error hierarchy covering all failure modes
//! in the LSM-tree: I/O errors, corruption, compaction failures, and
//! resource exhaustion.

use std::path::PathBuf;
use thiserror::Error;

/// Result type alias for KV engine operations.
pub type KvResult<T> = Result<T, KvError>;

/// Comprehensive error type for the LSM-tree KV engine.
#[derive(Debug, Error)]
pub enum KvError {
    // ── I/O Errors ──────────────────────────────────────────────
    /// Underlying I/O error from the operating system.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// File not found at the expected path.
    #[error("file not found: {0}")]
    FileNotFound(PathBuf),

    /// File already exists when it should not.
    #[error("file already exists: {0}")]
    FileExists(PathBuf),

    // ── Data Integrity ──────────────────────────────────────────
    /// Checksum mismatch indicating data corruption.
    #[error("checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    /// SSTable has an invalid or unrecognized magic number.
    #[error("invalid magic number: expected {expected:#018x}, got {actual:#018x}")]
    InvalidMagic { expected: u64, actual: u64 },

    /// Block data is corrupted or malformed.
    #[error("corrupted block at offset {offset}: {reason}")]
    CorruptedBlock { offset: u64, reason: String },

    /// SSTable file is corrupted.
    #[error("corrupted SSTable {path}: {reason}")]
    CorruptedTable { path: PathBuf, reason: String },

    /// Manifest file is corrupted or inconsistent.
    #[error("corrupted manifest: {0}")]
    CorruptedManifest(String),

    // ── Key/Value Constraints ───────────────────────────────────
    /// Key exceeds the maximum allowed size.
    #[error("key too large: {size} bytes (max: {max})")]
    KeyTooLarge { size: usize, max: usize },

    /// Value exceeds the maximum allowed size.
    #[error("value too large: {size} bytes (max: {max})")]
    ValueTooLarge { size: usize, max: usize },

    /// Key was not found in the store.
    #[error("key not found")]
    KeyNotFound,

    // ── Engine State ────────────────────────────────────────────
    /// The engine has been shut down and cannot process requests.
    #[error("engine is shut down")]
    Shutdown,

    /// Write stall: too many L0 files or pending compactions.
    #[error("write stall: {reason}")]
    WriteStall { reason: String },

    /// MemTable is full and cannot accept more writes.
    #[error("memtable is full ({size} bytes, max: {max})")]
    MemTableFull { size: usize, max: usize },

    // ── Compaction ──────────────────────────────────────────────
    /// Compaction failed for the specified reason.
    #[error("compaction failed: {0}")]
    CompactionFailed(String),

    /// Too many levels in the LSM tree.
    #[error("maximum levels exceeded: {0}")]
    MaxLevelsExceeded(usize),

    // ── Serialization ───────────────────────────────────────────
    /// Failed to serialize data.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Failed to deserialize data.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    // ── Configuration ───────────────────────────────────────────
    /// Invalid configuration parameter.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Data directory does not exist or is inaccessible.
    #[error("data directory error: {0}")]
    DataDirectory(String),

    // ── Catch-all ───────────────────────────────────────────────
    /// Internal error that doesn't fit other categories.
    #[error("internal error: {0}")]
    Internal(String),
}

impl KvError {
    /// Returns `true` if this error is retryable (e.g., transient I/O).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            KvError::Io(_) | KvError::WriteStall { .. } | KvError::MemTableFull { .. }
        )
    }

    /// Returns `true` if this indicates data corruption.
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            KvError::ChecksumMismatch { .. }
                | KvError::InvalidMagic { .. }
                | KvError::CorruptedBlock { .. }
                | KvError::CorruptedTable { .. }
                | KvError::CorruptedManifest(_)
        )
    }
}
