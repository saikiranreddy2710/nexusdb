//! Storage error types.

use std::fmt;

/// Storage error type.
#[derive(Debug)]
pub enum StorageError {
    /// Table not found.
    TableNotFound(String),
    /// Table already exists.
    TableExists(String),
    /// Column not found.
    ColumnNotFound(String),
    /// Primary key violation.
    PrimaryKeyViolation(String),
    /// Encoding/decoding error.
    EncodingError(String),
    /// Storage engine error.
    EngineError(String),
    /// Schema mismatch.
    SchemaMismatch(String),
    /// Invalid operation.
    InvalidOperation(String),
    /// Internal error.
    Internal(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::TableNotFound(name) => write!(f, "Table not found: {}", name),
            StorageError::TableExists(name) => write!(f, "Table already exists: {}", name),
            StorageError::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            StorageError::PrimaryKeyViolation(msg) => write!(f, "Primary key violation: {}", msg),
            StorageError::EncodingError(msg) => write!(f, "Encoding error: {}", msg),
            StorageError::EngineError(msg) => write!(f, "Storage engine error: {}", msg),
            StorageError::SchemaMismatch(msg) => write!(f, "Schema mismatch: {}", msg),
            StorageError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            StorageError::Internal(msg) => write!(f, "Internal storage error: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<nexus_storage::sagetree::SageTreeError> for StorageError {
    fn from(e: nexus_storage::sagetree::SageTreeError) -> Self {
        StorageError::EngineError(e.to_string())
    }
}

/// Storage result type.
pub type StorageResult<T> = Result<T, StorageError>;
