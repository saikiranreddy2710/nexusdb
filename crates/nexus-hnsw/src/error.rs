//! Error types for the HNSW index.

/// HNSW-specific errors.
#[derive(Debug, thiserror::Error)]
pub enum HnswError {
    /// Vector dimensions do not match the index configuration.
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Expected number of dimensions.
        expected: usize,
        /// Actual number of dimensions provided.
        got: usize,
    },

    /// A vector with this ID already exists.
    #[error("duplicate vector id: {0}")]
    DuplicateId(u64),

    /// Vector ID not found in the index.
    #[error("vector id not found: {0}")]
    NotFound(u64),

    /// Invalid number of dimensions (must be > 0).
    #[error("invalid dimensions: {0}")]
    InvalidDimensions(u32),

    /// Invalid configuration parameter.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Serialization / deserialization error.
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// I/O error (for persistence).
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Result type alias.
pub type HnswResult<T> = std::result::Result<T, HnswError>;
