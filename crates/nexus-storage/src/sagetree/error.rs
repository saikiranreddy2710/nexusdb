//! Error types for the SageTree storage engine.
//!
//! This module defines all error types that can occur during SageTree operations.

use nexus_common::types::PageId;
use thiserror::Error;

/// Result type for SageTree operations.
pub type SageTreeResult<T> = Result<T, SageTreeError>;

/// Errors that can occur in SageTree operations.
#[derive(Debug, Error)]
pub enum SageTreeError {
    /// Key not found in the tree.
    #[error("key not found")]
    KeyNotFound,

    /// Duplicate key found during insert.
    #[error("duplicate key")]
    DuplicateKey,

    /// Key is too large.
    #[error("key too large: {size} bytes (max: {max})")]
    KeyTooLarge {
        /// Actual size of the key.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },

    /// Value is too large.
    #[error("value too large: {size} bytes (max: {max})")]
    ValueTooLarge {
        /// Actual size of the value.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },

    /// Page not found in buffer pool.
    #[error("page not found: {0}")]
    PageNotFound(PageId),

    /// Page is corrupted or invalid.
    #[error("corrupted page: {page_id}, reason: {reason}")]
    CorruptedPage {
        /// The corrupted page ID.
        page_id: PageId,
        /// Description of corruption.
        reason: String,
    },

    /// Invalid node type encountered.
    #[error("invalid node type: expected {expected}, found {found}")]
    InvalidNodeType {
        /// Expected node type.
        expected: &'static str,
        /// Found node type.
        found: String,
    },

    /// Delta chain is too long and needs consolidation.
    #[error("delta chain too long: {length} (max: {max})")]
    DeltaChainTooLong {
        /// Current chain length.
        length: usize,
        /// Maximum allowed length.
        max: usize,
    },

    /// Node is full and cannot accept more entries.
    #[error("node overflow: page {page_id} is full")]
    NodeOverflow {
        /// The full node's page ID.
        page_id: PageId,
    },

    /// Node is underflow and needs merging or rebalancing.
    #[error("node underflow: page {page_id} is below minimum fill")]
    NodeUnderflow {
        /// The underflow node's page ID.
        page_id: PageId,
    },

    /// Tree structure is invalid.
    #[error("tree structure error: {0}")]
    TreeStructureError(String),

    /// Cursor is invalid or exhausted.
    #[error("cursor is invalid or exhausted")]
    InvalidCursor,

    /// Buffer pool error.
    #[error("buffer pool error: {0}")]
    BufferPool(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Tree is empty.
    #[error("tree is empty")]
    EmptyTree,

    /// Concurrent modification detected.
    #[error("concurrent modification detected at page {0}")]
    ConcurrentModification(PageId),

    /// Invalid page ID provided.
    #[error("invalid page ID")]
    InvalidPageId,

    /// Operation not supported.
    #[error("operation not supported: {0}")]
    NotSupported(String),
}

impl SageTreeError {
    /// Creates a new corrupted page error.
    pub fn corrupted_page(page_id: PageId, reason: impl Into<String>) -> Self {
        Self::CorruptedPage {
            page_id,
            reason: reason.into(),
        }
    }

    /// Creates a new tree structure error.
    pub fn structure_error(msg: impl Into<String>) -> Self {
        Self::TreeStructureError(msg.into())
    }

    /// Creates a new serialization error.
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization(msg.into())
    }

    /// Creates a new deserialization error.
    pub fn deserialization(msg: impl Into<String>) -> Self {
        Self::Deserialization(msg.into())
    }

    /// Creates a new buffer pool error.
    pub fn buffer_pool(msg: impl Into<String>) -> Self {
        Self::BufferPool(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = SageTreeError::KeyNotFound;
        assert_eq!(err.to_string(), "key not found");

        let err = SageTreeError::KeyTooLarge {
            size: 20000,
            max: 16384,
        };
        assert!(err.to_string().contains("20000"));
        assert!(err.to_string().contains("16384"));

        let err = SageTreeError::corrupted_page(PageId::new(42), "bad checksum");
        assert!(err.to_string().contains("42"));
        assert!(err.to_string().contains("bad checksum"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: SageTreeError = io_err.into();
        assert!(matches!(err, SageTreeError::Io(_)));
    }
}
