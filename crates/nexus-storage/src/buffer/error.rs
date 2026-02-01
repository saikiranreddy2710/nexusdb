//! Buffer pool errors.

use std::io;

use nexus_common::types::PageId;
use thiserror::Error;

/// Result type for buffer pool operations.
pub type BufferResult<T> = Result<T, BufferError>;

/// Errors that can occur during buffer pool operations.
#[derive(Debug, Error)]
#[allow(missing_docs)] // Fields are documented by variant docs
pub enum BufferError {
    /// No free frames available for eviction.
    #[error("no free frames available, all pages are pinned")]
    NoFreeFrames,

    /// Page not found in buffer pool.
    #[error("page {page_id} not found in buffer pool")]
    PageNotFound { page_id: PageId },

    /// Page not found on disk.
    #[error("page {page_id} not found on disk")]
    PageNotOnDisk { page_id: PageId },

    /// Invalid page ID.
    #[error("invalid page ID: {page_id}")]
    InvalidPageId { page_id: PageId },

    /// Invalid frame ID.
    #[error("invalid frame ID: {frame_id}")]
    InvalidFrameId { frame_id: usize },

    /// Frame is not pinned (cannot unpin).
    #[error("frame {frame_id} is not pinned")]
    NotPinned { frame_id: usize },

    /// Page is already in the buffer pool.
    #[error("page {page_id} is already in buffer pool")]
    PageAlreadyExists { page_id: PageId },

    /// I/O error during page read/write.
    #[error("I/O error: {source}")]
    Io {
        #[from]
        source: io::Error,
    },

    /// File I/O error.
    #[error("file I/O error: {0}")]
    FileIo(#[from] crate::file::IoError),

    /// Buffer pool is shutting down.
    #[error("buffer pool is shutting down")]
    ShuttingDown,

    /// Configuration error.
    #[error("configuration error: {message}")]
    Config { message: String },

    /// Checksum mismatch when reading page.
    #[error("checksum mismatch for page {page_id}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        page_id: PageId,
        expected: u32,
        actual: u32,
    },

    /// Page corruption detected.
    #[error("page {page_id} is corrupted: {reason}")]
    PageCorrupted { page_id: PageId, reason: String },
}

impl BufferError {
    /// Creates a configuration error.
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// Creates a page not found error.
    pub fn page_not_found(page_id: PageId) -> Self {
        Self::PageNotFound { page_id }
    }

    /// Creates an invalid frame ID error.
    pub fn invalid_frame(frame_id: usize) -> Self {
        Self::InvalidFrameId { frame_id }
    }

    /// Returns true if this is a transient error that can be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::NoFreeFrames)
    }

    /// Returns true if this is a fatal error.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::ChecksumMismatch { .. } | Self::PageCorrupted { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = BufferError::page_not_found(PageId::new(42));
        assert!(matches!(
            err,
            BufferError::PageNotFound {
                page_id
            } if page_id == PageId::new(42)
        ));
    }

    #[test]
    fn test_is_retryable() {
        assert!(BufferError::NoFreeFrames.is_retryable());
        assert!(!BufferError::page_not_found(PageId::new(1)).is_retryable());
    }

    #[test]
    fn test_is_fatal() {
        let err = BufferError::ChecksumMismatch {
            page_id: PageId::new(1),
            expected: 123,
            actual: 456,
        };
        assert!(err.is_fatal());

        assert!(!BufferError::NoFreeFrames.is_fatal());
    }
}
