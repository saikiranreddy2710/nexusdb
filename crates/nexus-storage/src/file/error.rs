//! I/O error types for the file module.

use std::io;
use std::path::PathBuf;

use thiserror::Error;

/// Result type for I/O operations.
pub type IoResult<T> = Result<T, IoError>;

/// Errors that can occur during file I/O operations.
#[derive(Debug, Error)]
#[allow(missing_docs)] // Fields are documented by variant docs
pub enum IoError {
    /// Standard I/O error.
    #[error("I/O error: {source}")]
    Io {
        #[from]
        source: io::Error,
    },

    /// File not found.
    #[error("file not found: {path}")]
    NotFound { path: PathBuf },

    /// Permission denied.
    #[error("permission denied: {path}")]
    PermissionDenied { path: PathBuf },

    /// File already exists.
    #[error("file already exists: {path}")]
    AlreadyExists { path: PathBuf },

    /// Invalid alignment for direct I/O.
    #[error("invalid alignment: expected {expected}, got {actual}")]
    InvalidAlignment { expected: usize, actual: usize },

    /// Buffer too small for operation.
    #[error("buffer too small: need {needed} bytes, have {available}")]
    BufferTooSmall { needed: usize, available: usize },

    /// Invalid offset for operation.
    #[error("invalid offset: {offset} exceeds file size {file_size}")]
    InvalidOffset { offset: u64, file_size: u64 },

    /// Operation would block (for non-blocking I/O).
    #[error("operation would block")]
    WouldBlock,

    /// I/O operation was interrupted.
    #[error("operation interrupted")]
    Interrupted,

    /// Short read/write (less data than expected).
    #[error("short {operation}: expected {expected} bytes, got {actual}")]
    ShortIo {
        operation: &'static str,
        expected: usize,
        actual: usize,
    },

    /// File is not open.
    #[error("file not open")]
    NotOpen,

    /// io_uring specific error.
    #[error("io_uring error: {message}")]
    UringError { message: String },

    /// Direct I/O not supported.
    #[error("direct I/O not supported on this platform/filesystem")]
    DirectIoNotSupported,

    /// Invalid operation for file mode.
    #[error("invalid operation: {operation} not allowed in {mode} mode")]
    InvalidOperation {
        operation: &'static str,
        mode: &'static str,
    },
}

impl IoError {
    /// Creates a new NotFound error.
    pub fn not_found(path: impl Into<PathBuf>) -> Self {
        Self::NotFound { path: path.into() }
    }

    /// Creates a new PermissionDenied error.
    pub fn permission_denied(path: impl Into<PathBuf>) -> Self {
        Self::PermissionDenied { path: path.into() }
    }

    /// Creates a new AlreadyExists error.
    pub fn already_exists(path: impl Into<PathBuf>) -> Self {
        Self::AlreadyExists { path: path.into() }
    }

    /// Creates an InvalidAlignment error.
    pub fn invalid_alignment(expected: usize, actual: usize) -> Self {
        Self::InvalidAlignment { expected, actual }
    }

    /// Creates a ShortIo error for reads.
    pub fn short_read(expected: usize, actual: usize) -> Self {
        Self::ShortIo {
            operation: "read",
            expected,
            actual,
        }
    }

    /// Creates a ShortIo error for writes.
    pub fn short_write(expected: usize, actual: usize) -> Self {
        Self::ShortIo {
            operation: "write",
            expected,
            actual,
        }
    }

    /// Returns true if this is a retryable error.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::WouldBlock | Self::Interrupted => true,
            Self::Io { source } => {
                source.kind() == io::ErrorKind::Interrupted
                    || source.kind() == io::ErrorKind::WouldBlock
            }
            _ => false,
        }
    }

    /// Returns true if this is a "not found" error.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
            || matches!(self, Self::Io { source } if source.kind() == io::ErrorKind::NotFound)
    }

    /// Returns true if this is a permission error.
    pub fn is_permission_denied(&self) -> bool {
        matches!(self, Self::PermissionDenied { .. })
            || matches!(self, Self::Io { source } if source.kind() == io::ErrorKind::PermissionDenied)
    }

    /// Converts from std::io::Error with path context.
    pub fn from_io_with_path(err: io::Error, path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        match err.kind() {
            io::ErrorKind::NotFound => Self::NotFound { path },
            io::ErrorKind::PermissionDenied => Self::PermissionDenied { path },
            io::ErrorKind::AlreadyExists => Self::AlreadyExists { path },
            _ => Self::Io { source: err },
        }
    }
}

impl From<IoError> for io::Error {
    fn from(err: IoError) -> Self {
        match err {
            IoError::Io { source } => source,
            IoError::NotFound { path } => io::Error::new(
                io::ErrorKind::NotFound,
                format!("not found: {}", path.display()),
            ),
            IoError::PermissionDenied { path } => io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("permission denied: {}", path.display()),
            ),
            IoError::AlreadyExists { path } => io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("already exists: {}", path.display()),
            ),
            IoError::WouldBlock => io::Error::new(io::ErrorKind::WouldBlock, "would block"),
            IoError::Interrupted => io::Error::new(io::ErrorKind::Interrupted, "interrupted"),
            other => io::Error::new(io::ErrorKind::Other, other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_creation() {
        let err = IoError::not_found("/tmp/test.db");
        assert!(err.is_not_found());

        let err = IoError::permission_denied("/tmp/test.db");
        assert!(err.is_permission_denied());
    }

    #[test]
    fn test_io_error_from_std() {
        let std_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err: IoError = std_err.into();
        assert!(matches!(err, IoError::Io { .. }));
    }

    #[test]
    fn test_retryable() {
        let err = IoError::WouldBlock;
        assert!(err.is_retryable());

        let err = IoError::Interrupted;
        assert!(err.is_retryable());

        let err = IoError::not_found("/tmp/test.db");
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_short_io() {
        let err = IoError::short_read(100, 50);
        assert!(matches!(
            err,
            IoError::ShortIo {
                operation: "read",
                expected: 100,
                actual: 50
            }
        ));

        let err = IoError::short_write(100, 50);
        assert!(matches!(
            err,
            IoError::ShortIo {
                operation: "write",
                expected: 100,
                actual: 50
            }
        ));
    }
}
