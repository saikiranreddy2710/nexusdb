//! Error types for the client library.

use std::fmt;
use thiserror::Error;

/// Client error type.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Connection failed.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Connection timeout.
    #[error("connection timeout after {0}ms")]
    ConnectionTimeout(u64),

    /// Connection closed.
    #[error("connection closed")]
    ConnectionClosed,

    /// Query execution failed.
    #[error("query failed: {0}")]
    QueryFailed(String),

    /// Transaction error.
    #[error("transaction error: {0}")]
    TransactionError(String),

    /// Transaction already active.
    #[error("transaction already active")]
    TransactionActive,

    /// No transaction active.
    #[error("no transaction active")]
    NoTransaction,

    /// Pool exhausted.
    #[error("connection pool exhausted")]
    PoolExhausted,

    /// Pool timeout.
    #[error("pool acquisition timeout after {0}ms")]
    PoolTimeout(u64),

    /// Invalid configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// Network error.
    #[error("network error: {0}")]
    NetworkError(String),

    /// Server error.
    #[error("server error: {0}")]
    ServerError(String),

    /// Authentication failed.
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Operation cancelled.
    #[error("operation cancelled")]
    Cancelled,

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for client operations.
pub type ClientResult<T> = Result<T, ClientError>;

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected.
    Disconnected,
    /// Connection in progress.
    Connecting,
    /// Connected and ready.
    Connected,
    /// In an active transaction.
    InTransaction,
    /// Connection failed.
    Failed,
    /// Connection closed.
    Closed,
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "disconnected"),
            ConnectionState::Connecting => write!(f, "connecting"),
            ConnectionState::Connected => write!(f, "connected"),
            ConnectionState::InTransaction => write!(f, "in_transaction"),
            ConnectionState::Failed => write!(f, "failed"),
            ConnectionState::Closed => write!(f, "closed"),
        }
    }
}
