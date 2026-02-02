//! Database error types.

use std::fmt;

use nexus_sql::storage::StorageError;

/// Database errors.
#[derive(Debug)]
pub enum DatabaseError {
    /// SQL parsing error.
    ParseError(String),
    /// Query planning error.
    PlanError(String),
    /// Query execution error.
    ExecutionError(String),
    /// Storage error.
    StorageError(StorageError),
    /// Transaction error.
    TransactionError(String),
    /// Session error.
    SessionError(String),
    /// Invalid state error.
    InvalidState(String),
    /// Not implemented.
    NotImplemented(String),
    /// Internal error.
    Internal(String),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseError::ParseError(msg) => write!(f, "parse error: {}", msg),
            DatabaseError::PlanError(msg) => write!(f, "plan error: {}", msg),
            DatabaseError::ExecutionError(msg) => write!(f, "execution error: {}", msg),
            DatabaseError::StorageError(e) => write!(f, "storage error: {}", e),
            DatabaseError::TransactionError(msg) => write!(f, "transaction error: {}", msg),
            DatabaseError::SessionError(msg) => write!(f, "session error: {}", msg),
            DatabaseError::InvalidState(msg) => write!(f, "invalid state: {}", msg),
            DatabaseError::NotImplemented(msg) => write!(f, "not implemented: {}", msg),
            DatabaseError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for DatabaseError {}

impl From<StorageError> for DatabaseError {
    fn from(e: StorageError) -> Self {
        DatabaseError::StorageError(e)
    }
}

impl From<nexus_sql::parser::ParseError> for DatabaseError {
    fn from(e: nexus_sql::parser::ParseError) -> Self {
        DatabaseError::ParseError(e.to_string())
    }
}

impl From<nexus_sql::executor::ExecutionError> for DatabaseError {
    fn from(e: nexus_sql::executor::ExecutionError) -> Self {
        DatabaseError::ExecutionError(e.to_string())
    }
}

/// Database result type.
pub type DatabaseResult<T> = Result<T, DatabaseError>;
