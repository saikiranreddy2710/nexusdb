//! # nexus-server
//!
//! Network server and database engine for NexusDB.
//!
//! This crate provides:
//!
//! - **Database Engine**: Unified API that wires together SQL execution,
//!   transactions, MVCC, and storage. This is the main entry point for
//!   using NexusDB as an embedded database.
//!
//! - **Session Management**: Connection-level state including transactions,
//!   prepared statements, and session variables.
//!
//! - **gRPC Server**: Network interface for distributed deployments.
//!
//! # Quick Start
//!
//! ```ignore
//! use nexus_server::database::Database;
//!
//! // Create an in-memory database
//! let db = Database::open_memory()?;
//!
//! // Execute SQL directly
//! db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")?;
//! db.execute("INSERT INTO users VALUES (1, 'Alice')")?;
//!
//! let result = db.execute("SELECT * FROM users")?;
//! println!("{}", result.display());
//!
//! // Or use sessions for explicit transaction control
//! let session = db.create_session();
//! // ... use session ...
//! db.close_session(session);
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Database engine - the main entry point for NexusDB.
///
/// This module provides the unified `Database` struct that combines
/// SQL parsing, query execution, transaction management, and storage.
pub mod database;

/// gRPC service implementation (placeholder).
pub mod grpc;

/// Connection pool (placeholder).
pub mod connection;

/// Request routing (placeholder).
pub mod router;

/// Server configuration.
pub mod config;

// Re-export commonly used types
pub use database::{
    Database, DatabaseConfig, DatabaseError, DatabaseResult, DatabaseStats, ExecuteResult, Session,
    SessionConfig, SessionId, SessionState, StatementResult,
};
