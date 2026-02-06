//! # nexus-client
//!
//! Client library for NexusDB.
//!
//! This crate provides a complete client library for connecting to and
//! interacting with NexusDB instances. It includes:
//!
//! - **Connection Management**: Establish and manage connections to NexusDB
//! - **Connection Pooling**: Efficient connection reuse with configurable pools
//! - **Query Building**: Fluent API for constructing SQL queries
//! - **Transaction Support**: RAII-style transaction management
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use nexus_client::{Client, ClientConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a client
//!     let client = Client::new(ClientConfig::default());
//!     client.connect().await?;
//!
//!     // Execute a query
//!     let result = client.execute("SELECT * FROM users").await?;
//!     println!("Found {} rows", result.row_count());
//!
//!     // Use the query builder
//!     let result = client.query()
//!         .select(&["id", "name"])
//!         .from("users")
//!         .where_eq("active", true)
//!         .limit(10)
//!         .execute()
//!         .await?;
//!
//!     // Use transactions
//!     let txn = client.begin().await?;
//!     txn.execute("INSERT INTO users (name) VALUES ('Alice')").await?;
//!     txn.commit().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Connection Pooling
//!
//! ```rust,ignore
//! use nexus_client::{ConnectionPool, PoolConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = PoolConfig::new()
//!         .min_connections(5)
//!         .max_connections(20);
//!
//!     let pool = ConnectionPool::new(config)?;
//!     pool.initialize().await?;
//!
//!     // Acquire a connection
//!     let conn = pool.acquire().await?;
//!     conn.execute("SELECT 1").await?;
//!
//!     // Connection is returned to pool when dropped
//!     Ok(())
//! }
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Error types.
pub mod error;

/// Client connection.
pub mod client;

/// Connection pool.
pub mod pool;

/// Query builder.
pub mod query;

/// Transaction handle.
pub mod transaction;

// Re-exports
pub use client::{Client, ClientConfig, ClientStats, FromValue, QueryResult, Value};
pub use error::{ClientError, ClientResult, ConnectionState};
pub use pool::{ConnectionPool, PoolConfig, PoolStats, PooledClient};
pub use query::{PreparedStatement, QueryBuilder};
pub use transaction::{
    AccessMode, IsolationLevel, Savepoint, Transaction, TransactionExt, TransactionOptions,
};
