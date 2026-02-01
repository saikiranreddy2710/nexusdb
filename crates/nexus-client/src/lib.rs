//! # nexus-client
//!
//! Client library for NexusDB.
//!
//! This crate provides:
//! - Connection management
//! - Query execution
//! - Transaction support
//! - Connection pooling

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Client connection
pub mod client;

/// Connection pool
pub mod pool;

/// Query builder
pub mod query;

/// Transaction handle
pub mod transaction;
