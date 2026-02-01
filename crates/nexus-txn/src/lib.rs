//! # nexus-txn
//!
//! Transaction manager for NexusDB.
//!
//! This crate implements:
//! - Serializable Snapshot Isolation (SSI)
//! - Distributed transactions (Percolator-style)
//! - Lock management
//! - Deadlock detection

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Transaction lifecycle management
pub mod manager;

/// Lock table implementation
pub mod lock;

/// Deadlock detection
pub mod deadlock;
