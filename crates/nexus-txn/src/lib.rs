//! # nexus-txn
//!
//! Transaction manager for NexusDB.
//!
//! This crate provides comprehensive transaction management with:
//!
//! - **Transaction Lifecycle**: Begin, commit, and abort operations with
//!   proper ACID guarantees.
//!
//! - **Isolation Levels**: Support for Read Committed, Snapshot Isolation,
//!   and Serializable Snapshot Isolation (SSI).
//!
//! - **Lock Management**: Row-level locking with shared (S) and exclusive (X)
//!   modes, plus intention locks for hierarchical locking.
//!
//! - **Deadlock Detection**: Wait-for graph based cycle detection with
//!   intelligent victim selection.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────┐
//! │                    TransactionManager                          │
//! │                           │                                    │
//! │    ┌──────────────────────┼──────────────────────┐            │
//! │    │                      │                      │            │
//! │    ▼                      ▼                      ▼            │
//! │ ┌──────────┐    ┌─────────────────┐    ┌──────────────────┐  │
//! │ │ Clock    │    │  VersionStore   │    │   LockManager    │  │
//! │ │  (HLC)   │    │   (from MVCC)   │    │                  │  │
//! │ └──────────┘    └─────────────────┘    └──────────────────┘  │
//! │                          │                      │             │
//! │                          │                      │             │
//! │                          ▼                      ▼             │
//! │                 ┌─────────────────┐    ┌──────────────────┐  │
//! │                 │ SnapshotManager │    │ DeadlockDetector │  │
//! │                 │   (from MVCC)   │    │                  │  │
//! │                 └─────────────────┘    └──────────────────┘  │
//! └────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example Usage
//!
//! ```ignore
//! use nexus_txn::{TransactionManager, TransactionManagerConfig};
//! use nexus_mvcc::{HybridLogicalClock, VersionStore, IsolationLevel};
//! use std::sync::Arc;
//! use bytes::Bytes;
//!
//! // Create the transaction manager
//! let clock = Arc::new(HybridLogicalClock::new(1));
//! let store = Arc::new(VersionStore::new());
//! let tm = TransactionManager::new(clock, store);
//!
//! // Begin a transaction
//! let txn_id = tm.begin().unwrap();
//!
//! // Perform operations
//! tm.put(txn_id, 1, Bytes::from("key"), Bytes::from("value")).unwrap();
//! let value = tm.get(txn_id, 1, &Bytes::from("key")).unwrap();
//!
//! // Commit
//! tm.commit(txn_id).unwrap();
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Transaction lifecycle management.
///
/// This module provides:
/// - [`manager::TransactionManager`]: Main coordinator for transactions
/// - [`manager::Transaction`]: Transaction handle and state
/// - [`manager::TransactionState`]: Transaction lifecycle states
pub mod manager;

/// Lock table implementation.
///
/// This module provides:
/// - [`lock::LockManager`]: Manages all locks
/// - [`lock::LockMode`]: Shared, Exclusive, and intention locks
/// - [`lock::ResourceId`]: Identifies lockable resources
pub mod lock;

/// Deadlock detection.
///
/// This module provides:
/// - [`deadlock::WaitForGraph`]: Tracks transaction dependencies
/// - [`deadlock::DeadlockDetector`]: Detects and resolves deadlocks
/// - [`deadlock::DeadlockInfo`]: Information about detected deadlocks
pub mod deadlock;

// Re-export commonly used types

pub use manager::{
    Transaction, TransactionError, TransactionManager, TransactionManagerConfig, TransactionState,
    TransactionStats, TxnResult,
};

pub use lock::{
    LockInfo, LockManager, LockManagerConfig, LockMode, LockRequest, LockResult, LockStats,
    ResourceId,
};

pub use deadlock::{DeadlockDetector, DeadlockInfo, DeadlockStats, WaitForGraph, WfgNode};
