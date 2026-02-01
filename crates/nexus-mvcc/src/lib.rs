//! # nexus-mvcc
//!
//! Multi-version concurrency control (MVCC) implementation for NexusDB.
//!
//! This crate provides the foundation for snapshot isolation and serializable
//! transactions in NexusDB. It implements:
//!
//! - **Hybrid Logical Clock (HLC)**: Provides monotonic, causally-ordered timestamps
//!   that combine physical time with logical counters for distributed coordination.
//!
//! - **Version Chain Management**: Maintains multiple versions of each record,
//!   allowing concurrent readers to see consistent snapshots without blocking writers.
//!
//! - **Snapshot Isolation**: Each transaction sees a consistent view of the database
//!   as of its start time, with support for Read Committed, Snapshot Isolation,
//!   and Serializable Snapshot Isolation (SSI).
//!
//! - **Epoch-based Garbage Collection**: Safely reclaims old versions that are
//!   no longer visible to any active transaction.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         Transaction                              │
//! │                            │                                     │
//! │    ┌───────────────────────┼───────────────────────┐            │
//! │    │                       ▼                       │            │
//! │    │              SnapshotManager                  │            │
//! │    │     (tracks active txns, isolation levels)    │            │
//! │    │                       │                       │            │
//! │    │    ┌──────────────────┼──────────────────┐   │            │
//! │    │    │                  ▼                  │   │            │
//! │    │    │            VersionStore             │   │            │
//! │    │    │    (manages version chains)         │   │            │
//! │    │    │                  │                  │   │            │
//! │    │    │    ┌─────────────┴─────────────┐   │   │            │
//! │    │    │    ▼                           ▼   │   │            │
//! │    │    │  VersionChain            VersionChain  │            │
//! │    │    │  (key: "a")              (key: "b")    │            │
//! │    │    │    │                         │        │            │
//! │    │    │    ▼                         ▼        │            │
//! │    │    │  [v3]→[v2]→[v1]        [v2]→[v1]     │            │
//! │    │    └────────────────────────────────────────┘            │
//! │    │                       │                                   │
//! │    │    ┌──────────────────┼──────────────────┐               │
//! │    │    │                  ▼                  │               │
//! │    │    │           EpochManager              │               │
//! │    │    │    (tracks reader epochs)           │               │
//! │    │    │                  │                  │               │
//! │    │    │                  ▼                  │               │
//! │    │    │         GarbageCollector            │               │
//! │    │    │    (reclaims old versions)          │               │
//! │    │    └─────────────────────────────────────┘               │
//! │    └───────────────────────────────────────────────────────────┘
//! │                            │                                     │
//! │                            ▼                                     │
//! │                   HybridLogicalClock                             │
//! │              (provides HLC timestamps)                           │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example Usage
//!
//! ```ignore
//! use nexus_mvcc::{
//!     hlc::HybridLogicalClock,
//!     version::VersionStore,
//!     snapshot::{SnapshotManager, IsolationLevel},
//!     gc::{EpochManager, GarbageCollector},
//! };
//! use std::sync::Arc;
//! use bytes::Bytes;
//!
//! // Create the clock
//! let clock = HybridLogicalClock::new(1); // node_id = 1
//!
//! // Create stores
//! let version_store = Arc::new(VersionStore::new());
//! let snapshot_manager = SnapshotManager::new();
//! let epoch_manager = Arc::new(EpochManager::new());
//! let gc = GarbageCollector::new(epoch_manager.clone(), version_store.clone());
//!
//! // Begin a transaction
//! let start_ts = clock.now();
//! let txn_info = snapshot_manager.begin_txn(start_ts, IsolationLevel::SnapshotIsolation);
//!
//! // Create a version
//! let key = Bytes::from("user:1");
//! let version_id = version_store.create_version(
//!     key.clone(),
//!     start_ts,
//!     txn_info.txn_id,
//!     Bytes::from("Alice"),
//! );
//!
//! // Commit
//! let commit_ts = clock.now();
//! version_store.commit(&key, version_id, commit_ts);
//! snapshot_manager.commit_txn(txn_info.txn_id);
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Hybrid Logical Clock for causally-ordered timestamps.
///
/// The HLC provides monotonic timestamps that combine:
/// - Physical time (wall clock)
/// - Logical counter (for events at same physical time)
/// - Node ID (for distributed tie-breaking)
pub mod hlc;

/// Version chain storage and management.
///
/// This module provides:
/// - [`version::Version`]: A single versioned record
/// - [`version::VersionChain`]: All versions of a key
/// - [`version::VersionStore`]: Storage for all version chains
pub mod version;

/// Epoch-based garbage collection.
///
/// This module provides safe memory reclamation:
/// - [`gc::Epoch`]: Monotonic epoch counter
/// - [`gc::EpochGuard`]: RAII guard for pinning epochs
/// - [`gc::EpochManager`]: Tracks active epochs
/// - [`gc::GarbageCollector`]: Collects old versions
pub mod gc;

/// Snapshot isolation implementation.
///
/// This module provides:
/// - [`snapshot::Snapshot`]: Consistent view at a timestamp
/// - [`snapshot::SnapshotManager`]: Manages active transactions
/// - [`snapshot::IsolationLevel`]: Read Committed, SI, SSI
pub mod snapshot;

// Re-export commonly used types at the crate root for convenience

pub use gc::{Epoch, EpochGuard, EpochManager, GarbageCollector, GcResult};
pub use hlc::{HlcError, HlcResult, HlcTimestamp, HybridLogicalClock};
pub use snapshot::{ConflictError, IsolationLevel, Snapshot, SnapshotManager, TransactionInfo};
pub use version::{Version, VersionChain, VersionId, VersionState, VersionStore};
