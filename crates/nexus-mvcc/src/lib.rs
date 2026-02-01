//! # nexus-mvcc
//!
//! Multi-version concurrency control for NexusDB.
//!
//! This crate implements:
//! - Hybrid Logical Clock (HLC) timestamps
//! - Version chain management
//! - Garbage collection (epoch-based)
//! - Snapshot visibility

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Hybrid Logical Clock
pub mod hlc;

/// Version chain storage
pub mod version;

/// Epoch-based garbage collection
pub mod gc;

/// Snapshot isolation
pub mod snapshot;
