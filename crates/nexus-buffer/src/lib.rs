//! # nexus-buffer
//!
//! Buffer pool manager for NexusDB.
//!
//! This crate implements a lock-free buffer pool with:
//! - Clock eviction algorithm
//! - Dirty page tracking
//! - Prefetching support
//! - NUMA-aware memory allocation

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Buffer pool implementation
pub mod pool;

/// Page handle and reference counting
pub mod handle;

/// Eviction policies
pub mod eviction;
