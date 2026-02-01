//! # nexus-storage
//! 
//! SageTree storage engine for NexusDB.
//! 
//! This crate implements the novel SageTree storage structure that provides:
//! - 10x less write amplification than LSM trees
//! - Fractional cascading for efficient range queries
//! - Delta chains for in-place updates
//! - Incremental merge operations

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Page layout and disk format
pub mod page;

/// File management and I/O
pub mod file;

/// SageTree implementation
pub mod sagetree;
