//! # nexus-test
//!
//! Integration tests for NexusDB.
//!
//! This crate contains:
//! - End-to-end tests
//! - Chaos testing
//! - Performance benchmarks
//! - Correctness verification

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Test utilities and helpers
pub mod utils;

/// Chaos testing framework
pub mod chaos;

/// Workload generators
pub mod workload;
