//! NexusDB Performance Benchmarks
//!
//! This crate contains benchmarks for various NexusDB components:
//! - Storage engine (SageTree B-tree)
//! - Buffer pool
//! - SQL parsing, planning, and execution
//! - End-to-end database operations
//!
//! Run benchmarks with:
//! ```bash
//! cargo bench -p nexus-bench
//! ```

pub mod utils;
