//! # nexus-query
//!
//! Query execution engine (Photon) for NexusDB.
//!
//! This crate implements:
//! - Vectorized execution with Apache Arrow
//! - DataFusion integration
//! - Predicate pushdown
//! - Parallel query execution

#![warn(missing_docs)]
#![warn(clippy::all)]

/// Physical query operators
pub mod operator;

/// Execution context
pub mod context;

/// Expression evaluation
pub mod expression;

/// Batch processing
pub mod batch;
