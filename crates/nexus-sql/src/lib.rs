//! # nexus-sql
//!
//! SQL parser, query planner, and executor for NexusDB.
//!
//! This crate implements:
//! - SQL parsing (PostgreSQL-compatible)
//! - Logical query planning
//! - Query optimization
//! - Physical plan generation
//! - Query execution

#![warn(missing_docs)]
#![warn(clippy::all)]

/// SQL tokenizer and parser
pub mod parser;

/// Logical plan representation
pub mod logical;

/// Query optimizer
pub mod optimizer;

/// Physical plan generation
pub mod physical;

/// Query execution
pub mod executor;
