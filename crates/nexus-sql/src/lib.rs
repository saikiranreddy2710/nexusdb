//! # nexus-sql
//!
//! SQL parser and query planner for NexusDB.
//!
//! This crate implements:
//! - SQL parsing (PostgreSQL-compatible)
//! - Logical query planning
//! - Query optimization
//! - Physical plan generation

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
