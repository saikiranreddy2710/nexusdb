//! Logical query plan representation.
//!
//! This module defines the logical plan tree used for query optimization.
//! Logical plans represent the abstract operations without specifying
//! physical implementations (e.g., "Join" vs "HashJoin" or "MergeJoin").

mod builder;
mod expr;
mod operator;
mod plan;
mod schema;

pub use builder::*;
pub use expr::*;
pub use operator::*;
pub use plan::*;
pub use schema::*;
