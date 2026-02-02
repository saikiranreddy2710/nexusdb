//! Physical plan generation and execution.
//!
//! This module converts logical plans into physical execution plans,
//! selecting concrete algorithms for each operation (e.g., HashJoin vs
//! MergeJoin, SeqScan vs IndexScan).
//!
//! # Architecture
//!
//! The physical layer consists of:
//!
//! - **PhysicalExpr**: Expressions that can be evaluated on data batches
//! - **PhysicalOperator**: Concrete execution operators with specific algorithms
//! - **PhysicalPlan**: Tree of physical operators ready for execution
//! - **ExecutionContext**: Runtime configuration and resources
//! - **PhysicalPlanner**: Converts logical plans to physical plans
//!
//! # Physical Operators
//!
//! | Logical Operator | Physical Options |
//! |-----------------|------------------|
//! | Scan | SeqScan, IndexScan |
//! | Join | HashJoin, MergeJoin, NestedLoopJoin |
//! | Aggregate | HashAggregate, SortAggregate |
//! | Sort | ExternalSort, InMemorySort |
//!
//! # Example
//!
//! ```ignore
//! use nexus_sql::physical::{PhysicalPlanner, ExecutionContext};
//! use nexus_sql::logical::LogicalPlan;
//!
//! let ctx = ExecutionContext::default();
//! let planner = PhysicalPlanner::new(&ctx);
//! let physical_plan = planner.create_physical_plan(&logical_plan)?;
//! ```

mod expr;
mod operator;
mod plan;
mod planner;
mod context;

pub use expr::*;
pub use operator::*;
pub use plan::*;
pub use planner::*;
pub use context::*;
