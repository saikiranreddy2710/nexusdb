//! Query executor for physical plans.
//!
//! This module executes physical plans and produces results. It uses a
//! pull-based iterator model where operators produce record batches on demand.
//!
//! # Architecture
//!
//! The executor consists of:
//!
//! - **Value**: Runtime values (integers, strings, etc.)
//! - **Row**: A single row of values
//! - **RecordBatch**: A batch of rows for vectorized processing
//! - **Operator**: Executable operators that produce record batches
//! - **ExprEvaluator**: Evaluates physical expressions on data
//!
//! # Execution Model
//!
//! Operators use a pull-based iterator model:
//!
//! ```ignore
//! let mut operator = create_scan_operator(...);
//! while let Some(batch) = operator.next_batch()? {
//!     // Process batch
//! }
//! ```
//!
//! # Example
//!
//! ```ignore
//! use nexus_sql::executor::{QueryExecutor, ExecutionResult};
//! use nexus_sql::physical::PhysicalPlan;
//!
//! let executor = QueryExecutor::new(ctx);
//! let result = executor.execute(&physical_plan)?;
//! for batch in result.batches {
//!     println!("Got {} rows", batch.num_rows());
//! }
//! ```

mod batch;
mod engine;
mod evaluator;
mod operators;
mod row;
mod value;

pub use batch::*;
pub use engine::*;
pub use evaluator::*;
pub use operators::*;
pub use row::*;
pub use value::*;
