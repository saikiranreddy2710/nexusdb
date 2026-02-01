//! Query optimizer for NexusDB.
//!
//! This module implements a rule-based query optimizer that transforms
//! logical plans into more efficient forms. The optimizer applies a
//! series of optimization rules in order, each rule attempting to
//! improve the plan based on heuristics or cost estimation.
//!
//! # Architecture
//!
//! The optimizer uses a rule-based approach with the following components:
//!
//! - **OptimizerRule**: Trait for individual optimization rules
//! - **Optimizer**: Coordinates rule application and manages the optimization process
//! - **CostModel**: Estimates costs for different plan alternatives
//!
//! # Optimization Rules
//!
//! The following rules are implemented:
//!
//! - **PredicatePushdown**: Pushes filter predicates closer to data sources
//! - **ProjectionPushdown**: Eliminates unused columns early in the plan
//! - **ConstantFolding**: Evaluates constant expressions at planning time
//! - **FilterSimplification**: Simplifies boolean expressions and removes redundant conditions
//! - **LimitPushdown**: Pushes LIMIT through compatible operators
//!
//! # Example
//!
//! ```ignore
//! use nexus_sql::optimizer::{Optimizer, OptimizerConfig};
//! use nexus_sql::logical::LogicalPlan;
//!
//! let optimizer = Optimizer::new(OptimizerConfig::default());
//! let optimized_plan = optimizer.optimize(plan)?;
//! ```

mod cost;
mod rules;

pub use cost::{CostEstimate, CostModel, SimpleCostModel, Statistics};
pub use rules::*;

use std::sync::Arc;
use thiserror::Error;

use crate::logical::{LogicalOperator, LogicalPlan};

/// Errors that can occur during optimization.
#[derive(Debug, Error)]
pub enum OptimizerError {
    /// Rule application failed.
    #[error("Rule '{rule}' failed: {message}")]
    RuleFailed {
        /// Rule name.
        rule: String,
        /// Error message.
        message: String,
    },

    /// Cost estimation failed.
    #[error("Cost estimation failed: {0}")]
    CostEstimationFailed(String),

    /// Internal optimizer error.
    #[error("Internal optimizer error: {0}")]
    Internal(String),
}

/// Result type for optimizer operations.
pub type OptimizerResult<T> = Result<T, OptimizerError>;

/// Configuration for the optimizer.
#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    /// Maximum number of optimization iterations.
    pub max_iterations: usize,
    /// Whether to enable predicate pushdown.
    pub enable_predicate_pushdown: bool,
    /// Whether to enable projection pushdown.
    pub enable_projection_pushdown: bool,
    /// Whether to enable constant folding.
    pub enable_constant_folding: bool,
    /// Whether to enable filter simplification.
    pub enable_filter_simplification: bool,
    /// Whether to enable limit pushdown.
    pub enable_limit_pushdown: bool,
    /// Whether to collect optimization statistics.
    pub collect_stats: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            max_iterations: 10,
            enable_predicate_pushdown: true,
            enable_projection_pushdown: true,
            enable_constant_folding: true,
            enable_filter_simplification: true,
            enable_limit_pushdown: true,
            collect_stats: false,
        }
    }
}

impl OptimizerConfig {
    /// Creates a minimal config with all optimizations disabled.
    pub fn minimal() -> Self {
        Self {
            max_iterations: 1,
            enable_predicate_pushdown: false,
            enable_projection_pushdown: false,
            enable_constant_folding: false,
            enable_filter_simplification: false,
            enable_limit_pushdown: false,
            collect_stats: false,
        }
    }

    /// Creates a config with all optimizations enabled.
    pub fn full() -> Self {
        Self::default()
    }
}

/// Trait for optimizer rules.
///
/// Each rule attempts to transform a logical plan into a more efficient form.
/// Rules should be idempotent - applying the same rule twice should not
/// change the plan after the first application.
pub trait OptimizerRule: std::fmt::Debug + Send + Sync {
    /// Returns the name of this rule.
    fn name(&self) -> &str;

    /// Attempts to optimize the given plan.
    ///
    /// Returns the optimized plan if any transformation was made,
    /// or None if the plan was not modified.
    fn optimize(&self, plan: &LogicalPlan) -> OptimizerResult<Option<LogicalPlan>>;
}

/// Statistics about optimization.
#[derive(Debug, Clone, Default)]
pub struct OptimizationStats {
    /// Number of iterations performed.
    pub iterations: usize,
    /// Number of rules applied.
    pub rules_applied: usize,
    /// Per-rule application counts.
    pub rule_applications: Vec<(String, usize)>,
    /// Total optimization time in microseconds.
    pub total_time_us: u64,
}

/// The query optimizer.
///
/// Coordinates the application of optimization rules to transform
/// logical plans into more efficient forms.
#[derive(Debug)]
pub struct Optimizer {
    config: OptimizerConfig,
    rules: Vec<Arc<dyn OptimizerRule>>,
}

impl Optimizer {
    /// Creates a new optimizer with the given configuration.
    pub fn new(config: OptimizerConfig) -> Self {
        let mut rules: Vec<Arc<dyn OptimizerRule>> = Vec::new();

        // Add rules in order of application
        if config.enable_constant_folding {
            rules.push(Arc::new(ConstantFoldingRule));
        }
        if config.enable_filter_simplification {
            rules.push(Arc::new(FilterSimplificationRule));
        }
        if config.enable_predicate_pushdown {
            rules.push(Arc::new(PredicatePushdownRule));
        }
        if config.enable_projection_pushdown {
            rules.push(Arc::new(ProjectionPushdownRule));
        }
        if config.enable_limit_pushdown {
            rules.push(Arc::new(LimitPushdownRule));
        }

        Self { config, rules }
    }

    /// Creates an optimizer with default configuration.
    pub fn default_optimizer() -> Self {
        Self::new(OptimizerConfig::default())
    }

    /// Optimizes a logical plan.
    pub fn optimize(&self, plan: LogicalPlan) -> OptimizerResult<LogicalPlan> {
        let (optimized, _) = self.optimize_with_stats(plan)?;
        Ok(optimized)
    }

    /// Optimizes a logical plan and returns optimization statistics.
    pub fn optimize_with_stats(
        &self,
        plan: LogicalPlan,
    ) -> OptimizerResult<(LogicalPlan, OptimizationStats)> {
        let start = std::time::Instant::now();
        let mut stats = OptimizationStats::default();
        let mut rule_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        let mut current_plan = plan;

        for iteration in 0..self.config.max_iterations {
            stats.iterations = iteration + 1;
            let mut plan_changed = false;

            for rule in &self.rules {
                if let Some(new_plan) = rule.optimize(&current_plan)? {
                    current_plan = new_plan;
                    plan_changed = true;
                    stats.rules_applied += 1;
                    *rule_counts.entry(rule.name().to_string()).or_insert(0) += 1;
                }
            }

            if !plan_changed {
                // No changes made, optimization complete
                break;
            }
        }

        stats.rule_applications = rule_counts.into_iter().collect();
        stats.total_time_us = start.elapsed().as_micros() as u64;

        Ok((current_plan, stats))
    }

    /// Adds a custom optimization rule.
    pub fn add_rule(&mut self, rule: Arc<dyn OptimizerRule>) {
        self.rules.push(rule);
    }

    /// Returns the current configuration.
    pub fn config(&self) -> &OptimizerConfig {
        &self.config
    }
}

/// Rewrite a logical operator tree, applying a transformation function.
pub fn rewrite_operator<F>(op: &LogicalOperator, f: &mut F) -> Option<LogicalOperator>
where
    F: FnMut(&LogicalOperator) -> Option<LogicalOperator>,
{
    // First try to transform this node
    if let Some(new_op) = f(op) {
        return Some(new_op);
    }

    // Otherwise, try to transform children
    match op {
        LogicalOperator::Scan(_)
        | LogicalOperator::Values(_)
        | LogicalOperator::EmptyRelation(_) => None,

        LogicalOperator::Projection(proj) => {
            let new_input = rewrite_operator(&proj.input, f)?;
            Some(LogicalOperator::Projection(
                crate::logical::ProjectionOperator {
                    input: Arc::new(new_input),
                    exprs: proj.exprs.clone(),
                    schema: proj.schema.clone(),
                },
            ))
        }

        LogicalOperator::Filter(filter) => {
            let new_input = rewrite_operator(&filter.input, f)?;
            Some(LogicalOperator::Filter(crate::logical::FilterOperator {
                input: Arc::new(new_input),
                predicate: filter.predicate.clone(),
            }))
        }

        LogicalOperator::Join(join) => {
            let left_changed = rewrite_operator(&join.left, f);
            let right_changed = rewrite_operator(&join.right, f);

            if left_changed.is_none() && right_changed.is_none() {
                return None;
            }

            Some(LogicalOperator::Join(crate::logical::JoinOperator {
                left: left_changed
                    .map(Arc::new)
                    .unwrap_or_else(|| join.left.clone()),
                right: right_changed
                    .map(Arc::new)
                    .unwrap_or_else(|| join.right.clone()),
                join_type: join.join_type,
                condition: join.condition.clone(),
                schema: join.schema.clone(),
                equi_keys: join.equi_keys.clone(),
                filter: join.filter.clone(),
            }))
        }

        LogicalOperator::Aggregate(agg) => {
            let new_input = rewrite_operator(&agg.input, f)?;
            Some(LogicalOperator::Aggregate(
                crate::logical::AggregateOperator {
                    input: Arc::new(new_input),
                    group_by: agg.group_by.clone(),
                    aggregates: agg.aggregates.clone(),
                    schema: agg.schema.clone(),
                },
            ))
        }

        LogicalOperator::Sort(sort) => {
            let new_input = rewrite_operator(&sort.input, f)?;
            Some(LogicalOperator::Sort(crate::logical::SortOperator {
                input: Arc::new(new_input),
                order_by: sort.order_by.clone(),
                fetch: sort.fetch,
            }))
        }

        LogicalOperator::Limit(limit) => {
            let new_input = rewrite_operator(&limit.input, f)?;
            Some(LogicalOperator::Limit(crate::logical::LimitOperator {
                input: Arc::new(new_input),
                offset: limit.offset,
                fetch: limit.fetch,
            }))
        }

        LogicalOperator::Distinct(distinct) => {
            let new_input = rewrite_operator(&distinct.input, f)?;
            Some(LogicalOperator::Distinct(
                crate::logical::DistinctOperator {
                    input: Arc::new(new_input),
                    on_columns: distinct.on_columns.clone(),
                },
            ))
        }

        LogicalOperator::SetOperation(setop) => {
            let left_changed = rewrite_operator(&setop.left, f);
            let right_changed = rewrite_operator(&setop.right, f);

            if left_changed.is_none() && right_changed.is_none() {
                return None;
            }

            Some(LogicalOperator::SetOperation(
                crate::logical::SetOperationOperator {
                    left: left_changed
                        .map(Arc::new)
                        .unwrap_or_else(|| setop.left.clone()),
                    right: right_changed
                        .map(Arc::new)
                        .unwrap_or_else(|| setop.right.clone()),
                    op: setop.op,
                },
            ))
        }

        LogicalOperator::SubqueryAlias(alias) => {
            let new_input = rewrite_operator(&alias.input, f)?;
            Some(LogicalOperator::SubqueryAlias(
                crate::logical::SubqueryAliasOperator {
                    input: Arc::new(new_input),
                    alias: alias.alias.clone(),
                    schema: alias.schema.clone(),
                },
            ))
        }

        LogicalOperator::Window(window) => {
            let new_input = rewrite_operator(&window.input, f)?;
            Some(LogicalOperator::Window(crate::logical::WindowOperator {
                input: Arc::new(new_input),
                window_exprs: window.window_exprs.clone(),
                schema: window.schema.clone(),
            }))
        }

        LogicalOperator::Cte(cte) => {
            let new_input = rewrite_operator(&cte.input, f)?;
            Some(LogicalOperator::Cte(crate::logical::CteOperator {
                input: Arc::new(new_input),
                cte_plans: cte.cte_plans.clone(),
                cte_names: cte.cte_names.clone(),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::{Field, Schema};
    use crate::logical::{FilterOperator, LogicalOperator, ScanOperator};
    use crate::parser::DataType;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
            Field::nullable("age", DataType::Int),
        ])
    }

    #[test]
    fn test_optimizer_config_default() {
        let config = OptimizerConfig::default();
        assert_eq!(config.max_iterations, 10);
        assert!(config.enable_predicate_pushdown);
        assert!(config.enable_projection_pushdown);
    }

    #[test]
    fn test_optimizer_config_minimal() {
        let config = OptimizerConfig::minimal();
        assert!(!config.enable_predicate_pushdown);
        assert!(!config.enable_projection_pushdown);
    }

    #[test]
    fn test_optimizer_creation() {
        let optimizer = Optimizer::new(OptimizerConfig::default());
        assert!(!optimizer.rules.is_empty());
    }

    #[test]
    fn test_optimizer_no_op() {
        // A simple scan should not be modified
        let optimizer = Optimizer::new(OptimizerConfig::default());
        let scan = ScanOperator::new("users", test_schema());
        let plan = LogicalPlan::new(LogicalOperator::Scan(scan));

        let optimized = optimizer.optimize(plan).unwrap();
        assert!(matches!(optimized.root.as_ref(), LogicalOperator::Scan(_)));
    }

    #[test]
    fn test_optimizer_with_filter() {
        use crate::logical::LogicalExpr;

        let optimizer = Optimizer::new(OptimizerConfig::default());
        let scan = ScanOperator::new("users", test_schema());
        let filter = FilterOperator {
            input: Arc::new(LogicalOperator::Scan(scan)),
            predicate: LogicalExpr::col("id").eq(LogicalExpr::lit_i64(1)),
        };
        let plan = LogicalPlan::new(LogicalOperator::Filter(filter));

        let (optimized, stats) = optimizer.optimize_with_stats(plan).unwrap();
        assert!(stats.iterations > 0);
        // The plan structure may change due to predicate pushdown
        let _ = optimized;
    }
}
