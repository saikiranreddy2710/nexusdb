//! Physical plan representation and display.
//!
//! A PhysicalPlan wraps a tree of PhysicalOperators and provides
//! methods for displaying, explaining, and analyzing the plan.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use super::context::ExecutionMetrics;
use super::operator::PhysicalOperator;
use crate::executor::OperatorStats;
use crate::logical::LogicalOperator;
use crate::optimizer::{CostModel, OptimizationStats, SimpleCostModel, Statistics};

/// A physical execution plan.
///
/// Wraps a tree of physical operators that can be executed to produce
/// query results.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    /// Root operator of the plan.
    pub root: Arc<PhysicalOperator>,
    /// Plan-level metadata.
    pub metadata: PlanMetadata,
}

/// Metadata about a physical plan.
#[derive(Debug, Clone, Default)]
pub struct PlanMetadata {
    /// Original SQL query (if available).
    pub sql: Option<String>,
    /// Planning time in microseconds.
    pub planning_time_us: u64,
    /// Estimated row count.
    pub estimated_rows: Option<usize>,
    /// Estimated cost.
    pub estimated_cost: Option<f64>,
}

impl PhysicalPlan {
    /// Creates a new physical plan.
    pub fn new(root: PhysicalOperator) -> Self {
        Self {
            root: Arc::new(root),
            metadata: PlanMetadata::default(),
        }
    }

    /// Creates a plan with the given root operator.
    pub fn with_root(root: Arc<PhysicalOperator>) -> Self {
        Self {
            root,
            metadata: PlanMetadata::default(),
        }
    }

    /// Sets the original SQL.
    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.metadata.sql = Some(sql.into());
        self
    }

    /// Sets the planning time.
    pub fn with_planning_time(mut self, time_us: u64) -> Self {
        self.metadata.planning_time_us = time_us;
        self
    }

    /// Executes the plan and collects all result rows.
    ///
    /// Used by subquery evaluation to materialize subquery results inline.
    /// This is a standalone execution path that doesn't require a QueryExecutor
    /// or pre-registered tables — the plan must be self-contained (e.g., SeqScan
    /// operators already hold their data).
    pub fn execute_collect(&self) -> Result<Vec<crate::executor::Row>, String> {
        let ctx = super::ExecutionContext::default();
        let executor = crate::executor::QueryExecutor::new(ctx);
        let result = executor
            .execute(self)
            .map_err(|e| format!("subquery execution failed: {}", e))?;
        Ok(result.batches.iter().flat_map(|b| b.rows()).collect())
    }

    /// Sets the estimated rows.
    pub fn with_estimated_rows(mut self, rows: usize) -> Self {
        self.metadata.estimated_rows = Some(rows);
        self
    }

    /// Sets the estimated cost.
    pub fn with_estimated_cost(mut self, cost: f64) -> Self {
        self.metadata.estimated_cost = Some(cost);
        self
    }

    /// Returns the root operator.
    pub fn root(&self) -> &PhysicalOperator {
        &self.root
    }

    /// Returns the output schema.
    pub fn schema(&self) -> super::operator::SchemaRef {
        self.root.schema()
    }

    /// Computes cost estimates from the logical plan and statistics,
    /// storing them in the plan metadata.
    pub fn with_cost_estimates(mut self, logical: &LogicalOperator, stats: &Statistics) -> Self {
        let cost_model = SimpleCostModel::default();
        let cost = cost_model.estimate_cost(logical, stats);
        self.metadata.estimated_rows = Some(cost.rows as usize);
        self.metadata.estimated_cost = Some(cost.total());
        self
    }

    /// Generates a rich PostgreSQL-style EXPLAIN output.
    ///
    /// When `analyze_metrics` is provided (from EXPLAIN ANALYZE), actual
    /// runtime statistics are included alongside the estimated values.
    /// When `optimizer_stats` is provided, planning and optimizer info is
    /// appended at the bottom.
    pub fn explain_rich(
        &self,
        analyze_metrics: Option<&HashMap<usize, OperatorStats>>,
        optimizer_stats: Option<&OptimizationStats>,
    ) -> String {
        let mut output = String::new();
        let mut op_id = 0;
        self.explain_rich_recursive(
            &self.root,
            0,
            false,
            analyze_metrics,
            &mut op_id,
            &mut output,
        );

        // Planning time
        if self.metadata.planning_time_us > 0 {
            output.push_str(&format!(
                "\nPlanning Time: {:.3} ms\n",
                self.metadata.planning_time_us as f64 / 1000.0
            ));
        }

        // Optimizer stats
        if let Some(stats) = optimizer_stats {
            output.push_str(&format!(
                "Optimizer: {} iterations, {} rules applied\n",
                stats.iterations, stats.rules_applied
            ));
            for (rule, count) in &stats.rule_applications {
                output.push_str(&format!("  - {}: {}\n", rule, count));
            }
        }

        output
    }

    /// Recursively builds the rich explain output.
    fn explain_rich_recursive(
        &self,
        op: &PhysicalOperator,
        indent: usize,
        is_child: bool,
        analyze_metrics: Option<&HashMap<usize, OperatorStats>>,
        op_id: &mut usize,
        output: &mut String,
    ) {
        let current_id = *op_id;
        *op_id += 1;

        // Indentation with arrow prefix for children
        if is_child {
            let prefix = "  ".repeat(indent.saturating_sub(1));
            output.push_str(&prefix);
            output.push_str("-> ");
        }

        // Operator name + key parameters
        output.push_str(op.name());
        self.append_operator_details(op, output);

        // Cost estimates
        if let (Some(rows), Some(cost)) =
            (self.metadata.estimated_rows, self.metadata.estimated_cost)
        {
            output.push_str(&format!("  (cost=0.00..{:.2} rows={})", cost, rows));
        }

        // Actual runtime stats from EXPLAIN ANALYZE
        if let Some(metrics) = analyze_metrics {
            if let Some(op_stats) = metrics.get(&current_id) {
                let time_ms = op_stats.time_ns as f64 / 1_000_000.0;
                output.push_str(&format!(
                    " (actual rows={} time={:.3}ms loops={})",
                    op_stats.rows_produced, time_ms, op_stats.calls
                ));
            }
        }

        output.push('\n');

        // Additional operator detail lines (e.g. Filter predicates)
        self.append_operator_extra_lines(op, indent, is_child, output);

        // Recurse into children
        for child in op.children() {
            self.explain_rich_recursive(child, indent + 1, true, analyze_metrics, op_id, output);
        }
    }

    /// Appends operator-specific inline details (parenthetical).
    fn append_operator_details(&self, op: &PhysicalOperator, output: &mut String) {
        match op {
            PhysicalOperator::SeqScan(scan) => {
                output.push_str(&format!(" on {}", scan.table_name));
            }
            PhysicalOperator::IndexScan(scan) => {
                output.push_str(&format!(
                    " on {} using {}",
                    scan.table_name, scan.index_name
                ));
            }
            PhysicalOperator::HashJoin(join) => {
                output.push_str(&format!(
                    " ({:?}, equi_keys: {})",
                    join.join_type,
                    join.left_keys.len()
                ));
            }
            PhysicalOperator::MergeJoin(join) => {
                output.push_str(&format!(" ({:?})", join.join_type));
            }
            PhysicalOperator::NestedLoopJoin(join) => {
                output.push_str(&format!(" ({:?})", join.join_type));
            }
            PhysicalOperator::HashAggregate(agg) => {
                output.push_str(&format!(
                    " (groups={}, aggs={}, mode={:?})",
                    agg.group_by.len(),
                    agg.aggregates.len(),
                    agg.mode
                ));
            }
            PhysicalOperator::SortAggregate(agg) => {
                output.push_str(&format!(
                    " (groups={}, aggs={})",
                    agg.group_by.len(),
                    agg.aggregates.len()
                ));
            }
            PhysicalOperator::Sort(sort) => {
                output.push_str(&format!(" (cols={})", sort.order_by.len()));
            }
            PhysicalOperator::TopN(topn) => {
                output.push_str(&format!(" (n={})", topn.n));
            }
            PhysicalOperator::Limit(limit) => {
                let mut parts = Vec::new();
                if limit.offset > 0 {
                    parts.push(format!("offset={}", limit.offset));
                }
                if let Some(fetch) = limit.fetch {
                    parts.push(format!("fetch={}", fetch));
                }
                if !parts.is_empty() {
                    output.push_str(&format!(" ({})", parts.join(", ")));
                }
            }
            PhysicalOperator::Projection(proj) => {
                output.push_str(&format!(" (cols={})", proj.exprs.len()));
            }
            PhysicalOperator::Values(vals) => {
                output.push_str(&format!(" (rows={})", vals.values.len()));
            }
            PhysicalOperator::Window(window) => {
                output.push_str(&format!(" (exprs={})", window.window_exprs.len()));
            }
            _ => {}
        }
    }

    /// Appends extra detail lines below the operator line (e.g. Filter expression).
    fn append_operator_extra_lines(
        &self,
        op: &PhysicalOperator,
        indent: usize,
        _is_child: bool,
        output: &mut String,
    ) {
        let pad = "  ".repeat(indent + 1);
        match op {
            PhysicalOperator::Filter(filter) => {
                output.push_str(&format!("{}Filter: ({})\n", pad, filter.predicate));
            }
            PhysicalOperator::SeqScan(scan) if !scan.filters.is_empty() => {
                for f in &scan.filters {
                    output.push_str(&format!("{}Filter: ({})\n", pad, f));
                }
            }
            _ => {}
        }
    }

    /// Generates a JSON-structured EXPLAIN output.
    ///
    /// Produces a nested JSON object with operator details, cost estimates,
    /// and optionally actual runtime metrics.
    pub fn explain_json(
        &self,
        analyze_metrics: Option<&HashMap<usize, OperatorStats>>,
        optimizer_stats: Option<&OptimizationStats>,
    ) -> String {
        let mut op_id = 0;
        let plan_json = self.build_json_node(&self.root, analyze_metrics, &mut op_id);

        let mut root = serde_json::Map::new();
        root.insert("plan".to_string(), plan_json);

        if self.metadata.planning_time_us > 0 {
            root.insert(
                "planning_time_ms".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(self.metadata.planning_time_us as f64 / 1000.0)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
            );
        }

        if let Some(stats) = optimizer_stats {
            let mut opt = serde_json::Map::new();
            opt.insert(
                "iterations".to_string(),
                serde_json::Value::Number(stats.iterations.into()),
            );
            opt.insert(
                "rules_applied".to_string(),
                serde_json::Value::Number(stats.rules_applied.into()),
            );
            let rules: serde_json::Map<String, serde_json::Value> = stats
                .rule_applications
                .iter()
                .map(|(name, count)| (name.clone(), serde_json::Value::Number((*count).into())))
                .collect();
            opt.insert("rules".to_string(), serde_json::Value::Object(rules));
            root.insert("optimizer".to_string(), serde_json::Value::Object(opt));
        }

        serde_json::to_string_pretty(&serde_json::Value::Object(root))
            .unwrap_or_else(|_| "{}".to_string())
    }

    /// Builds a JSON value for a single operator node.
    fn build_json_node(
        &self,
        op: &PhysicalOperator,
        analyze_metrics: Option<&HashMap<usize, OperatorStats>>,
        op_id: &mut usize,
    ) -> serde_json::Value {
        let current_id = *op_id;
        *op_id += 1;

        let mut node = serde_json::Map::new();
        node.insert(
            "operator".to_string(),
            serde_json::Value::String(op.name().to_string()),
        );

        // Details string
        let mut details = String::new();
        self.append_operator_details(op, &mut details);
        let details = details.trim().to_string();
        if !details.is_empty() {
            node.insert("details".to_string(), serde_json::Value::String(details));
        }

        // Estimated cost
        if let Some(rows) = self.metadata.estimated_rows {
            node.insert(
                "estimated_rows".to_string(),
                serde_json::Value::Number(rows.into()),
            );
        }
        if let Some(cost) = self.metadata.estimated_cost {
            node.insert(
                "estimated_cost".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(cost)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
            );
        }

        // Actual metrics
        if let Some(metrics) = analyze_metrics {
            if let Some(op_stats) = metrics.get(&current_id) {
                node.insert(
                    "actual_rows".to_string(),
                    serde_json::Value::Number(op_stats.rows_produced.into()),
                );
                node.insert(
                    "actual_time_ms".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(op_stats.time_ns as f64 / 1_000_000.0)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                node.insert(
                    "loops".to_string(),
                    serde_json::Value::Number(op_stats.calls.into()),
                );
            }
        }

        // Children
        let children: Vec<serde_json::Value> = op
            .children()
            .iter()
            .map(|child| self.build_json_node(child, analyze_metrics, op_id))
            .collect();
        if !children.is_empty() {
            node.insert("children".to_string(), serde_json::Value::Array(children));
        }

        serde_json::Value::Object(node)
    }

    /// Generates an enhanced JSON EXPLAIN output with LLM-friendly metadata.
    ///
    /// Includes warnings about potential performance issues and optimization
    /// suggestions in addition to the standard plan tree.
    pub fn explain_json_llm(
        &self,
        analyze_metrics: Option<&HashMap<usize, OperatorStats>>,
        optimizer_stats: Option<&OptimizationStats>,
        warnings: &[String],
        suggestions: &[String],
    ) -> String {
        let mut op_id = 0;
        let plan_json = self.build_json_node(&self.root, analyze_metrics, &mut op_id);

        let mut root = serde_json::Map::new();
        root.insert("plan".to_string(), plan_json);

        if self.metadata.planning_time_us > 0 {
            root.insert(
                "planning_time_ms".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(self.metadata.planning_time_us as f64 / 1000.0)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
            );
        }

        if let Some(stats) = optimizer_stats {
            let mut opt = serde_json::Map::new();
            opt.insert(
                "iterations".to_string(),
                serde_json::Value::Number(stats.iterations.into()),
            );
            opt.insert(
                "rules_applied".to_string(),
                serde_json::Value::Number(stats.rules_applied.into()),
            );
            let rules: serde_json::Map<String, serde_json::Value> = stats
                .rule_applications
                .iter()
                .map(|(name, count)| (name.clone(), serde_json::Value::Number((*count).into())))
                .collect();
            opt.insert("rules".to_string(), serde_json::Value::Object(rules));
            root.insert("optimizer".to_string(), serde_json::Value::Object(opt));
        }

        // Warnings
        if !warnings.is_empty() {
            let w: Vec<serde_json::Value> = warnings
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect();
            root.insert("warnings".to_string(), serde_json::Value::Array(w));
        }

        // Suggestions
        if !suggestions.is_empty() {
            let s: Vec<serde_json::Value> = suggestions
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect();
            root.insert("suggestions".to_string(), serde_json::Value::Array(s));
        }

        serde_json::to_string_pretty(&serde_json::Value::Object(root))
            .unwrap_or_else(|_| "{}".to_string())
    }

    /// Generates an EXPLAIN output for the plan.
    pub fn explain(&self, verbose: bool) -> String {
        let mut output = String::new();
        self.explain_recursive(&self.root, 0, verbose, &mut output);
        output
    }

    /// Recursively builds the explain output.
    fn explain_recursive(
        &self,
        op: &PhysicalOperator,
        indent: usize,
        verbose: bool,
        output: &mut String,
    ) {
        let prefix = "  ".repeat(indent);

        // Add operator name and details
        output.push_str(&prefix);
        output.push_str(op.name());

        // Add operator-specific details
        match op {
            PhysicalOperator::SeqScan(scan) => {
                output.push_str(&format!(" (table={})", scan.table_name));
                if let Some(ref proj) = scan.projection {
                    output.push_str(&format!(", cols={:?}", proj));
                }
                if !scan.filters.is_empty() {
                    output.push_str(&format!(", filters={}", scan.filters.len()));
                }
                if let Some(limit) = scan.limit {
                    output.push_str(&format!(", limit={}", limit));
                }
            }
            PhysicalOperator::IndexScan(scan) => {
                output.push_str(&format!(
                    " (table={}, index={})",
                    scan.table_name, scan.index_name
                ));
            }
            PhysicalOperator::Filter(filter) => {
                if verbose {
                    output.push_str(&format!(" ({})", filter.predicate));
                }
            }
            PhysicalOperator::Projection(proj) => {
                output.push_str(&format!(" (cols={})", proj.exprs.len()));
            }
            PhysicalOperator::HashJoin(join) => {
                output.push_str(&format!(" (type={:?})", join.join_type));
                output.push_str(&format!(", keys={}", join.left_keys.len()));
            }
            PhysicalOperator::MergeJoin(join) => {
                output.push_str(&format!(" (type={:?})", join.join_type));
            }
            PhysicalOperator::NestedLoopJoin(join) => {
                output.push_str(&format!(" (type={:?})", join.join_type));
            }
            PhysicalOperator::HashAggregate(agg) => {
                output.push_str(&format!(
                    " (groups={}, aggs={}, mode={:?})",
                    agg.group_by.len(),
                    agg.aggregates.len(),
                    agg.mode
                ));
            }
            PhysicalOperator::SortAggregate(agg) => {
                output.push_str(&format!(
                    " (groups={}, aggs={})",
                    agg.group_by.len(),
                    agg.aggregates.len()
                ));
            }
            PhysicalOperator::Sort(sort) => {
                output.push_str(&format!(" (cols={})", sort.order_by.len()));
                if let Some(fetch) = sort.fetch {
                    output.push_str(&format!(", fetch={}", fetch));
                }
            }
            PhysicalOperator::TopN(topn) => {
                output.push_str(&format!(" (n={}, cols={})", topn.n, topn.order_by.len()));
            }
            PhysicalOperator::Limit(limit) => {
                if limit.offset > 0 {
                    output.push_str(&format!(" (offset={})", limit.offset));
                }
                if let Some(fetch) = limit.fetch {
                    output.push_str(&format!(", fetch={}", fetch));
                }
            }
            PhysicalOperator::Distinct(_) => {}
            PhysicalOperator::SetOperation(setop) => {
                output.push_str(&format!(" ({:?})", setop.op));
            }
            PhysicalOperator::Exchange(ex) => {
                output.push_str(&format!(" (partitions={})", ex.target_partitions));
            }
            PhysicalOperator::Values(vals) => {
                output.push_str(&format!(" (rows={})", vals.values.len()));
            }
            PhysicalOperator::Empty(empty) => {
                if empty.produce_one_row {
                    output.push_str(" (one_row)");
                }
            }
            PhysicalOperator::Window(window) => {
                output.push_str(&format!(" (exprs={})", window.window_exprs.len()));
            }
        }

        output.push('\n');

        // Recursively process children
        for child in op.children() {
            self.explain_recursive(child, indent + 1, verbose, output);
        }
    }

    /// Generates a tree visualization of the plan.
    pub fn display_tree(&self) -> String {
        let mut output = String::new();
        self.display_tree_recursive(&self.root, "", true, &mut output);
        output
    }

    fn display_tree_recursive(
        &self,
        op: &PhysicalOperator,
        prefix: &str,
        is_last: bool,
        output: &mut String,
    ) {
        let connector = if is_last { "\\-- " } else { "|-- " };
        output.push_str(prefix);
        output.push_str(connector);
        output.push_str(op.name());
        output.push('\n');

        let children = op.children();
        let child_prefix = format!("{}{}   ", prefix, if is_last { " " } else { "|" });

        for (i, child) in children.iter().enumerate() {
            let is_last_child = i == children.len() - 1;
            self.display_tree_recursive(child, &child_prefix, is_last_child, output);
        }
    }

    /// Counts the total number of operators in the plan.
    pub fn operator_count(&self) -> usize {
        self.count_operators(&self.root)
    }

    fn count_operators(&self, op: &PhysicalOperator) -> usize {
        1 + op
            .children()
            .iter()
            .map(|c| self.count_operators(c))
            .sum::<usize>()
    }

    /// Returns the depth of the plan tree.
    pub fn depth(&self) -> usize {
        self.calculate_depth(&self.root)
    }

    fn calculate_depth(&self, op: &PhysicalOperator) -> usize {
        let children = op.children();
        if children.is_empty() {
            1
        } else {
            1 + children
                .iter()
                .map(|c| self.calculate_depth(c))
                .max()
                .unwrap_or(0)
        }
    }

    /// Collects all operators of a specific type.
    pub fn find_operators<F>(&self, predicate: F) -> Vec<&PhysicalOperator>
    where
        F: Fn(&PhysicalOperator) -> bool,
    {
        let mut result = Vec::new();
        self.collect_operators(&self.root, &predicate, &mut result);
        result
    }

    fn collect_operators<'a, F>(
        &'a self,
        op: &'a PhysicalOperator,
        predicate: &F,
        result: &mut Vec<&'a PhysicalOperator>,
    ) where
        F: Fn(&PhysicalOperator) -> bool,
    {
        if predicate(op) {
            result.push(op);
        }
        for child in op.children() {
            self.collect_operators(child, predicate, result);
        }
    }

    /// Returns true if the plan contains any joins.
    pub fn has_joins(&self) -> bool {
        !self
            .find_operators(|op| {
                matches!(
                    op,
                    PhysicalOperator::HashJoin(_)
                        | PhysicalOperator::MergeJoin(_)
                        | PhysicalOperator::NestedLoopJoin(_)
                )
            })
            .is_empty()
    }

    /// Returns true if the plan contains any aggregates.
    pub fn has_aggregates(&self) -> bool {
        !self
            .find_operators(|op| {
                matches!(
                    op,
                    PhysicalOperator::HashAggregate(_) | PhysicalOperator::SortAggregate(_)
                )
            })
            .is_empty()
    }

    /// Returns true if the plan contains any sorts.
    pub fn has_sorts(&self) -> bool {
        !self
            .find_operators(|op| {
                matches!(op, PhysicalOperator::Sort(_) | PhysicalOperator::TopN(_))
            })
            .is_empty()
    }
}

impl fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.explain(false))
    }
}

/// Execution result with metrics.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Whether execution succeeded.
    pub success: bool,
    /// Number of rows returned.
    pub rows_returned: usize,
    /// Execution metrics.
    pub metrics: ExecutionMetrics,
    /// Error message (if failed).
    pub error: Option<String>,
}

impl ExecutionResult {
    /// Creates a successful result.
    pub fn success(rows: usize, metrics: ExecutionMetrics) -> Self {
        Self {
            success: true,
            rows_returned: rows,
            metrics,
            error: None,
        }
    }

    /// Creates a failed result.
    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            success: false,
            rows_returned: 0,
            metrics: ExecutionMetrics::default(),
            error: Some(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::{AggregateFunc, BinaryOp};
    use crate::logical::{Field, Schema};
    use crate::parser::DataType;
    use crate::physical::{
        AggregationMode, FilterPhysicalOperator, HashAggregateOperator, LimitPhysicalOperator,
        PhysicalAggregateExpr, PhysicalExpr, SeqScanOperator,
    };

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
            Field::nullable("age", DataType::Int),
        ])
    }

    #[test]
    fn test_simple_plan() {
        let scan = SeqScanOperator::new("users", test_schema());
        let plan = PhysicalPlan::new(PhysicalOperator::SeqScan(scan));

        assert_eq!(plan.operator_count(), 1);
        assert_eq!(plan.depth(), 1);
        assert!(!plan.has_joins());
    }

    #[test]
    fn test_plan_with_filter() {
        let scan = Arc::new(PhysicalOperator::SeqScan(SeqScanOperator::new(
            "users",
            test_schema(),
        )));
        let filter = FilterPhysicalOperator {
            input: scan,
            predicate: PhysicalExpr::binary(
                PhysicalExpr::column("id", 0),
                BinaryOp::Gt,
                PhysicalExpr::lit_i64(10),
            ),
        };
        let plan = PhysicalPlan::new(PhysicalOperator::Filter(filter));

        assert_eq!(plan.operator_count(), 2);
        assert_eq!(plan.depth(), 2);
    }

    #[test]
    fn test_explain_output() {
        let scan = SeqScanOperator::new("users", test_schema());
        let plan = PhysicalPlan::new(PhysicalOperator::SeqScan(scan));

        let explain = plan.explain(false);
        assert!(explain.contains("SeqScan"));
        assert!(explain.contains("users"));
    }

    #[test]
    fn test_display_tree() {
        let scan = Arc::new(PhysicalOperator::SeqScan(SeqScanOperator::new(
            "users",
            test_schema(),
        )));
        let limit = LimitPhysicalOperator {
            input: scan,
            offset: 0,
            fetch: Some(10),
        };
        let plan = PhysicalPlan::new(PhysicalOperator::Limit(limit));

        let tree = plan.display_tree();
        assert!(tree.contains("Limit"));
        assert!(tree.contains("SeqScan"));
    }

    #[test]
    fn test_has_aggregates() {
        let schema = test_schema();
        let scan = Arc::new(PhysicalOperator::SeqScan(SeqScanOperator::new(
            "users",
            schema.clone(),
        )));

        // Schema for aggregate output: count result
        let agg_schema = Schema::new(vec![Field::not_null("count", DataType::BigInt)]);

        let agg = HashAggregateOperator {
            input: scan,
            group_by: vec![],
            aggregates: vec![PhysicalAggregateExpr::new(
                AggregateFunc::CountStar,
                vec![],
                false,
                "count",
            )],
            schema: Arc::new(agg_schema),
            mode: AggregationMode::Single,
        };
        let plan = PhysicalPlan::new(PhysicalOperator::HashAggregate(agg));

        assert!(plan.has_aggregates());
        assert!(!plan.has_joins());
    }

    #[test]
    fn test_plan_metadata() {
        let scan = SeqScanOperator::new("users", test_schema());
        let plan = PhysicalPlan::new(PhysicalOperator::SeqScan(scan))
            .with_sql("SELECT * FROM users")
            .with_planning_time(1000)
            .with_estimated_rows(100);

        assert_eq!(plan.metadata.sql, Some("SELECT * FROM users".to_string()));
        assert_eq!(plan.metadata.planning_time_us, 1000);
        assert_eq!(plan.metadata.estimated_rows, Some(100));
    }

    #[test]
    fn test_execution_result() {
        let success = ExecutionResult::success(100, ExecutionMetrics::default());
        assert!(success.success);
        assert_eq!(success.rows_returned, 100);

        let failure = ExecutionResult::failure("Query timeout");
        assert!(!failure.success);
        assert_eq!(failure.error, Some("Query timeout".to_string()));
    }
}
