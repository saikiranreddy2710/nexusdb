//! Cost model for query optimization.
//!
//! This module provides cost estimation for query plans, enabling
//! the optimizer to choose between alternative execution strategies.

use std::collections::HashMap;

use crate::logical::{JoinType, LogicalOperator};

/// Cost estimate for a query plan or sub-plan.
#[derive(Debug, Clone, Default)]
pub struct CostEstimate {
    /// Estimated number of rows.
    pub rows: f64,
    /// Estimated CPU cost (in arbitrary units).
    pub cpu_cost: f64,
    /// Estimated I/O cost (in arbitrary units).
    pub io_cost: f64,
    /// Estimated memory usage in bytes.
    pub memory_bytes: usize,
    /// Estimated network cost (for distributed queries).
    pub network_cost: f64,
}

impl CostEstimate {
    /// Creates a new cost estimate.
    pub fn new(rows: f64, cpu_cost: f64, io_cost: f64) -> Self {
        Self {
            rows,
            cpu_cost,
            io_cost,
            memory_bytes: 0,
            network_cost: 0.0,
        }
    }

    /// Returns the total cost as a weighted sum.
    pub fn total(&self) -> f64 {
        // Default weights: CPU and I/O are equally important
        self.cpu_cost + self.io_cost + self.network_cost
    }

    /// Returns the total cost with custom weights.
    pub fn total_weighted(&self, cpu_weight: f64, io_weight: f64, network_weight: f64) -> f64 {
        self.cpu_cost * cpu_weight + self.io_cost * io_weight + self.network_cost * network_weight
    }

    /// Adds another cost estimate to this one.
    pub fn add(&self, other: &CostEstimate) -> CostEstimate {
        CostEstimate {
            rows: self.rows, // Output rows typically from this node
            cpu_cost: self.cpu_cost + other.cpu_cost,
            io_cost: self.io_cost + other.io_cost,
            memory_bytes: self.memory_bytes + other.memory_bytes,
            network_cost: self.network_cost + other.network_cost,
        }
    }
}

/// Statistics for a table or column.
#[derive(Debug, Clone)]
pub struct Statistics {
    /// Number of rows in the table.
    pub row_count: usize,
    /// Number of distinct values (for columns).
    pub distinct_count: Option<usize>,
    /// Minimum value (for ordered types).
    pub min_value: Option<String>,
    /// Maximum value (for ordered types).
    pub max_value: Option<String>,
    /// Null fraction (0.0 to 1.0).
    pub null_fraction: f64,
    /// Average row size in bytes.
    pub avg_row_size: usize,
    /// Column statistics.
    pub column_stats: HashMap<String, ColumnStatistics>,
}

impl Default for Statistics {
    fn default() -> Self {
        Self {
            row_count: 1000, // Default estimate
            distinct_count: None,
            min_value: None,
            max_value: None,
            null_fraction: 0.0,
            avg_row_size: 100,
            column_stats: HashMap::new(),
        }
    }
}

impl Statistics {
    /// Creates new statistics with the given row count.
    pub fn with_row_count(row_count: usize) -> Self {
        Self {
            row_count,
            ..Default::default()
        }
    }
}

/// Statistics for a single column.
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Number of distinct values.
    pub distinct_count: usize,
    /// Null fraction (0.0 to 1.0).
    pub null_fraction: f64,
    /// Minimum value.
    pub min_value: Option<String>,
    /// Maximum value.
    pub max_value: Option<String>,
    /// Average size in bytes.
    pub avg_size: usize,
}

impl Default for ColumnStatistics {
    fn default() -> Self {
        Self {
            distinct_count: 100,
            null_fraction: 0.0,
            min_value: None,
            max_value: None,
            avg_size: 8,
        }
    }
}

/// Trait for cost estimation.
pub trait CostModel: std::fmt::Debug + Send + Sync {
    /// Estimates the cost of executing a logical operator.
    fn estimate_cost(&self, op: &LogicalOperator, stats: &Statistics) -> CostEstimate;

    /// Estimates the output row count for an operator.
    fn estimate_rows(&self, op: &LogicalOperator, stats: &Statistics) -> f64;

    /// Estimates selectivity of a filter (0.0 to 1.0).
    fn estimate_selectivity(&self, op: &LogicalOperator, stats: &Statistics) -> f64;
}

/// Simple cost model using heuristic estimates.
///
/// This model uses simple rules of thumb for cost estimation:
/// - Sequential scan: row_count * row_size
/// - Filter: input_cost + row_count * 0.1 (selectivity estimate)
/// - Hash join: left_cost + right_cost + hash_build + probe
/// - Sort: n * log(n)
#[derive(Debug, Default)]
pub struct SimpleCostModel {
    /// Default selectivity for equality predicates.
    pub eq_selectivity: f64,
    /// Default selectivity for range predicates.
    pub range_selectivity: f64,
    /// Default selectivity for LIKE predicates.
    pub like_selectivity: f64,
    /// Cost per row for CPU operations.
    pub cpu_cost_per_row: f64,
    /// Cost per page for I/O operations.
    pub io_cost_per_page: f64,
    /// Page size in bytes.
    pub page_size: usize,
}

impl SimpleCostModel {
    /// Creates a new simple cost model with default parameters.
    pub fn new() -> Self {
        Self {
            eq_selectivity: 0.1,     // 10% selectivity for equality
            range_selectivity: 0.33, // 33% selectivity for range
            like_selectivity: 0.25,  // 25% selectivity for LIKE
            cpu_cost_per_row: 0.01,  // CPU cost per row
            io_cost_per_page: 1.0,   // I/O cost per page
            page_size: 8192,         // 8KB pages
        }
    }

    fn estimate_scan_cost(&self, stats: &Statistics) -> CostEstimate {
        let rows = stats.row_count as f64;
        let pages = (rows * stats.avg_row_size as f64) / self.page_size as f64;

        CostEstimate {
            rows,
            cpu_cost: rows * self.cpu_cost_per_row,
            io_cost: pages * self.io_cost_per_page,
            memory_bytes: 0, // Streaming
            network_cost: 0.0,
        }
    }

    fn estimate_filter_cost(&self, input_cost: &CostEstimate, selectivity: f64) -> CostEstimate {
        let output_rows = input_cost.rows * selectivity;

        CostEstimate {
            rows: output_rows,
            cpu_cost: input_cost.cpu_cost + input_cost.rows * self.cpu_cost_per_row,
            io_cost: input_cost.io_cost,
            memory_bytes: 0, // Streaming
            network_cost: input_cost.network_cost,
        }
    }

    fn estimate_join_cost(
        &self,
        left_cost: &CostEstimate,
        right_cost: &CostEstimate,
        join_type: JoinType,
    ) -> CostEstimate {
        let build_rows = right_cost.rows;
        let probe_rows = left_cost.rows;

        // Estimate output rows based on join type
        let output_rows = match join_type {
            JoinType::Inner => (probe_rows * build_rows) * self.eq_selectivity,
            JoinType::Left => probe_rows,
            JoinType::Right => build_rows,
            JoinType::Full => probe_rows + build_rows,
            JoinType::Cross => probe_rows * build_rows,
            JoinType::Semi | JoinType::Anti => probe_rows * 0.5,
        };

        // Hash join costs
        let build_cost = build_rows * self.cpu_cost_per_row * 2.0; // Hash + insert
        let probe_cost = probe_rows * self.cpu_cost_per_row * 1.5; // Hash + lookup

        CostEstimate {
            rows: output_rows,
            cpu_cost: left_cost.cpu_cost + right_cost.cpu_cost + build_cost + probe_cost,
            io_cost: left_cost.io_cost + right_cost.io_cost,
            memory_bytes: (build_rows * 100.0) as usize, // Estimate hash table size
            network_cost: left_cost.network_cost + right_cost.network_cost,
        }
    }

    fn estimate_sort_cost(&self, input_cost: &CostEstimate) -> CostEstimate {
        let rows = input_cost.rows;
        let sort_cost = if rows > 1.0 {
            rows * rows.log2() * self.cpu_cost_per_row
        } else {
            0.0
        };

        CostEstimate {
            rows,
            cpu_cost: input_cost.cpu_cost + sort_cost,
            io_cost: input_cost.io_cost,
            memory_bytes: (rows * 100.0) as usize, // In-memory sort
            network_cost: input_cost.network_cost,
        }
    }

    fn estimate_aggregate_cost(&self, input_cost: &CostEstimate, groups: usize) -> CostEstimate {
        let output_rows = groups.max(1) as f64;

        CostEstimate {
            rows: output_rows,
            cpu_cost: input_cost.cpu_cost + input_cost.rows * self.cpu_cost_per_row,
            io_cost: input_cost.io_cost,
            memory_bytes: (output_rows * 100.0) as usize, // Hash aggregate
            network_cost: input_cost.network_cost,
        }
    }
}

impl CostModel for SimpleCostModel {
    fn estimate_cost(&self, op: &LogicalOperator, stats: &Statistics) -> CostEstimate {
        match op {
            LogicalOperator::Scan(_) => self.estimate_scan_cost(stats),

            LogicalOperator::Filter(filter) => {
                let input_cost = self.estimate_cost(&filter.input, stats);
                let selectivity = self.estimate_selectivity(op, stats);
                self.estimate_filter_cost(&input_cost, selectivity)
            }

            LogicalOperator::Projection(proj) => {
                // Projection is essentially free (streaming)
                let input_cost = self.estimate_cost(&proj.input, stats);
                CostEstimate {
                    rows: input_cost.rows,
                    cpu_cost: input_cost.cpu_cost + input_cost.rows * self.cpu_cost_per_row * 0.1,
                    io_cost: input_cost.io_cost,
                    memory_bytes: 0,
                    network_cost: input_cost.network_cost,
                }
            }

            LogicalOperator::Join(join) => {
                let left_cost = self.estimate_cost(&join.left, stats);
                let right_cost = self.estimate_cost(&join.right, stats);
                self.estimate_join_cost(&left_cost, &right_cost, join.join_type)
            }

            LogicalOperator::Sort(sort) => {
                let input_cost = self.estimate_cost(&sort.input, stats);
                self.estimate_sort_cost(&input_cost)
            }

            LogicalOperator::Aggregate(agg) => {
                let input_cost = self.estimate_cost(&agg.input, stats);
                let groups = agg.group_by.len().max(1);
                self.estimate_aggregate_cost(&input_cost, groups)
            }

            LogicalOperator::Limit(limit) => {
                let input_cost = self.estimate_cost(&limit.input, stats);
                let output_rows = match limit.fetch {
                    Some(fetch) => (fetch as f64).min(input_cost.rows),
                    None => input_cost.rows,
                };
                CostEstimate {
                    rows: output_rows,
                    cpu_cost: input_cost.cpu_cost,
                    io_cost: input_cost.io_cost,
                    memory_bytes: 0,
                    network_cost: input_cost.network_cost,
                }
            }

            LogicalOperator::Distinct(distinct) => {
                let input_cost = self.estimate_cost(&distinct.input, stats);
                // Distinct requires hashing or sorting
                let distinct_cost = input_cost.rows * self.cpu_cost_per_row;
                CostEstimate {
                    rows: input_cost.rows * 0.8, // Assume 20% duplicates
                    cpu_cost: input_cost.cpu_cost + distinct_cost,
                    io_cost: input_cost.io_cost,
                    memory_bytes: (input_cost.rows * 50.0) as usize,
                    network_cost: input_cost.network_cost,
                }
            }

            _ => CostEstimate::default(),
        }
    }

    fn estimate_rows(&self, op: &LogicalOperator, stats: &Statistics) -> f64 {
        self.estimate_cost(op, stats).rows
    }

    fn estimate_selectivity(&self, op: &LogicalOperator, _stats: &Statistics) -> f64 {
        match op {
            LogicalOperator::Filter(filter) => {
                // Simple heuristic based on predicate structure
                use crate::logical::LogicalExpr;

                fn estimate_expr_selectivity(expr: &LogicalExpr, default_sel: f64) -> f64 {
                    match expr {
                        LogicalExpr::BinaryOp { op, .. } => {
                            use crate::logical::BinaryOp;
                            match op {
                                BinaryOp::Eq => 0.1,
                                BinaryOp::NotEq => 0.9,
                                BinaryOp::Lt | BinaryOp::LtEq => 0.33,
                                BinaryOp::Gt | BinaryOp::GtEq => 0.33,
                                BinaryOp::And => 0.25, // Product of child selectivities
                                BinaryOp::Or => 0.75,  // Sum minus product
                                BinaryOp::Like => 0.25,
                                _ => default_sel,
                            }
                        }
                        LogicalExpr::IsNull(_) => 0.05,
                        LogicalExpr::IsNotNull(_) => 0.95,
                        LogicalExpr::InList { list, negated, .. } => {
                            let sel = (list.len() as f64 * 0.1).min(0.5);
                            if *negated {
                                1.0 - sel
                            } else {
                                sel
                            }
                        }
                        LogicalExpr::Between { negated, .. } => {
                            if *negated {
                                0.67
                            } else {
                                0.33
                            }
                        }
                        _ => default_sel,
                    }
                }

                estimate_expr_selectivity(&filter.predicate, 0.5)
            }
            _ => 1.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::{Field, Schema};
    use crate::logical::{FilterOperator, LogicalExpr, ScanOperator};
    use crate::parser::DataType;
    use std::sync::Arc;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
        ])
    }

    #[test]
    fn test_scan_cost() {
        let model = SimpleCostModel::new();
        let stats = Statistics::with_row_count(1000);

        let scan = ScanOperator::new("users", test_schema());
        let op = LogicalOperator::Scan(scan);

        let cost = model.estimate_cost(&op, &stats);
        assert_eq!(cost.rows, 1000.0);
        assert!(cost.cpu_cost > 0.0);
        assert!(cost.io_cost > 0.0);
    }

    #[test]
    fn test_filter_cost() {
        let model = SimpleCostModel::new();
        let stats = Statistics::with_row_count(1000);

        let scan = ScanOperator::new("users", test_schema());
        let filter = FilterOperator {
            input: Arc::new(LogicalOperator::Scan(scan)),
            predicate: LogicalExpr::col("id").eq(LogicalExpr::lit_i64(1)),
        };
        let op = LogicalOperator::Filter(filter);

        let cost = model.estimate_cost(&op, &stats);
        // Filter should reduce row count
        assert!(cost.rows < 1000.0);
    }

    #[test]
    fn test_cost_estimate_total() {
        let cost = CostEstimate::new(100.0, 10.0, 20.0);
        assert_eq!(cost.total(), 30.0);
    }

    #[test]
    fn test_selectivity_estimation() {
        let model = SimpleCostModel::new();
        let stats = Statistics::default();

        let scan = ScanOperator::new("users", test_schema());
        let filter = FilterOperator {
            input: Arc::new(LogicalOperator::Scan(scan)),
            predicate: LogicalExpr::col("id").eq(LogicalExpr::lit_i64(1)),
        };
        let op = LogicalOperator::Filter(filter);

        let selectivity = model.estimate_selectivity(&op, &stats);
        assert!(selectivity > 0.0 && selectivity < 1.0);
    }
}
