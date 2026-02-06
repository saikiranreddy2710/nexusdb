//! Physical planner for converting logical plans to physical execution plans.
//!
//! The physical planner selects concrete algorithms for each operation:
//! - Join algorithms: HashJoin, MergeJoin, NestedLoopJoin
//! - Aggregate algorithms: HashAggregate, SortAggregate
//! - Scan methods: SeqScan, IndexScan
//! - Sort optimization: TopN for Sort + Limit

use std::sync::Arc;

use crate::logical::{
    AggregateOperator, CteOperator, DistinctOperator, EmptyRelationOperator, FilterOperator,
    JoinOperator, JoinType, LimitOperator, LogicalOperator, ProjectionOperator, ScanOperator,
    SetOperationOperator, SortOperator, SubqueryAliasOperator, ValuesOperator, WindowOperator,
};

use super::context::ExecutionContext;
use super::expr::{
    create_physical_aggregate, create_physical_expr, PhysicalExpr, PhysicalSortExpr,
};
use super::operator::{
    AggregationMode, DistinctPhysicalOperator, EmptyOperator, FilterPhysicalOperator,
    HashAggregateOperator, HashJoinOperator, JoinKey, LimitPhysicalOperator, MergeJoinOperator,
    NestedLoopJoinOperator, PhysicalOperator, ProjectionPhysicalOperator, SeqScanOperator,
    SetOperationPhysicalOperator, SortAggregateOperator, SortPhysicalOperator, TopNOperator,
    ValuesPhysicalOperator,
};
use super::plan::PhysicalPlan;

/// Physical planner that converts logical plans to physical execution plans.
#[derive(Debug)]
pub struct PhysicalPlanner<'a> {
    /// Execution context with configuration.
    ctx: &'a ExecutionContext,
}

impl<'a> PhysicalPlanner<'a> {
    /// Creates a new physical planner.
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self { ctx }
    }

    /// Converts a logical operator tree to a physical plan.
    pub fn create_physical_plan(
        &self,
        logical: &LogicalOperator,
    ) -> Result<PhysicalPlan, PlanningError> {
        let start = std::time::Instant::now();
        let root = self.plan_operator(logical)?;
        let elapsed = start.elapsed().as_micros() as u64;

        Ok(PhysicalPlan::with_root(Arc::new(root)).with_planning_time(elapsed))
    }

    /// Plans a single logical operator.
    fn plan_operator(&self, logical: &LogicalOperator) -> Result<PhysicalOperator, PlanningError> {
        match logical {
            LogicalOperator::Scan(scan) => self.plan_scan(scan),
            LogicalOperator::Projection(proj) => self.plan_projection(proj),
            LogicalOperator::Filter(filter) => self.plan_filter(filter),
            LogicalOperator::Join(join) => self.plan_join(join),
            LogicalOperator::Aggregate(agg) => self.plan_aggregate(agg),
            LogicalOperator::Sort(sort) => self.plan_sort(sort),
            LogicalOperator::Limit(limit) => self.plan_limit(limit),
            LogicalOperator::Distinct(distinct) => self.plan_distinct(distinct),
            LogicalOperator::SetOperation(setop) => self.plan_set_operation(setop),
            LogicalOperator::SubqueryAlias(alias) => self.plan_subquery_alias(alias),
            LogicalOperator::Values(values) => self.plan_values(values),
            LogicalOperator::EmptyRelation(empty) => self.plan_empty(empty),
            LogicalOperator::Window(window) => self.plan_window(window),
            LogicalOperator::Cte(cte) => self.plan_cte(cte),
        }
    }

    /// Plans a table scan.
    fn plan_scan(&self, scan: &ScanOperator) -> Result<PhysicalOperator, PlanningError> {
        // For now, always use sequential scan
        // TODO: Use index scan when appropriate based on filters and available indexes
        let mut phys_scan =
            SeqScanOperator::new(scan.table_name.clone(), (*scan.table_schema).clone());

        // Apply projection
        if let Some(ref proj) = scan.projection {
            phys_scan = phys_scan.with_projection(proj.clone());
        }

        // Convert and push down filters
        // IMPORTANT: Use table_schema (not projected_schema) because filters may reference
        // columns that are not in the final projection. The executor will apply filters
        // on the full row before applying the projection.
        for filter in &scan.filters {
            if let Ok(phys_filter) = create_physical_expr(filter, &scan.table_schema) {
                phys_scan = phys_scan.with_filter(phys_filter);
            }
        }

        // Apply limit
        if let Some(limit) = scan.limit {
            phys_scan = phys_scan.with_limit(limit);
        }

        Ok(PhysicalOperator::SeqScan(phys_scan))
    }

    /// Plans a projection.
    fn plan_projection(
        &self,
        proj: &ProjectionOperator,
    ) -> Result<PhysicalOperator, PlanningError> {
        let input = self.plan_operator(&proj.input)?;
        let input_schema = input.schema();

        let exprs: Result<Vec<_>, _> = proj
            .exprs
            .iter()
            .map(|e| create_physical_expr(e, &input_schema))
            .collect();

        Ok(PhysicalOperator::Projection(ProjectionPhysicalOperator {
            input: Arc::new(input),
            exprs: exprs?,
            schema: proj.schema.clone(),
        }))
    }

    /// Plans a filter.
    fn plan_filter(&self, filter: &FilterOperator) -> Result<PhysicalOperator, PlanningError> {
        let input = self.plan_operator(&filter.input)?;
        let input_schema = input.schema();
        let predicate = create_physical_expr(&filter.predicate, &input_schema)?;

        Ok(PhysicalOperator::Filter(FilterPhysicalOperator {
            input: Arc::new(input),
            predicate,
        }))
    }

    /// Plans a join operation.
    ///
    /// Algorithm selection:
    /// 1. Cross join or no equi-keys → NestedLoopJoin
    /// 2. Small build side → HashJoin
    /// 3. Pre-sorted inputs → MergeJoin
    /// 4. Default → HashJoin
    fn plan_join(&self, join: &JoinOperator) -> Result<PhysicalOperator, PlanningError> {
        let left = self.plan_operator(&join.left)?;
        let right = self.plan_operator(&join.right)?;
        let left_schema = left.schema();
        let right_schema = right.schema();

        // For cross joins or joins without equi-keys, use nested loop
        if join.join_type == JoinType::Cross || join.equi_keys.is_empty() {
            let condition = join
                .condition
                .as_ref()
                .map(|c| create_physical_expr(c, &join.schema))
                .transpose()?;

            return Ok(PhysicalOperator::NestedLoopJoin(NestedLoopJoinOperator {
                left: Arc::new(left),
                right: Arc::new(right),
                join_type: join.join_type,
                condition,
                schema: join.schema.clone(),
            }));
        }

        // Convert equi-keys to physical expressions
        let left_keys: Result<Vec<JoinKey>, PlanningError> = join
            .equi_keys
            .iter()
            .map(|(l, _)| {
                let expr = create_physical_expr(l, &left_schema)?;
                Ok(JoinKey::new(expr))
            })
            .collect();
        let right_keys: Result<Vec<JoinKey>, PlanningError> = join
            .equi_keys
            .iter()
            .map(|(_, r)| {
                let expr = create_physical_expr(r, &right_schema)?;
                Ok(JoinKey::new(expr))
            })
            .collect();

        let left_keys = left_keys?;
        let right_keys = right_keys?;

        // Convert non-equi filter
        let filter = join
            .filter
            .as_ref()
            .map(|f| create_physical_expr(f, &join.schema))
            .transpose()?;

        // Check if inputs are already sorted on join keys for merge join
        let left_sorted = self.is_sorted_on(&left, &left_keys);
        let right_sorted = self.is_sorted_on(&right, &right_keys);

        // Choose join algorithm
        if left_sorted && right_sorted {
            // Both inputs are sorted - use merge join
            Ok(PhysicalOperator::MergeJoin(MergeJoinOperator {
                left: Arc::new(left),
                right: Arc::new(right),
                join_type: join.join_type,
                left_keys,
                right_keys,
                filter,
                schema: join.schema.clone(),
            }))
        } else {
            // Use hash join (default)
            Ok(PhysicalOperator::HashJoin(HashJoinOperator {
                left: Arc::new(left),
                right: Arc::new(right),
                join_type: join.join_type,
                left_keys,
                right_keys,
                filter,
                schema: join.schema.clone(),
            }))
        }
    }

    /// Checks if an operator's output is sorted on the given keys.
    fn is_sorted_on(&self, op: &PhysicalOperator, keys: &[JoinKey]) -> bool {
        let props = op.properties();
        if let Some(ordering) = &props.ordering {
            if ordering.exprs.len() >= keys.len() {
                // Check if first N ordering expressions match keys
                for (sort_expr, key) in ordering.exprs.iter().zip(keys.iter()) {
                    // Simple check: both must be column references to the same column
                    match (&sort_expr.expr, &key.expr) {
                        (
                            PhysicalExpr::Column { index: i1, .. },
                            PhysicalExpr::Column { index: i2, .. },
                        ) if i1 == i2 => continue,
                        _ => return false,
                    }
                }
                return true;
            }
        }
        false
    }

    /// Plans an aggregate operation.
    ///
    /// Algorithm selection:
    /// 1. If input is sorted on group-by keys → SortAggregate
    /// 2. Default → HashAggregate
    fn plan_aggregate(&self, agg: &AggregateOperator) -> Result<PhysicalOperator, PlanningError> {
        let input = self.plan_operator(&agg.input)?;
        let input_schema = input.schema();

        // Convert group-by expressions
        let group_by: Result<Vec<_>, _> = agg
            .group_by
            .iter()
            .map(|e| create_physical_expr(e, &input_schema))
            .collect();
        let group_by = group_by?;

        // Convert aggregate expressions
        let aggregates: Result<Vec<_>, _> = agg
            .aggregates
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let name = format!("agg_{}", i);
                create_physical_aggregate(e, &input_schema, &name)
            })
            .collect();
        let aggregates = aggregates?;

        // Check if input is sorted on group-by keys for sort aggregate
        let input_sorted = self.is_sorted_on_exprs(&input, &group_by);

        if input_sorted && !group_by.is_empty() {
            Ok(PhysicalOperator::SortAggregate(SortAggregateOperator {
                input: Arc::new(input),
                group_by,
                aggregates,
                schema: agg.schema.clone(),
            }))
        } else {
            Ok(PhysicalOperator::HashAggregate(HashAggregateOperator {
                input: Arc::new(input),
                group_by,
                aggregates,
                schema: agg.schema.clone(),
                mode: AggregationMode::Single,
            }))
        }
    }

    /// Checks if operator output is sorted on the given expressions.
    fn is_sorted_on_exprs(&self, op: &PhysicalOperator, exprs: &[PhysicalExpr]) -> bool {
        let props = op.properties();
        if let Some(ordering) = &props.ordering {
            if ordering.exprs.len() >= exprs.len() {
                for (sort_expr, expr) in ordering.exprs.iter().zip(exprs.iter()) {
                    match (&sort_expr.expr, expr) {
                        (
                            PhysicalExpr::Column { index: i1, .. },
                            PhysicalExpr::Column { index: i2, .. },
                        ) if i1 == i2 => continue,
                        _ => return false,
                    }
                }
                return true;
            }
        }
        false
    }

    /// Plans a sort operation.
    ///
    /// Optimizations:
    /// - If Sort has a fetch limit, use it for early termination
    /// - Could use external sort for large datasets
    fn plan_sort(&self, sort: &SortOperator) -> Result<PhysicalOperator, PlanningError> {
        let input = self.plan_operator(&sort.input)?;
        let input_schema = input.schema();

        // Convert sort expressions
        let order_by: Result<Vec<PhysicalSortExpr>, PlanningError> = sort
            .order_by
            .iter()
            .map(|s| {
                let expr = create_physical_expr(&s.expr, &input_schema)?;
                Ok(PhysicalSortExpr::new(expr, s.asc, s.nulls_first))
            })
            .collect();
        let order_by = order_by?;

        // If there's a small fetch limit, use TopN operator
        if let Some(fetch) = sort.fetch {
            if fetch <= self.ctx.config.batch_size {
                return Ok(PhysicalOperator::TopN(TopNOperator {
                    input: Arc::new(input),
                    order_by,
                    n: fetch,
                }));
            }
        }

        Ok(PhysicalOperator::Sort(SortPhysicalOperator {
            input: Arc::new(input),
            order_by,
            fetch: sort.fetch,
            preserve_partitioning: false,
        }))
    }

    /// Plans a limit operation.
    ///
    /// Optimization: If parent is Sort, combine into TopN (handled in plan_sort)
    fn plan_limit(&self, limit: &LimitOperator) -> Result<PhysicalOperator, PlanningError> {
        // Check if input is Sort - if so, we can optimize to TopN
        if let LogicalOperator::Sort(sort) = limit.input.as_ref() {
            if limit.offset == 0 && limit.fetch.is_some() {
                // Create TopN directly
                let fetch = limit.fetch.unwrap();
                if fetch <= self.ctx.config.batch_size {
                    let input = self.plan_operator(&sort.input)?;
                    let input_schema = input.schema();

                    let order_by: Result<Vec<PhysicalSortExpr>, PlanningError> = sort
                        .order_by
                        .iter()
                        .map(|s| {
                            let expr = create_physical_expr(&s.expr, &input_schema)?;
                            Ok(PhysicalSortExpr::new(expr, s.asc, s.nulls_first))
                        })
                        .collect();

                    return Ok(PhysicalOperator::TopN(TopNOperator {
                        input: Arc::new(input),
                        order_by: order_by?,
                        n: fetch,
                    }));
                }
            }
        }

        let input = self.plan_operator(&limit.input)?;

        Ok(PhysicalOperator::Limit(LimitPhysicalOperator {
            input: Arc::new(input),
            offset: limit.offset,
            fetch: limit.fetch,
        }))
    }

    /// Plans a distinct operation.
    fn plan_distinct(
        &self,
        distinct: &DistinctOperator,
    ) -> Result<PhysicalOperator, PlanningError> {
        let input = self.plan_operator(&distinct.input)?;

        Ok(PhysicalOperator::Distinct(DistinctPhysicalOperator {
            input: Arc::new(input),
        }))
    }

    /// Plans a set operation (UNION, INTERSECT, EXCEPT).
    fn plan_set_operation(
        &self,
        setop: &SetOperationOperator,
    ) -> Result<PhysicalOperator, PlanningError> {
        let left = self.plan_operator(&setop.left)?;
        let right = self.plan_operator(&setop.right)?;

        Ok(PhysicalOperator::SetOperation(
            SetOperationPhysicalOperator {
                left: Arc::new(left),
                right: Arc::new(right),
                op: setop.op,
            },
        ))
    }

    /// Plans a subquery alias (just unwrap and plan the inner query).
    fn plan_subquery_alias(
        &self,
        alias: &SubqueryAliasOperator,
    ) -> Result<PhysicalOperator, PlanningError> {
        // Subquery alias is just logical - plan the inner query
        self.plan_operator(&alias.input)
    }

    /// Plans a VALUES clause.
    fn plan_values(&self, values: &ValuesOperator) -> Result<PhysicalOperator, PlanningError> {
        // Convert logical expressions to physical
        let phys_values: Result<Vec<Vec<PhysicalExpr>>, _> = values
            .values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|e| create_physical_expr(e, &values.schema))
                    .collect()
            })
            .collect();

        Ok(PhysicalOperator::Values(ValuesPhysicalOperator {
            values: phys_values?,
            schema: values.schema.clone(),
        }))
    }

    /// Plans an empty relation.
    fn plan_empty(&self, empty: &EmptyRelationOperator) -> Result<PhysicalOperator, PlanningError> {
        Ok(PhysicalOperator::Empty(EmptyOperator {
            schema: empty.schema.clone(),
            produce_one_row: empty.produce_one_row,
        }))
    }

    /// Plans a window function.
    ///
    /// Window functions require sorting the input by partition and order keys,
    /// then computing the window function for each partition.
    fn plan_window(&self, window: &WindowOperator) -> Result<PhysicalOperator, PlanningError> {
        use super::operator::{
            SortExpr as PhysSortExpr, WindowExpr, WindowFrame, WindowFrameBound, WindowFrameType,
            WindowFunc, WindowPhysicalOperator,
        };
        use crate::logical::{LogicalExpr, WindowFunc as LogicalWindowFunc};

        // Plan the child operator
        let input = Arc::new(self.plan_operator(&window.input)?);
        let input_schema = window.input.schema();

        // Convert logical window expressions to physical
        let mut physical_window_exprs = Vec::new();

        for (i, expr) in window.window_exprs.iter().enumerate() {
            if let LogicalExpr::WindowFunction {
                func,
                args,
                partition_by,
                order_by,
                window_frame,
            } = expr
            {
                // Convert the window function type
                let physical_func = match func {
                    LogicalWindowFunc::RowNumber => WindowFunc::RowNumber,
                    LogicalWindowFunc::Rank => WindowFunc::Rank,
                    LogicalWindowFunc::DenseRank => WindowFunc::DenseRank,
                    LogicalWindowFunc::Ntile => WindowFunc::Ntile,
                    LogicalWindowFunc::Lag => WindowFunc::Lag,
                    LogicalWindowFunc::Lead => WindowFunc::Lead,
                    LogicalWindowFunc::FirstValue => WindowFunc::FirstValue,
                    LogicalWindowFunc::LastValue => WindowFunc::LastValue,
                    LogicalWindowFunc::NthValue => WindowFunc::NthValue,
                    LogicalWindowFunc::Aggregate(agg) => WindowFunc::Aggregate(*agg),
                    LogicalWindowFunc::PercentRank | LogicalWindowFunc::CumeDist => {
                        return Err(PlanningError::Unsupported(format!(
                            "Window function {:?} not yet implemented",
                            func
                        )));
                    }
                };

                // Convert arguments
                let physical_args: Result<Vec<PhysicalExpr>, PlanningError> = args
                    .iter()
                    .map(|a| {
                        create_physical_expr(a, &input_schema)
                            .map_err(|e| PlanningError::ExpressionError(e.to_string()))
                    })
                    .collect();
                let physical_args = physical_args?;

                // Convert partition by (if any)
                let physical_partition_by: Result<Vec<PhysicalExpr>, PlanningError> = partition_by
                    .iter()
                    .map(|e| {
                        create_physical_expr(e, &input_schema)
                            .map_err(|e| PlanningError::ExpressionError(e.to_string()))
                    })
                    .collect();
                let physical_partition_by = physical_partition_by?;

                // Convert order by (if any)
                let physical_order_by: Vec<PhysSortExpr> = order_by
                    .iter()
                    .filter_map(|o| {
                        create_physical_expr(&o.expr, &input_schema)
                            .ok()
                            .map(|e| PhysSortExpr {
                                expr: e,
                                asc: o.asc,
                                nulls_first: o.nulls_first,
                            })
                    })
                    .collect();

                // Convert window frame (if any)
                let physical_frame = window_frame.as_ref().map(|f| {
                    use crate::logical::{WindowFrameBound as LB, WindowFrameType as LT};

                    let frame_type = match f.frame_type {
                        LT::Rows => WindowFrameType::Rows,
                        LT::Range => WindowFrameType::Range,
                        LT::Groups => WindowFrameType::Groups,
                    };

                    let convert_bound = |b: &LB| match b {
                        LB::UnboundedPreceding => WindowFrameBound::UnboundedPreceding,
                        LB::UnboundedFollowing => WindowFrameBound::UnboundedFollowing,
                        LB::CurrentRow => WindowFrameBound::CurrentRow,
                        LB::Preceding(n) => WindowFrameBound::Preceding(*n),
                        LB::Following(n) => WindowFrameBound::Following(*n),
                    };

                    WindowFrame {
                        frame_type,
                        start: convert_bound(&f.start),
                        end: convert_bound(&f.end),
                    }
                });

                physical_window_exprs.push(WindowExpr {
                    func: physical_func,
                    args: physical_args,
                    partition_by: physical_partition_by,
                    order_by: physical_order_by,
                    frame: physical_frame,
                    name: format!("window_{}", i),
                });
            } else {
                return Err(PlanningError::Internal(
                    "Expected window function expression".to_string(),
                ));
            }
        }

        Ok(PhysicalOperator::Window(WindowPhysicalOperator {
            input,
            window_exprs: physical_window_exprs,
            schema: window.schema.clone(),
        }))
    }

    /// Plans a CTE (Common Table Expression).
    fn plan_cte(&self, cte: &CteOperator) -> Result<PhysicalOperator, PlanningError> {
        // TODO: Implement CTE planning with materialization
        // For now, just plan the main query
        self.plan_operator(&cte.input)
    }
}

/// Helper function to create a physical planner with default context.
pub fn create_physical_plan(logical: &LogicalOperator) -> Result<PhysicalPlan, PlanningError> {
    let ctx = ExecutionContext::default();
    let planner = PhysicalPlanner::new(&ctx);
    planner.create_physical_plan(logical)
}

/// Error type for physical planning.
#[derive(Debug, Clone)]
pub enum PlanningError {
    /// Expression conversion error.
    ExpressionError(String),
    /// Unsupported operation.
    Unsupported(String),
    /// Schema mismatch.
    SchemaMismatch(String),
    /// Internal error.
    Internal(String),
}

impl std::fmt::Display for PlanningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanningError::ExpressionError(msg) => write!(f, "Expression error: {}", msg),
            PlanningError::Unsupported(msg) => write!(f, "Unsupported: {}", msg),
            PlanningError::SchemaMismatch(msg) => write!(f, "Schema mismatch: {}", msg),
            PlanningError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for PlanningError {}

impl From<String> for PlanningError {
    fn from(s: String) -> Self {
        PlanningError::ExpressionError(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::{Field, LogicalExpr, Schema};
    use crate::parser::DataType;

    fn users_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
            Field::nullable("age", DataType::Int),
        ])
    }

    fn orders_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("order_id", DataType::Int),
            Field::not_null("user_id", DataType::Int),
            Field::nullable(
                "amount",
                DataType::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
            ),
        ])
    }

    #[test]
    fn test_plan_simple_scan() {
        let logical = LogicalOperator::Scan(ScanOperator::new("users", users_schema()));
        let plan = create_physical_plan(&logical).unwrap();

        assert_eq!(plan.operator_count(), 1);
        assert!(matches!(plan.root(), PhysicalOperator::SeqScan(_)));
    }

    #[test]
    fn test_plan_scan_with_projection() {
        let scan = ScanOperator::new("users", users_schema()).with_projection(vec![0, 1]);
        let logical = LogicalOperator::Scan(scan);
        let plan = create_physical_plan(&logical).unwrap();

        if let PhysicalOperator::SeqScan(scan) = plan.root() {
            assert_eq!(scan.projection, Some(vec![0, 1]));
            assert_eq!(scan.projected_schema.len(), 2);
        } else {
            panic!("Expected SeqScan");
        }
    }

    #[test]
    fn test_plan_filter() {
        let schema = users_schema();
        let scan = LogicalOperator::Scan(ScanOperator::new("users", schema));
        let filter = LogicalOperator::Filter(FilterOperator {
            input: Arc::new(scan),
            predicate: LogicalExpr::col("age").gt(LogicalExpr::lit_i64(18)),
        });

        let plan = create_physical_plan(&filter).unwrap();

        assert_eq!(plan.operator_count(), 2);
        assert!(matches!(plan.root(), PhysicalOperator::Filter(_)));
    }

    #[test]
    fn test_plan_projection() {
        let schema = users_schema();
        let scan = Arc::new(LogicalOperator::Scan(ScanOperator::new(
            "users",
            schema.clone(),
        )));

        let proj_schema = Schema::new(vec![Field::not_null("id", DataType::Int)]);

        let proj = LogicalOperator::Projection(ProjectionOperator {
            input: scan,
            exprs: vec![LogicalExpr::col("id")],
            schema: Arc::new(proj_schema),
        });

        let plan = create_physical_plan(&proj).unwrap();

        assert_eq!(plan.operator_count(), 2);
        assert!(matches!(plan.root(), PhysicalOperator::Projection(_)));
    }

    #[test]
    fn test_plan_hash_join() {
        let users = Arc::new(LogicalOperator::Scan(ScanOperator::new(
            "users",
            users_schema(),
        )));
        let orders = Arc::new(LogicalOperator::Scan(ScanOperator::new(
            "orders",
            orders_schema(),
        )));

        let join = JoinOperator::new(
            users,
            orders,
            JoinType::Inner,
            Some(LogicalExpr::col("id").eq(LogicalExpr::col("user_id"))),
        );
        let logical = LogicalOperator::Join(join);

        let plan = create_physical_plan(&logical).unwrap();

        assert!(plan.has_joins());
        assert!(matches!(plan.root(), PhysicalOperator::HashJoin(_)));
    }

    #[test]
    fn test_plan_cross_join() {
        let users = Arc::new(LogicalOperator::Scan(ScanOperator::new(
            "users",
            users_schema(),
        )));
        let orders = Arc::new(LogicalOperator::Scan(ScanOperator::new(
            "orders",
            orders_schema(),
        )));

        let join = JoinOperator::new(users, orders, JoinType::Cross, None);
        let logical = LogicalOperator::Join(join);

        let plan = create_physical_plan(&logical).unwrap();

        assert!(matches!(plan.root(), PhysicalOperator::NestedLoopJoin(_)));
    }

    #[test]
    fn test_plan_hash_aggregate() {
        let schema = users_schema();
        let scan = Arc::new(LogicalOperator::Scan(ScanOperator::new("users", schema)));

        let agg_schema = Schema::new(vec![Field::not_null("count", DataType::BigInt)]);

        // Create COUNT(*) using AggregateFunction variant
        let count_star = LogicalExpr::AggregateFunction {
            name: crate::logical::AggregateFunc::CountStar,
            args: vec![],
            distinct: false,
            filter: None,
        };

        let agg = AggregateOperator {
            input: scan,
            group_by: vec![],
            aggregates: vec![count_star],
            schema: Arc::new(agg_schema),
        };
        let logical = LogicalOperator::Aggregate(agg);

        let plan = create_physical_plan(&logical).unwrap();

        assert!(plan.has_aggregates());
        assert!(matches!(plan.root(), PhysicalOperator::HashAggregate(_)));
    }

    #[test]
    fn test_plan_sort() {
        let schema = users_schema();
        let scan = Arc::new(LogicalOperator::Scan(ScanOperator::new("users", schema)));

        let sort = SortOperator {
            input: scan,
            order_by: vec![crate::logical::SortExpr::asc(LogicalExpr::col("name"))],
            fetch: None,
        };
        let logical = LogicalOperator::Sort(sort);

        let plan = create_physical_plan(&logical).unwrap();

        assert!(plan.has_sorts());
        assert!(matches!(plan.root(), PhysicalOperator::Sort(_)));
    }

    #[test]
    fn test_plan_topn() {
        let schema = users_schema();
        let scan = Arc::new(LogicalOperator::Scan(ScanOperator::new("users", schema)));

        let sort = Arc::new(LogicalOperator::Sort(SortOperator {
            input: scan,
            order_by: vec![crate::logical::SortExpr::asc(LogicalExpr::col("name"))],
            fetch: None,
        }));

        let limit = LimitOperator {
            input: sort,
            offset: 0,
            fetch: Some(10),
        };
        let logical = LogicalOperator::Limit(limit);

        let plan = create_physical_plan(&logical).unwrap();

        // Should optimize Sort + Limit into TopN
        assert!(matches!(plan.root(), PhysicalOperator::TopN(_)));
    }

    #[test]
    fn test_plan_limit() {
        let schema = users_schema();
        let scan = Arc::new(LogicalOperator::Scan(ScanOperator::new("users", schema)));

        let limit = LimitOperator {
            input: scan,
            offset: 10,
            fetch: Some(20),
        };
        let logical = LogicalOperator::Limit(limit);

        let plan = create_physical_plan(&logical).unwrap();

        if let PhysicalOperator::Limit(lim) = plan.root() {
            assert_eq!(lim.offset, 10);
            assert_eq!(lim.fetch, Some(20));
        } else {
            panic!("Expected Limit");
        }
    }

    #[test]
    fn test_plan_distinct() {
        let schema = users_schema();
        let scan = Arc::new(LogicalOperator::Scan(ScanOperator::new("users", schema)));

        let distinct = DistinctOperator {
            input: scan,
            on_columns: None,
        };
        let logical = LogicalOperator::Distinct(distinct);

        let plan = create_physical_plan(&logical).unwrap();

        assert!(matches!(plan.root(), PhysicalOperator::Distinct(_)));
    }

    #[test]
    fn test_plan_empty_relation() {
        let schema = Schema::new(vec![Field::not_null("x", DataType::Int)]);
        let empty = EmptyRelationOperator {
            produce_one_row: true,
            schema: Arc::new(schema),
        };
        let logical = LogicalOperator::EmptyRelation(empty);

        let plan = create_physical_plan(&logical).unwrap();

        if let PhysicalOperator::Empty(e) = plan.root() {
            assert!(e.produce_one_row);
        } else {
            panic!("Expected Empty");
        }
    }

    #[test]
    fn test_plan_values() {
        let schema = Schema::new(vec![
            Field::not_null("a", DataType::Int),
            Field::not_null("b", DataType::Int),
        ]);
        let values = ValuesOperator {
            values: vec![
                vec![LogicalExpr::lit_i64(1), LogicalExpr::lit_i64(2)],
                vec![LogicalExpr::lit_i64(3), LogicalExpr::lit_i64(4)],
            ],
            schema: Arc::new(schema),
        };
        let logical = LogicalOperator::Values(values);

        let plan = create_physical_plan(&logical).unwrap();

        if let PhysicalOperator::Values(v) = plan.root() {
            assert_eq!(v.values.len(), 2);
        } else {
            panic!("Expected Values");
        }
    }

    #[test]
    fn test_explain_output() {
        let schema = users_schema();
        let scan = Arc::new(LogicalOperator::Scan(ScanOperator::new("users", schema)));

        let filter = LogicalOperator::Filter(FilterOperator {
            input: scan,
            predicate: LogicalExpr::col("age").gt(LogicalExpr::lit_i64(21)),
        });

        let plan = create_physical_plan(&filter).unwrap();
        let explain = plan.explain(false);

        assert!(explain.contains("Filter"));
        assert!(explain.contains("SeqScan"));
        assert!(explain.contains("users"));
    }

    #[test]
    fn test_plan_having_clause() {
        // Test planning a filter on top of an aggregate (HAVING clause)
        let schema = orders_schema();
        let scan = Arc::new(LogicalOperator::Scan(ScanOperator::new("orders", schema)));

        // Create aggregate with SUM(amount) grouped by user_id
        let agg_schema = Schema::new(vec![
            Field::nullable("user_id", DataType::Int),
            Field::not_null("SUM(amount)", DataType::BigInt),
        ]);

        let agg = Arc::new(LogicalOperator::Aggregate(AggregateOperator {
            input: scan,
            group_by: vec![LogicalExpr::col("user_id")],
            aggregates: vec![LogicalExpr::AggregateFunction {
                name: crate::logical::AggregateFunc::Sum,
                args: vec![LogicalExpr::col("amount")],
                distinct: false,
                filter: None,
            }],
            schema: Arc::new(agg_schema),
        }));

        // Create filter (HAVING SUM(amount) > 100)
        // The predicate references the aggregate output column, not the aggregate function
        let filter = LogicalOperator::Filter(FilterOperator {
            input: agg,
            predicate: LogicalExpr::col("SUM(amount)").gt(LogicalExpr::lit_i64(100)),
        });

        let plan = create_physical_plan(&filter);
        assert!(plan.is_ok(), "HAVING physical plan failed: {:?}", plan);

        let plan = plan.unwrap();
        assert!(matches!(plan.root(), PhysicalOperator::Filter(_)));
    }
}
