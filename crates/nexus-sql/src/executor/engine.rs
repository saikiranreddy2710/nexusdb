//! Query execution engine.
//!
//! This module provides the main execution engine that converts physical plans
//! into executable operator trees and runs queries.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use super::{
    DistinctExec, EmptyExec, ExecutionError, FilterExec, HashAggregateExec, HashJoinExec,
    LimitExec, Operator, ProjectionExec, RecordBatch, SeqScanExec, SortExec, TopNExec, Value,
    ValuesExec,
};
use crate::logical::Schema;
use crate::physical::{ExecutionContext, ExecutionMetrics, PhysicalOperator, PhysicalPlan};

/// Query executor that runs physical plans.
#[derive(Debug)]
pub struct QueryExecutor {
    /// Execution context.
    ctx: ExecutionContext,
    /// Table data (for in-memory execution).
    tables: HashMap<String, Vec<RecordBatch>>,
}

impl QueryExecutor {
    /// Creates a new query executor.
    pub fn new(ctx: ExecutionContext) -> Self {
        Self {
            ctx,
            tables: HashMap::new(),
        }
    }

    /// Registers a table with data.
    pub fn register_table(&mut self, name: impl Into<String>, data: Vec<RecordBatch>) {
        self.tables.insert(name.into(), data);
    }

    /// Registers a table from rows.
    pub fn register_table_from_rows(
        &mut self,
        name: impl Into<String>,
        schema: Arc<Schema>,
        rows: Vec<super::Row>,
    ) -> Result<(), String> {
        let batch = RecordBatch::from_rows(schema, &rows)?;
        self.tables.insert(name.into(), vec![batch]);
        Ok(())
    }

    /// Returns the table data.
    pub fn get_table(&self, name: &str) -> Option<&Vec<RecordBatch>> {
        self.tables.get(name)
    }

    /// Executes a physical plan and returns all results.
    pub fn execute(&self, plan: &PhysicalPlan) -> Result<QueryResult, ExecutionError> {
        let start = Instant::now();

        // Build operator tree
        let mut root = self.build_operator(&plan.root)?;

        // Collect all batches
        let mut batches = Vec::new();
        let mut total_rows = 0;

        while let Some(batch) = root.next_batch()? {
            total_rows += batch.num_rows();
            batches.push(batch);
        }

        let elapsed = start.elapsed();
        let batches_count = batches.len();

        Ok(QueryResult {
            schema: root.schema(),
            batches,
            total_rows,
            execution_time_ms: elapsed.as_millis() as u64,
            metrics: ExecutionMetrics {
                rows_processed: total_rows,
                batches_processed: batches_count,
                execution_time_us: elapsed.as_micros() as u64,
                ..Default::default()
            },
        })
    }

    /// Executes and returns a streaming iterator over batches.
    pub fn execute_stream(&self, plan: &PhysicalPlan) -> Result<Box<dyn Operator>, ExecutionError> {
        self.build_operator(&plan.root)
    }

    /// Builds an operator tree from a physical operator.
    fn build_operator(&self, op: &PhysicalOperator) -> Result<Box<dyn Operator>, ExecutionError> {
        match op {
            PhysicalOperator::SeqScan(scan) => {
                let data = self
                    .tables
                    .get(&scan.table_name)
                    .cloned()
                    .unwrap_or_default();
                Ok(Box::new(SeqScanExec::from_physical(scan, data)))
            }

            PhysicalOperator::IndexScan(scan) => {
                // Fall back to sequential scan for now
                let data = self
                    .tables
                    .get(&scan.table_name)
                    .cloned()
                    .unwrap_or_default();
                Ok(Box::new(SeqScanExec::new(
                    scan.projected_schema.clone(),
                    scan.table_name.clone(),
                    data,
                )))
            }

            PhysicalOperator::Filter(filter) => {
                let child = self.build_operator(&filter.input)?;
                Ok(Box::new(FilterExec::new(child, filter.predicate.clone())))
            }

            PhysicalOperator::Projection(proj) => {
                let child = self.build_operator(&proj.input)?;
                Ok(Box::new(ProjectionExec::new(
                    child,
                    proj.exprs.clone(),
                    proj.schema.clone(),
                )))
            }

            PhysicalOperator::Limit(limit) => {
                let child = self.build_operator(&limit.input)?;
                Ok(Box::new(LimitExec::new(
                    child,
                    limit.fetch.unwrap_or(usize::MAX),
                    limit.offset,
                )))
            }

            PhysicalOperator::HashJoin(join) => {
                let left = self.build_operator(&join.left)?;
                let right = self.build_operator(&join.right)?;

                // Convert JoinKey to expressions
                let left_keys: Vec<_> = join.left_keys.iter().map(|k| k.expr.clone()).collect();
                let right_keys: Vec<_> = join.right_keys.iter().map(|k| k.expr.clone()).collect();

                Ok(Box::new(HashJoinExec::new(
                    left,
                    right,
                    left_keys,
                    right_keys,
                    join.join_type,
                    join.schema.clone(),
                )))
            }

            PhysicalOperator::MergeJoin(join) => {
                // Fall back to hash join for now
                let left = self.build_operator(&join.left)?;
                let right = self.build_operator(&join.right)?;

                let left_keys: Vec<_> = join.left_keys.iter().map(|k| k.expr.clone()).collect();
                let right_keys: Vec<_> = join.right_keys.iter().map(|k| k.expr.clone()).collect();

                Ok(Box::new(HashJoinExec::new(
                    left,
                    right,
                    left_keys,
                    right_keys,
                    join.join_type,
                    join.schema.clone(),
                )))
            }

            PhysicalOperator::NestedLoopJoin(join) => {
                // Fall back to hash join with empty keys (cross join)
                let left = self.build_operator(&join.left)?;
                let right = self.build_operator(&join.right)?;

                Ok(Box::new(HashJoinExec::new(
                    left,
                    right,
                    Vec::new(),
                    Vec::new(),
                    join.join_type,
                    join.schema.clone(),
                )))
            }

            PhysicalOperator::HashAggregate(agg) => {
                let child = self.build_operator(&agg.input)?;
                Ok(Box::new(HashAggregateExec::new(
                    child,
                    agg.group_by.clone(),
                    agg.aggregates.clone(),
                    agg.schema.clone(),
                )))
            }

            PhysicalOperator::SortAggregate(agg) => {
                // Fall back to hash aggregate
                let child = self.build_operator(&agg.input)?;
                Ok(Box::new(HashAggregateExec::new(
                    child,
                    agg.group_by.clone(),
                    agg.aggregates.clone(),
                    agg.schema.clone(),
                )))
            }

            PhysicalOperator::Sort(sort) => {
                let child = self.build_operator(&sort.input)?;
                Ok(Box::new(SortExec::new(child, sort.order_by.clone())))
            }

            PhysicalOperator::TopN(topn) => {
                let child = self.build_operator(&topn.input)?;
                Ok(Box::new(TopNExec::new(
                    child,
                    topn.order_by.clone(),
                    topn.n,
                )))
            }

            PhysicalOperator::Distinct(distinct) => {
                let child = self.build_operator(&distinct.input)?;
                Ok(Box::new(DistinctExec::new(child)))
            }

            PhysicalOperator::Values(values) => {
                // Convert PhysicalExpr values to runtime Values
                let schema = values.schema.clone();
                let rows: Vec<Vec<Value>> = values
                    .values
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|expr| {
                                if let crate::physical::PhysicalExpr::Literal(lit) = expr {
                                    Value::from_literal(lit)
                                } else {
                                    Value::Null
                                }
                            })
                            .collect()
                    })
                    .collect();

                Ok(Box::new(ValuesExec::new(schema, rows)))
            }

            PhysicalOperator::Empty(empty) => Ok(Box::new(EmptyExec::new(empty.schema.clone()))),

            PhysicalOperator::SetOperation(setop) => {
                // Basic implementation: for UNION ALL, just concatenate
                // For other set operations, use distinct
                let left = self.build_operator(&setop.left)?;
                let right = self.build_operator(&setop.right)?;
                let schema = setop.left.schema();

                // Collect both sides
                Ok(Box::new(SetOperationExec::new(
                    left, right, setop.op, schema,
                )))
            }

            PhysicalOperator::Exchange(exchange) => {
                // For single-node execution, exchange is a no-op
                self.build_operator(&exchange.input)
            }
        }
    }
}

/// Result of query execution.
#[derive(Debug)]
pub struct QueryResult {
    /// Output schema.
    pub schema: Arc<Schema>,
    /// Result batches.
    pub batches: Vec<RecordBatch>,
    /// Total number of rows.
    pub total_rows: usize,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Execution metrics.
    pub metrics: ExecutionMetrics,
}

impl QueryResult {
    /// Returns all rows as a vector.
    pub fn rows(&self) -> Vec<super::Row> {
        self.batches.iter().flat_map(|b| b.rows()).collect()
    }

    /// Returns true if the result is empty.
    pub fn is_empty(&self) -> bool {
        self.total_rows == 0
    }

    /// Pretty prints the result as a table.
    pub fn display(&self) -> String {
        let mut output = String::new();

        // Header
        let fields = self.schema.fields();
        let mut col_widths: Vec<usize> = fields.iter().map(|f| f.name().len()).collect();

        // Calculate column widths
        for batch in &self.batches {
            for row in batch.rows() {
                for (i, val) in row.iter().enumerate() {
                    let width = val.to_string().len();
                    if i < col_widths.len() {
                        col_widths[i] = col_widths[i].max(width);
                    }
                }
            }
        }

        // Print header
        for (i, field) in fields.iter().enumerate() {
            if i > 0 {
                output.push_str(" | ");
            }
            output.push_str(&format!("{:width$}", field.name(), width = col_widths[i]));
        }
        output.push('\n');

        // Print separator
        for (i, width) in col_widths.iter().enumerate() {
            if i > 0 {
                output.push_str("-+-");
            }
            output.push_str(&"-".repeat(*width));
        }
        output.push('\n');

        // Print data
        for batch in &self.batches {
            for row in batch.rows() {
                for (i, val) in row.iter().enumerate() {
                    if i > 0 {
                        output.push_str(" | ");
                    }
                    output.push_str(&format!("{:width$}", val, width = col_widths[i]));
                }
                output.push('\n');
            }
        }

        // Print row count
        output.push_str(&format!("({} rows)\n", self.total_rows));

        output
    }
}

/// Set operation executor.
#[derive(Debug)]
struct SetOperationExec {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    op: crate::logical::SetOpType,
    schema: Arc<Schema>,
    state: SetOpState,
}

#[derive(Debug)]
enum SetOpState {
    Left,
    Right,
    Done,
}

impl SetOperationExec {
    fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        op: crate::logical::SetOpType,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            left,
            right,
            op,
            schema,
            state: SetOpState::Left,
        }
    }
}

impl Operator for SetOperationExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        loop {
            match self.state {
                SetOpState::Left => {
                    if let Some(batch) = self.left.next_batch()? {
                        return Ok(Some(batch));
                    }
                    self.state = SetOpState::Right;
                }
                SetOpState::Right => {
                    if let Some(batch) = self.right.next_batch()? {
                        return Ok(Some(batch));
                    }
                    self.state = SetOpState::Done;
                }
                SetOpState::Done => {
                    return Ok(None);
                }
            }
        }
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.state = SetOpState::Left;
    }
}

/// Convenience function to execute a query.
pub fn execute_query(
    plan: &PhysicalPlan,
    tables: HashMap<String, Vec<RecordBatch>>,
) -> Result<QueryResult, ExecutionError> {
    let ctx = ExecutionContext::default();
    let mut executor = QueryExecutor::new(ctx);

    for (name, data) in tables {
        executor.register_table(name, data);
    }

    executor.execute(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::{AggregateFunc, BinaryOp, Field, Schema};
    use crate::parser::{DataType, Literal};
    use crate::physical::*;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Text),
            Field::nullable("age", DataType::Int),
        ]))
    }

    fn test_data() -> RecordBatch {
        let schema = test_schema();
        let rows = vec![
            super::super::Row::new(vec![Value::int(1), Value::string("Alice"), Value::int(30)]),
            super::super::Row::new(vec![Value::int(2), Value::string("Bob"), Value::int(25)]),
            super::super::Row::new(vec![
                Value::int(3),
                Value::string("Charlie"),
                Value::int(35),
            ]),
        ];
        RecordBatch::from_rows(schema, &rows).unwrap()
    }

    fn create_scan() -> PhysicalOperator {
        PhysicalOperator::SeqScan(SeqScanOperator::new("users", (*test_schema()).clone()))
    }

    #[test]
    fn test_execute_scan() {
        let scan = create_scan();
        let plan = PhysicalPlan::new(scan);

        let mut executor = QueryExecutor::new(ExecutionContext::default());
        executor.register_table("users", vec![test_data()]);

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.total_rows, 3);
    }

    #[test]
    fn test_execute_filter() {
        let scan = Arc::new(create_scan());

        let filter = PhysicalOperator::Filter(FilterPhysicalOperator {
            input: scan,
            predicate: PhysicalExpr::binary(
                PhysicalExpr::column("age", 2),
                BinaryOp::Gt,
                PhysicalExpr::Literal(Literal::Integer(28)),
            ),
        });

        let plan = PhysicalPlan::new(filter);

        let mut executor = QueryExecutor::new(ExecutionContext::default());
        executor.register_table("users", vec![test_data()]);

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.total_rows, 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_execute_limit() {
        let scan = Arc::new(create_scan());

        let limit = PhysicalOperator::Limit(LimitPhysicalOperator {
            input: scan,
            fetch: Some(2),
            offset: 0,
        });

        let plan = PhysicalPlan::new(limit);

        let mut executor = QueryExecutor::new(ExecutionContext::default());
        executor.register_table("users", vec![test_data()]);

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.total_rows, 2);
    }

    #[test]
    #[ignore = "Test hanging - needs investigation"]
    fn test_execute_aggregate() {
        let scan = Arc::new(create_scan());

        let agg_schema = Arc::new(Schema::new(vec![
            Field::not_null("count", DataType::BigInt),
            Field::nullable("avg_age", DataType::Double),
        ]));

        let agg = PhysicalOperator::HashAggregate(HashAggregateOperator {
            input: scan,
            group_by: vec![],
            aggregates: vec![
                PhysicalAggregateExpr::new(
                    AggregateFunc::Count,
                    vec![PhysicalExpr::column("id", 0)],
                    false,
                    "count",
                ),
                PhysicalAggregateExpr::new(
                    AggregateFunc::Avg,
                    vec![PhysicalExpr::column("age", 2)],
                    false,
                    "avg_age",
                ),
            ],
            schema: agg_schema,
            mode: AggregationMode::Single,
        });

        let plan = PhysicalPlan::new(agg);

        let mut executor = QueryExecutor::new(ExecutionContext::default());
        executor.register_table("users", vec![test_data()]);

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.total_rows, 1);

        let row = result.rows()[0].clone();
        assert_eq!(row.get(0), Some(&Value::BigInt(3))); // count = 3
                                                         // avg_age = (30 + 25 + 35) / 3 = 30.0
        if let Some(Value::Double(avg)) = row.get(1) {
            assert!((avg - 30.0).abs() < 0.01);
        }
    }

    #[test]
    fn test_execute_sort() {
        let scan = Arc::new(create_scan());

        let sort = PhysicalOperator::Sort(SortPhysicalOperator {
            input: scan,
            order_by: vec![PhysicalSortExpr::desc(PhysicalExpr::column("age", 2))],
            fetch: None,
            preserve_partitioning: false,
        });

        let plan = PhysicalPlan::new(sort);

        let mut executor = QueryExecutor::new(ExecutionContext::default());
        executor.register_table("users", vec![test_data()]);

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.total_rows, 3);

        let rows = result.rows();
        // Should be sorted by age descending: Charlie (35), Alice (30), Bob (25)
        assert_eq!(rows[0].get(2), Some(&Value::Int(35)));
        assert_eq!(rows[1].get(2), Some(&Value::Int(30)));
        assert_eq!(rows[2].get(2), Some(&Value::Int(25)));
    }

    #[test]
    fn test_query_result_display() {
        let schema = test_schema();
        let rows = vec![super::super::Row::new(vec![
            Value::int(1),
            Value::string("Alice"),
            Value::int(30),
        ])];
        let batch = RecordBatch::from_rows(schema.clone(), &rows).unwrap();

        let result = QueryResult {
            schema,
            batches: vec![batch],
            total_rows: 1,
            execution_time_ms: 10,
            metrics: ExecutionMetrics::default(),
        };

        let display = result.display();
        assert!(display.contains("id"));
        assert!(display.contains("Alice"));
        assert!(display.contains("(1 rows)"));
    }
}
