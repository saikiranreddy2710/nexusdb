//! Operator execution implementations.
//!
//! This module implements executable operators that consume and produce
//! record batches in a pull-based iterator model.

use std::collections::HashMap;
use std::sync::Arc;

use super::{
    evaluate_expr, Accumulator, Column, EvalError, RecordBatch, RecordBatchBuilder, Row, Value,
};
use crate::logical::{Field, JoinType, Schema};
use crate::parser::DataType;
use crate::physical::{
    AggregationMode, DistinctPhysicalOperator, FilterPhysicalOperator, HashAggregateOperator,
    HashJoinOperator, LimitPhysicalOperator, PhysicalAggregateExpr, PhysicalExpr, PhysicalOperator,
    PhysicalSortExpr, ProjectionPhysicalOperator, SeqScanOperator, SortPhysicalOperator,
    TopNOperator, ValuesPhysicalOperator,
};

/// Trait for executable operators.
pub trait Operator: std::fmt::Debug {
    /// Returns the output schema.
    fn schema(&self) -> Arc<Schema>;

    /// Returns the next batch of results, or None if exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError>;

    /// Resets the operator to its initial state.
    fn reset(&mut self);
}

/// Sequential scan operator that reads from a data source.
#[derive(Debug)]
pub struct SeqScanExec {
    /// Output schema.
    schema: Arc<Schema>,
    /// Table name.
    table_name: String,
    /// Data to scan (for in-memory tables).
    data: Vec<RecordBatch>,
    /// Current batch index.
    current_batch: usize,
    /// Projection indices.
    projection: Option<Vec<usize>>,
}

impl SeqScanExec {
    /// Creates a new sequential scan operator.
    pub fn new(schema: Arc<Schema>, table_name: String, data: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            table_name,
            data,
            current_batch: 0,
            projection: None,
        }
    }

    /// Creates a scan with projection.
    pub fn with_projection(mut self, indices: Vec<usize>) -> Self {
        // Update schema to reflect projection
        let fields: Vec<Field> = indices
            .iter()
            .filter_map(|&i| self.schema.field(i).cloned())
            .collect();
        self.schema = Arc::new(Schema::new(fields));
        self.projection = Some(indices);
        self
    }

    /// Creates from a SeqScanOperator.
    pub fn from_physical(op: &SeqScanOperator, data: Vec<RecordBatch>) -> Self {
        let mut exec = Self::new(op.projected_schema.clone(), op.table_name.clone(), data);
        exec.projection = op.projection.clone();
        exec
    }
}

impl Operator for SeqScanExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        if self.current_batch >= self.data.len() {
            return Ok(None);
        }

        let batch = &self.data[self.current_batch];
        self.current_batch += 1;

        // Apply projection if specified
        if let Some(ref indices) = self.projection {
            batch
                .project(indices)
                .map(Some)
                .map_err(ExecutionError::Internal)
        } else {
            Ok(Some(batch.clone()))
        }
    }

    fn reset(&mut self) {
        self.current_batch = 0;
    }
}

/// Filter operator that applies a predicate to each row.
#[derive(Debug)]
pub struct FilterExec {
    /// Child operator.
    child: Box<dyn Operator>,
    /// Filter predicate.
    predicate: PhysicalExpr,
}

impl FilterExec {
    /// Creates a new filter operator.
    pub fn new(child: Box<dyn Operator>, predicate: PhysicalExpr) -> Self {
        Self { child, predicate }
    }
}

impl Operator for FilterExec {
    fn schema(&self) -> Arc<Schema> {
        self.child.schema()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        while let Some(batch) = self.child.next_batch()? {
            if batch.is_empty() {
                continue;
            }

            let schema = batch.schema();
            let mut mask = Vec::with_capacity(batch.num_rows());

            for row in batch.rows() {
                let result = evaluate_expr(&self.predicate, &row, schema)?;
                mask.push(result.to_bool().unwrap_or(false));
            }

            // Check if any rows pass the filter
            if mask.iter().any(|&b| b) {
                let filtered = batch.filter(&mask).map_err(ExecutionError::Internal)?;
                if !filtered.is_empty() {
                    return Ok(Some(filtered));
                }
            }
        }

        Ok(None)
    }

    fn reset(&mut self) {
        self.child.reset();
    }
}

/// Projection operator that selects and transforms columns.
#[derive(Debug)]
pub struct ProjectionExec {
    /// Child operator.
    child: Box<dyn Operator>,
    /// Projection expressions.
    exprs: Vec<PhysicalExpr>,
    /// Output schema.
    schema: Arc<Schema>,
}

impl ProjectionExec {
    /// Creates a new projection operator.
    pub fn new(child: Box<dyn Operator>, exprs: Vec<PhysicalExpr>, schema: Arc<Schema>) -> Self {
        Self {
            child,
            exprs,
            schema,
        }
    }
}

impl Operator for ProjectionExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        if let Some(batch) = self.child.next_batch()? {
            let input_schema = batch.schema();
            let mut columns = Vec::with_capacity(self.exprs.len());

            for expr in &self.exprs {
                let mut values = Vec::with_capacity(batch.num_rows());
                for row in batch.rows() {
                    let value = evaluate_expr(expr, &row, input_schema)?;
                    values.push(value);
                }
                let data_type = self
                    .schema
                    .fields()
                    .get(columns.len())
                    .map(|f| f.data_type.clone())
                    .unwrap_or(DataType::Text);
                columns.push(Column::new(data_type, values));
            }

            let result =
                RecordBatch::new(self.schema.clone(), columns).map_err(ExecutionError::Internal)?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) {
        self.child.reset();
    }
}

/// Limit operator that restricts the number of output rows.
#[derive(Debug)]
pub struct LimitExec {
    /// Child operator.
    child: Box<dyn Operator>,
    /// Maximum rows to return.
    limit: usize,
    /// Rows to skip.
    offset: usize,
    /// Rows emitted so far.
    emitted: usize,
    /// Rows skipped so far.
    skipped: usize,
}

impl LimitExec {
    /// Creates a new limit operator.
    pub fn new(child: Box<dyn Operator>, limit: usize, offset: usize) -> Self {
        Self {
            child,
            limit,
            offset,
            emitted: 0,
            skipped: 0,
        }
    }
}

impl Operator for LimitExec {
    fn schema(&self) -> Arc<Schema> {
        self.child.schema()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        while self.emitted < self.limit {
            if let Some(batch) = self.child.next_batch()? {
                // Handle offset
                let skip_in_batch = (self.offset - self.skipped).min(batch.num_rows());
                self.skipped += skip_in_batch;

                if skip_in_batch >= batch.num_rows() {
                    continue;
                }

                // Calculate how many rows to take
                let remaining = self.limit - self.emitted;
                let take = (batch.num_rows() - skip_in_batch).min(remaining);

                if take == 0 {
                    continue;
                }

                let result = batch
                    .slice(skip_in_batch, take)
                    .map_err(ExecutionError::Internal)?;
                self.emitted += result.num_rows();

                return Ok(Some(result));
            } else {
                break;
            }
        }

        Ok(None)
    }

    fn reset(&mut self) {
        self.child.reset();
        self.emitted = 0;
        self.skipped = 0;
    }
}

/// Hash join operator for joining two inputs.
#[derive(Debug)]
pub struct HashJoinExec {
    /// Left (build) input.
    left: Box<dyn Operator>,
    /// Right (probe) input.
    right: Box<dyn Operator>,
    /// Left join keys.
    left_keys: Vec<PhysicalExpr>,
    /// Right join keys.
    right_keys: Vec<PhysicalExpr>,
    /// Join type.
    join_type: JoinType,
    /// Output schema.
    schema: Arc<Schema>,
    /// Hash table built from left side.
    hash_table: Option<HashMap<Vec<Value>, Vec<Row>>>,
    /// Buffer for output rows.
    output_buffer: Vec<Row>,
    /// Current position in output buffer.
    output_pos: usize,
}

impl HashJoinExec {
    /// Creates a new hash join operator.
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        left_keys: Vec<PhysicalExpr>,
        right_keys: Vec<PhysicalExpr>,
        join_type: JoinType,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            schema,
            hash_table: None,
            output_buffer: Vec::new(),
            output_pos: 0,
        }
    }

    fn build_hash_table(&mut self) -> Result<(), ExecutionError> {
        let mut hash_table: HashMap<Vec<Value>, Vec<Row>> = HashMap::new();
        let left_schema = self.left.schema();

        while let Some(batch) = self.left.next_batch()? {
            for row in batch.rows() {
                let key: Result<Vec<Value>, _> = self
                    .left_keys
                    .iter()
                    .map(|k| evaluate_expr(k, &row, &left_schema))
                    .collect();
                let key = key?;

                // Skip NULL keys for inner/left joins
                if key.iter().any(|v| v.is_null()) && self.join_type == JoinType::Inner {
                    continue;
                }

                hash_table.entry(key).or_default().push(row);
            }
        }

        self.hash_table = Some(hash_table);
        Ok(())
    }

    fn probe(&mut self) -> Result<bool, ExecutionError> {
        let hash_table = self.hash_table.as_ref().unwrap();
        let right_schema = self.right.schema();
        let left_schema = self.left.schema();
        let left_nulls = Row::nulls(left_schema.fields().len());

        while let Some(batch) = self.right.next_batch()? {
            for right_row in batch.rows() {
                let key: Result<Vec<Value>, _> = self
                    .right_keys
                    .iter()
                    .map(|k| evaluate_expr(k, &right_row, &right_schema))
                    .collect();
                let key = key?;

                // Handle NULL keys
                if key.iter().any(|v| v.is_null()) {
                    match self.join_type {
                        JoinType::Left | JoinType::Full => {
                            self.output_buffer.push(left_nulls.concat(&right_row));
                        }
                        _ => continue,
                    }
                    continue;
                }

                if let Some(left_rows) = hash_table.get(&key) {
                    for left_row in left_rows {
                        let combined = left_row.concat(&right_row);
                        self.output_buffer.push(combined);
                    }
                } else {
                    // No match found
                    match self.join_type {
                        JoinType::Left | JoinType::Full => {
                            self.output_buffer.push(left_nulls.concat(&right_row));
                        }
                        JoinType::Right => {
                            // Will be handled separately
                        }
                        _ => {}
                    }
                }
            }

            if !self.output_buffer.is_empty() {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

impl Operator for HashJoinExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        // Build hash table on first call
        if self.hash_table.is_none() {
            self.build_hash_table()?;
        }

        // Return buffered output if available
        if self.output_pos < self.output_buffer.len() {
            let batch_size = 1024.min(self.output_buffer.len() - self.output_pos);
            let rows: Vec<Row> =
                self.output_buffer[self.output_pos..self.output_pos + batch_size].to_vec();
            self.output_pos += batch_size;

            let batch = RecordBatch::from_rows(self.schema.clone(), &rows)
                .map_err(ExecutionError::Internal)?;
            return Ok(Some(batch));
        }

        // Probe right side
        self.output_buffer.clear();
        self.output_pos = 0;

        if self.probe()? {
            // Return first batch of results
            let batch_size = 1024.min(self.output_buffer.len());
            let rows: Vec<Row> = self.output_buffer[..batch_size].to_vec();
            self.output_pos = batch_size;

            let batch = RecordBatch::from_rows(self.schema.clone(), &rows)
                .map_err(ExecutionError::Internal)?;
            return Ok(Some(batch));
        }

        Ok(None)
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.hash_table = None;
        self.output_buffer.clear();
        self.output_pos = 0;
    }
}

/// Hash aggregate operator for grouping and aggregation.
#[derive(Debug)]
pub struct HashAggregateExec {
    /// Child operator.
    child: Box<dyn Operator>,
    /// Group by expressions.
    group_by: Vec<PhysicalExpr>,
    /// Aggregate expressions.
    aggregates: Vec<PhysicalAggregateExpr>,
    /// Output schema.
    schema: Arc<Schema>,
    /// Hash table for groups.
    groups: Option<HashMap<Vec<Value>, Vec<Accumulator>>>,
    /// Results iterator.
    results: Option<std::vec::IntoIter<Row>>,
}

impl HashAggregateExec {
    /// Creates a new hash aggregate operator.
    pub fn new(
        child: Box<dyn Operator>,
        group_by: Vec<PhysicalExpr>,
        aggregates: Vec<PhysicalAggregateExpr>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            child,
            group_by,
            aggregates,
            schema,
            groups: None,
            results: None,
        }
    }

    fn aggregate(&mut self) -> Result<(), ExecutionError> {
        let mut groups: HashMap<Vec<Value>, Vec<Accumulator>> = HashMap::new();
        let input_schema = self.child.schema();

        while let Some(batch) = self.child.next_batch()? {
            for row in batch.rows() {
                // Compute group key
                let group_key: Result<Vec<Value>, _> = self
                    .group_by
                    .iter()
                    .map(|e| evaluate_expr(e, &row, &input_schema))
                    .collect();
                let group_key = group_key?;

                // Get or create accumulators for this group
                let accumulators = groups.entry(group_key).or_insert_with(|| {
                    self.aggregates
                        .iter()
                        .map(|agg| Accumulator::new(agg))
                        .collect()
                });

                // Accumulate values
                for (i, agg) in self.aggregates.iter().enumerate() {
                    if agg.args.is_empty() {
                        // COUNT(*) doesn't need an argument
                        accumulators[i].accumulate(&Value::int(1));
                    } else {
                        let value = evaluate_expr(&agg.args[0], &row, &input_schema)?;
                        accumulators[i].accumulate(&value);
                    }
                }
            }
        }

        self.groups = Some(groups);
        Ok(())
    }

    fn build_results(&mut self) -> Vec<Row> {
        let groups = self.groups.take().unwrap_or_default();
        let mut results = Vec::with_capacity(groups.len());

        for (group_key, accumulators) in groups {
            let mut values = group_key;
            for acc in accumulators {
                values.push(acc.result());
            }
            results.push(Row::new(values));
        }

        // Handle empty input with no group by (return one row with aggregate defaults)
        if results.is_empty() && self.group_by.is_empty() {
            let values: Vec<Value> = self
                .aggregates
                .iter()
                .map(|agg| Accumulator::new(agg).result())
                .collect();
            results.push(Row::new(values));
        }

        results
    }
}

impl Operator for HashAggregateExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        // Aggregate all input on first call
        if self.groups.is_none() {
            self.aggregate()?;
            let results = self.build_results();
            self.results = Some(results.into_iter());
        }

        // Return results
        let results = self.results.as_mut().unwrap();
        let batch_size = 1024;
        let rows: Vec<Row> = results.take(batch_size).collect();

        if rows.is_empty() {
            return Ok(None);
        }

        let batch =
            RecordBatch::from_rows(self.schema.clone(), &rows).map_err(ExecutionError::Internal)?;
        Ok(Some(batch))
    }

    fn reset(&mut self) {
        self.child.reset();
        self.groups = None;
        self.results = None;
    }
}

/// Sort operator for ordering results.
#[derive(Debug)]
pub struct SortExec {
    /// Child operator.
    child: Box<dyn Operator>,
    /// Sort expressions.
    sort_exprs: Vec<PhysicalSortExpr>,
    /// Buffered and sorted rows.
    sorted_rows: Option<Vec<Row>>,
    /// Current position.
    current_pos: usize,
}

impl SortExec {
    /// Creates a new sort operator.
    pub fn new(child: Box<dyn Operator>, sort_exprs: Vec<PhysicalSortExpr>) -> Self {
        Self {
            child,
            sort_exprs,
            sorted_rows: None,
            current_pos: 0,
        }
    }

    fn sort(&mut self) -> Result<(), ExecutionError> {
        let schema = self.child.schema();
        let mut rows = Vec::new();

        // Collect all rows
        while let Some(batch) = self.child.next_batch()? {
            for row in batch.rows() {
                rows.push(row);
            }
        }

        // Sort rows
        let sort_exprs = &self.sort_exprs;
        rows.sort_by(|a, b| {
            for expr in sort_exprs {
                let a_val = evaluate_expr(&expr.expr, a, &schema).unwrap_or(Value::Null);
                let b_val = evaluate_expr(&expr.expr, b, &schema).unwrap_or(Value::Null);

                let cmp = if expr.asc {
                    a_val.cmp(&b_val)
                } else {
                    b_val.cmp(&a_val)
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        self.sorted_rows = Some(rows);
        Ok(())
    }
}

impl Operator for SortExec {
    fn schema(&self) -> Arc<Schema> {
        self.child.schema()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        // Sort on first call
        if self.sorted_rows.is_none() {
            self.sort()?;
        }

        let rows = self.sorted_rows.as_ref().unwrap();
        if self.current_pos >= rows.len() {
            return Ok(None);
        }

        let batch_size = 1024.min(rows.len() - self.current_pos);
        let batch_rows: Vec<Row> = rows[self.current_pos..self.current_pos + batch_size].to_vec();
        self.current_pos += batch_size;

        let batch = RecordBatch::from_rows(self.child.schema(), &batch_rows)
            .map_err(ExecutionError::Internal)?;
        Ok(Some(batch))
    }

    fn reset(&mut self) {
        self.child.reset();
        self.sorted_rows = None;
        self.current_pos = 0;
    }
}

/// TopN operator for efficient ORDER BY ... LIMIT.
#[derive(Debug)]
pub struct TopNExec {
    /// Child operator.
    child: Box<dyn Operator>,
    /// Sort expressions.
    sort_exprs: Vec<PhysicalSortExpr>,
    /// Number of rows to keep.
    limit: usize,
    /// Top rows.
    top_rows: Option<Vec<Row>>,
    /// Current position.
    current_pos: usize,
}

impl TopNExec {
    /// Creates a new TopN operator.
    pub fn new(child: Box<dyn Operator>, sort_exprs: Vec<PhysicalSortExpr>, limit: usize) -> Self {
        Self {
            child,
            sort_exprs,
            limit,
            top_rows: None,
            current_pos: 0,
        }
    }

    fn collect_top(&mut self) -> Result<(), ExecutionError> {
        let schema = self.child.schema();
        let mut heap = Vec::new();

        while let Some(batch) = self.child.next_batch()? {
            for row in batch.rows() {
                heap.push(row);

                // Keep heap sorted and trimmed to limit
                if heap.len() > self.limit * 2 {
                    // Sort and trim
                    let sort_exprs = &self.sort_exprs;
                    heap.sort_by(|a, b| {
                        for expr in sort_exprs {
                            let a_val =
                                evaluate_expr(&expr.expr, a, &schema).unwrap_or(Value::Null);
                            let b_val =
                                evaluate_expr(&expr.expr, b, &schema).unwrap_or(Value::Null);

                            let cmp = if expr.asc {
                                a_val.cmp(&b_val)
                            } else {
                                b_val.cmp(&a_val)
                            };

                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    heap.truncate(self.limit);
                }
            }
        }

        // Final sort
        let sort_exprs = &self.sort_exprs;
        heap.sort_by(|a, b| {
            for expr in sort_exprs {
                let a_val = evaluate_expr(&expr.expr, a, &schema).unwrap_or(Value::Null);
                let b_val = evaluate_expr(&expr.expr, b, &schema).unwrap_or(Value::Null);

                let cmp = if expr.asc {
                    a_val.cmp(&b_val)
                } else {
                    b_val.cmp(&a_val)
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
        heap.truncate(self.limit);

        self.top_rows = Some(heap);
        Ok(())
    }
}

impl Operator for TopNExec {
    fn schema(&self) -> Arc<Schema> {
        self.child.schema()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        if self.top_rows.is_none() {
            self.collect_top()?;
        }

        let rows = self.top_rows.as_ref().unwrap();
        if self.current_pos >= rows.len() {
            return Ok(None);
        }

        let batch_rows: Vec<Row> = rows[self.current_pos..].to_vec();
        self.current_pos = rows.len();

        let batch = RecordBatch::from_rows(self.child.schema(), &batch_rows)
            .map_err(ExecutionError::Internal)?;
        Ok(Some(batch))
    }

    fn reset(&mut self) {
        self.child.reset();
        self.top_rows = None;
        self.current_pos = 0;
    }
}

/// Distinct operator that removes duplicate rows.
#[derive(Debug)]
pub struct DistinctExec {
    /// Child operator.
    child: Box<dyn Operator>,
    /// Seen rows.
    seen: std::collections::HashSet<Row>,
}

impl DistinctExec {
    /// Creates a new distinct operator.
    pub fn new(child: Box<dyn Operator>) -> Self {
        Self {
            child,
            seen: std::collections::HashSet::new(),
        }
    }
}

impl Operator for DistinctExec {
    fn schema(&self) -> Arc<Schema> {
        self.child.schema()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        while let Some(batch) = self.child.next_batch()? {
            let mut unique_rows = Vec::new();

            for row in batch.rows() {
                if self.seen.insert(row.clone()) {
                    unique_rows.push(row);
                }
            }

            if !unique_rows.is_empty() {
                let batch = RecordBatch::from_rows(self.child.schema(), &unique_rows)
                    .map_err(ExecutionError::Internal)?;
                return Ok(Some(batch));
            }
        }

        Ok(None)
    }

    fn reset(&mut self) {
        self.child.reset();
        self.seen.clear();
    }
}

/// Values operator that produces literal values.
#[derive(Debug)]
pub struct ValuesExec {
    /// Output schema.
    schema: Arc<Schema>,
    /// Values to return.
    values: Vec<Vec<Value>>,
    /// Whether values have been returned.
    done: bool,
}

impl ValuesExec {
    /// Creates a new values operator.
    pub fn new(schema: Arc<Schema>, values: Vec<Vec<Value>>) -> Self {
        Self {
            schema,
            values,
            done: false,
        }
    }
}

impl Operator for ValuesExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        if self.done {
            return Ok(None);
        }

        self.done = true;

        if self.values.is_empty() {
            return Ok(Some(RecordBatch::empty(self.schema.clone())));
        }

        let rows: Vec<Row> = self.values.iter().map(|v| Row::new(v.clone())).collect();
        let batch =
            RecordBatch::from_rows(self.schema.clone(), &rows).map_err(ExecutionError::Internal)?;
        Ok(Some(batch))
    }

    fn reset(&mut self) {
        self.done = false;
    }
}

/// Empty operator that produces no rows.
#[derive(Debug)]
pub struct EmptyExec {
    /// Output schema.
    schema: Arc<Schema>,
}

impl EmptyExec {
    /// Creates a new empty operator.
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

impl Operator for EmptyExec {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        Ok(None)
    }

    fn reset(&mut self) {}
}

/// Error type for execution.
#[derive(Debug)]
pub enum ExecutionError {
    /// Internal error.
    Internal(String),
    /// Expression evaluation error.
    Eval(EvalError),
    /// Operator error.
    Operator(String),
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionError::Internal(msg) => write!(f, "Internal error: {}", msg),
            ExecutionError::Eval(e) => write!(f, "Evaluation error: {}", e),
            ExecutionError::Operator(msg) => write!(f, "Operator error: {}", msg),
        }
    }
}

impl std::error::Error for ExecutionError {}

impl From<EvalError> for ExecutionError {
    fn from(e: EvalError) -> Self {
        ExecutionError::Eval(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::Field;
    use crate::parser::Literal;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Text),
            Field::nullable("age", DataType::Int),
        ]))
    }

    fn test_batch() -> RecordBatch {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![Value::int(1), Value::string("Alice"), Value::int(30)]),
            Row::new(vec![Value::int(2), Value::string("Bob"), Value::int(25)]),
            Row::new(vec![
                Value::int(3),
                Value::string("Charlie"),
                Value::int(35),
            ]),
        ];
        RecordBatch::from_rows(schema, &rows).unwrap()
    }

    #[test]
    fn test_seq_scan() {
        let batch = test_batch();
        let mut scan = SeqScanExec::new(test_schema(), "test".to_string(), vec![batch]);

        let result = scan.next_batch().unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);

        let result = scan.next_batch().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_filter() {
        let batch = test_batch();
        let scan = SeqScanExec::new(test_schema(), "test".to_string(), vec![batch]);

        // Filter: age > 25
        let predicate = PhysicalExpr::binary(
            PhysicalExpr::column("age", 2),
            crate::logical::BinaryOp::Gt,
            PhysicalExpr::Literal(Literal::Integer(25)),
        );
        let mut filter = FilterExec::new(Box::new(scan), predicate);

        let result = filter.next_batch().unwrap().unwrap();
        assert_eq!(result.num_rows(), 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_projection() {
        let batch = test_batch();
        let scan = SeqScanExec::new(test_schema(), "test".to_string(), vec![batch]);

        let exprs = vec![
            PhysicalExpr::column("name", 1),
            PhysicalExpr::column("id", 0),
        ];
        let proj_schema = Arc::new(Schema::new(vec![
            Field::nullable("name", DataType::Text),
            Field::not_null("id", DataType::Int),
        ]));
        let mut proj = ProjectionExec::new(Box::new(scan), exprs, proj_schema);

        let result = proj.next_batch().unwrap().unwrap();
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn test_limit() {
        let batch = test_batch();
        let scan = SeqScanExec::new(test_schema(), "test".to_string(), vec![batch]);

        let mut limit = LimitExec::new(Box::new(scan), 2, 0);

        let result = limit.next_batch().unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_limit_with_offset() {
        let batch = test_batch();
        let scan = SeqScanExec::new(test_schema(), "test".to_string(), vec![batch]);

        let mut limit = LimitExec::new(Box::new(scan), 1, 1);

        let result = limit.next_batch().unwrap().unwrap();
        assert_eq!(result.num_rows(), 1);
        // Should be Bob (skipped Alice)
    }

    #[test]
    fn test_distinct() {
        let schema = Arc::new(Schema::new(vec![Field::not_null("x", DataType::Int)]));
        let rows = vec![
            Row::new(vec![Value::int(1)]),
            Row::new(vec![Value::int(2)]),
            Row::new(vec![Value::int(1)]),
            Row::new(vec![Value::int(3)]),
            Row::new(vec![Value::int(2)]),
        ];
        let batch = RecordBatch::from_rows(schema.clone(), &rows).unwrap();
        let scan = SeqScanExec::new(schema, "test".to_string(), vec![batch]);

        let mut distinct = DistinctExec::new(Box::new(scan));

        let mut total = 0;
        while let Some(batch) = distinct.next_batch().unwrap() {
            total += batch.num_rows();
        }
        assert_eq!(total, 3); // 1, 2, 3
    }

    #[test]
    fn test_sort() {
        let batch = test_batch();
        let scan = SeqScanExec::new(test_schema(), "test".to_string(), vec![batch]);

        let sort_exprs = vec![PhysicalSortExpr::desc(PhysicalExpr::column("age", 2))];
        let mut sort = SortExec::new(Box::new(scan), sort_exprs);

        let result = sort.next_batch().unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);
        // First row should be Charlie (35)
        assert_eq!(result.row(0).unwrap().get(2), Some(&Value::Int(35)));
    }

    #[test]
    fn test_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::not_null("x", DataType::Int),
            Field::nullable("y", DataType::Text),
        ]));
        let values = vec![
            vec![Value::int(1), Value::string("a")],
            vec![Value::int(2), Value::string("b")],
        ];
        let mut values_exec = ValuesExec::new(schema, values);

        let result = values_exec.next_batch().unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_empty() {
        let schema = test_schema();
        let mut empty = EmptyExec::new(schema);

        let result = empty.next_batch().unwrap();
        assert!(result.is_none());
    }
}
