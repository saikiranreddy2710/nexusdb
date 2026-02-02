//! Physical operators for query execution.
//!
//! Physical operators represent concrete execution algorithms for each
//! operation in the query plan. Unlike logical operators which describe
//! "what" to compute, physical operators describe "how" to compute it.

use std::fmt;
use std::sync::Arc;

use crate::logical::{JoinType, Schema, SetOpType};

use super::expr::{PhysicalAggregateExpr, PhysicalExpr, PhysicalSortExpr};

/// Type alias for schema reference.
pub type SchemaRef = Arc<Schema>;

/// Distribution of data across partitions.
#[derive(Debug, Clone, PartialEq)]
pub enum Distribution {
    /// Data is not partitioned (single partition).
    SinglePartition,
    /// Data is hash-partitioned on the given expressions.
    HashPartitioned(Vec<PhysicalExpr>),
    /// Data is range-partitioned.
    RangePartitioned(Vec<PhysicalSortExpr>),
    /// Data can be on any partition (no guarantees).
    UnspecifiedPartitioning,
}

/// Ordering of data within a partition.
#[derive(Debug, Clone)]
pub struct Ordering {
    /// Sort expressions.
    pub exprs: Vec<PhysicalSortExpr>,
}

impl Ordering {
    /// Creates a new ordering.
    pub fn new(exprs: Vec<PhysicalSortExpr>) -> Self {
        Self { exprs }
    }

    /// Returns true if this ordering is empty.
    pub fn is_empty(&self) -> bool {
        self.exprs.is_empty()
    }
}

/// Physical properties of an operator's output.
#[derive(Debug, Clone)]
pub struct PhysicalProperties {
    /// Data distribution.
    pub distribution: Distribution,
    /// Data ordering within partitions.
    pub ordering: Option<Ordering>,
    /// Number of partitions.
    pub partitions: usize,
}

impl Default for PhysicalProperties {
    fn default() -> Self {
        Self {
            distribution: Distribution::UnspecifiedPartitioning,
            ordering: None,
            partitions: 1,
        }
    }
}

/// A physical operator in the execution plan.
#[derive(Debug, Clone)]
pub enum PhysicalOperator {
    /// Sequential table scan.
    SeqScan(SeqScanOperator),

    /// Index scan.
    IndexScan(IndexScanOperator),

    /// Filter operator.
    Filter(FilterPhysicalOperator),

    /// Projection operator.
    Projection(ProjectionPhysicalOperator),

    /// Hash join.
    HashJoin(HashJoinOperator),

    /// Merge join (requires sorted inputs).
    MergeJoin(MergeJoinOperator),

    /// Nested loop join.
    NestedLoopJoin(NestedLoopJoinOperator),

    /// Hash aggregate.
    HashAggregate(HashAggregateOperator),

    /// Sort aggregate (requires sorted input).
    SortAggregate(SortAggregateOperator),

    /// Sort operator.
    Sort(SortPhysicalOperator),

    /// Top-N operator (optimized sort + limit).
    TopN(TopNOperator),

    /// Limit operator.
    Limit(LimitPhysicalOperator),

    /// Distinct operator.
    Distinct(DistinctPhysicalOperator),

    /// Set operation.
    SetOperation(SetOperationPhysicalOperator),

    /// Exchange operator (for repartitioning).
    Exchange(ExchangeOperator),

    /// Values operator (inline data).
    Values(ValuesPhysicalOperator),

    /// Empty result.
    Empty(EmptyOperator),
}

impl PhysicalOperator {
    /// Returns the output schema.
    pub fn schema(&self) -> SchemaRef {
        match self {
            PhysicalOperator::SeqScan(op) => op.projected_schema.clone(),
            PhysicalOperator::IndexScan(op) => op.projected_schema.clone(),
            PhysicalOperator::Filter(op) => op.input.schema(),
            PhysicalOperator::Projection(op) => op.schema.clone(),
            PhysicalOperator::HashJoin(op) => op.schema.clone(),
            PhysicalOperator::MergeJoin(op) => op.schema.clone(),
            PhysicalOperator::NestedLoopJoin(op) => op.schema.clone(),
            PhysicalOperator::HashAggregate(op) => op.schema.clone(),
            PhysicalOperator::SortAggregate(op) => op.schema.clone(),
            PhysicalOperator::Sort(op) => op.input.schema(),
            PhysicalOperator::TopN(op) => op.input.schema(),
            PhysicalOperator::Limit(op) => op.input.schema(),
            PhysicalOperator::Distinct(op) => op.input.schema(),
            PhysicalOperator::SetOperation(op) => op.left.schema(),
            PhysicalOperator::Exchange(op) => op.input.schema(),
            PhysicalOperator::Values(op) => op.schema.clone(),
            PhysicalOperator::Empty(op) => op.schema.clone(),
        }
    }

    /// Returns child operators.
    pub fn children(&self) -> Vec<&Arc<PhysicalOperator>> {
        match self {
            PhysicalOperator::SeqScan(_)
            | PhysicalOperator::IndexScan(_)
            | PhysicalOperator::Values(_)
            | PhysicalOperator::Empty(_) => vec![],

            PhysicalOperator::Filter(op) => vec![&op.input],
            PhysicalOperator::Projection(op) => vec![&op.input],
            PhysicalOperator::HashAggregate(op) => vec![&op.input],
            PhysicalOperator::SortAggregate(op) => vec![&op.input],
            PhysicalOperator::Sort(op) => vec![&op.input],
            PhysicalOperator::TopN(op) => vec![&op.input],
            PhysicalOperator::Limit(op) => vec![&op.input],
            PhysicalOperator::Distinct(op) => vec![&op.input],
            PhysicalOperator::Exchange(op) => vec![&op.input],

            PhysicalOperator::HashJoin(op) => vec![&op.left, &op.right],
            PhysicalOperator::MergeJoin(op) => vec![&op.left, &op.right],
            PhysicalOperator::NestedLoopJoin(op) => vec![&op.left, &op.right],
            PhysicalOperator::SetOperation(op) => vec![&op.left, &op.right],
        }
    }

    /// Returns the operator name.
    pub fn name(&self) -> &'static str {
        match self {
            PhysicalOperator::SeqScan(_) => "SeqScan",
            PhysicalOperator::IndexScan(_) => "IndexScan",
            PhysicalOperator::Filter(_) => "Filter",
            PhysicalOperator::Projection(_) => "Projection",
            PhysicalOperator::HashJoin(_) => "HashJoin",
            PhysicalOperator::MergeJoin(_) => "MergeJoin",
            PhysicalOperator::NestedLoopJoin(_) => "NestedLoopJoin",
            PhysicalOperator::HashAggregate(_) => "HashAggregate",
            PhysicalOperator::SortAggregate(_) => "SortAggregate",
            PhysicalOperator::Sort(_) => "Sort",
            PhysicalOperator::TopN(_) => "TopN",
            PhysicalOperator::Limit(_) => "Limit",
            PhysicalOperator::Distinct(_) => "Distinct",
            PhysicalOperator::SetOperation(_) => "SetOperation",
            PhysicalOperator::Exchange(_) => "Exchange",
            PhysicalOperator::Values(_) => "Values",
            PhysicalOperator::Empty(_) => "Empty",
        }
    }

    /// Returns physical properties.
    pub fn properties(&self) -> PhysicalProperties {
        match self {
            PhysicalOperator::SeqScan(_) => PhysicalProperties::default(),
            PhysicalOperator::IndexScan(op) => PhysicalProperties {
                ordering: op.ordering.clone(),
                ..Default::default()
            },
            PhysicalOperator::Sort(op) => PhysicalProperties {
                ordering: Some(Ordering::new(op.order_by.clone())),
                ..Default::default()
            },
            PhysicalOperator::TopN(op) => PhysicalProperties {
                ordering: Some(Ordering::new(op.order_by.clone())),
                ..Default::default()
            },
            PhysicalOperator::Exchange(op) => PhysicalProperties {
                distribution: op.distribution.clone(),
                partitions: op.target_partitions,
                ..Default::default()
            },
            PhysicalOperator::HashJoin(op) => PhysicalProperties {
                distribution: Distribution::HashPartitioned(
                    op.left_keys.iter().map(|k| k.expr.clone()).collect(),
                ),
                ..Default::default()
            },
            _ => PhysicalProperties::default(),
        }
    }
}

impl fmt::Display for PhysicalOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

// ============================================================================
// Scan Operators
// ============================================================================

/// Sequential scan operator.
#[derive(Debug, Clone)]
pub struct SeqScanOperator {
    /// Table name.
    pub table_name: String,
    /// Full table schema.
    pub table_schema: SchemaRef,
    /// Column indices to project.
    pub projection: Option<Vec<usize>>,
    /// Projected schema.
    pub projected_schema: SchemaRef,
    /// Pushed-down filter predicates.
    pub filters: Vec<PhysicalExpr>,
    /// Row limit.
    pub limit: Option<usize>,
}

impl SeqScanOperator {
    /// Creates a new sequential scan.
    pub fn new(table_name: impl Into<String>, schema: Schema) -> Self {
        let schema = Arc::new(schema);
        Self {
            table_name: table_name.into(),
            table_schema: schema.clone(),
            projection: None,
            projected_schema: schema,
            filters: Vec::new(),
            limit: None,
        }
    }

    /// Adds a projection.
    pub fn with_projection(mut self, indices: Vec<usize>) -> Self {
        self.projected_schema = Arc::new(self.table_schema.project(&indices));
        self.projection = Some(indices);
        self
    }

    /// Adds a filter.
    pub fn with_filter(mut self, filter: PhysicalExpr) -> Self {
        self.filters.push(filter);
        self
    }

    /// Adds a limit.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Index scan operator.
#[derive(Debug, Clone)]
pub struct IndexScanOperator {
    /// Table name.
    pub table_name: String,
    /// Index name.
    pub index_name: String,
    /// Table schema.
    pub table_schema: SchemaRef,
    /// Projected schema.
    pub projected_schema: SchemaRef,
    /// Index lookup conditions.
    pub index_conditions: Vec<PhysicalExpr>,
    /// Additional filter conditions.
    pub filters: Vec<PhysicalExpr>,
    /// Whether the scan is a range scan.
    pub is_range_scan: bool,
    /// Output ordering (if any).
    pub ordering: Option<Ordering>,
    /// Row limit.
    pub limit: Option<usize>,
}

// ============================================================================
// Filter and Projection
// ============================================================================

/// Physical filter operator.
#[derive(Debug, Clone)]
pub struct FilterPhysicalOperator {
    /// Input operator.
    pub input: Arc<PhysicalOperator>,
    /// Filter predicate.
    pub predicate: PhysicalExpr,
}

/// Physical projection operator.
#[derive(Debug, Clone)]
pub struct ProjectionPhysicalOperator {
    /// Input operator.
    pub input: Arc<PhysicalOperator>,
    /// Projection expressions.
    pub exprs: Vec<PhysicalExpr>,
    /// Output schema.
    pub schema: SchemaRef,
}

// ============================================================================
// Join Operators
// ============================================================================

/// Hash join operator.
///
/// Builds a hash table on the right (build) side, then probes with the left side.
#[derive(Debug, Clone)]
pub struct HashJoinOperator {
    /// Left (probe) input.
    pub left: Arc<PhysicalOperator>,
    /// Right (build) input.
    pub right: Arc<PhysicalOperator>,
    /// Join type.
    pub join_type: JoinType,
    /// Left join keys.
    pub left_keys: Vec<JoinKey>,
    /// Right join keys.
    pub right_keys: Vec<JoinKey>,
    /// Additional join filter.
    pub filter: Option<PhysicalExpr>,
    /// Output schema.
    pub schema: SchemaRef,
}

/// A join key expression with its sort order.
#[derive(Debug, Clone)]
pub struct JoinKey {
    /// Key expression.
    pub expr: PhysicalExpr,
    /// Whether this key should be used for output ordering.
    pub preserve_order: bool,
}

impl JoinKey {
    /// Creates a new join key.
    pub fn new(expr: PhysicalExpr) -> Self {
        Self {
            expr,
            preserve_order: false,
        }
    }
}

/// Merge join operator (requires sorted inputs).
#[derive(Debug, Clone)]
pub struct MergeJoinOperator {
    /// Left input.
    pub left: Arc<PhysicalOperator>,
    /// Right input.
    pub right: Arc<PhysicalOperator>,
    /// Join type.
    pub join_type: JoinType,
    /// Left join keys.
    pub left_keys: Vec<JoinKey>,
    /// Right join keys.
    pub right_keys: Vec<JoinKey>,
    /// Additional join filter.
    pub filter: Option<PhysicalExpr>,
    /// Output schema.
    pub schema: SchemaRef,
}

/// Nested loop join operator.
///
/// Simple but expensive O(n*m) join. Used for small tables or cross joins.
#[derive(Debug, Clone)]
pub struct NestedLoopJoinOperator {
    /// Left (outer) input.
    pub left: Arc<PhysicalOperator>,
    /// Right (inner) input.
    pub right: Arc<PhysicalOperator>,
    /// Join type.
    pub join_type: JoinType,
    /// Join condition.
    pub condition: Option<PhysicalExpr>,
    /// Output schema.
    pub schema: SchemaRef,
}

// ============================================================================
// Aggregate Operators
// ============================================================================

/// Hash aggregate operator.
///
/// Uses a hash table to group rows by the group-by keys.
#[derive(Debug, Clone)]
pub struct HashAggregateOperator {
    /// Input operator.
    pub input: Arc<PhysicalOperator>,
    /// Group-by expressions.
    pub group_by: Vec<PhysicalExpr>,
    /// Aggregate expressions.
    pub aggregates: Vec<PhysicalAggregateExpr>,
    /// Output schema.
    pub schema: SchemaRef,
    /// Aggregation mode.
    pub mode: AggregationMode,
}

/// Sort aggregate operator (requires sorted input).
#[derive(Debug, Clone)]
pub struct SortAggregateOperator {
    /// Input operator.
    pub input: Arc<PhysicalOperator>,
    /// Group-by expressions.
    pub group_by: Vec<PhysicalExpr>,
    /// Aggregate expressions.
    pub aggregates: Vec<PhysicalAggregateExpr>,
    /// Output schema.
    pub schema: SchemaRef,
}

/// Aggregation mode for multi-stage aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationMode {
    /// Compute partial aggregates (first stage).
    Partial,
    /// Finalize aggregates (second stage).
    Final,
    /// Single-stage aggregation.
    Single,
}

// ============================================================================
// Sort and TopN
// ============================================================================

/// Sort operator.
#[derive(Debug, Clone)]
pub struct SortPhysicalOperator {
    /// Input operator.
    pub input: Arc<PhysicalOperator>,
    /// Sort expressions.
    pub order_by: Vec<PhysicalSortExpr>,
    /// Limit for TopN optimization.
    pub fetch: Option<usize>,
    /// Whether to preserve partitioning.
    pub preserve_partitioning: bool,
}

/// Top-N operator (optimized sort + limit).
#[derive(Debug, Clone)]
pub struct TopNOperator {
    /// Input operator.
    pub input: Arc<PhysicalOperator>,
    /// Sort expressions.
    pub order_by: Vec<PhysicalSortExpr>,
    /// Number of rows to return.
    pub n: usize,
}

// ============================================================================
// Other Operators
// ============================================================================

/// Limit operator.
#[derive(Debug, Clone)]
pub struct LimitPhysicalOperator {
    /// Input operator.
    pub input: Arc<PhysicalOperator>,
    /// Number of rows to skip.
    pub offset: usize,
    /// Maximum rows to return.
    pub fetch: Option<usize>,
}

/// Distinct operator.
#[derive(Debug, Clone)]
pub struct DistinctPhysicalOperator {
    /// Input operator.
    pub input: Arc<PhysicalOperator>,
}

/// Set operation operator.
#[derive(Debug, Clone)]
pub struct SetOperationPhysicalOperator {
    /// Left input.
    pub left: Arc<PhysicalOperator>,
    /// Right input.
    pub right: Arc<PhysicalOperator>,
    /// Operation type.
    pub op: SetOpType,
}

/// Exchange operator for repartitioning.
#[derive(Debug, Clone)]
pub struct ExchangeOperator {
    /// Input operator.
    pub input: Arc<PhysicalOperator>,
    /// Target distribution.
    pub distribution: Distribution,
    /// Target number of partitions.
    pub target_partitions: usize,
}

/// Values operator (inline data).
#[derive(Debug, Clone)]
pub struct ValuesPhysicalOperator {
    /// Rows of values.
    pub values: Vec<Vec<PhysicalExpr>>,
    /// Output schema.
    pub schema: SchemaRef,
}

/// Empty result operator.
#[derive(Debug, Clone)]
pub struct EmptyOperator {
    /// Output schema.
    pub schema: SchemaRef,
    /// Whether to produce a single empty row.
    pub produce_one_row: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::Field;
    use crate::parser::DataType;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
        ])
    }

    #[test]
    fn test_seq_scan() {
        let scan = SeqScanOperator::new("users", test_schema());
        let op = PhysicalOperator::SeqScan(scan);
        assert_eq!(op.name(), "SeqScan");
        assert_eq!(op.schema().len(), 2);
    }

    #[test]
    fn test_seq_scan_with_projection() {
        let scan = SeqScanOperator::new("users", test_schema()).with_projection(vec![0]);
        assert_eq!(scan.projected_schema.len(), 1);
    }

    #[test]
    fn test_filter_operator() {
        let scan = Arc::new(PhysicalOperator::SeqScan(SeqScanOperator::new(
            "users",
            test_schema(),
        )));
        let filter = FilterPhysicalOperator {
            input: scan.clone(),
            predicate: PhysicalExpr::binary(
                PhysicalExpr::column("id", 0),
                crate::logical::BinaryOp::Eq,
                PhysicalExpr::lit_i64(1),
            ),
        };
        let op = PhysicalOperator::Filter(filter);
        assert_eq!(op.name(), "Filter");
        assert_eq!(op.children().len(), 1);
    }

    #[test]
    fn test_distribution() {
        let dist = Distribution::SinglePartition;
        assert_eq!(dist, Distribution::SinglePartition);

        let hash_dist = Distribution::HashPartitioned(vec![PhysicalExpr::column("id", 0)]);
        assert!(matches!(hash_dist, Distribution::HashPartitioned(_)));
    }

    #[test]
    fn test_aggregation_mode() {
        assert_eq!(AggregationMode::Partial, AggregationMode::Partial);
        assert_ne!(AggregationMode::Partial, AggregationMode::Final);
    }
}
