//! Logical operators for query plans.
//!
//! These represent the abstract operations in a query plan tree.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::expr::{LogicalExpr, SortExpr};
use super::schema::{Schema, SchemaRef, TableMeta};

/// Join type for join operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    /// Semi join - returns rows from left that have a match in right.
    Semi,
    /// Anti join - returns rows from left that have no match in right.
    Anti,
}

impl JoinType {
    /// Returns true if this join preserves all rows from the left side.
    pub fn preserves_left(&self) -> bool {
        matches!(self, JoinType::Left | JoinType::Full | JoinType::Cross)
    }

    /// Returns true if this join preserves all rows from the right side.
    pub fn preserves_right(&self) -> bool {
        matches!(self, JoinType::Right | JoinType::Full | JoinType::Cross)
    }

    /// Returns true if this is a cross join.
    pub fn is_cross(&self) -> bool {
        matches!(self, JoinType::Cross)
    }

    /// Returns true if this is an outer join.
    pub fn is_outer(&self) -> bool {
        matches!(self, JoinType::Left | JoinType::Right | JoinType::Full)
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Left => write!(f, "LEFT"),
            JoinType::Right => write!(f, "RIGHT"),
            JoinType::Full => write!(f, "FULL"),
            JoinType::Cross => write!(f, "CROSS"),
            JoinType::Semi => write!(f, "SEMI"),
            JoinType::Anti => write!(f, "ANTI"),
        }
    }
}

/// Set operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SetOpType {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

impl fmt::Display for SetOpType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetOpType::Union => write!(f, "UNION"),
            SetOpType::UnionAll => write!(f, "UNION ALL"),
            SetOpType::Intersect => write!(f, "INTERSECT"),
            SetOpType::IntersectAll => write!(f, "INTERSECT ALL"),
            SetOpType::Except => write!(f, "EXCEPT"),
            SetOpType::ExceptAll => write!(f, "EXCEPT ALL"),
        }
    }
}

/// A logical operator in a query plan.
#[derive(Debug, Clone)]
pub enum LogicalOperator {
    /// Table scan.
    Scan(ScanOperator),

    /// Projection (SELECT clause).
    Projection(ProjectionOperator),

    /// Filter (WHERE clause).
    Filter(FilterOperator),

    /// Join.
    Join(JoinOperator),

    /// Aggregate (GROUP BY).
    Aggregate(AggregateOperator),

    /// Sort (ORDER BY).
    Sort(SortOperator),

    /// Limit (LIMIT/OFFSET).
    Limit(LimitOperator),

    /// Distinct (SELECT DISTINCT).
    Distinct(DistinctOperator),

    /// Set operation (UNION, INTERSECT, EXCEPT).
    SetOperation(SetOperationOperator),

    /// Subquery alias.
    SubqueryAlias(SubqueryAliasOperator),

    /// Values list (for INSERT).
    Values(ValuesOperator),

    /// Empty relation (for queries that return no rows).
    EmptyRelation(EmptyRelationOperator),

    /// Window function.
    Window(WindowOperator),

    /// Common Table Expression.
    Cte(CteOperator),
}

impl LogicalOperator {
    /// Returns the output schema of this operator.
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalOperator::Scan(op) => op.projected_schema.clone(),
            LogicalOperator::Projection(op) => op.schema.clone(),
            LogicalOperator::Filter(op) => op.input.schema(),
            LogicalOperator::Join(op) => op.schema.clone(),
            LogicalOperator::Aggregate(op) => op.schema.clone(),
            LogicalOperator::Sort(op) => op.input.schema(),
            LogicalOperator::Limit(op) => op.input.schema(),
            LogicalOperator::Distinct(op) => op.input.schema(),
            LogicalOperator::SetOperation(op) => op.left.schema(),
            LogicalOperator::SubqueryAlias(op) => op.schema.clone(),
            LogicalOperator::Values(op) => op.schema.clone(),
            LogicalOperator::EmptyRelation(op) => op.schema.clone(),
            LogicalOperator::Window(op) => op.schema.clone(),
            LogicalOperator::Cte(op) => op.input.schema(),
        }
    }

    /// Returns the child operators.
    pub fn children(&self) -> Vec<&Arc<LogicalOperator>> {
        match self {
            LogicalOperator::Scan(_)
            | LogicalOperator::Values(_)
            | LogicalOperator::EmptyRelation(_) => vec![],
            LogicalOperator::Projection(op) => vec![&op.input],
            LogicalOperator::Filter(op) => vec![&op.input],
            LogicalOperator::Join(op) => vec![&op.left, &op.right],
            LogicalOperator::Aggregate(op) => vec![&op.input],
            LogicalOperator::Sort(op) => vec![&op.input],
            LogicalOperator::Limit(op) => vec![&op.input],
            LogicalOperator::Distinct(op) => vec![&op.input],
            LogicalOperator::SetOperation(op) => vec![&op.left, &op.right],
            LogicalOperator::SubqueryAlias(op) => vec![&op.input],
            LogicalOperator::Window(op) => vec![&op.input],
            LogicalOperator::Cte(op) => {
                let mut children = vec![&op.input];
                children.extend(op.cte_plans.iter());
                children
            }
        }
    }

    /// Returns true if this is a leaf operator.
    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            LogicalOperator::Scan(_)
                | LogicalOperator::Values(_)
                | LogicalOperator::EmptyRelation(_)
        )
    }

    /// Returns the operator name.
    pub fn name(&self) -> &'static str {
        match self {
            LogicalOperator::Scan(_) => "Scan",
            LogicalOperator::Projection(_) => "Projection",
            LogicalOperator::Filter(_) => "Filter",
            LogicalOperator::Join(_) => "Join",
            LogicalOperator::Aggregate(_) => "Aggregate",
            LogicalOperator::Sort(_) => "Sort",
            LogicalOperator::Limit(_) => "Limit",
            LogicalOperator::Distinct(_) => "Distinct",
            LogicalOperator::SetOperation(_) => "SetOp",
            LogicalOperator::SubqueryAlias(_) => "SubqueryAlias",
            LogicalOperator::Values(_) => "Values",
            LogicalOperator::EmptyRelation(_) => "EmptyRelation",
            LogicalOperator::Window(_) => "Window",
            LogicalOperator::Cte(_) => "CTE",
        }
    }
}

/// Table scan operator.
#[derive(Debug, Clone)]
pub struct ScanOperator {
    /// Table name.
    pub table_name: String,
    /// Table schema.
    pub table_schema: SchemaRef,
    /// Projected columns (None = all).
    pub projection: Option<Vec<usize>>,
    /// Schema after projection.
    pub projected_schema: SchemaRef,
    /// Filters pushed down to scan.
    pub filters: Vec<LogicalExpr>,
    /// Maximum rows to scan.
    pub limit: Option<usize>,
    /// Table metadata (for optimization).
    pub table_meta: Option<TableMeta>,
}

impl ScanOperator {
    /// Creates a new scan operator.
    pub fn new(table_name: impl Into<String>, schema: Schema) -> Self {
        let schema = Arc::new(schema);
        Self {
            table_name: table_name.into(),
            table_schema: schema.clone(),
            projection: None,
            projected_schema: schema,
            filters: Vec::new(),
            limit: None,
            table_meta: None,
        }
    }

    /// Sets the projection.
    pub fn with_projection(mut self, indices: Vec<usize>) -> Self {
        self.projected_schema = Arc::new(self.table_schema.project(&indices));
        self.projection = Some(indices);
        self
    }

    /// Adds a filter.
    pub fn with_filter(mut self, filter: LogicalExpr) -> Self {
        self.filters.push(filter);
        self
    }

    /// Sets the limit.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Projection operator.
#[derive(Debug, Clone)]
pub struct ProjectionOperator {
    /// Input operator.
    pub input: Arc<LogicalOperator>,
    /// Projection expressions.
    pub exprs: Vec<LogicalExpr>,
    /// Output schema.
    pub schema: SchemaRef,
}

/// Filter operator.
#[derive(Debug, Clone)]
pub struct FilterOperator {
    /// Input operator.
    pub input: Arc<LogicalOperator>,
    /// Filter predicate.
    pub predicate: LogicalExpr,
}

impl FilterOperator {
    /// Extracts conjuncts (ANDed conditions) from the predicate.
    pub fn conjuncts(&self) -> Vec<&LogicalExpr> {
        extract_conjuncts(&self.predicate)
    }
}

/// Extracts all conjuncts from an expression.
pub fn extract_conjuncts(expr: &LogicalExpr) -> Vec<&LogicalExpr> {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: super::expr::BinaryOp::And,
            right,
        } => {
            let mut result = extract_conjuncts(left);
            result.extend(extract_conjuncts(right));
            result
        }
        _ => vec![expr],
    }
}

/// Combines expressions with AND.
pub fn conjoin(exprs: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    exprs.into_iter().reduce(|a, b| a.and(b))
}

/// Join operator.
#[derive(Debug, Clone)]
pub struct JoinOperator {
    /// Left input.
    pub left: Arc<LogicalOperator>,
    /// Right input.
    pub right: Arc<LogicalOperator>,
    /// Join type.
    pub join_type: JoinType,
    /// Join condition.
    pub condition: Option<LogicalExpr>,
    /// Output schema.
    pub schema: SchemaRef,
    /// Equijoin keys (left_col, right_col) for hash/merge join.
    pub equi_keys: Vec<(LogicalExpr, LogicalExpr)>,
    /// Non-equi conditions (for complex join conditions).
    pub filter: Option<LogicalExpr>,
}

impl JoinOperator {
    /// Creates a new join operator.
    pub fn new(
        left: Arc<LogicalOperator>,
        right: Arc<LogicalOperator>,
        join_type: JoinType,
        condition: Option<LogicalExpr>,
    ) -> Self {
        let schema = Arc::new(left.schema().merge(&right.schema()));
        let (equi_keys, filter) = condition
            .as_ref()
            .map(|c| extract_equi_keys(c, &left.schema(), &right.schema()))
            .unwrap_or_default();

        Self {
            left,
            right,
            join_type,
            condition,
            schema,
            equi_keys,
            filter,
        }
    }

    /// Returns true if this is an equi-join.
    pub fn is_equi_join(&self) -> bool {
        !self.equi_keys.is_empty() && self.filter.is_none()
    }
}

/// Extracts equi-join keys from a join condition.
fn extract_equi_keys(
    condition: &LogicalExpr,
    left_schema: &Schema,
    right_schema: &Schema,
) -> (Vec<(LogicalExpr, LogicalExpr)>, Option<LogicalExpr>) {
    let mut equi_keys = Vec::new();
    let mut other_conditions = Vec::new();

    for conjunct in extract_conjuncts(condition) {
        if let LogicalExpr::BinaryOp {
            left,
            op: super::expr::BinaryOp::Eq,
            right,
        } = conjunct
        {
            let left_cols = left.columns();
            let right_cols = right.columns();

            // Check if left expression references only left schema
            // and right expression references only right schema
            let left_on_left = left_cols
                .iter()
                .all(|c| left_schema.index_of_column(c).is_some());
            let right_on_right = right_cols
                .iter()
                .all(|c| right_schema.index_of_column(c).is_some());
            let left_on_right = left_cols
                .iter()
                .all(|c| right_schema.index_of_column(c).is_some());
            let right_on_left = right_cols
                .iter()
                .all(|c| left_schema.index_of_column(c).is_some());

            if left_on_left && right_on_right {
                equi_keys.push(((**left).clone(), (**right).clone()));
                continue;
            } else if left_on_right && right_on_left {
                equi_keys.push(((**right).clone(), (**left).clone()));
                continue;
            }
        }
        other_conditions.push(conjunct.clone());
    }

    let filter = conjoin(other_conditions);
    (equi_keys, filter)
}

/// Aggregate operator.
#[derive(Debug, Clone)]
pub struct AggregateOperator {
    /// Input operator.
    pub input: Arc<LogicalOperator>,
    /// Group-by expressions.
    pub group_by: Vec<LogicalExpr>,
    /// Aggregate expressions.
    pub aggregates: Vec<LogicalExpr>,
    /// Output schema.
    pub schema: SchemaRef,
}

/// Sort operator.
#[derive(Debug, Clone)]
pub struct SortOperator {
    /// Input operator.
    pub input: Arc<LogicalOperator>,
    /// Sort expressions.
    pub order_by: Vec<SortExpr>,
    /// Fetch limit (for TopN optimization).
    pub fetch: Option<usize>,
}

/// Limit operator.
#[derive(Debug, Clone)]
pub struct LimitOperator {
    /// Input operator.
    pub input: Arc<LogicalOperator>,
    /// Number of rows to skip.
    pub offset: usize,
    /// Maximum number of rows to return.
    pub fetch: Option<usize>,
}

/// Distinct operator.
#[derive(Debug, Clone)]
pub struct DistinctOperator {
    /// Input operator.
    pub input: Arc<LogicalOperator>,
    /// Columns to deduplicate on (None = all columns).
    pub on_columns: Option<Vec<LogicalExpr>>,
}

/// Set operation operator.
#[derive(Debug, Clone)]
pub struct SetOperationOperator {
    /// Left input.
    pub left: Arc<LogicalOperator>,
    /// Right input.
    pub right: Arc<LogicalOperator>,
    /// Operation type.
    pub op: SetOpType,
}

/// Subquery alias operator.
#[derive(Debug, Clone)]
pub struct SubqueryAliasOperator {
    /// Input operator.
    pub input: Arc<LogicalOperator>,
    /// Alias name.
    pub alias: String,
    /// Aliased schema.
    pub schema: SchemaRef,
}

/// Values operator (inline data).
#[derive(Debug, Clone)]
pub struct ValuesOperator {
    /// Rows of values.
    pub values: Vec<Vec<LogicalExpr>>,
    /// Schema.
    pub schema: SchemaRef,
}

/// Empty relation operator.
#[derive(Debug, Clone)]
pub struct EmptyRelationOperator {
    /// Whether to produce a single row (for SELECT without FROM).
    pub produce_one_row: bool,
    /// Schema.
    pub schema: SchemaRef,
}

/// Window operator.
#[derive(Debug, Clone)]
pub struct WindowOperator {
    /// Input operator.
    pub input: Arc<LogicalOperator>,
    /// Window expressions.
    pub window_exprs: Vec<LogicalExpr>,
    /// Output schema.
    pub schema: SchemaRef,
}

/// CTE operator.
#[derive(Debug, Clone)]
pub struct CteOperator {
    /// Input operator (main query).
    pub input: Arc<LogicalOperator>,
    /// CTE definitions (name -> plan).
    pub cte_plans: Vec<Arc<LogicalOperator>>,
    /// CTE names.
    pub cte_names: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::schema::Field;
    use crate::parser::DataType;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
        ])
    }

    #[test]
    fn test_scan_operator() {
        let scan = ScanOperator::new("users", test_schema());
        assert_eq!(scan.table_name, "users");
        assert_eq!(scan.projected_schema.len(), 2);
    }

    #[test]
    fn test_scan_with_projection() {
        let scan = ScanOperator::new("users", test_schema()).with_projection(vec![0]);
        assert_eq!(scan.projected_schema.len(), 1);
        assert_eq!(scan.projected_schema.field(0).unwrap().name(), "id");
    }

    #[test]
    fn test_join_type_properties() {
        assert!(JoinType::Left.preserves_left());
        assert!(!JoinType::Left.preserves_right());
        assert!(JoinType::Full.is_outer());
        assert!(!JoinType::Inner.is_outer());
    }

    #[test]
    fn test_extract_conjuncts() {
        let expr = LogicalExpr::col("a")
            .eq(LogicalExpr::lit_i64(1))
            .and(LogicalExpr::col("b").gt(LogicalExpr::lit_i64(2)))
            .and(LogicalExpr::col("c").is_not_null());

        let conjuncts = extract_conjuncts(&expr);
        assert_eq!(conjuncts.len(), 3);
    }

    #[test]
    fn test_conjoin() {
        let exprs = vec![
            LogicalExpr::col("a").eq(LogicalExpr::lit_i64(1)),
            LogicalExpr::col("b").eq(LogicalExpr::lit_i64(2)),
        ];
        let combined = conjoin(exprs);
        assert!(combined.is_some());
        assert!(matches!(
            combined.unwrap(),
            LogicalExpr::BinaryOp {
                op: super::super::expr::BinaryOp::And,
                ..
            }
        ));
    }
}
