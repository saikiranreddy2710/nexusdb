//! Logical expressions for query plans.
//!
//! These expressions are used in logical plans and are independent of
//! physical execution. They support type inference and various transformations.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use super::schema::{Column, Schema};
use crate::parser::{DataType, Literal};

/// A logical expression.
///
/// Note: We implement PartialEq manually because some variants contain
/// Arc<LogicalPlan> which requires special handling for equality comparison.
#[derive(Debug, Clone)]
pub enum LogicalExpr {
    /// Column reference.
    Column(Column),

    /// Literal value.
    Literal(Literal),

    /// Alias (expression AS name).
    Alias {
        expr: Box<LogicalExpr>,
        name: String,
    },

    /// Binary operation.
    BinaryOp {
        left: Box<LogicalExpr>,
        op: BinaryOp,
        right: Box<LogicalExpr>,
    },

    /// Unary operation.
    UnaryOp { op: UnaryOp, expr: Box<LogicalExpr> },

    /// IS NULL check.
    IsNull(Box<LogicalExpr>),

    /// IS NOT NULL check.
    IsNotNull(Box<LogicalExpr>),

    /// BETWEEN expression.
    Between {
        expr: Box<LogicalExpr>,
        low: Box<LogicalExpr>,
        high: Box<LogicalExpr>,
        negated: bool,
    },

    /// IN list.
    InList {
        expr: Box<LogicalExpr>,
        list: Vec<LogicalExpr>,
        negated: bool,
    },

    /// IN subquery.
    InSubquery {
        expr: Box<LogicalExpr>,
        subquery: Arc<super::LogicalPlan>,
        negated: bool,
    },

    /// CASE expression.
    Case {
        operand: Option<Box<LogicalExpr>>,
        when_then: Vec<(LogicalExpr, LogicalExpr)>,
        else_result: Option<Box<LogicalExpr>>,
    },

    /// CAST expression.
    Cast {
        expr: Box<LogicalExpr>,
        data_type: DataType,
    },

    /// Scalar function call.
    ScalarFunction {
        name: String,
        args: Vec<LogicalExpr>,
    },

    /// Aggregate function call.
    AggregateFunction {
        name: AggregateFunc,
        args: Vec<LogicalExpr>,
        distinct: bool,
        filter: Option<Box<LogicalExpr>>,
    },

    /// Window function.
    WindowFunction {
        func: WindowFunc,
        args: Vec<LogicalExpr>,
        partition_by: Vec<LogicalExpr>,
        order_by: Vec<SortExpr>,
        window_frame: Option<WindowFrame>,
    },

    /// Scalar subquery (returns single value).
    ScalarSubquery(Arc<super::LogicalPlan>),

    /// EXISTS subquery.
    Exists {
        subquery: Arc<super::LogicalPlan>,
        negated: bool,
    },

    /// Placeholder for prepared statements.
    Placeholder(usize),

    /// Wildcard (*).
    Wildcard,

    /// Qualified wildcard (table.*).
    QualifiedWildcard(String),
}

impl PartialEq for LogicalExpr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Column(a), Self::Column(b)) => a == b,
            (Self::Literal(a), Self::Literal(b)) => a == b,
            (Self::Alias { expr: e1, name: n1 }, Self::Alias { expr: e2, name: n2 }) => {
                e1 == e2 && n1 == n2
            }
            (
                Self::BinaryOp {
                    left: l1,
                    op: o1,
                    right: r1,
                },
                Self::BinaryOp {
                    left: l2,
                    op: o2,
                    right: r2,
                },
            ) => l1 == l2 && o1 == o2 && r1 == r2,
            (Self::UnaryOp { op: o1, expr: e1 }, Self::UnaryOp { op: o2, expr: e2 }) => {
                o1 == o2 && e1 == e2
            }
            (Self::IsNull(a), Self::IsNull(b)) => a == b,
            (Self::IsNotNull(a), Self::IsNotNull(b)) => a == b,
            (
                Self::Between {
                    expr: e1,
                    low: l1,
                    high: h1,
                    negated: n1,
                },
                Self::Between {
                    expr: e2,
                    low: l2,
                    high: h2,
                    negated: n2,
                },
            ) => e1 == e2 && l1 == l2 && h1 == h2 && n1 == n2,
            (
                Self::InList {
                    expr: e1,
                    list: l1,
                    negated: n1,
                },
                Self::InList {
                    expr: e2,
                    list: l2,
                    negated: n2,
                },
            ) => e1 == e2 && l1 == l2 && n1 == n2,
            (
                Self::InSubquery {
                    expr: e1,
                    subquery: s1,
                    negated: n1,
                },
                Self::InSubquery {
                    expr: e2,
                    subquery: s2,
                    negated: n2,
                },
            ) => e1 == e2 && Arc::ptr_eq(s1, s2) && n1 == n2,
            (
                Self::Case {
                    operand: o1,
                    when_then: w1,
                    else_result: e1,
                },
                Self::Case {
                    operand: o2,
                    when_then: w2,
                    else_result: e2,
                },
            ) => o1 == o2 && w1 == w2 && e1 == e2,
            (
                Self::Cast {
                    expr: e1,
                    data_type: d1,
                },
                Self::Cast {
                    expr: e2,
                    data_type: d2,
                },
            ) => e1 == e2 && d1 == d2,
            (
                Self::ScalarFunction { name: n1, args: a1 },
                Self::ScalarFunction { name: n2, args: a2 },
            ) => n1 == n2 && a1 == a2,
            (
                Self::AggregateFunction {
                    name: n1,
                    args: a1,
                    distinct: d1,
                    filter: f1,
                },
                Self::AggregateFunction {
                    name: n2,
                    args: a2,
                    distinct: d2,
                    filter: f2,
                },
            ) => n1 == n2 && a1 == a2 && d1 == d2 && f1 == f2,
            (
                Self::WindowFunction {
                    func: f1,
                    args: a1,
                    partition_by: p1,
                    order_by: o1,
                    window_frame: w1,
                },
                Self::WindowFunction {
                    func: f2,
                    args: a2,
                    partition_by: p2,
                    order_by: o2,
                    window_frame: w2,
                },
            ) => f1 == f2 && a1 == a2 && p1 == p2 && o1 == o2 && w1 == w2,
            (Self::ScalarSubquery(s1), Self::ScalarSubquery(s2)) => Arc::ptr_eq(s1, s2),
            (
                Self::Exists {
                    subquery: s1,
                    negated: n1,
                },
                Self::Exists {
                    subquery: s2,
                    negated: n2,
                },
            ) => Arc::ptr_eq(s1, s2) && n1 == n2,
            (Self::Placeholder(a), Self::Placeholder(b)) => a == b,
            (Self::Wildcard, Self::Wildcard) => true,
            (Self::QualifiedWildcard(a), Self::QualifiedWildcard(b)) => a == b,
            _ => false,
        }
    }
}

impl LogicalExpr {
    // ===== Constructors =====

    /// Creates a column reference.
    pub fn col(name: impl Into<String>) -> Self {
        LogicalExpr::Column(Column::new(name))
    }

    /// Creates a qualified column reference.
    pub fn qualified_col(qualifier: impl Into<String>, name: impl Into<String>) -> Self {
        LogicalExpr::Column(Column::qualified(qualifier, name))
    }

    /// Creates a literal integer.
    pub fn lit_i64(value: i64) -> Self {
        LogicalExpr::Literal(Literal::Integer(value))
    }

    /// Creates a literal float.
    pub fn lit_f64(value: f64) -> Self {
        LogicalExpr::Literal(Literal::Float(value))
    }

    /// Creates a literal string.
    pub fn lit_str(value: impl Into<String>) -> Self {
        LogicalExpr::Literal(Literal::String(value.into()))
    }

    /// Creates a literal boolean.
    pub fn lit_bool(value: bool) -> Self {
        LogicalExpr::Literal(Literal::Boolean(value))
    }

    /// Creates a NULL literal.
    pub fn lit_null() -> Self {
        LogicalExpr::Literal(Literal::Null)
    }

    /// Creates an alias.
    pub fn alias(self, name: impl Into<String>) -> Self {
        LogicalExpr::Alias {
            expr: Box::new(self),
            name: name.into(),
        }
    }

    // ===== Binary operations =====

    /// Equality comparison.
    pub fn eq(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::Eq, other)
    }

    /// Not equal comparison.
    pub fn not_eq(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::NotEq, other)
    }

    /// Less than comparison.
    pub fn lt(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::Lt, other)
    }

    /// Less than or equal comparison.
    pub fn lt_eq(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::LtEq, other)
    }

    /// Greater than comparison.
    pub fn gt(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::Gt, other)
    }

    /// Greater than or equal comparison.
    pub fn gt_eq(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::GtEq, other)
    }

    /// Logical AND.
    pub fn and(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::And, other)
    }

    /// Logical OR.
    pub fn or(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::Or, other)
    }

    /// Addition.
    pub fn add(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::Plus, other)
    }

    /// Subtraction.
    pub fn sub(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::Minus, other)
    }

    /// Multiplication.
    pub fn mul(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::Multiply, other)
    }

    /// Division.
    pub fn div(self, other: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::Divide, other)
    }

    /// LIKE pattern matching.
    pub fn like(self, pattern: LogicalExpr) -> Self {
        self.binary_op(BinaryOp::Like, pattern)
    }

    fn binary_op(self, op: BinaryOp, right: LogicalExpr) -> Self {
        LogicalExpr::BinaryOp {
            left: Box::new(self),
            op,
            right: Box::new(right),
        }
    }

    // ===== Unary operations =====

    /// Logical NOT.
    pub fn not(self) -> Self {
        LogicalExpr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(self),
        }
    }

    /// Negation.
    pub fn neg(self) -> Self {
        LogicalExpr::UnaryOp {
            op: UnaryOp::Minus,
            expr: Box::new(self),
        }
    }

    /// IS NULL.
    pub fn is_null(self) -> Self {
        LogicalExpr::IsNull(Box::new(self))
    }

    /// IS NOT NULL.
    pub fn is_not_null(self) -> Self {
        LogicalExpr::IsNotNull(Box::new(self))
    }

    // ===== Aggregate functions =====

    /// COUNT aggregate.
    pub fn count(self) -> Self {
        LogicalExpr::AggregateFunction {
            name: AggregateFunc::Count,
            args: vec![self],
            distinct: false,
            filter: None,
        }
    }

    /// SUM aggregate.
    pub fn sum(self) -> Self {
        LogicalExpr::AggregateFunction {
            name: AggregateFunc::Sum,
            args: vec![self],
            distinct: false,
            filter: None,
        }
    }

    /// AVG aggregate.
    pub fn avg(self) -> Self {
        LogicalExpr::AggregateFunction {
            name: AggregateFunc::Avg,
            args: vec![self],
            distinct: false,
            filter: None,
        }
    }

    /// MIN aggregate.
    pub fn min(self) -> Self {
        LogicalExpr::AggregateFunction {
            name: AggregateFunc::Min,
            args: vec![self],
            distinct: false,
            filter: None,
        }
    }

    /// MAX aggregate.
    pub fn max(self) -> Self {
        LogicalExpr::AggregateFunction {
            name: AggregateFunc::Max,
            args: vec![self],
            distinct: false,
            filter: None,
        }
    }

    // ===== Analysis methods =====

    /// Returns all column references in this expression.
    pub fn columns(&self) -> HashSet<Column> {
        let mut cols = HashSet::new();
        self.collect_columns(&mut cols);
        cols
    }

    fn collect_columns(&self, cols: &mut HashSet<Column>) {
        match self {
            LogicalExpr::Column(c) => {
                cols.insert(c.clone());
            }
            LogicalExpr::Alias { expr, .. } => expr.collect_columns(cols),
            LogicalExpr::BinaryOp { left, right, .. } => {
                left.collect_columns(cols);
                right.collect_columns(cols);
            }
            LogicalExpr::UnaryOp { expr, .. } => expr.collect_columns(cols),
            LogicalExpr::IsNull(e) | LogicalExpr::IsNotNull(e) => e.collect_columns(cols),
            LogicalExpr::Between {
                expr, low, high, ..
            } => {
                expr.collect_columns(cols);
                low.collect_columns(cols);
                high.collect_columns(cols);
            }
            LogicalExpr::InList { expr, list, .. } => {
                expr.collect_columns(cols);
                for e in list {
                    e.collect_columns(cols);
                }
            }
            LogicalExpr::Case {
                operand,
                when_then,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    op.collect_columns(cols);
                }
                for (w, t) in when_then {
                    w.collect_columns(cols);
                    t.collect_columns(cols);
                }
                if let Some(e) = else_result {
                    e.collect_columns(cols);
                }
            }
            LogicalExpr::Cast { expr, .. } => expr.collect_columns(cols),
            LogicalExpr::ScalarFunction { args, .. } => {
                for arg in args {
                    arg.collect_columns(cols);
                }
            }
            LogicalExpr::AggregateFunction { args, filter, .. } => {
                for arg in args {
                    arg.collect_columns(cols);
                }
                if let Some(f) = filter {
                    f.collect_columns(cols);
                }
            }
            LogicalExpr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    arg.collect_columns(cols);
                }
                for p in partition_by {
                    p.collect_columns(cols);
                }
                for o in order_by {
                    o.expr.collect_columns(cols);
                }
            }
            _ => {}
        }
    }

    /// Returns true if this expression contains any aggregate functions.
    pub fn contains_aggregate(&self) -> bool {
        match self {
            LogicalExpr::AggregateFunction { .. } => true,
            LogicalExpr::Alias { expr, .. } => expr.contains_aggregate(),
            LogicalExpr::BinaryOp { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            LogicalExpr::UnaryOp { expr, .. } => expr.contains_aggregate(),
            LogicalExpr::Case {
                operand,
                when_then,
                else_result,
                ..
            } => {
                operand.as_ref().map_or(false, |e| e.contains_aggregate())
                    || when_then
                        .iter()
                        .any(|(w, t)| w.contains_aggregate() || t.contains_aggregate())
                    || else_result
                        .as_ref()
                        .map_or(false, |e| e.contains_aggregate())
            }
            LogicalExpr::Cast { expr, .. } => expr.contains_aggregate(),
            LogicalExpr::ScalarFunction { args, .. } => args.iter().any(|a| a.contains_aggregate()),
            _ => false,
        }
    }

    /// Returns true if this expression is deterministic.
    pub fn is_deterministic(&self) -> bool {
        match self {
            LogicalExpr::Column(_) | LogicalExpr::Literal(_) | LogicalExpr::Placeholder(_) => true,
            LogicalExpr::Alias { expr, .. } => expr.is_deterministic(),
            LogicalExpr::BinaryOp { left, right, .. } => {
                left.is_deterministic() && right.is_deterministic()
            }
            LogicalExpr::UnaryOp { expr, .. } => expr.is_deterministic(),
            LogicalExpr::IsNull(e) | LogicalExpr::IsNotNull(e) => e.is_deterministic(),
            LogicalExpr::ScalarFunction { name, args, .. } => {
                // Most functions are deterministic, but some aren't
                let non_deterministic = ["random", "now", "current_timestamp", "uuid"];
                !non_deterministic.contains(&name.to_lowercase().as_str())
                    && args.iter().all(|a| a.is_deterministic())
            }
            _ => true,
        }
    }

    /// Infer the output data type of this expression given an input schema.
    pub fn data_type(&self, schema: &Schema) -> Option<DataType> {
        match self {
            LogicalExpr::Column(col) => schema
                .field_by_name(&col.qualified_name())
                .or_else(|| schema.field_by_name(&col.name))
                .map(|f| f.data_type.clone()),
            LogicalExpr::Literal(lit) => Some(literal_data_type(lit)),
            LogicalExpr::Alias { expr, .. } => expr.data_type(schema),
            LogicalExpr::BinaryOp { left, op, right } => {
                binary_op_data_type(op, left.data_type(schema)?, right.data_type(schema)?)
            }
            LogicalExpr::UnaryOp { op, expr } => unary_op_data_type(op, expr.data_type(schema)?),
            LogicalExpr::IsNull(_) | LogicalExpr::IsNotNull(_) => Some(DataType::Boolean),
            LogicalExpr::Between { .. }
            | LogicalExpr::InList { .. }
            | LogicalExpr::InSubquery { .. } => Some(DataType::Boolean),
            LogicalExpr::Case {
                when_then,
                else_result,
                ..
            } => {
                // Type is the type of the first THEN clause
                when_then
                    .first()
                    .and_then(|(_, t)| t.data_type(schema))
                    .or_else(|| else_result.as_ref().and_then(|e| e.data_type(schema)))
            }
            LogicalExpr::Cast { data_type, .. } => Some(data_type.clone()),
            LogicalExpr::AggregateFunction { name, args, .. } => {
                aggregate_return_type(name, args.first().and_then(|a| a.data_type(schema)))
            }
            LogicalExpr::Exists { .. } => Some(DataType::Boolean),
            _ => None,
        }
    }

    /// Returns the output name of this expression.
    pub fn output_name(&self) -> String {
        match self {
            LogicalExpr::Column(col) => col.name.clone(),
            LogicalExpr::Alias { name, .. } => name.clone(),
            LogicalExpr::Literal(lit) => lit.to_string(),
            LogicalExpr::BinaryOp { left, op, right } => {
                format!("{} {} {}", left.output_name(), op, right.output_name())
            }
            LogicalExpr::AggregateFunction { name, args, .. } => {
                let args_str: Vec<_> = args.iter().map(|a| a.output_name()).collect();
                format!("{}({})", name, args_str.join(", "))
            }
            LogicalExpr::ScalarFunction { name, args, .. } => {
                let args_str: Vec<_> = args.iter().map(|a| a.output_name()).collect();
                format!("{}({})", name, args_str.join(", "))
            }
            _ => "?".to_string(),
        }
    }
}

fn literal_data_type(lit: &Literal) -> DataType {
    match lit {
        Literal::Null => DataType::Nullable(Box::new(DataType::Int)),
        Literal::Boolean(_) => DataType::Boolean,
        Literal::Integer(_) => DataType::BigInt,
        Literal::Float(_) => DataType::Double,
        Literal::String(_) => DataType::Text,
        Literal::Blob(_) => DataType::Blob,
    }
}

fn binary_op_data_type(op: &BinaryOp, left: DataType, right: DataType) -> Option<DataType> {
    match op {
        // Comparison operators return boolean
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq
        | BinaryOp::Like
        | BinaryOp::ILike
        | BinaryOp::And
        | BinaryOp::Or => Some(DataType::Boolean),

        // Arithmetic preserves the wider type
        BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Multiply => {
            Some(coerce_numeric_types(&left, &right))
        }
        BinaryOp::Divide => Some(DataType::Double),
        BinaryOp::Modulo => Some(coerce_numeric_types(&left, &right)),

        // String concatenation
        BinaryOp::Concat => Some(DataType::Text),

        _ => None,
    }
}

fn unary_op_data_type(op: &UnaryOp, input: DataType) -> Option<DataType> {
    match op {
        UnaryOp::Not => Some(DataType::Boolean),
        UnaryOp::Minus | UnaryOp::Plus => Some(input),
        UnaryOp::BitwiseNot => Some(input),
    }
}

fn coerce_numeric_types(left: &DataType, right: &DataType) -> DataType {
    match (left, right) {
        (DataType::Double, _) | (_, DataType::Double) => DataType::Double,
        (DataType::Float, _) | (_, DataType::Float) => DataType::Float,
        (DataType::Decimal { .. }, _) | (_, DataType::Decimal { .. }) => DataType::Decimal {
            precision: None,
            scale: None,
        },
        (DataType::BigInt, _) | (_, DataType::BigInt) => DataType::BigInt,
        _ => DataType::Int,
    }
}

fn aggregate_return_type(func: &AggregateFunc, input: Option<DataType>) -> Option<DataType> {
    match func {
        AggregateFunc::Count | AggregateFunc::CountStar => Some(DataType::BigInt),
        AggregateFunc::Sum => input.map(|t| coerce_sum_type(&t)),
        AggregateFunc::Avg => Some(DataType::Double),
        AggregateFunc::Min | AggregateFunc::Max => input,
        AggregateFunc::First | AggregateFunc::Last => input,
        AggregateFunc::StringAgg => Some(DataType::Text),
        AggregateFunc::ArrayAgg => input.map(|t| DataType::Array(Box::new(t))),
        AggregateFunc::BoolAnd | AggregateFunc::BoolOr => Some(DataType::Boolean),
    }
}

fn coerce_sum_type(input: &DataType) -> DataType {
    match input {
        DataType::TinyInt | DataType::SmallInt | DataType::Int => DataType::BigInt,
        DataType::Float => DataType::Double,
        other => other.clone(),
    }
}

/// Binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,

    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,

    // Logical
    And,
    Or,

    // String
    Like,
    ILike,
    Concat,

    // Bitwise
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
}

impl BinaryOp {
    /// Returns true if this is a comparison operator.
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
        )
    }

    /// Returns true if this is a logical operator.
    pub fn is_logical(&self) -> bool {
        matches!(self, BinaryOp::And | BinaryOp::Or)
    }

    /// Returns true if this is an arithmetic operator.
    pub fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            BinaryOp::Plus
                | BinaryOp::Minus
                | BinaryOp::Multiply
                | BinaryOp::Divide
                | BinaryOp::Modulo
        )
    }

    /// Returns true if this operator is commutative.
    pub fn is_commutative(&self) -> bool {
        matches!(
            self,
            BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Plus
                | BinaryOp::Multiply
                | BinaryOp::And
                | BinaryOp::Or
                | BinaryOp::BitwiseAnd
                | BinaryOp::BitwiseOr
                | BinaryOp::BitwiseXor
        )
    }

    /// Returns the negation of this comparison operator.
    pub fn negate(&self) -> Option<BinaryOp> {
        match self {
            BinaryOp::Eq => Some(BinaryOp::NotEq),
            BinaryOp::NotEq => Some(BinaryOp::Eq),
            BinaryOp::Lt => Some(BinaryOp::GtEq),
            BinaryOp::LtEq => Some(BinaryOp::Gt),
            BinaryOp::Gt => Some(BinaryOp::LtEq),
            BinaryOp::GtEq => Some(BinaryOp::Lt),
            _ => None,
        }
    }

    /// Swaps the operands (for normalizing column on left).
    pub fn swap(&self) -> Option<BinaryOp> {
        match self {
            BinaryOp::Eq => Some(BinaryOp::Eq),
            BinaryOp::NotEq => Some(BinaryOp::NotEq),
            BinaryOp::Lt => Some(BinaryOp::Gt),
            BinaryOp::LtEq => Some(BinaryOp::GtEq),
            BinaryOp::Gt => Some(BinaryOp::Lt),
            BinaryOp::GtEq => Some(BinaryOp::LtEq),
            BinaryOp::Plus | BinaryOp::Multiply | BinaryOp::And | BinaryOp::Or => Some(*self),
            _ => None,
        }
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::NotEq => write!(f, "<>"),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::LtEq => write!(f, "<="),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::GtEq => write!(f, ">="),
            BinaryOp::Plus => write!(f, "+"),
            BinaryOp::Minus => write!(f, "-"),
            BinaryOp::Multiply => write!(f, "*"),
            BinaryOp::Divide => write!(f, "/"),
            BinaryOp::Modulo => write!(f, "%"),
            BinaryOp::And => write!(f, "AND"),
            BinaryOp::Or => write!(f, "OR"),
            BinaryOp::Like => write!(f, "LIKE"),
            BinaryOp::ILike => write!(f, "ILIKE"),
            BinaryOp::Concat => write!(f, "||"),
            BinaryOp::BitwiseAnd => write!(f, "&"),
            BinaryOp::BitwiseOr => write!(f, "|"),
            BinaryOp::BitwiseXor => write!(f, "^"),
        }
    }
}

/// Unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    Not,
    Minus,
    Plus,
    BitwiseNot,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOp::Not => write!(f, "NOT"),
            UnaryOp::Minus => write!(f, "-"),
            UnaryOp::Plus => write!(f, "+"),
            UnaryOp::BitwiseNot => write!(f, "~"),
        }
    }
}

/// Aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunc {
    Count,
    CountStar,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    StringAgg,
    ArrayAgg,
    BoolAnd,
    BoolOr,
}

impl fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunc::Count => write!(f, "COUNT"),
            AggregateFunc::CountStar => write!(f, "COUNT(*)"),
            AggregateFunc::Sum => write!(f, "SUM"),
            AggregateFunc::Avg => write!(f, "AVG"),
            AggregateFunc::Min => write!(f, "MIN"),
            AggregateFunc::Max => write!(f, "MAX"),
            AggregateFunc::First => write!(f, "FIRST"),
            AggregateFunc::Last => write!(f, "LAST"),
            AggregateFunc::StringAgg => write!(f, "STRING_AGG"),
            AggregateFunc::ArrayAgg => write!(f, "ARRAY_AGG"),
            AggregateFunc::BoolAnd => write!(f, "BOOL_AND"),
            AggregateFunc::BoolOr => write!(f, "BOOL_OR"),
        }
    }
}

/// Window function.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunc {
    /// Aggregate used as window function.
    Aggregate(AggregateFunc),
    /// ROW_NUMBER().
    RowNumber,
    /// RANK().
    Rank,
    /// DENSE_RANK().
    DenseRank,
    /// PERCENT_RANK().
    PercentRank,
    /// CUME_DIST().
    CumeDist,
    /// NTILE(n).
    Ntile,
    /// LAG(expr, offset, default).
    Lag,
    /// LEAD(expr, offset, default).
    Lead,
    /// FIRST_VALUE(expr).
    FirstValue,
    /// LAST_VALUE(expr).
    LastValue,
    /// NTH_VALUE(expr, n).
    NthValue,
}

impl fmt::Display for WindowFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowFunc::Aggregate(agg) => write!(f, "{}", agg),
            WindowFunc::RowNumber => write!(f, "ROW_NUMBER"),
            WindowFunc::Rank => write!(f, "RANK"),
            WindowFunc::DenseRank => write!(f, "DENSE_RANK"),
            WindowFunc::PercentRank => write!(f, "PERCENT_RANK"),
            WindowFunc::CumeDist => write!(f, "CUME_DIST"),
            WindowFunc::Ntile => write!(f, "NTILE"),
            WindowFunc::Lag => write!(f, "LAG"),
            WindowFunc::Lead => write!(f, "LEAD"),
            WindowFunc::FirstValue => write!(f, "FIRST_VALUE"),
            WindowFunc::LastValue => write!(f, "LAST_VALUE"),
            WindowFunc::NthValue => write!(f, "NTH_VALUE"),
        }
    }
}

/// Sort expression.
#[derive(Debug, Clone, PartialEq)]
pub struct SortExpr {
    /// Expression to sort by.
    pub expr: LogicalExpr,
    /// Sort direction.
    pub asc: bool,
    /// NULL handling.
    pub nulls_first: bool,
}

impl SortExpr {
    /// Creates a new ascending sort.
    pub fn asc(expr: LogicalExpr) -> Self {
        Self {
            expr,
            asc: true,
            nulls_first: false,
        }
    }

    /// Creates a new descending sort.
    pub fn desc(expr: LogicalExpr) -> Self {
        Self {
            expr,
            asc: false,
            nulls_first: true,
        }
    }
}

impl fmt::Display for SortExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr.output_name())?;
        if self.asc {
            write!(f, " ASC")?;
        } else {
            write!(f, " DESC")?;
        }
        if self.nulls_first {
            write!(f, " NULLS FIRST")?;
        } else {
            write!(f, " NULLS LAST")?;
        }
        Ok(())
    }
}

/// Window frame specification.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    /// Frame type.
    pub frame_type: WindowFrameType,
    /// Start bound.
    pub start: WindowFrameBound,
    /// End bound.
    pub end: WindowFrameBound,
}

/// Window frame type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameType {
    Rows,
    Range,
    Groups,
}

/// Window frame bound.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(u64),
    Following(u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr_constructors() {
        let col = LogicalExpr::col("id");
        assert!(matches!(col, LogicalExpr::Column(_)));

        let lit = LogicalExpr::lit_i64(42);
        assert!(matches!(lit, LogicalExpr::Literal(Literal::Integer(42))));
    }

    #[test]
    fn test_binary_ops() {
        let expr = LogicalExpr::col("a").eq(LogicalExpr::lit_i64(1));
        assert!(matches!(
            expr,
            LogicalExpr::BinaryOp {
                op: BinaryOp::Eq,
                ..
            }
        ));

        let and_expr = LogicalExpr::col("a")
            .eq(LogicalExpr::lit_i64(1))
            .and(LogicalExpr::col("b").gt(LogicalExpr::lit_i64(2)));
        assert!(matches!(
            and_expr,
            LogicalExpr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn test_columns_extraction() {
        let expr = LogicalExpr::col("a")
            .add(LogicalExpr::col("b"))
            .mul(LogicalExpr::col("c"));
        let cols = expr.columns();
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn test_contains_aggregate() {
        let simple = LogicalExpr::col("a").add(LogicalExpr::lit_i64(1));
        assert!(!simple.contains_aggregate());

        let agg = LogicalExpr::col("a").sum();
        assert!(agg.contains_aggregate());
    }

    #[test]
    fn test_operator_properties() {
        assert!(BinaryOp::Eq.is_comparison());
        assert!(BinaryOp::And.is_logical());
        assert!(BinaryOp::Plus.is_arithmetic());
        assert!(BinaryOp::Eq.is_commutative());
        assert!(!BinaryOp::Minus.is_commutative());
    }

    #[test]
    fn test_operator_negate() {
        assert_eq!(BinaryOp::Eq.negate(), Some(BinaryOp::NotEq));
        assert_eq!(BinaryOp::Lt.negate(), Some(BinaryOp::GtEq));
    }

    #[test]
    fn test_data_type_inference() {
        let schema = Schema::new(vec![
            super::super::schema::Field::not_null("a", DataType::Int),
            super::super::schema::Field::not_null("b", DataType::BigInt),
        ]);

        let col_a = LogicalExpr::col("a");
        assert_eq!(col_a.data_type(&schema), Some(DataType::Int));

        let sum = LogicalExpr::col("a").add(LogicalExpr::col("b"));
        assert_eq!(sum.data_type(&schema), Some(DataType::BigInt));
    }
}
