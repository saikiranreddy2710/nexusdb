//! Physical expressions for execution.
//!
//! Physical expressions are concrete implementations of logical expressions
//! that can be evaluated on data batches during query execution.

use std::fmt;

use crate::logical::{AggregateFunc, BinaryOp, LogicalExpr, Schema, UnaryOp};
use crate::parser::{DataType, Literal};

/// A physical expression that can be evaluated on data.
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalExpr {
    /// Column reference by index.
    Column {
        /// Column name for display.
        name: String,
        /// Column index in the input schema.
        index: usize,
    },

    /// Literal value.
    Literal(Literal),

    /// Binary operation.
    BinaryExpr {
        /// Left operand.
        left: Box<PhysicalExpr>,
        /// Operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<PhysicalExpr>,
    },

    /// Unary operation.
    UnaryExpr {
        /// Operator.
        op: UnaryOp,
        /// Operand.
        expr: Box<PhysicalExpr>,
    },

    /// IS NULL check.
    IsNull(Box<PhysicalExpr>),

    /// IS NOT NULL check.
    IsNotNull(Box<PhysicalExpr>),

    /// CAST expression.
    Cast {
        /// Expression to cast.
        expr: Box<PhysicalExpr>,
        /// Target data type.
        data_type: DataType,
    },

    /// CASE expression.
    Case {
        /// Optional operand for simple CASE.
        operand: Option<Box<PhysicalExpr>>,
        /// WHEN-THEN pairs.
        when_then: Vec<(PhysicalExpr, PhysicalExpr)>,
        /// ELSE result.
        else_result: Option<Box<PhysicalExpr>>,
    },

    /// Scalar function.
    ScalarFunction {
        /// Function name.
        name: String,
        /// Arguments.
        args: Vec<PhysicalExpr>,
        /// Return type.
        return_type: DataType,
    },

    /// IN list check.
    InList {
        /// Expression to check.
        expr: Box<PhysicalExpr>,
        /// List of values.
        list: Vec<PhysicalExpr>,
        /// Whether negated (NOT IN).
        negated: bool,
    },

    /// BETWEEN check.
    Between {
        /// Expression to check.
        expr: Box<PhysicalExpr>,
        /// Lower bound.
        low: Box<PhysicalExpr>,
        /// Upper bound.
        high: Box<PhysicalExpr>,
        /// Whether negated.
        negated: bool,
    },

    /// LIKE pattern matching.
    Like {
        /// Expression to match.
        expr: Box<PhysicalExpr>,
        /// Pattern.
        pattern: Box<PhysicalExpr>,
        /// Whether negated.
        negated: bool,
        /// Case insensitive (ILIKE).
        case_insensitive: bool,
    },
}

impl PhysicalExpr {
    /// Creates a column reference.
    pub fn column(name: impl Into<String>, index: usize) -> Self {
        PhysicalExpr::Column {
            name: name.into(),
            index,
        }
    }

    /// Creates a literal expression.
    pub fn literal(lit: Literal) -> Self {
        PhysicalExpr::Literal(lit)
    }

    /// Creates a literal integer.
    pub fn lit_i64(value: i64) -> Self {
        PhysicalExpr::Literal(Literal::Integer(value))
    }

    /// Creates a literal boolean.
    pub fn lit_bool(value: bool) -> Self {
        PhysicalExpr::Literal(Literal::Boolean(value))
    }

    /// Creates a binary expression.
    pub fn binary(left: PhysicalExpr, op: BinaryOp, right: PhysicalExpr) -> Self {
        PhysicalExpr::BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Creates a unary expression.
    pub fn unary(op: UnaryOp, expr: PhysicalExpr) -> Self {
        PhysicalExpr::UnaryExpr {
            op,
            expr: Box::new(expr),
        }
    }

    /// Returns the output data type of this expression given an input schema.
    pub fn data_type(&self, schema: &Schema) -> DataType {
        match self {
            PhysicalExpr::Column { index, .. } => schema
                .field(*index)
                .map(|f| f.data_type.clone())
                .unwrap_or(DataType::Text),
            PhysicalExpr::Literal(lit) => literal_data_type(lit),
            PhysicalExpr::BinaryExpr { left, op, right } => {
                binary_op_return_type(op, &left.data_type(schema), &right.data_type(schema))
            }
            PhysicalExpr::UnaryExpr { op, expr } => {
                unary_op_return_type(op, &expr.data_type(schema))
            }
            PhysicalExpr::IsNull(_) | PhysicalExpr::IsNotNull(_) => DataType::Boolean,
            PhysicalExpr::Cast { data_type, .. } => data_type.clone(),
            PhysicalExpr::Case {
                when_then,
                else_result,
                ..
            } => {
                // Return type is the type of the THEN expressions
                if let Some((_, then_expr)) = when_then.first() {
                    then_expr.data_type(schema)
                } else if let Some(else_expr) = else_result {
                    else_expr.data_type(schema)
                } else {
                    DataType::Text
                }
            }
            PhysicalExpr::ScalarFunction { return_type, .. } => return_type.clone(),
            PhysicalExpr::InList { .. }
            | PhysicalExpr::Between { .. }
            | PhysicalExpr::Like { .. } => DataType::Boolean,
        }
    }

    /// Returns true if this expression is a literal.
    pub fn is_literal(&self) -> bool {
        matches!(self, PhysicalExpr::Literal(_))
    }

    /// Returns true if this expression is a column reference.
    pub fn is_column(&self) -> bool {
        matches!(self, PhysicalExpr::Column { .. })
    }

    /// Returns the display name for this expression.
    pub fn name(&self) -> String {
        match self {
            PhysicalExpr::Column { name, .. } => name.clone(),
            PhysicalExpr::Literal(lit) => format!("{}", lit),
            PhysicalExpr::BinaryExpr { left, op, right } => {
                format!("{} {} {}", left.name(), op, right.name())
            }
            PhysicalExpr::UnaryExpr { op, expr } => {
                format!("{}{}", op, expr.name())
            }
            PhysicalExpr::IsNull(expr) => format!("{} IS NULL", expr.name()),
            PhysicalExpr::IsNotNull(expr) => format!("{} IS NOT NULL", expr.name()),
            PhysicalExpr::Cast { expr, data_type } => {
                format!("CAST({} AS {:?})", expr.name(), data_type)
            }
            PhysicalExpr::Case { .. } => "CASE".to_string(),
            PhysicalExpr::ScalarFunction { name, .. } => format!("{}(...)", name),
            PhysicalExpr::InList { expr, negated, .. } => {
                format!(
                    "{} {}IN (...)",
                    expr.name(),
                    if *negated { "NOT " } else { "" }
                )
            }
            PhysicalExpr::Between { expr, negated, .. } => {
                format!(
                    "{} {}BETWEEN ...",
                    expr.name(),
                    if *negated { "NOT " } else { "" }
                )
            }
            PhysicalExpr::Like {
                expr,
                negated,
                case_insensitive,
                ..
            } => {
                let op = if *case_insensitive { "ILIKE" } else { "LIKE" };
                format!(
                    "{} {}{}",
                    expr.name(),
                    if *negated { "NOT " } else { "" },
                    op
                )
            }
        }
    }
}

impl fmt::Display for PhysicalExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Returns the data type of a literal value.
fn literal_data_type(lit: &Literal) -> DataType {
    match lit {
        Literal::Null => DataType::Text, // NULL can be any type
        Literal::Boolean(_) => DataType::Boolean,
        Literal::Integer(_) => DataType::BigInt,
        Literal::Float(_) => DataType::Double,
        Literal::String(_) => DataType::Text,
        Literal::Blob(_) => DataType::Blob,
    }
}

/// Returns the return type of a binary operation.
fn binary_op_return_type(op: &BinaryOp, left: &DataType, right: &DataType) -> DataType {
    match op {
        // Comparison operators return boolean
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq
        | BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Like
        | BinaryOp::ILike => DataType::Boolean,

        // Arithmetic operators return the wider type
        BinaryOp::Plus
        | BinaryOp::Minus
        | BinaryOp::Multiply
        | BinaryOp::Divide
        | BinaryOp::Modulo => coerce_numeric_types(left, right),

        // String concatenation
        BinaryOp::Concat => DataType::Text,

        // Bitwise operators
        BinaryOp::BitwiseAnd | BinaryOp::BitwiseOr | BinaryOp::BitwiseXor => left.clone(),
    }
}

/// Returns the return type of a unary operation.
fn unary_op_return_type(op: &UnaryOp, operand: &DataType) -> DataType {
    match op {
        UnaryOp::Not => DataType::Boolean,
        UnaryOp::Minus | UnaryOp::Plus => operand.clone(),
        UnaryOp::BitwiseNot => operand.clone(),
    }
}

/// Coerces two numeric types to a common type.
fn coerce_numeric_types(left: &DataType, right: &DataType) -> DataType {
    use DataType::*;

    match (left, right) {
        // If either is double, result is double
        (Double, _) | (_, Double) => Double,
        (Float, _) | (_, Float) => Float,
        (
            Decimal {
                precision: p1,
                scale: s1,
            },
            Decimal {
                precision: p2,
                scale: s2,
            },
        ) => {
            let max_p = match (p1, p2) {
                (Some(a), Some(b)) => Some((*a).max(*b)),
                (Some(a), None) | (None, Some(a)) => Some(*a),
                (None, None) => Some(38),
            };
            let max_s = match (s1, s2) {
                (Some(a), Some(b)) => Some((*a).max(*b)),
                (Some(a), None) | (None, Some(a)) => Some(*a),
                (None, None) => Some(0),
            };
            Decimal {
                precision: max_p,
                scale: max_s,
            }
        }
        (BigInt, _) | (_, BigInt) => BigInt,
        (Int, _) | (_, Int) => Int,
        (SmallInt, _) | (_, SmallInt) => SmallInt,
        _ => left.clone(),
    }
}

/// Physical aggregate expression.
#[derive(Debug, Clone)]
pub struct PhysicalAggregateExpr {
    /// Aggregate function.
    pub func: AggregateFunc,
    /// Arguments.
    pub args: Vec<PhysicalExpr>,
    /// Whether DISTINCT.
    pub distinct: bool,
    /// Filter condition.
    pub filter: Option<Box<PhysicalExpr>>,
    /// Output name.
    pub name: String,
}

impl PhysicalAggregateExpr {
    /// Creates a new aggregate expression.
    pub fn new(
        func: AggregateFunc,
        args: Vec<PhysicalExpr>,
        distinct: bool,
        name: impl Into<String>,
    ) -> Self {
        Self {
            func,
            args,
            distinct,
            filter: None,
            name: name.into(),
        }
    }

    /// Returns the return type of this aggregate.
    pub fn return_type(&self, _schema: &Schema) -> DataType {
        match self.func {
            AggregateFunc::Count | AggregateFunc::CountStar => DataType::BigInt,
            AggregateFunc::Sum => DataType::Decimal {
                precision: Some(38),
                scale: Some(0),
            },
            AggregateFunc::Avg => DataType::Double,
            AggregateFunc::Min
            | AggregateFunc::Max
            | AggregateFunc::First
            | AggregateFunc::Last => {
                // Return type matches input type
                if let Some(arg) = self.args.first() {
                    arg.data_type(_schema)
                } else {
                    DataType::Text
                }
            }
            AggregateFunc::BoolAnd | AggregateFunc::BoolOr => DataType::Boolean,
            AggregateFunc::StringAgg => DataType::Text,
            AggregateFunc::ArrayAgg => DataType::Text, // Use Text as fallback for unknown array type
        }
    }
}

impl fmt::Display for PhysicalAggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.func)?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", arg)?;
        }
        write!(f, ")")
    }
}

/// Sort expression for ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalSortExpr {
    /// Expression to sort by.
    pub expr: PhysicalExpr,
    /// Sort direction (true = ascending).
    pub asc: bool,
    /// Nulls first.
    pub nulls_first: bool,
}

impl PhysicalSortExpr {
    /// Creates a new sort expression.
    pub fn new(expr: PhysicalExpr, asc: bool, nulls_first: bool) -> Self {
        Self {
            expr,
            asc,
            nulls_first,
        }
    }

    /// Creates an ascending sort expression.
    pub fn asc(expr: PhysicalExpr) -> Self {
        Self::new(expr, true, false)
    }

    /// Creates a descending sort expression.
    pub fn desc(expr: PhysicalExpr) -> Self {
        Self::new(expr, false, true)
    }
}

impl fmt::Display for PhysicalSortExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.expr,
            if self.asc { "ASC" } else { "DESC" },
            if self.nulls_first {
                "NULLS FIRST"
            } else {
                "NULLS LAST"
            }
        )
    }
}

/// Converts a logical expression to a physical expression.
pub fn create_physical_expr(expr: &LogicalExpr, schema: &Schema) -> Result<PhysicalExpr, String> {
    match expr {
        LogicalExpr::Column(col) => {
            let index = schema
                .index_of_column(col)
                .ok_or_else(|| format!("Column '{}' not found in schema", col.name))?;
            Ok(PhysicalExpr::column(&col.name, index))
        }

        LogicalExpr::Literal(lit) => Ok(PhysicalExpr::literal(lit.clone())),

        LogicalExpr::Alias { expr, .. } => create_physical_expr(expr, schema),

        LogicalExpr::BinaryOp { left, op, right } => {
            let left_expr = create_physical_expr(left, schema)?;
            let right_expr = create_physical_expr(right, schema)?;
            Ok(PhysicalExpr::binary(left_expr, *op, right_expr))
        }

        LogicalExpr::UnaryOp { op, expr } => {
            let inner = create_physical_expr(expr, schema)?;
            Ok(PhysicalExpr::unary(*op, inner))
        }

        LogicalExpr::IsNull(expr) => {
            let inner = create_physical_expr(expr, schema)?;
            Ok(PhysicalExpr::IsNull(Box::new(inner)))
        }

        LogicalExpr::IsNotNull(expr) => {
            let inner = create_physical_expr(expr, schema)?;
            Ok(PhysicalExpr::IsNotNull(Box::new(inner)))
        }

        LogicalExpr::Cast { expr, data_type } => {
            let inner = create_physical_expr(expr, schema)?;
            Ok(PhysicalExpr::Cast {
                expr: Box::new(inner),
                data_type: data_type.clone(),
            })
        }

        LogicalExpr::Case {
            operand,
            when_then,
            else_result,
        } => {
            let phys_operand = operand
                .as_ref()
                .map(|o| create_physical_expr(o, schema).map(Box::new))
                .transpose()?;
            let phys_when_then: Result<Vec<(PhysicalExpr, PhysicalExpr)>, String> = when_then
                .iter()
                .map(|(w, t)| {
                    Ok((
                        create_physical_expr(w, schema)?,
                        create_physical_expr(t, schema)?,
                    ))
                })
                .collect();
            let phys_else = else_result
                .as_ref()
                .map(|e| create_physical_expr(e, schema).map(Box::new))
                .transpose()?;
            Ok(PhysicalExpr::Case {
                operand: phys_operand,
                when_then: phys_when_then?,
                else_result: phys_else,
            })
        }

        LogicalExpr::ScalarFunction { name, args } => {
            let phys_args: Result<Vec<_>, _> = args
                .iter()
                .map(|a| create_physical_expr(a, schema))
                .collect();
            Ok(PhysicalExpr::ScalarFunction {
                name: name.clone(),
                args: phys_args?,
                return_type: DataType::Text, // Would need function registry
            })
        }

        LogicalExpr::InList {
            expr,
            list,
            negated,
        } => {
            let phys_expr = create_physical_expr(expr, schema)?;
            let phys_list: Result<Vec<_>, _> = list
                .iter()
                .map(|e| create_physical_expr(e, schema))
                .collect();
            Ok(PhysicalExpr::InList {
                expr: Box::new(phys_expr),
                list: phys_list?,
                negated: *negated,
            })
        }

        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let phys_expr = create_physical_expr(expr, schema)?;
            let phys_low = create_physical_expr(low, schema)?;
            let phys_high = create_physical_expr(high, schema)?;
            Ok(PhysicalExpr::Between {
                expr: Box::new(phys_expr),
                low: Box::new(phys_low),
                high: Box::new(phys_high),
                negated: *negated,
            })
        }

        // Unsupported expressions
        LogicalExpr::AggregateFunction { .. } => {
            Err("Aggregate functions should be handled separately".to_string())
        }
        LogicalExpr::WindowFunction { .. } => {
            Err("Window functions should be handled separately".to_string())
        }
        LogicalExpr::ScalarSubquery(_) => Err("Scalar subqueries not yet supported".to_string()),
        LogicalExpr::InSubquery { .. } => Err("IN subqueries not yet supported".to_string()),
        LogicalExpr::Exists { .. } => Err("EXISTS subqueries not yet supported".to_string()),
        LogicalExpr::Placeholder(_) => {
            Err("Placeholders should be bound before execution".to_string())
        }
        LogicalExpr::Wildcard | LogicalExpr::QualifiedWildcard(_) => {
            Err("Wildcards should be expanded before physical planning".to_string())
        }
    }
}

/// Creates a physical aggregate expression from a logical one.
pub fn create_physical_aggregate(
    expr: &LogicalExpr,
    schema: &Schema,
    name: &str,
) -> Result<PhysicalAggregateExpr, String> {
    match expr {
        LogicalExpr::AggregateFunction {
            name: func,
            args,
            distinct,
            filter,
        } => {
            let phys_args: Result<Vec<_>, _> = args
                .iter()
                .map(|a| create_physical_expr(a, schema))
                .collect();
            let phys_filter = filter
                .as_ref()
                .map(|f| create_physical_expr(f, schema).map(Box::new))
                .transpose()?;

            Ok(PhysicalAggregateExpr {
                func: *func,
                args: phys_args?,
                distinct: *distinct,
                filter: phys_filter,
                name: name.to_string(),
            })
        }
        _ => Err(format!("Expected aggregate function, got {:?}", expr)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::Field;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
            Field::nullable("age", DataType::Int),
        ])
    }

    #[test]
    fn test_physical_column() {
        let expr = PhysicalExpr::column("id", 0);
        assert!(expr.is_column());
        assert_eq!(expr.name(), "id");
    }

    #[test]
    fn test_physical_literal() {
        let expr = PhysicalExpr::lit_i64(42);
        assert!(expr.is_literal());
    }

    #[test]
    fn test_physical_binary() {
        let left = PhysicalExpr::column("id", 0);
        let right = PhysicalExpr::lit_i64(10);
        let expr = PhysicalExpr::binary(left, BinaryOp::Gt, right);

        let schema = test_schema();
        assert_eq!(expr.data_type(&schema), DataType::Boolean);
    }

    #[test]
    fn test_create_physical_expr() {
        let schema = test_schema();
        let logical = LogicalExpr::col("id").eq(LogicalExpr::lit_i64(1));

        let physical = create_physical_expr(&logical, &schema);
        assert!(physical.is_ok());
    }

    #[test]
    fn test_sort_expr() {
        let expr = PhysicalExpr::column("id", 0);
        let sort = PhysicalSortExpr::asc(expr);
        assert!(sort.asc);
        assert!(!sort.nulls_first);
    }
}
