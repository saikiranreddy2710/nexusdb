//! SQL Expression parsing and representation.
//!
//! This module defines the expression types used in SQL queries, including
//! column references, literals, operators, and function calls.

use std::fmt;

use serde::{Deserialize, Serialize};
use sqlparser::ast as sql_ast;

use super::{ColumnRef, DataType, Literal, NullsOrder, OrderDirection, ParseError, ParseResult};

/// A SQL expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// A column reference.
    Column(ColumnRef),
    /// A literal value.
    Literal(Literal),
    /// A binary operation (e.g., a + b, x = y).
    BinaryOp {
        /// Left operand.
        left: Box<Expr>,
        /// Operator.
        op: BinaryOperator,
        /// Right operand.
        right: Box<Expr>,
    },
    /// A unary operation (e.g., NOT x, -y).
    UnaryOp {
        /// Operator.
        op: UnaryOperator,
        /// Operand.
        expr: Box<Expr>,
    },
    /// A function call.
    Function(FunctionCall),
    /// A CASE expression.
    Case {
        /// The operand (for simple CASE).
        operand: Option<Box<Expr>>,
        /// WHEN branches.
        when_clauses: Vec<(Expr, Expr)>,
        /// ELSE branch.
        else_clause: Option<Box<Expr>>,
    },
    /// A CAST expression.
    Cast {
        /// Expression to cast.
        expr: Box<Expr>,
        /// Target type.
        data_type: DataType,
    },
    /// IS NULL check.
    IsNull(Box<Expr>),
    /// IS NOT NULL check.
    IsNotNull(Box<Expr>),
    /// BETWEEN expression.
    Between {
        /// Expression to test.
        expr: Box<Expr>,
        /// Low bound.
        low: Box<Expr>,
        /// High bound.
        high: Box<Expr>,
        /// Whether this is NOT BETWEEN.
        negated: bool,
    },
    /// IN expression.
    InList {
        /// Expression to test.
        expr: Box<Expr>,
        /// List of values.
        list: Vec<Expr>,
        /// Whether this is NOT IN.
        negated: bool,
    },
    /// IN subquery.
    InSubquery {
        /// Expression to test.
        expr: Box<Expr>,
        /// Subquery.
        subquery: Box<SelectExpr>,
        /// Whether this is NOT IN.
        negated: bool,
    },
    /// EXISTS subquery.
    Exists {
        /// Subquery.
        subquery: Box<SelectExpr>,
        /// Whether this is NOT EXISTS.
        negated: bool,
    },
    /// Scalar subquery.
    Subquery(Box<SelectExpr>),
    /// A parameter placeholder ($1, $2, etc.).
    Parameter(usize),
    /// Wildcard (*).
    Wildcard,
    /// Qualified wildcard (table.*).
    QualifiedWildcard(String),
    /// A nested expression in parentheses.
    Nested(Box<Expr>),
}

impl Expr {
    /// Creates a column expression.
    pub fn column(name: impl Into<String>) -> Self {
        Expr::Column(ColumnRef::new(name))
    }

    /// Creates a qualified column expression.
    pub fn qualified_column(table: impl Into<String>, column: impl Into<String>) -> Self {
        Expr::Column(ColumnRef::qualified(table, column))
    }

    /// Creates an integer literal expression.
    pub fn int(value: i64) -> Self {
        Expr::Literal(Literal::Integer(value))
    }

    /// Creates a string literal expression.
    pub fn string(value: impl Into<String>) -> Self {
        Expr::Literal(Literal::String(value.into()))
    }

    /// Creates a boolean literal expression.
    pub fn bool(value: bool) -> Self {
        Expr::Literal(Literal::Boolean(value))
    }

    /// Creates a NULL literal expression.
    pub fn null() -> Self {
        Expr::Literal(Literal::Null)
    }

    /// Creates an equality expression.
    pub fn eq(self, other: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Eq,
            right: Box::new(other),
        }
    }

    /// Creates an AND expression.
    pub fn and(self, other: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::And,
            right: Box::new(other),
        }
    }

    /// Creates an OR expression.
    pub fn or(self, other: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOperator::Or,
            right: Box::new(other),
        }
    }

    /// Converts from sqlparser's Expr.
    pub fn from_sql_ast(expr: sql_ast::Expr) -> ParseResult<Self> {
        match expr {
            sql_ast::Expr::Identifier(ident) => Ok(Expr::Column(ColumnRef::new(ident.value))),
            sql_ast::Expr::CompoundIdentifier(idents) => match idents.len() {
                1 => Ok(Expr::Column(ColumnRef::new(&idents[0].value))),
                2 => Ok(Expr::Column(ColumnRef::qualified(
                    &idents[0].value,
                    &idents[1].value,
                ))),
                _ => Err(ParseError::Unsupported(
                    "Compound identifier with more than 2 parts".to_string(),
                )),
            },
            sql_ast::Expr::Value(value) => Ok(Expr::Literal(Literal::from_sql_ast(&value)?)),
            sql_ast::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(Expr::from_sql_ast(*left)?),
                op: BinaryOperator::from_sql_ast(&op)?,
                right: Box::new(Expr::from_sql_ast(*right)?),
            }),
            sql_ast::Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
                op: UnaryOperator::from_sql_ast(&op)?,
                expr: Box::new(Expr::from_sql_ast(*expr)?),
            }),
            sql_ast::Expr::Function(func) => Ok(Expr::Function(FunctionCall::from_sql_ast(func)?)),
            sql_ast::Expr::Cast {
                expr, data_type, ..
            } => Ok(Expr::Cast {
                expr: Box::new(Expr::from_sql_ast(*expr)?),
                data_type: DataType::from_sql_ast(&data_type)?,
            }),
            sql_ast::Expr::IsNull(expr) => Ok(Expr::IsNull(Box::new(Expr::from_sql_ast(*expr)?))),
            sql_ast::Expr::IsNotNull(expr) => {
                Ok(Expr::IsNotNull(Box::new(Expr::from_sql_ast(*expr)?)))
            }
            sql_ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between {
                expr: Box::new(Expr::from_sql_ast(*expr)?),
                low: Box::new(Expr::from_sql_ast(*low)?),
                high: Box::new(Expr::from_sql_ast(*high)?),
                negated,
            }),
            sql_ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let list: ParseResult<Vec<_>> = list.into_iter().map(Expr::from_sql_ast).collect();
                Ok(Expr::InList {
                    expr: Box::new(Expr::from_sql_ast(*expr)?),
                    list: list?,
                    negated,
                })
            }
            sql_ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let operand = operand
                    .map(|e| Expr::from_sql_ast(*e))
                    .transpose()?
                    .map(Box::new);

                let when_clauses: ParseResult<Vec<_>> = conditions
                    .into_iter()
                    .zip(results.into_iter())
                    .map(|(cond, result)| {
                        Ok((Expr::from_sql_ast(cond)?, Expr::from_sql_ast(result)?))
                    })
                    .collect();

                let else_clause = else_result
                    .map(|e| Expr::from_sql_ast(*e))
                    .transpose()?
                    .map(Box::new);

                Ok(Expr::Case {
                    operand,
                    when_clauses: when_clauses?,
                    else_clause,
                })
            }
            sql_ast::Expr::Nested(expr) => Ok(Expr::Nested(Box::new(Expr::from_sql_ast(*expr)?))),
            _ => Err(ParseError::Unsupported(format!("Expression: {:?}", expr))),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(col) => write!(f, "{}", col),
            Expr::Literal(lit) => write!(f, "{}", lit),
            Expr::BinaryOp { left, op, right } => write!(f, "({} {} {})", left, op, right),
            Expr::UnaryOp { op, expr } => write!(f, "{} {}", op, expr),
            Expr::Function(func) => write!(f, "{}", func),
            Expr::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr, data_type),
            Expr::IsNull(expr) => write!(f, "{} IS NULL", expr),
            Expr::IsNotNull(expr) => write!(f, "{} IS NOT NULL", expr),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                if *negated {
                    write!(f, "{} NOT BETWEEN {} AND {}", expr, low, high)
                } else {
                    write!(f, "{} BETWEEN {} AND {}", expr, low, high)
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let list_str: Vec<_> = list.iter().map(|e| e.to_string()).collect();
                if *negated {
                    write!(f, "{} NOT IN ({})", expr, list_str.join(", "))
                } else {
                    write!(f, "{} IN ({})", expr, list_str.join(", "))
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                write!(f, "CASE")?;
                if let Some(op) = operand {
                    write!(f, " {}", op)?;
                }
                for (when, then) in when_clauses {
                    write!(f, " WHEN {} THEN {}", when, then)?;
                }
                if let Some(else_expr) = else_clause {
                    write!(f, " ELSE {}", else_expr)?;
                }
                write!(f, " END")
            }
            Expr::Wildcard => write!(f, "*"),
            Expr::QualifiedWildcard(table) => write!(f, "{}.*", table),
            Expr::Nested(expr) => write!(f, "({})", expr),
            Expr::Parameter(n) => write!(f, "${}", n),
            _ => write!(f, "?"),
        }
    }
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOperator {
    // Comparison
    /// Equal.
    Eq,
    /// Not equal.
    NotEq,
    /// Less than.
    Lt,
    /// Less than or equal.
    LtEq,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    GtEq,

    // Arithmetic
    /// Addition.
    Plus,
    /// Subtraction.
    Minus,
    /// Multiplication.
    Multiply,
    /// Division.
    Divide,
    /// Modulo.
    Modulo,

    // Logical
    /// Logical AND.
    And,
    /// Logical OR.
    Or,

    // String
    /// String concatenation.
    Concat,
    /// LIKE pattern matching.
    Like,
    /// NOT LIKE pattern matching.
    NotLike,
    /// ILIKE (case-insensitive LIKE).
    ILike,
    /// Regular expression match.
    RegexMatch,

    // Bitwise
    /// Bitwise AND.
    BitwiseAnd,
    /// Bitwise OR.
    BitwiseOr,
    /// Bitwise XOR.
    BitwiseXor,
}

impl BinaryOperator {
    /// Returns true if this is a comparison operator.
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
        )
    }

    /// Returns true if this is an arithmetic operator.
    pub fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo
        )
    }

    /// Returns true if this is a logical operator.
    pub fn is_logical(&self) -> bool {
        matches!(self, BinaryOperator::And | BinaryOperator::Or)
    }

    /// Converts from sqlparser's BinaryOperator.
    pub fn from_sql_ast(op: &sql_ast::BinaryOperator) -> ParseResult<Self> {
        match op {
            sql_ast::BinaryOperator::Eq => Ok(BinaryOperator::Eq),
            sql_ast::BinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
            sql_ast::BinaryOperator::Lt => Ok(BinaryOperator::Lt),
            sql_ast::BinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
            sql_ast::BinaryOperator::Gt => Ok(BinaryOperator::Gt),
            sql_ast::BinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
            sql_ast::BinaryOperator::Plus => Ok(BinaryOperator::Plus),
            sql_ast::BinaryOperator::Minus => Ok(BinaryOperator::Minus),
            sql_ast::BinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
            sql_ast::BinaryOperator::Divide => Ok(BinaryOperator::Divide),
            sql_ast::BinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
            sql_ast::BinaryOperator::And => Ok(BinaryOperator::And),
            sql_ast::BinaryOperator::Or => Ok(BinaryOperator::Or),
            sql_ast::BinaryOperator::StringConcat => Ok(BinaryOperator::Concat),
            sql_ast::BinaryOperator::BitwiseAnd => Ok(BinaryOperator::BitwiseAnd),
            sql_ast::BinaryOperator::BitwiseOr => Ok(BinaryOperator::BitwiseOr),
            sql_ast::BinaryOperator::BitwiseXor => Ok(BinaryOperator::BitwiseXor),
            _ => Err(ParseError::Unsupported(format!(
                "Binary operator: {:?}",
                op
            ))),
        }
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOperator::Eq => write!(f, "="),
            BinaryOperator::NotEq => write!(f, "<>"),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::LtEq => write!(f, "<="),
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::GtEq => write!(f, ">="),
            BinaryOperator::Plus => write!(f, "+"),
            BinaryOperator::Minus => write!(f, "-"),
            BinaryOperator::Multiply => write!(f, "*"),
            BinaryOperator::Divide => write!(f, "/"),
            BinaryOperator::Modulo => write!(f, "%"),
            BinaryOperator::And => write!(f, "AND"),
            BinaryOperator::Or => write!(f, "OR"),
            BinaryOperator::Concat => write!(f, "||"),
            BinaryOperator::Like => write!(f, "LIKE"),
            BinaryOperator::NotLike => write!(f, "NOT LIKE"),
            BinaryOperator::ILike => write!(f, "ILIKE"),
            BinaryOperator::RegexMatch => write!(f, "~"),
            BinaryOperator::BitwiseAnd => write!(f, "&"),
            BinaryOperator::BitwiseOr => write!(f, "|"),
            BinaryOperator::BitwiseXor => write!(f, "^"),
        }
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOperator {
    /// Logical NOT.
    Not,
    /// Numeric negation.
    Minus,
    /// Numeric positive (no-op).
    Plus,
    /// Bitwise NOT.
    BitwiseNot,
}

impl UnaryOperator {
    /// Converts from sqlparser's UnaryOperator.
    pub fn from_sql_ast(op: &sql_ast::UnaryOperator) -> ParseResult<Self> {
        match op {
            sql_ast::UnaryOperator::Not => Ok(UnaryOperator::Not),
            sql_ast::UnaryOperator::Minus => Ok(UnaryOperator::Minus),
            sql_ast::UnaryOperator::Plus => Ok(UnaryOperator::Plus),
            _ => Err(ParseError::Unsupported(format!("Unary operator: {:?}", op))),
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperator::Not => write!(f, "NOT"),
            UnaryOperator::Minus => write!(f, "-"),
            UnaryOperator::Plus => write!(f, "+"),
            UnaryOperator::BitwiseNot => write!(f, "~"),
        }
    }
}

/// A function call expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    /// Function name.
    pub name: String,
    /// Arguments.
    pub args: Vec<Expr>,
    /// DISTINCT modifier (for aggregates).
    pub distinct: bool,
    /// Filter clause (for aggregates).
    pub filter: Option<Box<Expr>>,
    /// Window specification (for window functions).
    pub over: Option<WindowSpec>,
}

impl FunctionCall {
    /// Creates a new function call.
    pub fn new(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Self {
            name: name.into(),
            args,
            distinct: false,
            filter: None,
            over: None,
        }
    }

    /// Creates an aggregate function with DISTINCT.
    pub fn distinct(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Self {
            name: name.into(),
            args,
            distinct: true,
            filter: None,
            over: None,
        }
    }

    /// Converts from sqlparser's Function.
    pub fn from_sql_ast(func: sql_ast::Function) -> ParseResult<Self> {
        let name = func.name.to_string().to_uppercase();

        let args: ParseResult<Vec<_>> = func
            .args
            .into_iter()
            .filter_map(|arg| match arg {
                sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Expr(e)) => {
                    Some(Expr::from_sql_ast(e))
                }
                sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Wildcard) => {
                    Some(Ok(Expr::Wildcard))
                }
                sql_ast::FunctionArg::Named {
                    arg: sql_ast::FunctionArgExpr::Expr(e),
                    ..
                } => Some(Expr::from_sql_ast(e)),
                _ => None,
            })
            .collect();

        let distinct = func.distinct;

        let over = func.over.map(|w| WindowSpec::from_sql_ast(w)).transpose()?;

        Ok(Self {
            name,
            args: args?,
            distinct,
            filter: None,
            over,
        })
    }
}

impl fmt::Display for FunctionCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        let args: Vec<_> = self.args.iter().map(|a| a.to_string()).collect();
        write!(f, "{})", args.join(", "))
    }
}

/// Window specification for window functions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowSpec {
    /// PARTITION BY expressions.
    pub partition_by: Vec<Expr>,
    /// ORDER BY clauses.
    pub order_by: Vec<OrderByExpr>,
    /// Window frame.
    pub frame: Option<WindowFrame>,
}

impl WindowSpec {
    /// Converts from sqlparser's WindowType.
    pub fn from_sql_ast(window: sql_ast::WindowType) -> ParseResult<Self> {
        match window {
            sql_ast::WindowType::WindowSpec(spec) => {
                let partition_by: ParseResult<Vec<_>> = spec
                    .partition_by
                    .into_iter()
                    .map(Expr::from_sql_ast)
                    .collect();

                let order_by: ParseResult<Vec<_>> = spec
                    .order_by
                    .into_iter()
                    .map(OrderByExpr::from_sql_ast)
                    .collect();

                Ok(Self {
                    partition_by: partition_by?,
                    order_by: order_by?,
                    frame: None, // TODO: Parse window frame
                })
            }
            sql_ast::WindowType::NamedWindow(_) => Err(ParseError::Unsupported(
                "Named window references".to_string(),
            )),
        }
    }
}

/// Window frame specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowFrame {
    /// Frame units (ROWS or RANGE).
    pub units: WindowFrameUnits,
    /// Start bound.
    pub start: WindowFrameBound,
    /// End bound.
    pub end: Option<WindowFrameBound>,
}

/// Window frame units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowFrameUnits {
    /// ROWS frame.
    Rows,
    /// RANGE frame.
    Range,
    /// GROUPS frame.
    Groups,
}

/// Window frame bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowFrameBound {
    /// UNBOUNDED PRECEDING.
    UnboundedPreceding,
    /// N PRECEDING.
    Preceding(u64),
    /// CURRENT ROW.
    CurrentRow,
    /// N FOLLOWING.
    Following(u64),
    /// UNBOUNDED FOLLOWING.
    UnboundedFollowing,
}

/// ORDER BY expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByExpr {
    /// Expression to order by.
    pub expr: Expr,
    /// Order direction.
    pub direction: OrderDirection,
    /// NULL handling.
    pub nulls: Option<NullsOrder>,
}

impl OrderByExpr {
    /// Creates a new order by expression.
    pub fn new(expr: Expr, direction: OrderDirection) -> Self {
        Self {
            expr,
            direction,
            nulls: None,
        }
    }

    /// Creates an ascending order by expression.
    pub fn asc(expr: Expr) -> Self {
        Self::new(expr, OrderDirection::Asc)
    }

    /// Creates a descending order by expression.
    pub fn desc(expr: Expr) -> Self {
        Self::new(expr, OrderDirection::Desc)
    }

    /// Converts from sqlparser's OrderByExpr.
    pub fn from_sql_ast(order: sql_ast::OrderByExpr) -> ParseResult<Self> {
        let direction = if order.asc.unwrap_or(true) {
            OrderDirection::Asc
        } else {
            OrderDirection::Desc
        };

        let nulls = order.nulls_first.map(|first| {
            if first {
                NullsOrder::First
            } else {
                NullsOrder::Last
            }
        });

        Ok(Self {
            expr: Expr::from_sql_ast(order.expr)?,
            direction,
            nulls,
        })
    }
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.expr, self.direction)?;
        if let Some(nulls) = self.nulls {
            write!(f, " {}", nulls)?;
        }
        Ok(())
    }
}

/// Placeholder for SELECT expressions in subqueries.
/// The actual SelectStatement will be defined in statement.rs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectExpr {
    /// Placeholder - actual implementation references SelectStatement.
    _placeholder: (),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr_builders() {
        let col = Expr::column("id");
        assert!(matches!(col, Expr::Column(_)));

        let lit = Expr::int(42);
        assert!(matches!(lit, Expr::Literal(Literal::Integer(42))));

        let eq = Expr::column("x").eq(Expr::int(1));
        assert!(matches!(eq, Expr::BinaryOp { .. }));
    }

    #[test]
    fn test_binary_operator_predicates() {
        assert!(BinaryOperator::Eq.is_comparison());
        assert!(BinaryOperator::Lt.is_comparison());
        assert!(!BinaryOperator::Plus.is_comparison());

        assert!(BinaryOperator::Plus.is_arithmetic());
        assert!(BinaryOperator::Divide.is_arithmetic());
        assert!(!BinaryOperator::And.is_arithmetic());

        assert!(BinaryOperator::And.is_logical());
        assert!(BinaryOperator::Or.is_logical());
        assert!(!BinaryOperator::Eq.is_logical());
    }

    #[test]
    fn test_expr_display() {
        let expr = Expr::column("x").eq(Expr::int(42));
        assert_eq!(expr.to_string(), "(x = 42)");

        let expr = Expr::column("a").and(Expr::column("b"));
        assert_eq!(expr.to_string(), "(a AND b)");
    }

    #[test]
    fn test_function_call() {
        let func = FunctionCall::new("COUNT", vec![Expr::Wildcard]);
        assert_eq!(func.to_string(), "COUNT(*)");

        let func = FunctionCall::distinct("COUNT", vec![Expr::column("id")]);
        assert_eq!(func.to_string(), "COUNT(DISTINCT id)");
    }
}
