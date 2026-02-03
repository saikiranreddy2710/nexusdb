//! Expression evaluator for physical expressions.
//!
//! This module evaluates physical expressions on rows and record batches.

use super::{Column, RecordBatch, Row, Value};
use crate::logical::{AggregateFunc, BinaryOp, Schema, UnaryOp};
use crate::physical::{PhysicalAggregateExpr, PhysicalExpr};

/// Evaluates a physical expression on a single row.
pub fn evaluate_expr(expr: &PhysicalExpr, row: &Row, schema: &Schema) -> Result<Value, EvalError> {
    match expr {
        PhysicalExpr::Column { index, .. } => Ok(row.get(*index).cloned().unwrap_or(Value::Null)),

        PhysicalExpr::Literal(lit) => Ok(Value::from_literal(lit)),

        PhysicalExpr::BinaryExpr { left, op, right } => {
            let left_val = evaluate_expr(left, row, schema)?;
            let right_val = evaluate_expr(right, row, schema)?;
            evaluate_binary_op(op, &left_val, &right_val)
        }

        PhysicalExpr::UnaryExpr { op, expr } => {
            let val = evaluate_expr(expr, row, schema)?;
            evaluate_unary_op(op, &val)
        }

        PhysicalExpr::IsNull(expr) => {
            let val = evaluate_expr(expr, row, schema)?;
            Ok(Value::Boolean(val.is_null()))
        }

        PhysicalExpr::IsNotNull(expr) => {
            let val = evaluate_expr(expr, row, schema)?;
            Ok(Value::Boolean(!val.is_null()))
        }

        PhysicalExpr::Cast { expr, data_type } => {
            let val = evaluate_expr(expr, row, schema)?;
            val.cast(data_type).map_err(EvalError::Cast)
        }

        PhysicalExpr::Case {
            operand,
            when_then,
            else_result,
        } => evaluate_case(
            operand.as_deref(),
            when_then,
            else_result.as_deref(),
            row,
            schema,
        ),

        PhysicalExpr::ScalarFunction { name, args, .. } => {
            let arg_vals: Result<Vec<_>, _> =
                args.iter().map(|a| evaluate_expr(a, row, schema)).collect();
            evaluate_scalar_function(name, &arg_vals?)
        }

        PhysicalExpr::InList {
            expr,
            list,
            negated,
        } => {
            let val = evaluate_expr(expr, row, schema)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            let list_vals: Result<Vec<_>, _> =
                list.iter().map(|e| evaluate_expr(e, row, schema)).collect();
            let list_vals = list_vals?;
            let found = list_vals.iter().any(|v| !v.is_null() && v == &val);
            Ok(Value::Boolean(if *negated { !found } else { found }))
        }

        PhysicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = evaluate_expr(expr, row, schema)?;
            let low_val = evaluate_expr(low, row, schema)?;
            let high_val = evaluate_expr(high, row, schema)?;

            if val.is_null() || low_val.is_null() || high_val.is_null() {
                return Ok(Value::Null);
            }

            let in_range = val >= low_val && val <= high_val;
            Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
        }

        PhysicalExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let val = evaluate_expr(expr, row, schema)?;
            let pattern_val = evaluate_expr(pattern, row, schema)?;

            if val.is_null() || pattern_val.is_null() {
                return Ok(Value::Null);
            }

            let val_str = val.to_string_value().unwrap_or_default();
            let pattern_str = pattern_val.to_string_value().unwrap_or_default();

            let matches = match_like(&val_str, &pattern_str, *case_insensitive);
            Ok(Value::Boolean(if *negated { !matches } else { matches }))
        }
    }
}

/// Evaluates a physical expression on all rows in a batch, returning a column.
pub fn evaluate_expr_batch(expr: &PhysicalExpr, batch: &RecordBatch) -> Result<Column, EvalError> {
    let schema = batch.schema();
    let values: Result<Vec<_>, _> = batch
        .rows()
        .map(|row| evaluate_expr(expr, &row, schema))
        .collect();

    Ok(Column::new(expr.data_type(schema), values?))
}

/// Evaluates a binary operation.
fn evaluate_binary_op(op: &BinaryOp, left: &Value, right: &Value) -> Result<Value, EvalError> {
    // Handle NULL propagation for most operators
    if matches!(
        op,
        BinaryOp::Plus
            | BinaryOp::Minus
            | BinaryOp::Multiply
            | BinaryOp::Divide
            | BinaryOp::Modulo
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::Like
            | BinaryOp::ILike
            | BinaryOp::Concat
    ) {
        if left.is_null() || right.is_null() {
            return Ok(Value::Null);
        }
    }

    match op {
        // Comparison operators
        BinaryOp::Eq => {
            if left.is_null() || right.is_null() {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(left == right))
            }
        }
        BinaryOp::NotEq => {
            if left.is_null() || right.is_null() {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(left != right))
            }
        }
        BinaryOp::Lt => Ok(Value::Boolean(left < right)),
        BinaryOp::LtEq => Ok(Value::Boolean(left <= right)),
        BinaryOp::Gt => Ok(Value::Boolean(left > right)),
        BinaryOp::GtEq => Ok(Value::Boolean(left >= right)),

        // Arithmetic operators
        BinaryOp::Plus => evaluate_arithmetic(left, right, |a, b| a + b, |a, b| a + b),
        BinaryOp::Minus => evaluate_arithmetic(left, right, |a, b| a - b, |a, b| a - b),
        BinaryOp::Multiply => evaluate_arithmetic(left, right, |a, b| a * b, |a, b| a * b),
        BinaryOp::Divide => {
            if let Some(r) = right.to_f64() {
                if r == 0.0 {
                    return Err(EvalError::DivisionByZero);
                }
            }
            evaluate_arithmetic(left, right, |a, b| a / b, |a, b| a / b)
        }
        BinaryOp::Modulo => {
            if let Some(r) = right.to_i64() {
                if r == 0 {
                    return Err(EvalError::DivisionByZero);
                }
            }
            let l = left.to_i64().ok_or(EvalError::TypeMismatch)?;
            let r = right.to_i64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::BigInt(l % r))
        }

        // Logical operators
        BinaryOp::And => {
            // Three-valued logic for AND
            match (left.to_bool(), right.to_bool()) {
                (Some(false), _) | (_, Some(false)) => Ok(Value::Boolean(false)),
                (Some(true), Some(true)) => Ok(Value::Boolean(true)),
                _ => Ok(Value::Null),
            }
        }
        BinaryOp::Or => {
            // Three-valued logic for OR
            match (left.to_bool(), right.to_bool()) {
                (Some(true), _) | (_, Some(true)) => Ok(Value::Boolean(true)),
                (Some(false), Some(false)) => Ok(Value::Boolean(false)),
                _ => Ok(Value::Null),
            }
        }

        // String operators
        BinaryOp::Like | BinaryOp::ILike => {
            let val_str = left.to_string_value().unwrap_or_default();
            let pattern_str = right.to_string_value().unwrap_or_default();
            let case_insensitive = matches!(op, BinaryOp::ILike);
            Ok(Value::Boolean(match_like(
                &val_str,
                &pattern_str,
                case_insensitive,
            )))
        }
        BinaryOp::Concat => {
            let l = left.to_string_value().unwrap_or_default();
            let r = right.to_string_value().unwrap_or_default();
            Ok(Value::String(format!("{}{}", l, r)))
        }

        // Bitwise operators
        BinaryOp::BitwiseAnd => {
            let l = left.to_i64().ok_or(EvalError::TypeMismatch)?;
            let r = right.to_i64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::BigInt(l & r))
        }
        BinaryOp::BitwiseOr => {
            let l = left.to_i64().ok_or(EvalError::TypeMismatch)?;
            let r = right.to_i64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::BigInt(l | r))
        }
        BinaryOp::BitwiseXor => {
            let l = left.to_i64().ok_or(EvalError::TypeMismatch)?;
            let r = right.to_i64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::BigInt(l ^ r))
        }
    }
}

/// Evaluates arithmetic operations.
fn evaluate_arithmetic<F, G>(
    left: &Value,
    right: &Value,
    int_op: F,
    float_op: G,
) -> Result<Value, EvalError>
where
    F: Fn(i64, i64) -> i64,
    G: Fn(f64, f64) -> f64,
{
    // Use floating point if either operand is float
    match (left, right) {
        (Value::Float(_), _)
        | (_, Value::Float(_))
        | (Value::Double(_), _)
        | (_, Value::Double(_)) => {
            let l = left.to_f64().ok_or(EvalError::TypeMismatch)?;
            let r = right.to_f64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::Double(float_op(l, r)))
        }
        _ => {
            let l = left.to_i64().ok_or(EvalError::TypeMismatch)?;
            let r = right.to_i64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::BigInt(int_op(l, r)))
        }
    }
}

/// Evaluates a unary operation.
fn evaluate_unary_op(op: &UnaryOp, val: &Value) -> Result<Value, EvalError> {
    if val.is_null() {
        return Ok(Value::Null);
    }

    match op {
        UnaryOp::Not => {
            let b = val.to_bool().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::Boolean(!b))
        }
        UnaryOp::Minus => match val {
            Value::TinyInt(i) => Ok(Value::TinyInt(-i)),
            Value::SmallInt(i) => Ok(Value::SmallInt(-i)),
            Value::Int(i) => Ok(Value::Int(-i)),
            Value::BigInt(i) => Ok(Value::BigInt(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            Value::Double(f) => Ok(Value::Double(-f)),
            _ => Err(EvalError::TypeMismatch),
        },
        UnaryOp::Plus => Ok(val.clone()),
        UnaryOp::BitwiseNot => {
            let i = val.to_i64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::BigInt(!i))
        }
    }
}

/// Evaluates a CASE expression.
fn evaluate_case(
    operand: Option<&PhysicalExpr>,
    when_then: &[(PhysicalExpr, PhysicalExpr)],
    else_result: Option<&PhysicalExpr>,
    row: &Row,
    schema: &Schema,
) -> Result<Value, EvalError> {
    let operand_val = operand.map(|o| evaluate_expr(o, row, schema)).transpose()?;

    for (when_expr, then_expr) in when_then {
        let when_val = evaluate_expr(when_expr, row, schema)?;

        let matches = if let Some(ref op_val) = operand_val {
            // Simple CASE: compare operand with WHEN values
            !when_val.is_null() && when_val == *op_val
        } else {
            // Searched CASE: evaluate WHEN as boolean
            when_val.to_bool() == Some(true)
        };

        if matches {
            return evaluate_expr(then_expr, row, schema);
        }
    }

    // No WHEN matched, return ELSE or NULL
    if let Some(else_expr) = else_result {
        evaluate_expr(else_expr, row, schema)
    } else {
        Ok(Value::Null)
    }
}

/// Evaluates a scalar function.
fn evaluate_scalar_function(name: &str, args: &[Value]) -> Result<Value, EvalError> {
    match name.to_lowercase().as_str() {
        // String functions
        "upper" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            let s = args[0].to_string_value().unwrap_or_default();
            Ok(Value::String(s.to_uppercase()))
        }
        "lower" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            let s = args[0].to_string_value().unwrap_or_default();
            Ok(Value::String(s.to_lowercase()))
        }
        "length" | "char_length" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            let s = args[0].to_string_value().unwrap_or_default();
            Ok(Value::BigInt(s.len() as i64))
        }
        "concat" => {
            let result: String = args
                .iter()
                .filter(|v| !v.is_null())
                .filter_map(|v| v.to_string_value())
                .collect();
            Ok(Value::String(result))
        }
        "substring" | "substr" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            let s = args[0].to_string_value().unwrap_or_default();
            let start = args.get(1).and_then(|v| v.to_i64()).unwrap_or(1) as usize;
            let len = args.get(2).and_then(|v| v.to_i64());

            let start_idx = start.saturating_sub(1); // SQL is 1-indexed
            if start_idx >= s.len() {
                return Ok(Value::String(String::new()));
            }

            let result = if let Some(l) = len {
                s.chars().skip(start_idx).take(l as usize).collect()
            } else {
                s.chars().skip(start_idx).collect()
            };
            Ok(Value::String(result))
        }
        "trim" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            let s = args[0].to_string_value().unwrap_or_default();
            Ok(Value::String(s.trim().to_string()))
        }

        // Numeric functions
        "abs" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            match &args[0] {
                Value::TinyInt(i) => Ok(Value::TinyInt(i.abs())),
                Value::SmallInt(i) => Ok(Value::SmallInt(i.abs())),
                Value::Int(i) => Ok(Value::Int(i.abs())),
                Value::BigInt(i) => Ok(Value::BigInt(i.abs())),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                Value::Double(f) => Ok(Value::Double(f.abs())),
                _ => Err(EvalError::TypeMismatch),
            }
        }
        "ceil" | "ceiling" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            let f = args[0].to_f64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::BigInt(f.ceil() as i64))
        }
        "floor" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            let f = args[0].to_f64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::BigInt(f.floor() as i64))
        }
        "round" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            let f = args[0].to_f64().ok_or(EvalError::TypeMismatch)?;
            let decimals = args.get(1).and_then(|v| v.to_i64()).unwrap_or(0);
            let multiplier = 10f64.powi(decimals as i32);
            Ok(Value::Double((f * multiplier).round() / multiplier))
        }
        "sqrt" => {
            if args.is_empty() || args[0].is_null() {
                return Ok(Value::Null);
            }
            let f = args[0].to_f64().ok_or(EvalError::TypeMismatch)?;
            if f < 0.0 {
                return Err(EvalError::InvalidArgument(
                    "sqrt of negative number".to_string(),
                ));
            }
            Ok(Value::Double(f.sqrt()))
        }
        "power" | "pow" => {
            if args.len() < 2 || args[0].is_null() || args[1].is_null() {
                return Ok(Value::Null);
            }
            let base = args[0].to_f64().ok_or(EvalError::TypeMismatch)?;
            let exp = args[1].to_f64().ok_or(EvalError::TypeMismatch)?;
            Ok(Value::Double(base.powf(exp)))
        }

        // Null handling functions
        "coalesce" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        "nullif" => {
            if args.len() < 2 {
                return Err(EvalError::InvalidArgument(
                    "nullif requires 2 arguments".to_string(),
                ));
            }
            if args[0] == args[1] {
                Ok(Value::Null)
            } else {
                Ok(args[0].clone())
            }
        }

        _ => Err(EvalError::UnknownFunction(name.to_string())),
    }
}

/// Matches a value against a SQL LIKE pattern.
fn match_like(value: &str, pattern: &str, case_insensitive: bool) -> bool {
    let (value, pattern) = if case_insensitive {
        (value.to_lowercase(), pattern.to_lowercase())
    } else {
        (value.to_string(), pattern.to_string())
    };

    // Convert SQL LIKE pattern to regex
    // % matches any sequence of characters
    // _ matches any single character
    // Escape special regex characters
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);
    regex_pattern.push('^');

    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            '\\' => {
                // Handle escaped characters
                if let Some(&next) = chars.peek() {
                    if next == '%' || next == '_' || next == '\\' {
                        regex_pattern.push(chars.next().unwrap());
                        continue;
                    }
                }
                regex_pattern.push_str("\\\\");
            }
            // Escape regex special characters
            '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' => {
                regex_pattern.push('\\');
                regex_pattern.push(c);
            }
            _ => regex_pattern.push(c),
        }
    }

    regex_pattern.push('$');

    // Simple pattern matching without regex library
    // This is a simplified implementation
    simple_like_match(&value, &pattern)
}

/// Simple LIKE pattern matching without regex.
fn simple_like_match(value: &str, pattern: &str) -> bool {
    let value_chars: Vec<char> = value.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();

    fn matches(v: &[char], p: &[char]) -> bool {
        match (v.is_empty(), p.is_empty()) {
            (true, true) => true,
            (_, true) => false,
            (true, false) => p.iter().all(|&c| c == '%'),
            _ => match p[0] {
                '%' => {
                    // % matches zero or more characters
                    // Try matching rest of pattern at each position
                    (0..=v.len()).any(|i| matches(&v[i..], &p[1..]))
                }
                '_' => {
                    // _ matches exactly one character
                    matches(&v[1..], &p[1..])
                }
                c => {
                    // Regular character must match
                    v[0] == c && matches(&v[1..], &p[1..])
                }
            },
        }
    }

    matches(&value_chars, &pattern_chars)
}

/// Accumulator for aggregate functions.
#[derive(Debug, Clone)]
pub struct Accumulator {
    /// The aggregate function.
    func: AggregateFunc,
    /// Whether DISTINCT.
    distinct: bool,
    /// Accumulated state.
    state: AccumulatorState,
}

/// State for different aggregate functions.
#[derive(Debug, Clone)]
enum AccumulatorState {
    Count(i64),
    Sum(Option<f64>),
    Avg {
        sum: f64,
        count: i64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    First(Option<Value>),
    Last(Option<Value>),
    BoolAnd(Option<bool>),
    BoolOr(Option<bool>),
    StringAgg {
        values: Vec<String>,
        separator: String,
    },
}

impl Accumulator {
    /// Creates a new accumulator for the given aggregate expression.
    pub fn new(agg: &PhysicalAggregateExpr) -> Self {
        let state = match agg.func {
            AggregateFunc::Count | AggregateFunc::CountStar => AccumulatorState::Count(0),
            AggregateFunc::Sum => AccumulatorState::Sum(None),
            AggregateFunc::Avg => AccumulatorState::Avg { sum: 0.0, count: 0 },
            AggregateFunc::Min => AccumulatorState::Min(None),
            AggregateFunc::Max => AccumulatorState::Max(None),
            AggregateFunc::First => AccumulatorState::First(None),
            AggregateFunc::Last => AccumulatorState::Last(None),
            AggregateFunc::BoolAnd => AccumulatorState::BoolAnd(None),
            AggregateFunc::BoolOr => AccumulatorState::BoolOr(None),
            AggregateFunc::StringAgg => AccumulatorState::StringAgg {
                values: Vec::new(),
                separator: ",".to_string(),
            },
            AggregateFunc::ArrayAgg => AccumulatorState::StringAgg {
                values: Vec::new(),
                separator: ",".to_string(),
            },
        };

        Self {
            func: agg.func,
            distinct: agg.distinct,
            state,
        }
    }

    /// Accumulates a value.
    pub fn accumulate(&mut self, value: &Value) {
        if value.is_null() && !matches!(self.func, AggregateFunc::CountStar) {
            return; // Skip NULL values for most aggregates
        }

        match &mut self.state {
            AccumulatorState::Count(count) => {
                *count += 1;
            }
            AccumulatorState::Sum(sum) => {
                if let Some(v) = value.to_f64() {
                    *sum = Some(sum.unwrap_or(0.0) + v);
                }
            }
            AccumulatorState::Avg { sum, count } => {
                if let Some(v) = value.to_f64() {
                    *sum += v;
                    *count += 1;
                }
            }
            AccumulatorState::Min(min) => {
                if min.is_none() || value < min.as_ref().unwrap() {
                    *min = Some(value.clone());
                }
            }
            AccumulatorState::Max(max) => {
                if max.is_none() || value > max.as_ref().unwrap() {
                    *max = Some(value.clone());
                }
            }
            AccumulatorState::First(first) => {
                if first.is_none() {
                    *first = Some(value.clone());
                }
            }
            AccumulatorState::Last(last) => {
                *last = Some(value.clone());
            }
            AccumulatorState::BoolAnd(result) => {
                if let Some(b) = value.to_bool() {
                    *result = Some(result.unwrap_or(true) && b);
                }
            }
            AccumulatorState::BoolOr(result) => {
                if let Some(b) = value.to_bool() {
                    *result = Some(result.unwrap_or(false) || b);
                }
            }
            AccumulatorState::StringAgg { values, .. } => {
                if let Some(s) = value.to_string_value() {
                    values.push(s);
                }
            }
        }
    }

    /// Returns the final result.
    pub fn result(&self) -> Value {
        match &self.state {
            AccumulatorState::Count(count) => Value::BigInt(*count),
            AccumulatorState::Sum(sum) => sum.map(Value::Double).unwrap_or(Value::Null),
            AccumulatorState::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Double(*sum / *count as f64)
                }
            }
            AccumulatorState::Min(min) => min.clone().unwrap_or(Value::Null),
            AccumulatorState::Max(max) => max.clone().unwrap_or(Value::Null),
            AccumulatorState::First(first) => first.clone().unwrap_or(Value::Null),
            AccumulatorState::Last(last) => last.clone().unwrap_or(Value::Null),
            AccumulatorState::BoolAnd(result) => result.map(Value::Boolean).unwrap_or(Value::Null),
            AccumulatorState::BoolOr(result) => result.map(Value::Boolean).unwrap_or(Value::Null),
            AccumulatorState::StringAgg { values, separator } => {
                if values.is_empty() {
                    Value::Null
                } else {
                    Value::String(values.join(separator))
                }
            }
        }
    }

    /// Resets the accumulator.
    pub fn reset(&mut self) {
        match &mut self.state {
            AccumulatorState::Count(count) => *count = 0,
            AccumulatorState::Sum(sum) => *sum = None,
            AccumulatorState::Avg { sum, count } => {
                *sum = 0.0;
                *count = 0;
            }
            AccumulatorState::Min(min) => *min = None,
            AccumulatorState::Max(max) => *max = None,
            AccumulatorState::First(first) => *first = None,
            AccumulatorState::Last(last) => *last = None,
            AccumulatorState::BoolAnd(result) => *result = None,
            AccumulatorState::BoolOr(result) => *result = None,
            AccumulatorState::StringAgg { values, .. } => values.clear(),
        }
    }
}

/// Error type for expression evaluation.
#[derive(Debug, Clone)]
pub enum EvalError {
    /// Column not found.
    ColumnNotFound(usize),
    /// Type mismatch in operation.
    TypeMismatch,
    /// Division by zero.
    DivisionByZero,
    /// Cast error.
    Cast(String),
    /// Unknown function.
    UnknownFunction(String),
    /// Invalid argument.
    InvalidArgument(String),
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::ColumnNotFound(idx) => write!(f, "Column not found: {}", idx),
            EvalError::TypeMismatch => write!(f, "Type mismatch"),
            EvalError::DivisionByZero => write!(f, "Division by zero"),
            EvalError::Cast(msg) => write!(f, "Cast error: {}", msg),
            EvalError::UnknownFunction(name) => write!(f, "Unknown function: {}", name),
            EvalError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
        }
    }
}

impl std::error::Error for EvalError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::Field;
    use crate::parser::{DataType, Literal};

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Text),
            Field::nullable("age", DataType::Int),
        ])
    }

    fn test_row() -> Row {
        Row::new(vec![Value::int(1), Value::string("Alice"), Value::int(30)])
    }

    #[test]
    fn test_evaluate_column() {
        let schema = test_schema();
        let row = test_row();

        let expr = PhysicalExpr::column("id", 0);
        let result = evaluate_expr(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_evaluate_literal() {
        let schema = test_schema();
        let row = test_row();

        let expr = PhysicalExpr::Literal(Literal::Integer(42));
        let result = evaluate_expr(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::BigInt(42));
    }

    #[test]
    fn test_evaluate_binary_comparison() {
        let schema = test_schema();
        let row = test_row();

        let expr = PhysicalExpr::binary(
            PhysicalExpr::column("id", 0),
            BinaryOp::Eq,
            PhysicalExpr::Literal(Literal::Integer(1)),
        );
        let result = evaluate_expr(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_evaluate_arithmetic() {
        let schema = test_schema();
        let row = test_row();

        let expr = PhysicalExpr::binary(
            PhysicalExpr::column("age", 2),
            BinaryOp::Plus,
            PhysicalExpr::Literal(Literal::Integer(5)),
        );
        let result = evaluate_expr(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::BigInt(35));
    }

    #[test]
    fn test_evaluate_is_null() {
        let schema = test_schema();
        let row = Row::new(vec![Value::int(1), Value::Null, Value::int(30)]);

        let expr = PhysicalExpr::IsNull(Box::new(PhysicalExpr::column("name", 1)));
        let result = evaluate_expr(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_evaluate_like() {
        let schema = test_schema();
        let row = test_row();

        let expr = PhysicalExpr::Like {
            expr: Box::new(PhysicalExpr::column("name", 1)),
            pattern: Box::new(PhysicalExpr::Literal(Literal::String("A%".to_string()))),
            negated: false,
            case_insensitive: false,
        };
        let result = evaluate_expr(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_evaluate_case() {
        let schema = test_schema();
        let row = test_row();

        // CASE WHEN age > 25 THEN 'adult' ELSE 'young' END
        let expr = PhysicalExpr::Case {
            operand: None,
            when_then: vec![(
                PhysicalExpr::binary(
                    PhysicalExpr::column("age", 2),
                    BinaryOp::Gt,
                    PhysicalExpr::Literal(Literal::Integer(25)),
                ),
                PhysicalExpr::Literal(Literal::String("adult".to_string())),
            )],
            else_result: Some(Box::new(PhysicalExpr::Literal(Literal::String(
                "young".to_string(),
            )))),
        };
        let result = evaluate_expr(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::String("adult".to_string()));
    }

    #[test]
    fn test_like_pattern_matching() {
        assert!(simple_like_match("hello", "hello"));
        assert!(simple_like_match("hello", "h%"));
        assert!(simple_like_match("hello", "%o"));
        assert!(simple_like_match("hello", "h%o"));
        assert!(simple_like_match("hello", "%ell%"));
        assert!(simple_like_match("hello", "_ello"));
        assert!(simple_like_match("hello", "h_llo"));
        assert!(!simple_like_match("hello", "world"));
        assert!(!simple_like_match("hello", "h_o"));
    }

    #[test]
    fn test_scalar_functions() {
        assert_eq!(
            evaluate_scalar_function("upper", &[Value::string("hello")]).unwrap(),
            Value::String("HELLO".to_string())
        );
        assert_eq!(
            evaluate_scalar_function("lower", &[Value::string("HELLO")]).unwrap(),
            Value::String("hello".to_string())
        );
        assert_eq!(
            evaluate_scalar_function("length", &[Value::string("hello")]).unwrap(),
            Value::BigInt(5)
        );
        assert_eq!(
            evaluate_scalar_function("abs", &[Value::int(-5)]).unwrap(),
            Value::Int(5)
        );
        assert_eq!(
            evaluate_scalar_function("coalesce", &[Value::Null, Value::int(5)]).unwrap(),
            Value::Int(5)
        );
    }

    #[test]
    fn test_accumulator_count() {
        let agg = PhysicalAggregateExpr::new(AggregateFunc::Count, vec![], false, "count");
        let mut acc = Accumulator::new(&agg);

        acc.accumulate(&Value::int(1));
        acc.accumulate(&Value::int(2));
        acc.accumulate(&Value::Null);
        acc.accumulate(&Value::int(3));

        assert_eq!(acc.result(), Value::BigInt(3)); // NULL is skipped
    }

    #[test]
    fn test_accumulator_sum() {
        let agg = PhysicalAggregateExpr::new(AggregateFunc::Sum, vec![], false, "sum");
        let mut acc = Accumulator::new(&agg);

        acc.accumulate(&Value::int(10));
        acc.accumulate(&Value::int(20));
        acc.accumulate(&Value::int(30));

        assert_eq!(acc.result(), Value::Double(60.0));
    }

    #[test]
    fn test_accumulator_avg() {
        let agg = PhysicalAggregateExpr::new(AggregateFunc::Avg, vec![], false, "avg");
        let mut acc = Accumulator::new(&agg);

        acc.accumulate(&Value::int(10));
        acc.accumulate(&Value::int(20));
        acc.accumulate(&Value::int(30));

        assert_eq!(acc.result(), Value::Double(20.0));
    }

    #[test]
    fn test_accumulator_min_max() {
        let agg = PhysicalAggregateExpr::new(AggregateFunc::Min, vec![], false, "min");
        let mut acc = Accumulator::new(&agg);

        acc.accumulate(&Value::int(30));
        acc.accumulate(&Value::int(10));
        acc.accumulate(&Value::int(20));

        assert_eq!(acc.result(), Value::Int(10));

        let agg = PhysicalAggregateExpr::new(AggregateFunc::Max, vec![], false, "max");
        let mut acc = Accumulator::new(&agg);

        acc.accumulate(&Value::int(30));
        acc.accumulate(&Value::int(10));
        acc.accumulate(&Value::int(20));

        assert_eq!(acc.result(), Value::Int(30));
    }
}
