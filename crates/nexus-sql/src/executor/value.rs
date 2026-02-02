//! Runtime values for query execution.
//!
//! This module defines the `Value` type which represents runtime SQL values
//! during query execution.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::parser::{DataType, Literal};

/// A runtime value during query execution.
#[derive(Debug, Clone)]
pub enum Value {
    /// NULL value.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// 8-bit signed integer.
    TinyInt(i8),
    /// 16-bit signed integer.
    SmallInt(i16),
    /// 32-bit signed integer.
    Int(i32),
    /// 64-bit signed integer.
    BigInt(i64),
    /// 32-bit floating point.
    Float(f32),
    /// 64-bit floating point.
    Double(f64),
    /// Decimal value (stored as scaled integer).
    Decimal { value: i128, scale: u8 },
    /// String value.
    String(String),
    /// Binary data.
    Bytes(Vec<u8>),
    /// Date (days since epoch).
    Date(i32),
    /// Time (microseconds since midnight).
    Time(i64),
    /// Timestamp (microseconds since epoch).
    Timestamp(i64),
}

impl Value {
    /// Creates a NULL value.
    pub fn null() -> Self {
        Value::Null
    }

    /// Creates a boolean value.
    pub fn boolean(v: bool) -> Self {
        Value::Boolean(v)
    }

    /// Creates an integer value.
    pub fn int(v: i32) -> Self {
        Value::Int(v)
    }

    /// Creates a bigint value.
    pub fn bigint(v: i64) -> Self {
        Value::BigInt(v)
    }

    /// Creates a double value.
    pub fn double(v: f64) -> Self {
        Value::Double(v)
    }

    /// Creates a string value.
    pub fn string(v: impl Into<String>) -> Self {
        Value::String(v.into())
    }

    /// Returns true if this value is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns true if this value is truthy.
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Boolean(b) => *b,
            Value::TinyInt(i) => *i != 0,
            Value::SmallInt(i) => *i != 0,
            Value::Int(i) => *i != 0,
            Value::BigInt(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::Double(f) => *f != 0.0,
            Value::Decimal { value, .. } => *value != 0,
            Value::String(s) => !s.is_empty(),
            Value::Bytes(b) => !b.is_empty(),
            Value::Date(_) | Value::Time(_) | Value::Timestamp(_) => true,
        }
    }

    /// Converts this value to a boolean.
    pub fn to_bool(&self) -> Option<bool> {
        match self {
            Value::Null => None,
            Value::Boolean(b) => Some(*b),
            _ => Some(self.is_truthy()),
        }
    }

    /// Converts this value to an i64.
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            Value::Null => None,
            Value::Boolean(b) => Some(if *b { 1 } else { 0 }),
            Value::TinyInt(i) => Some(*i as i64),
            Value::SmallInt(i) => Some(*i as i64),
            Value::Int(i) => Some(*i as i64),
            Value::BigInt(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            Value::Double(f) => Some(*f as i64),
            Value::Decimal { value, scale } => {
                let divisor = 10i128.pow(*scale as u32);
                Some((*value / divisor) as i64)
            }
            Value::String(s) => s.parse().ok(),
            Value::Date(d) => Some(*d as i64),
            Value::Time(t) => Some(*t),
            Value::Timestamp(t) => Some(*t),
            Value::Bytes(_) => None,
        }
    }

    /// Converts this value to an f64.
    pub fn to_f64(&self) -> Option<f64> {
        match self {
            Value::Null => None,
            Value::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            Value::TinyInt(i) => Some(*i as f64),
            Value::SmallInt(i) => Some(*i as f64),
            Value::Int(i) => Some(*i as f64),
            Value::BigInt(i) => Some(*i as f64),
            Value::Float(f) => Some(*f as f64),
            Value::Double(f) => Some(*f),
            Value::Decimal { value, scale } => {
                let divisor = 10f64.powi(*scale as i32);
                Some(*value as f64 / divisor)
            }
            Value::String(s) => s.parse().ok(),
            Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::Bytes(_) => None,
        }
    }

    /// Converts this value to a string.
    pub fn to_string_value(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Boolean(b) => Some(if *b { "true" } else { "false" }.to_string()),
            Value::TinyInt(i) => Some(i.to_string()),
            Value::SmallInt(i) => Some(i.to_string()),
            Value::Int(i) => Some(i.to_string()),
            Value::BigInt(i) => Some(i.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Double(f) => Some(f.to_string()),
            Value::Decimal { value, scale } => {
                if *scale == 0 {
                    Some(value.to_string())
                } else {
                    let divisor = 10i128.pow(*scale as u32);
                    let int_part = *value / divisor;
                    let frac_part = (*value % divisor).abs();
                    Some(format!(
                        "{}.{:0>width$}",
                        int_part,
                        frac_part,
                        width = *scale as usize
                    ))
                }
            }
            Value::String(s) => Some(s.clone()),
            Value::Bytes(b) => Some(format!("0x{}", hex::encode(b))),
            Value::Date(d) => Some(format!("date:{}", d)),
            Value::Time(t) => Some(format!("time:{}", t)),
            Value::Timestamp(t) => Some(format!("ts:{}", t)),
        }
    }

    /// Returns the data type of this value.
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Text, // NULL can be any type
            Value::Boolean(_) => DataType::Boolean,
            Value::TinyInt(_) => DataType::TinyInt,
            Value::SmallInt(_) => DataType::SmallInt,
            Value::Int(_) => DataType::Int,
            Value::BigInt(_) => DataType::BigInt,
            Value::Float(_) => DataType::Float,
            Value::Double(_) => DataType::Double,
            Value::Decimal { scale, .. } => DataType::Decimal {
                precision: Some(38),
                scale: Some(*scale),
            },
            Value::String(_) => DataType::Text,
            Value::Bytes(_) => DataType::Blob,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::Timestamp(_) => DataType::Timestamp,
        }
    }

    /// Casts this value to the specified type.
    pub fn cast(&self, target: &DataType) -> Result<Value, String> {
        if self.is_null() {
            return Ok(Value::Null);
        }

        match target {
            DataType::Boolean => self
                .to_bool()
                .map(Value::Boolean)
                .ok_or_else(|| "Cannot cast to boolean".to_string()),
            DataType::TinyInt => self
                .to_i64()
                .map(|v| Value::TinyInt(v as i8))
                .ok_or_else(|| "Cannot cast to tinyint".to_string()),
            DataType::SmallInt => self
                .to_i64()
                .map(|v| Value::SmallInt(v as i16))
                .ok_or_else(|| "Cannot cast to smallint".to_string()),
            DataType::Int => self
                .to_i64()
                .map(|v| Value::Int(v as i32))
                .ok_or_else(|| "Cannot cast to int".to_string()),
            DataType::BigInt => self
                .to_i64()
                .map(Value::BigInt)
                .ok_or_else(|| "Cannot cast to bigint".to_string()),
            DataType::Float => self
                .to_f64()
                .map(|v| Value::Float(v as f32))
                .ok_or_else(|| "Cannot cast to float".to_string()),
            DataType::Double => self
                .to_f64()
                .map(Value::Double)
                .ok_or_else(|| "Cannot cast to double".to_string()),
            DataType::Text | DataType::Varchar(_) | DataType::Char(_) => self
                .to_string_value()
                .map(Value::String)
                .ok_or_else(|| "Cannot cast to string".to_string()),
            _ => Err(format!("Unsupported cast to {:?}", target)),
        }
    }

    /// Creates a Value from a Literal.
    pub fn from_literal(lit: &Literal) -> Self {
        match lit {
            Literal::Null => Value::Null,
            Literal::Boolean(b) => Value::Boolean(*b),
            Literal::Integer(i) => Value::BigInt(*i),
            Literal::Float(f) => Value::Double(*f),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Blob(b) => Value::Bytes(b.clone()),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::TinyInt(a), Value::TinyInt(b)) => a == b,
            (Value::SmallInt(a), Value::SmallInt(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::BigInt(a), Value::BigInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Double(a), Value::Double(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Time(a), Value::Time(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            // Cross-type numeric comparisons
            (a, b) => {
                if let (Some(a_f), Some(b_f)) = (a.to_f64(), b.to_f64()) {
                    (a_f - b_f).abs() < f64::EPSILON
                } else {
                    false
                }
            }
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // NULL comparisons: NULL is considered less than any non-NULL value
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            // Same-type comparisons
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::TinyInt(a), Value::TinyInt(b)) => a.cmp(b),
            (Value::SmallInt(a), Value::SmallInt(b)) => a.cmp(b),
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Double(a), Value::Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),

            // Cross-type numeric comparisons via f64
            (a, b) => {
                if let (Some(a_f), Some(b_f)) = (a.to_f64(), b.to_f64()) {
                    a_f.partial_cmp(&b_f).unwrap_or(Ordering::Equal)
                } else {
                    // Fall back to string comparison for incompatible types
                    let a_s = a.to_string_value().unwrap_or_default();
                    let b_s = b.to_string_value().unwrap_or_default();
                    a_s.cmp(&b_s)
                }
            }
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Boolean(b) => b.hash(state),
            Value::TinyInt(i) => i.hash(state),
            Value::SmallInt(i) => i.hash(state),
            Value::Int(i) => i.hash(state),
            Value::BigInt(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::Double(f) => f.to_bits().hash(state),
            Value::Decimal { value, scale } => {
                value.hash(state);
                scale.hash(state);
            }
            Value::String(s) => s.hash(state),
            Value::Bytes(b) => b.hash(state),
            Value::Date(d) => d.hash(state),
            Value::Time(t) => t.hash(state),
            Value::Timestamp(t) => t.hash(state),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            Value::TinyInt(i) => write!(f, "{}", i),
            Value::SmallInt(i) => write!(f, "{}", i),
            Value::Int(i) => write!(f, "{}", i),
            Value::BigInt(i) => write!(f, "{}", i),
            Value::Float(v) => write!(f, "{}", v),
            Value::Double(v) => write!(f, "{}", v),
            Value::Decimal { value, scale } => {
                if *scale == 0 {
                    write!(f, "{}", value)
                } else {
                    let divisor = 10i128.pow(*scale as u32);
                    let int_part = *value / divisor;
                    let frac_part = (*value % divisor).abs();
                    write!(
                        f,
                        "{}.{:0>width$}",
                        int_part,
                        frac_part,
                        width = *scale as usize
                    )
                }
            }
            Value::String(s) => write!(f, "{}", s),
            Value::Bytes(b) => write!(f, "0x{}", hex::encode(b)),
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
            Value::Timestamp(t) => write!(f, "{}", t),
        }
    }
}

/// Helper module for hex encoding (simple implementation).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_null() {
        let v = Value::null();
        assert!(v.is_null());
        assert!(!v.is_truthy());
    }

    #[test]
    fn test_value_boolean() {
        let v = Value::boolean(true);
        assert!(v.is_truthy());
        assert_eq!(v.to_bool(), Some(true));

        let v = Value::boolean(false);
        assert!(!v.is_truthy());
        assert_eq!(v.to_bool(), Some(false));
    }

    #[test]
    fn test_value_int() {
        let v = Value::int(42);
        assert_eq!(v.to_i64(), Some(42));
        assert_eq!(v.to_f64(), Some(42.0));
    }

    #[test]
    fn test_value_string() {
        let v = Value::string("hello");
        assert_eq!(v.to_string_value(), Some("hello".to_string()));
    }

    #[test]
    fn test_value_comparison() {
        assert!(Value::int(10) < Value::int(20));
        assert!(Value::int(10) == Value::int(10));
        assert!(Value::Null < Value::int(0));
    }

    #[test]
    fn test_value_cross_type_comparison() {
        // Integer and double comparison
        assert!(Value::int(10) == Value::double(10.0));
        assert!(Value::int(10) < Value::double(10.5));
    }

    #[test]
    fn test_value_from_literal() {
        let lit = Literal::Integer(42);
        let v = Value::from_literal(&lit);
        assert_eq!(v.to_i64(), Some(42));

        let lit = Literal::String("hello".to_string());
        let v = Value::from_literal(&lit);
        assert_eq!(v.to_string_value(), Some("hello".to_string()));
    }

    #[test]
    fn test_value_cast() {
        let v = Value::int(42);
        let casted = v.cast(&DataType::BigInt).unwrap();
        assert_eq!(casted.to_i64(), Some(42));

        let v = Value::int(42);
        let casted = v.cast(&DataType::Text).unwrap();
        assert_eq!(casted.to_string_value(), Some("42".to_string()));
    }

    #[test]
    fn test_value_hash() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert(Value::int(1), "one");
        map.insert(Value::int(2), "two");

        assert_eq!(map.get(&Value::int(1)), Some(&"one"));
        assert_eq!(map.get(&Value::int(2)), Some(&"two"));
    }
}
