//! SQL data types.
//!
//! This module defines the data types used in NexusDB SQL.

use std::fmt;

use serde::{Deserialize, Serialize};
use sqlparser::ast as sql_ast;

use super::{ParseError, ParseResult};

/// SQL data types supported by NexusDB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    /// Boolean type.
    Boolean,
    /// 8-bit signed integer.
    TinyInt,
    /// 16-bit signed integer.
    SmallInt,
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    BigInt,
    /// 32-bit floating point.
    Float,
    /// 64-bit floating point.
    Double,
    /// Arbitrary precision decimal.
    Decimal {
        /// Total number of digits.
        precision: Option<u8>,
        /// Digits after decimal point.
        scale: Option<u8>,
    },
    /// Fixed-length string.
    Char(Option<u32>),
    /// Variable-length string.
    Varchar(Option<u32>),
    /// Unlimited text.
    Text,
    /// Binary data.
    Blob,
    /// Date (year, month, day).
    Date,
    /// Time (hour, minute, second).
    Time,
    /// Timestamp (date + time).
    Timestamp,
    /// Timestamp with timezone.
    TimestampTz,
    /// Interval type.
    Interval,
    /// UUID type.
    Uuid,
    /// JSON type.
    Json,
    /// JSONB (binary JSON).
    Jsonb,
    /// Array of another type.
    Array(Box<DataType>),
    /// Nullable wrapper.
    Nullable(Box<DataType>),
}

impl DataType {
    /// Returns true if this type is numeric.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt
                | DataType::SmallInt
                | DataType::Int
                | DataType::BigInt
                | DataType::Float
                | DataType::Double
                | DataType::Decimal { .. }
        )
    }

    /// Returns true if this type is a string type.
    pub fn is_string(&self) -> bool {
        matches!(
            self,
            DataType::Char(_) | DataType::Varchar(_) | DataType::Text
        )
    }

    /// Returns true if this type is a temporal type.
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            DataType::Date
                | DataType::Time
                | DataType::Timestamp
                | DataType::TimestampTz
                | DataType::Interval
        )
    }

    /// Converts from sqlparser's DataType.
    pub fn from_sql_ast(dt: &sql_ast::DataType) -> ParseResult<Self> {
        match dt {
            sql_ast::DataType::Boolean => Ok(DataType::Boolean),
            sql_ast::DataType::TinyInt(_) => Ok(DataType::TinyInt),
            sql_ast::DataType::SmallInt(_) => Ok(DataType::SmallInt),
            sql_ast::DataType::Int(_) | sql_ast::DataType::Integer(_) => Ok(DataType::Int),
            sql_ast::DataType::BigInt(_) => Ok(DataType::BigInt),
            sql_ast::DataType::Float(_) | sql_ast::DataType::Real => Ok(DataType::Float),
            sql_ast::DataType::Double | sql_ast::DataType::DoublePrecision => Ok(DataType::Double),
            sql_ast::DataType::Decimal(info) | sql_ast::DataType::Numeric(info) => {
                let (precision, scale) = match info {
                    sql_ast::ExactNumberInfo::None => (None, None),
                    sql_ast::ExactNumberInfo::Precision(p) => (Some(*p as u8), None),
                    sql_ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                        (Some(*p as u8), Some(*s as u8))
                    }
                };
                Ok(DataType::Decimal { precision, scale })
            }
            sql_ast::DataType::Char(len) => {
                let len = len.as_ref().and_then(|l| extract_char_length(l));
                Ok(DataType::Char(len))
            }
            sql_ast::DataType::Varchar(len) | sql_ast::DataType::CharacterVarying(len) => {
                let len = len.as_ref().and_then(|l| extract_char_length(l));
                Ok(DataType::Varchar(len))
            }
            sql_ast::DataType::Text => Ok(DataType::Text),
            sql_ast::DataType::Blob(_) | sql_ast::DataType::Bytea => Ok(DataType::Blob),
            sql_ast::DataType::Date => Ok(DataType::Date),
            sql_ast::DataType::Time(_, _) => Ok(DataType::Time),
            sql_ast::DataType::Timestamp(_, tz) => {
                if *tz == sql_ast::TimezoneInfo::WithTimeZone {
                    Ok(DataType::TimestampTz)
                } else {
                    Ok(DataType::Timestamp)
                }
            }
            sql_ast::DataType::Interval => Ok(DataType::Interval),
            sql_ast::DataType::Uuid => Ok(DataType::Uuid),
            sql_ast::DataType::JSON => Ok(DataType::Json),
            sql_ast::DataType::JSONB => Ok(DataType::Jsonb),
            sql_ast::DataType::Array(inner) => {
                let inner = match inner {
                    sql_ast::ArrayElemTypeDef::AngleBracket(dt) => DataType::from_sql_ast(dt)?,
                    sql_ast::ArrayElemTypeDef::SquareBracket(dt) => DataType::from_sql_ast(dt)?,
                    sql_ast::ArrayElemTypeDef::None => {
                        return Err(ParseError::Unsupported(
                            "Array without element type".to_string(),
                        ))
                    }
                };
                Ok(DataType::Array(Box::new(inner)))
            }
            _ => Err(ParseError::Unsupported(format!("Data type: {:?}", dt))),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::TinyInt => write!(f, "TINYINT"),
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::Int => write!(f, "INT"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => write!(f, "DECIMAL({}, {})", p, s),
                (Some(p), None) => write!(f, "DECIMAL({})", p),
                _ => write!(f, "DECIMAL"),
            },
            DataType::Char(len) => match len {
                Some(l) => write!(f, "CHAR({})", l),
                None => write!(f, "CHAR"),
            },
            DataType::Varchar(len) => match len {
                Some(l) => write!(f, "VARCHAR({})", l),
                None => write!(f, "VARCHAR"),
            },
            DataType::Text => write!(f, "TEXT"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::Date => write!(f, "DATE"),
            DataType::Time => write!(f, "TIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::TimestampTz => write!(f, "TIMESTAMPTZ"),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Json => write!(f, "JSON"),
            DataType::Jsonb => write!(f, "JSONB"),
            DataType::Array(inner) => write!(f, "{}[]", inner),
            DataType::Nullable(inner) => write!(f, "{}", inner),
        }
    }
}

/// A literal value in SQL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    /// NULL value.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// Integer value.
    Integer(i64),
    /// Floating point value.
    Float(f64),
    /// String value.
    String(String),
    /// Binary data.
    Blob(Vec<u8>),
}

impl Literal {
    /// Returns true if this is a NULL value.
    pub fn is_null(&self) -> bool {
        matches!(self, Literal::Null)
    }

    /// Converts from sqlparser's Value.
    pub fn from_sql_ast(value: &sql_ast::Value) -> ParseResult<Self> {
        match value {
            sql_ast::Value::Null => Ok(Literal::Null),
            sql_ast::Value::Boolean(b) => Ok(Literal::Boolean(*b)),
            sql_ast::Value::Number(n, _) => {
                // Try to parse as integer first, then as float
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Literal::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Literal::Float(f))
                } else {
                    Err(ParseError::InvalidLiteral(format!("Invalid number: {}", n)))
                }
            }
            sql_ast::Value::SingleQuotedString(s) | sql_ast::Value::DoubleQuotedString(s) => {
                Ok(Literal::String(s.clone()))
            }
            sql_ast::Value::HexStringLiteral(h) => {
                let bytes = hex_to_bytes(h)?;
                Ok(Literal::Blob(bytes))
            }
            _ => Err(ParseError::Unsupported(format!(
                "Literal value: {:?}",
                value
            ))),
        }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Boolean(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::Float(v) => write!(f, "{}", v),
            Literal::String(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Literal::Blob(b) => write!(f, "X'{}'", bytes_to_hex(b)),
        }
    }
}

/// Converts a hex string to bytes.
fn hex_to_bytes(hex: &str) -> ParseResult<Vec<u8>> {
    let hex = hex.trim_start_matches("0x").trim_start_matches("0X");
    if hex.len() % 2 != 0 {
        return Err(ParseError::InvalidLiteral(
            "Odd hex string length".to_string(),
        ));
    }

    hex.as_bytes()
        .chunks(2)
        .map(|chunk| {
            let s = std::str::from_utf8(chunk)
                .map_err(|_| ParseError::InvalidLiteral("Invalid hex character".to_string()))?;
            u8::from_str_radix(s, 16)
                .map_err(|_| ParseError::InvalidLiteral(format!("Invalid hex: {}", s)))
        })
        .collect()
}

/// Converts bytes to a hex string.
fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02X}", b)).collect()
}

/// Extracts length from CharacterLength enum.
fn extract_char_length(cl: &sql_ast::CharacterLength) -> Option<u32> {
    match cl {
        sql_ast::CharacterLength::IntegerLength { length, .. } => Some(*length as u32),
        sql_ast::CharacterLength::Max => None,
    }
}

/// Order direction for ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderDirection {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

impl Default for OrderDirection {
    fn default() -> Self {
        Self::Asc
    }
}

impl fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// NULL handling for ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NullsOrder {
    /// NULLs come first.
    First,
    /// NULLs come last.
    Last,
}

impl fmt::Display for NullsOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NullsOrder::First => write!(f, "NULLS FIRST"),
            NullsOrder::Last => write!(f, "NULLS LAST"),
        }
    }
}

/// Join type for table joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    /// Inner join - only matching rows.
    Inner,
    /// Left outer join - all left rows, matching right rows.
    Left,
    /// Right outer join - all right rows, matching left rows.
    Right,
    /// Full outer join - all rows from both tables.
    Full,
    /// Cross join - cartesian product.
    Cross,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER JOIN"),
            JoinType::Left => write!(f, "LEFT JOIN"),
            JoinType::Right => write!(f, "RIGHT JOIN"),
            JoinType::Full => write!(f, "FULL JOIN"),
            JoinType::Cross => write!(f, "CROSS JOIN"),
        }
    }
}

impl JoinType {
    /// Converts from sqlparser's JoinOperator.
    pub fn from_sql_ast(op: &sql_ast::JoinOperator) -> ParseResult<Self> {
        match op {
            sql_ast::JoinOperator::Inner(_) => Ok(JoinType::Inner),
            sql_ast::JoinOperator::LeftOuter(_) => Ok(JoinType::Left),
            sql_ast::JoinOperator::RightOuter(_) => Ok(JoinType::Right),
            sql_ast::JoinOperator::FullOuter(_) => Ok(JoinType::Full),
            sql_ast::JoinOperator::CrossJoin => Ok(JoinType::Cross),
            _ => Err(ParseError::Unsupported(format!("Join type: {:?}", op))),
        }
    }
}

/// Set operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOpType {
    /// UNION - combine results, remove duplicates.
    Union,
    /// UNION ALL - combine results, keep duplicates.
    UnionAll,
    /// INTERSECT - only rows in both.
    Intersect,
    /// EXCEPT - rows in first but not second.
    Except,
}

impl fmt::Display for SetOpType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetOpType::Union => write!(f, "UNION"),
            SetOpType::UnionAll => write!(f, "UNION ALL"),
            SetOpType::Intersect => write!(f, "INTERSECT"),
            SetOpType::Except => write!(f, "EXCEPT"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_display() {
        assert_eq!(DataType::Int.to_string(), "INT");
        assert_eq!(DataType::Varchar(Some(255)).to_string(), "VARCHAR(255)");
        assert_eq!(
            DataType::Decimal {
                precision: Some(10),
                scale: Some(2)
            }
            .to_string(),
            "DECIMAL(10, 2)"
        );
    }

    #[test]
    fn test_data_type_predicates() {
        assert!(DataType::Int.is_numeric());
        assert!(DataType::Float.is_numeric());
        assert!(!DataType::Varchar(None).is_numeric());

        assert!(DataType::Text.is_string());
        assert!(DataType::Varchar(Some(100)).is_string());
        assert!(!DataType::Int.is_string());

        assert!(DataType::Timestamp.is_temporal());
        assert!(!DataType::Int.is_temporal());
    }

    #[test]
    fn test_literal_display() {
        assert_eq!(Literal::Null.to_string(), "NULL");
        assert_eq!(Literal::Integer(42).to_string(), "42");
        assert_eq!(Literal::String("hello".to_string()).to_string(), "'hello'");
        assert_eq!(Literal::Boolean(true).to_string(), "TRUE");
    }

    #[test]
    fn test_hex_to_bytes() {
        assert_eq!(
            hex_to_bytes("DEADBEEF").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(hex_to_bytes("0x1234").unwrap(), vec![0x12, 0x34]);
    }
}
