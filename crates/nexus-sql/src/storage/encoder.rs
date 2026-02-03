//! Row encoding and decoding.
//!
//! This module converts between SQL executor values and the key-value
//! format used by the SageTree storage engine.
//!
//! # Encoding Format
//!
//! ## Key Encoding
//! Keys are encoded to preserve sort order:
//! - Table ID (4 bytes, big-endian)
//! - Primary key values (type-specific encoding)
//!
//! ## Value Encoding
//! Values use a simple binary format:
//! - Number of columns (4 bytes)
//! - For each column:
//!   - Null flag (1 byte)
//!   - If not null: type tag (1 byte) + length (4 bytes) + data

use std::sync::Arc;

use nexus_common::types::{Key, Value as StorageValue};

use crate::executor::{Row, Value};
use crate::logical::Schema;

use super::error::{StorageError, StorageResult};

/// Encoding format for rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingFormat {
    /// Binary encoding (efficient, not human-readable).
    Binary,
    /// JSON encoding (less efficient, human-readable).
    Json,
}

impl Default for EncodingFormat {
    fn default() -> Self {
        Self::Binary
    }
}

/// Type tags for binary encoding.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
enum TypeTag {
    Null = 0,
    Boolean = 1,
    TinyInt = 2,
    SmallInt = 3,
    Int = 4,
    BigInt = 5,
    Float = 6,
    Double = 7,
    Decimal = 8,
    String = 9,
    Bytes = 10,
    Date = 11,
    Time = 12,
    Timestamp = 13,
}

impl TypeTag {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(TypeTag::Null),
            1 => Some(TypeTag::Boolean),
            2 => Some(TypeTag::TinyInt),
            3 => Some(TypeTag::SmallInt),
            4 => Some(TypeTag::Int),
            5 => Some(TypeTag::BigInt),
            6 => Some(TypeTag::Float),
            7 => Some(TypeTag::Double),
            8 => Some(TypeTag::Decimal),
            9 => Some(TypeTag::String),
            10 => Some(TypeTag::Bytes),
            11 => Some(TypeTag::Date),
            12 => Some(TypeTag::Time),
            13 => Some(TypeTag::Timestamp),
            _ => None,
        }
    }
}

/// Row encoder for converting SQL values to storage keys/values.
#[derive(Debug)]
pub struct RowEncoder {
    /// Table ID.
    table_id: u64,
    /// Primary key column indices.
    primary_key_indices: Vec<usize>,
    /// Schema.
    #[allow(dead_code)]
    schema: Arc<Schema>,
    /// Encoding format.
    format: EncodingFormat,
}

impl RowEncoder {
    /// Creates a new row encoder.
    pub fn new(table_id: u64, primary_key_indices: Vec<usize>, schema: Arc<Schema>) -> Self {
        Self {
            table_id,
            primary_key_indices,
            schema,
            format: EncodingFormat::Binary,
        }
    }

    /// Sets the encoding format.
    pub fn with_format(mut self, format: EncodingFormat) -> Self {
        self.format = format;
        self
    }

    /// Encodes a row's primary key to a storage key.
    pub fn encode_key(&self, row: &Row) -> StorageResult<Key> {
        let mut buf = Vec::with_capacity(64);

        // Write table ID prefix
        buf.extend_from_slice(&self.table_id.to_be_bytes());

        // Write primary key values in order-preserving format
        for &idx in &self.primary_key_indices {
            let value = row.get(idx).ok_or_else(|| {
                StorageError::EncodingError(format!("Missing primary key column {}", idx))
            })?;
            self.encode_key_value(value, &mut buf)?;
        }

        Ok(Key::from_vec(buf))
    }

    /// Encodes a single value for key encoding (order-preserving).
    fn encode_key_value(&self, value: &Value, buf: &mut Vec<u8>) -> StorageResult<()> {
        match value {
            Value::Null => {
                // NULL sorts first
                buf.push(0x00);
            }
            Value::Boolean(b) => {
                buf.push(0x01);
                buf.push(if *b { 1 } else { 0 });
            }
            Value::TinyInt(i) => {
                buf.push(0x02);
                // Flip sign bit for proper ordering
                buf.push((*i as u8) ^ 0x80);
            }
            Value::SmallInt(i) => {
                buf.push(0x03);
                let v = (*i as u16) ^ 0x8000;
                buf.extend_from_slice(&v.to_be_bytes());
            }
            Value::Int(i) => {
                buf.push(0x04);
                let v = (*i as u32) ^ 0x8000_0000;
                buf.extend_from_slice(&v.to_be_bytes());
            }
            Value::BigInt(i) => {
                buf.push(0x05);
                let v = (*i as u64) ^ 0x8000_0000_0000_0000;
                buf.extend_from_slice(&v.to_be_bytes());
            }
            Value::String(s) => {
                buf.push(0x09);
                // Length-prefixed string
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Bytes(b) => {
                buf.push(0x0A);
                buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
                buf.extend_from_slice(b);
            }
            _ => {
                // For other types, use string representation
                buf.push(0x09);
                let s = value.to_string();
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                buf.extend_from_slice(bytes);
            }
        }
        Ok(())
    }

    /// Encodes a row to a storage value.
    pub fn encode_value(&self, row: &Row) -> StorageResult<StorageValue> {
        match self.format {
            EncodingFormat::Binary => self.encode_value_binary(row),
            EncodingFormat::Json => self.encode_value_json(row),
        }
    }

    fn encode_value_binary(&self, row: &Row) -> StorageResult<StorageValue> {
        let mut buf = Vec::with_capacity(256);

        // Write number of columns
        buf.extend_from_slice(&(row.num_columns() as u32).to_be_bytes());

        // Write each column value
        for i in 0..row.num_columns() {
            let value = row.get(i).unwrap_or(&Value::Null);
            self.encode_single_value(value, &mut buf)?;
        }

        Ok(StorageValue::from_vec(buf))
    }

    fn encode_single_value(&self, value: &Value, buf: &mut Vec<u8>) -> StorageResult<()> {
        match value {
            Value::Null => {
                buf.push(TypeTag::Null as u8);
            }
            Value::Boolean(b) => {
                buf.push(TypeTag::Boolean as u8);
                buf.push(if *b { 1 } else { 0 });
            }
            Value::TinyInt(i) => {
                buf.push(TypeTag::TinyInt as u8);
                buf.push(*i as u8);
            }
            Value::SmallInt(i) => {
                buf.push(TypeTag::SmallInt as u8);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::Int(i) => {
                buf.push(TypeTag::Int as u8);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::BigInt(i) => {
                buf.push(TypeTag::BigInt as u8);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(TypeTag::Float as u8);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::Double(f) => {
                buf.push(TypeTag::Double as u8);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::Decimal { value, scale } => {
                buf.push(TypeTag::Decimal as u8);
                buf.extend_from_slice(&value.to_le_bytes());
                buf.push(*scale);
            }
            Value::String(s) => {
                buf.push(TypeTag::String as u8);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Bytes(b) => {
                buf.push(TypeTag::Bytes as u8);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::Date(d) => {
                buf.push(TypeTag::Date as u8);
                buf.extend_from_slice(&d.to_le_bytes());
            }
            Value::Time(t) => {
                buf.push(TypeTag::Time as u8);
                buf.extend_from_slice(&t.to_le_bytes());
            }
            Value::Timestamp(ts) => {
                buf.push(TypeTag::Timestamp as u8);
                buf.extend_from_slice(&ts.to_le_bytes());
            }
        }
        Ok(())
    }

    fn encode_value_json(&self, row: &Row) -> StorageResult<StorageValue> {
        // Simple JSON array format
        let mut parts = Vec::with_capacity(row.num_columns());
        for i in 0..row.num_columns() {
            let value = row.get(i).unwrap_or(&Value::Null);
            parts.push(value_to_json(value));
        }
        let json = format!("[{}]", parts.join(","));
        Ok(StorageValue::from_vec(json.into_bytes()))
    }
}

fn value_to_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::Int(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Decimal { value, scale } => {
            let divisor = 10f64.powi(*scale as i32);
            (*value as f64 / divisor).to_string()
        }
        Value::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::Bytes(b) => format!("\"base64:{}\"", base64_encode(b)),
        Value::Date(d) => format!("\"{}\"", d),
        Value::Time(t) => format!("\"{}\"", t),
        Value::Timestamp(ts) => format!("\"{}\"", ts),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    // Simple base64 encoding
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((bytes.len() + 2) / 3 * 4);

    for chunk in bytes.chunks(3) {
        let mut n = (chunk[0] as u32) << 16;
        if chunk.len() > 1 {
            n |= (chunk[1] as u32) << 8;
        }
        if chunk.len() > 2 {
            n |= chunk[2] as u32;
        }

        result.push(ALPHABET[(n >> 18) as usize & 0x3F] as char);
        result.push(ALPHABET[(n >> 12) as usize & 0x3F] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[(n >> 6) as usize & 0x3F] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[n as usize & 0x3F] as char);
        } else {
            result.push('=');
        }
    }

    result
}

/// Row decoder for converting storage keys/values back to SQL values.
#[derive(Debug)]
pub struct RowDecoder {
    /// Schema.
    #[allow(dead_code)]
    schema: Arc<Schema>,
    /// Encoding format.
    format: EncodingFormat,
}

impl RowDecoder {
    /// Creates a new row decoder.
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            format: EncodingFormat::Binary,
        }
    }

    /// Sets the encoding format.
    pub fn with_format(mut self, format: EncodingFormat) -> Self {
        self.format = format;
        self
    }

    /// Decodes a storage value to a row.
    pub fn decode(&self, value: &StorageValue) -> StorageResult<Row> {
        match self.format {
            EncodingFormat::Binary => self.decode_binary(value),
            EncodingFormat::Json => self.decode_json(value),
        }
    }

    fn decode_binary(&self, storage_value: &StorageValue) -> StorageResult<Row> {
        let bytes = storage_value.as_bytes();
        let mut pos = 0;

        // Read number of columns
        if bytes.len() < 4 {
            return Err(StorageError::EncodingError("Value too short".to_string()));
        }
        let num_cols = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        pos += 4;

        // Read each column value
        let mut values = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            let (value, consumed) = self.decode_single_value(&bytes[pos..])?;
            values.push(value);
            pos += consumed;
        }

        Ok(Row::new(values))
    }

    fn decode_single_value(&self, bytes: &[u8]) -> StorageResult<(Value, usize)> {
        if bytes.is_empty() {
            return Err(StorageError::EncodingError(
                "Unexpected end of data".to_string(),
            ));
        }

        let tag = TypeTag::from_u8(bytes[0]).ok_or_else(|| {
            StorageError::EncodingError(format!("Unknown type tag: {}", bytes[0]))
        })?;

        let (value, size) = match tag {
            TypeTag::Null => (Value::Null, 1),
            TypeTag::Boolean => {
                if bytes.len() < 2 {
                    return Err(StorageError::EncodingError(
                        "Missing boolean value".to_string(),
                    ));
                }
                (Value::Boolean(bytes[1] != 0), 2)
            }
            TypeTag::TinyInt => {
                if bytes.len() < 2 {
                    return Err(StorageError::EncodingError(
                        "Missing tinyint value".to_string(),
                    ));
                }
                (Value::TinyInt(bytes[1] as i8), 2)
            }
            TypeTag::SmallInt => {
                if bytes.len() < 3 {
                    return Err(StorageError::EncodingError(
                        "Missing smallint value".to_string(),
                    ));
                }
                let v = i16::from_le_bytes([bytes[1], bytes[2]]);
                (Value::SmallInt(v), 3)
            }
            TypeTag::Int => {
                if bytes.len() < 5 {
                    return Err(StorageError::EncodingError("Missing int value".to_string()));
                }
                let v = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                (Value::Int(v), 5)
            }
            TypeTag::BigInt => {
                if bytes.len() < 9 {
                    return Err(StorageError::EncodingError(
                        "Missing bigint value".to_string(),
                    ));
                }
                let v = i64::from_le_bytes(bytes[1..9].try_into().unwrap());
                (Value::BigInt(v), 9)
            }
            TypeTag::Float => {
                if bytes.len() < 5 {
                    return Err(StorageError::EncodingError(
                        "Missing float value".to_string(),
                    ));
                }
                let v = f32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                (Value::Float(v), 5)
            }
            TypeTag::Double => {
                if bytes.len() < 9 {
                    return Err(StorageError::EncodingError(
                        "Missing double value".to_string(),
                    ));
                }
                let v = f64::from_le_bytes(bytes[1..9].try_into().unwrap());
                (Value::Double(v), 9)
            }
            TypeTag::Decimal => {
                if bytes.len() < 18 {
                    return Err(StorageError::EncodingError(
                        "Missing decimal value".to_string(),
                    ));
                }
                let value = i128::from_le_bytes(bytes[1..17].try_into().unwrap());
                let scale = bytes[17];
                (Value::Decimal { value, scale }, 18)
            }
            TypeTag::String => {
                if bytes.len() < 5 {
                    return Err(StorageError::EncodingError(
                        "Missing string length".to_string(),
                    ));
                }
                let len = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
                if bytes.len() < 5 + len {
                    return Err(StorageError::EncodingError(
                        "String data truncated".to_string(),
                    ));
                }
                let s = String::from_utf8_lossy(&bytes[5..5 + len]).to_string();
                (Value::String(s), 5 + len)
            }
            TypeTag::Bytes => {
                if bytes.len() < 5 {
                    return Err(StorageError::EncodingError(
                        "Missing bytes length".to_string(),
                    ));
                }
                let len = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
                if bytes.len() < 5 + len {
                    return Err(StorageError::EncodingError(
                        "Bytes data truncated".to_string(),
                    ));
                }
                let b = bytes[5..5 + len].to_vec();
                (Value::Bytes(b), 5 + len)
            }
            TypeTag::Date => {
                if bytes.len() < 5 {
                    return Err(StorageError::EncodingError(
                        "Missing date value".to_string(),
                    ));
                }
                let v = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                (Value::Date(v), 5)
            }
            TypeTag::Time => {
                if bytes.len() < 9 {
                    return Err(StorageError::EncodingError(
                        "Missing time value".to_string(),
                    ));
                }
                let v = i64::from_le_bytes(bytes[1..9].try_into().unwrap());
                (Value::Time(v), 9)
            }
            TypeTag::Timestamp => {
                if bytes.len() < 9 {
                    return Err(StorageError::EncodingError(
                        "Missing timestamp value".to_string(),
                    ));
                }
                let v = i64::from_le_bytes(bytes[1..9].try_into().unwrap());
                (Value::Timestamp(v), 9)
            }
        };

        Ok((value, size))
    }

    fn decode_json(&self, _storage_value: &StorageValue) -> StorageResult<Row> {
        // JSON decoding would require a JSON parser
        // For now, return an error
        Err(StorageError::EncodingError(
            "JSON decoding not implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::Field;
    use crate::parser::DataType;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Text),
            Field::nullable("age", DataType::Int),
        ]))
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let schema = test_schema();
        let encoder = RowEncoder::new(1, vec![0], schema.clone());
        let decoder = RowDecoder::new(schema);

        let row = Row::new(vec![
            Value::Int(42),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);

        let encoded = encoder.encode_value(&row).unwrap();
        let decoded = decoder.decode(&encoded).unwrap();

        assert_eq!(decoded.num_columns(), 3);
        assert_eq!(decoded.get(0), Some(&Value::Int(42)));
        assert_eq!(decoded.get(1), Some(&Value::String("Alice".to_string())));
        assert_eq!(decoded.get(2), Some(&Value::Int(30)));
    }

    #[test]
    fn test_encode_key() {
        let schema = test_schema();
        let encoder = RowEncoder::new(1, vec![0], schema);

        let row = Row::new(vec![
            Value::Int(42),
            Value::String("Alice".to_string()),
            Value::Int(30),
        ]);

        let key = encoder.encode_key(&row).unwrap();

        // Key should start with table ID
        assert!(key.len() >= 8);
    }

    #[test]
    fn test_key_ordering() {
        let schema = test_schema();
        let encoder = RowEncoder::new(1, vec![0], schema);

        let row1 = Row::new(vec![Value::Int(10), Value::Null, Value::Null]);
        let row2 = Row::new(vec![Value::Int(20), Value::Null, Value::Null]);
        let row3 = Row::new(vec![Value::Int(-5), Value::Null, Value::Null]);

        let key1 = encoder.encode_key(&row1).unwrap();
        let key2 = encoder.encode_key(&row2).unwrap();
        let key3 = encoder.encode_key(&row3).unwrap();

        // Keys should maintain integer ordering
        assert!(key3 < key1);
        assert!(key1 < key2);
    }

    #[test]
    fn test_encode_null_values() {
        let schema = test_schema();
        let encoder = RowEncoder::new(1, vec![0], schema.clone());
        let decoder = RowDecoder::new(schema);

        let row = Row::new(vec![Value::Int(1), Value::Null, Value::Null]);

        let encoded = encoder.encode_value(&row).unwrap();
        let decoded = decoder.decode(&encoded).unwrap();

        assert_eq!(decoded.get(1), Some(&Value::Null));
        assert_eq!(decoded.get(2), Some(&Value::Null));
    }

    #[test]
    fn test_encode_all_types() {
        let schema = Arc::new(Schema::new(vec![Field::not_null("col", DataType::Int)]));
        let encoder = RowEncoder::new(1, vec![], schema.clone());
        let decoder = RowDecoder::new(schema);

        let test_values = vec![
            Value::Boolean(true),
            Value::TinyInt(127),
            Value::SmallInt(-1000),
            Value::Int(123456),
            Value::BigInt(9876543210),
            Value::Float(3.14),
            Value::Double(2.71828),
            Value::String("hello world".to_string()),
            Value::Bytes(vec![1, 2, 3, 4, 5]),
        ];

        for value in test_values {
            let row = Row::new(vec![value.clone()]);
            let encoded = encoder.encode_value(&row).unwrap();
            let decoded = decoder.decode(&encoded).unwrap();
            assert_eq!(decoded.get(0), Some(&value), "Failed for {:?}", value);
        }
    }
}
