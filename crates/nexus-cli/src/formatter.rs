//! Output formatting for query results.
//!
//! Supports table, JSON, CSV, and raw output formats.

use comfy_table::{Cell, ContentArrangement, Table};
use serde_json::{json, Value as JsonValue};

use nexus_client::QueryResult;

/// Output format options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// Formatted table output.
    Table,
    /// JSON output.
    Json,
    /// CSV output.
    Csv,
    /// Raw output (values separated by tabs).
    Raw,
}

/// Formats a query result according to the specified format.
pub fn format_result(result: &QueryResult, format: OutputFormat) -> String {
    match format {
        OutputFormat::Table => format_table(result),
        OutputFormat::Json => format_json(result),
        OutputFormat::Csv => format_csv(result),
        OutputFormat::Raw => format_raw(result),
    }
}

/// Formats the result as a table.
fn format_table(result: &QueryResult) -> String {
    let mut table = Table::new();

    table
        .set_content_arrangement(ContentArrangement::Dynamic)
        .load_preset(comfy_table::presets::UTF8_FULL)
        .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS);

    // Add header row
    if !result.columns.is_empty() {
        table.set_header(result.columns.iter().map(|c| Cell::new(c)));
    }

    // Add data rows
    for row in &result.rows {
        let cells: Vec<Cell> = row.iter().map(|v| Cell::new(v.to_string())).collect();
        table.add_row(cells);
    }

    table.to_string()
}

/// Formats the result as JSON.
fn format_json(result: &QueryResult) -> String {
    let rows: Vec<JsonValue> = result
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (i, value) in row.iter().enumerate() {
                let col_name = result
                    .columns
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("column_{}", i));

                let json_val = value_to_json(value);
                obj.insert(col_name, json_val);
            }
            JsonValue::Object(obj)
        })
        .collect();

    serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".to_string())
}

/// Converts a client Value to a JSON value.
fn value_to_json(value: &nexus_client::Value) -> JsonValue {
    match value {
        nexus_client::Value::Null => JsonValue::Null,
        nexus_client::Value::Boolean(b) => json!(*b),
        nexus_client::Value::Integer(i) => json!(*i),
        nexus_client::Value::Float(f) => json!(*f),
        nexus_client::Value::String(s) => json!(s),
        nexus_client::Value::Bytes(b) => {
            // Encode bytes as base64
            json!(base64_encode(b))
        }
    }
}

/// Simple base64 encoding (avoiding extra dependency).
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    let mut chunks = data.chunks_exact(3);

    for chunk in chunks.by_ref() {
        let n = (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8 | chunk[2] as u32;
        result.push(ALPHABET[(n >> 18 & 0x3F) as usize] as char);
        result.push(ALPHABET[(n >> 12 & 0x3F) as usize] as char);
        result.push(ALPHABET[(n >> 6 & 0x3F) as usize] as char);
        result.push(ALPHABET[(n & 0x3F) as usize] as char);
    }

    let remainder = chunks.remainder();
    match remainder.len() {
        1 => {
            let n = (remainder[0] as u32) << 16;
            result.push(ALPHABET[(n >> 18 & 0x3F) as usize] as char);
            result.push(ALPHABET[(n >> 12 & 0x3F) as usize] as char);
            result.push_str("==");
        }
        2 => {
            let n = (remainder[0] as u32) << 16 | (remainder[1] as u32) << 8;
            result.push(ALPHABET[(n >> 18 & 0x3F) as usize] as char);
            result.push(ALPHABET[(n >> 12 & 0x3F) as usize] as char);
            result.push(ALPHABET[(n >> 6 & 0x3F) as usize] as char);
            result.push('=');
        }
        _ => {}
    }

    result
}

/// Formats the result as CSV.
fn format_csv(result: &QueryResult) -> String {
    let mut output = String::new();

    // Header row
    if !result.columns.is_empty() {
        let header: Vec<String> = result.columns.iter().map(|c| escape_csv(c)).collect();
        output.push_str(&header.join(","));
        output.push('\n');
    }

    // Data rows
    for row in &result.rows {
        let values: Vec<String> = row.iter().map(|v| escape_csv(&v.to_string())).collect();
        output.push_str(&values.join(","));
        output.push('\n');
    }

    output
}

/// Escapes a value for CSV output.
fn escape_csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Formats the result as raw tab-separated values.
fn format_raw(result: &QueryResult) -> String {
    let mut output = String::new();

    // Header row
    if !result.columns.is_empty() {
        output.push_str(&result.columns.join("\t"));
        output.push('\n');
    }

    // Data rows
    for row in &result.rows {
        let values: Vec<String> = row.iter().map(|v| v.to_string()).collect();
        output.push_str(&values.join("\t"));
        output.push('\n');
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_test_result() -> QueryResult {
        QueryResult {
            columns: vec!["id".to_string(), "name".to_string(), "active".to_string()],
            rows: vec![
                vec![
                    nexus_client::Value::Integer(1),
                    nexus_client::Value::String("Alice".to_string()),
                    nexus_client::Value::Boolean(true),
                ],
                vec![
                    nexus_client::Value::Integer(2),
                    nexus_client::Value::String("Bob".to_string()),
                    nexus_client::Value::Boolean(false),
                ],
            ],
            rows_affected: 0,
            execution_time: Duration::from_millis(10),
        }
    }

    #[test]
    fn test_format_table() {
        let result = make_test_result();
        let output = format_table(&result);
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }

    #[test]
    fn test_format_json() {
        let result = make_test_result();
        let output = format_json(&result);
        assert!(output.contains("\"id\""));
        assert!(output.contains("\"name\""));
        assert!(output.contains("\"Alice\""));

        // Verify it's valid JSON
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_format_csv() {
        let result = make_test_result();
        let output = format_csv(&result);

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 3); // Header + 2 rows
        assert_eq!(lines[0], "id,name,active");
        assert!(lines[1].contains("Alice"));
    }

    #[test]
    fn test_escape_csv() {
        assert_eq!(escape_csv("hello"), "hello");
        assert_eq!(escape_csv("hello,world"), "\"hello,world\"");
        assert_eq!(escape_csv("hello\"world"), "\"hello\"\"world\"");
    }

    #[test]
    fn test_format_raw() {
        let result = make_test_result();
        let output = format_raw(&result);

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "id\tname\tactive");
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b"hi"), "aGk=");
        assert_eq!(base64_encode(b"hey"), "aGV5");
        assert_eq!(base64_encode(b""), "");
    }
}
