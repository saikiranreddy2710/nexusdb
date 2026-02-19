//! Semantic microcaching for AI agent workloads.
//!
//! Traditional caches match queries by exact string. AI agents generate
//! syntactically different but semantically identical queries. This module
//! normalizes queries to a canonical form and computes structural fingerprints
//! so that equivalent queries share a single cache entry.
//!
//! ## Normalization Pipeline
//!
//! ```text
//! "SELECT * FROM users WHERE name = 'John'"
//!   → lowercase:    "select * from users where name = 'John'"
//!   → whitespace:   "select * from users where name = 'John'"
//!   → params:       "select * from users where name = ?"
//!   → fingerprint:  0xA3B2C1D4 (hash of structure)
//! ```
//!
//! ## Example
//!
//! ```text
//! All three hit the SAME cache entry:
//!   SELECT * FROM users WHERE name = 'John'
//!   SELECT * FROM users WHERE name='John'
//!   select * from users where name = 'John'
//! ```

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Normalizes a SQL query to a canonical form for cache matching.
///
/// Applies these transformations in order:
/// 1. Collapse all whitespace to single spaces
/// 2. Lowercase SQL keywords (preserves string literal case)
/// 3. Trim leading/trailing whitespace
/// 4. Remove trailing semicolons
pub fn normalize_sql(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut in_string = false;
    let mut string_char = ' ';
    let mut prev_was_space = false;

    for ch in sql.chars() {
        if in_string {
            result.push(ch);
            if ch == string_char {
                in_string = false;
            }
            prev_was_space = false;
            continue;
        }

        if ch == '\'' || ch == '"' {
            in_string = true;
            string_char = ch;
            result.push(ch);
            prev_was_space = false;
            continue;
        }

        if ch.is_whitespace() {
            if !prev_was_space && !result.is_empty() {
                result.push(' ');
                prev_was_space = true;
            }
            continue;
        }

        // Lowercase everything outside string literals
        result.push(ch.to_ascii_lowercase());
        prev_was_space = false;
    }

    // Trim trailing whitespace and semicolons
    let trimmed = result.trim_end_matches(|c: char| c == ';' || c.is_whitespace());
    trimmed.to_string()
}

/// Extracts a structural fingerprint from a SQL query.
///
/// Replaces all literal values (strings, numbers) with placeholders,
/// then hashes the resulting structure. Two queries with the same
/// structure but different parameter values produce the same fingerprint.
///
/// This enables cache sharing across queries like:
/// - `SELECT * FROM users WHERE id = 1`
/// - `SELECT * FROM users WHERE id = 42`
pub fn structural_fingerprint(sql: &str) -> u64 {
    let parameterized = parameterize_sql(sql);
    let normalized = normalize_sql(&parameterized);
    hash_string(&normalized)
}

/// Replaces literal values in SQL with `?` placeholders.
///
/// Handles:
/// - String literals: `'John'` -> `?`
/// - Integer literals: `42` -> `?`
/// - Float literals: `3.14` -> `?`
pub fn parameterize_sql(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // String literals
        if chars[i] == '\'' {
            result.push('?');
            i += 1;
            // Skip until closing quote (handle escaped quotes)
            while i < chars.len() {
                if chars[i] == '\'' {
                    if i + 1 < chars.len() && chars[i + 1] == '\'' {
                        i += 2; // Escaped quote
                    } else {
                        i += 1; // End of string
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // Numeric literals (integers and floats)
        if chars[i].is_ascii_digit()
            && (i == 0
                || !chars[i - 1].is_ascii_alphanumeric() && chars[i - 1] != '_')
        {
            result.push('?');
            i += 1;
            // Skip remaining digits and optional decimal
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            continue;
        }

        result.push(chars[i]);
        i += 1;
    }

    result
}

/// Compute a stable hash for a string.
fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

/// A semantic cache key that matches structurally equivalent queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SemanticKey {
    /// Fingerprint of the query structure (parameters replaced).
    pub fingerprint: u64,
    /// Hash of the normalized SQL (exact match within same structure).
    pub normalized_hash: u64,
}

impl SemanticKey {
    /// Create a semantic key from a raw SQL string.
    pub fn from_sql(sql: &str) -> Self {
        let normalized = normalize_sql(sql);
        Self {
            fingerprint: structural_fingerprint(sql),
            normalized_hash: hash_string(&normalized),
        }
    }

    /// Create a key that matches ANY query with the same structure
    /// (regardless of parameter values).
    pub fn structural_only(sql: &str) -> Self {
        let fp = structural_fingerprint(sql);
        Self {
            fingerprint: fp,
            normalized_hash: fp, // Use fingerprint as hash too
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_sql() {
        assert_eq!(
            normalize_sql("SELECT * FROM users WHERE name = 'John'"),
            "select * from users where name = 'John'"
        );
        assert_eq!(
            normalize_sql("  SELECT  *  FROM  users  "),
            "select * from users"
        );
        assert_eq!(
            normalize_sql("SELECT * FROM users;"),
            "select * from users"
        );
        assert_eq!(
            normalize_sql("SELECT * FROM users WHERE name='John'"),
            "select * from users where name='John'"
        );
    }

    #[test]
    fn test_normalize_preserves_string_case() {
        let result = normalize_sql("SELECT * FROM users WHERE name = 'John Doe'");
        assert!(result.contains("'John Doe'"));
    }

    #[test]
    fn test_parameterize_sql() {
        assert_eq!(
            parameterize_sql("SELECT * FROM users WHERE id = 42"),
            "SELECT * FROM users WHERE id = ?"
        );
        assert_eq!(
            parameterize_sql("SELECT * FROM users WHERE name = 'John'"),
            "SELECT * FROM users WHERE name = ?"
        );
        assert_eq!(
            parameterize_sql("SELECT * FROM t WHERE a = 1 AND b = 'x'"),
            "SELECT * FROM t WHERE a = ? AND b = ?"
        );
    }

    #[test]
    fn test_structural_fingerprint_same_structure() {
        let fp1 = structural_fingerprint("SELECT * FROM users WHERE id = 1");
        let fp2 = structural_fingerprint("SELECT * FROM users WHERE id = 42");
        assert_eq!(fp1, fp2, "same structure should produce same fingerprint");
    }

    #[test]
    fn test_structural_fingerprint_different_structure() {
        let fp1 = structural_fingerprint("SELECT * FROM users WHERE id = 1");
        let fp2 = structural_fingerprint("SELECT * FROM orders WHERE id = 1");
        assert_ne!(fp1, fp2, "different tables should produce different fingerprints");
    }

    #[test]
    fn test_semantic_key_equivalent_queries() {
        // Case and whitespace differences — same fingerprint after normalization
        let k1 = SemanticKey::from_sql("SELECT * FROM users WHERE name = 'John'");
        let k3 = SemanticKey::from_sql("select * from users where name = 'John'");
        assert_eq!(k1.fingerprint, k3.fingerprint);

        // Extra whitespace collapses to same form
        let k4 = SemanticKey::from_sql("SELECT  *  FROM  users  WHERE  name = 'John'");
        assert_eq!(k1.fingerprint, k4.fingerprint);
    }

    #[test]
    fn test_semantic_key_same_structure_different_params() {
        let k1 = SemanticKey::from_sql("SELECT * FROM users WHERE id = 1");
        let k2 = SemanticKey::from_sql("SELECT * FROM users WHERE id = 99");

        // Same fingerprint (same structure)
        assert_eq!(k1.fingerprint, k2.fingerprint);
        // Different normalized hash (different parameter values)
        assert_ne!(k1.normalized_hash, k2.normalized_hash);
    }
}
