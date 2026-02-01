//! Key and value types for NexusDB.
//!
//! These types provide variable-length byte wrappers for database keys and values.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;

/// Maximum key size in bytes (16 KB).
pub const MAX_KEY_SIZE: usize = 16 * 1024;

/// Maximum value size in bytes (1 MB).
pub const MAX_VALUE_SIZE: usize = 1024 * 1024;

/// A database key.
///
/// Keys are variable-length byte sequences that are used to identify records.
/// They support efficient comparison and hashing for use in indexes.
///
/// # Size Limits
///
/// Keys are limited to [`MAX_KEY_SIZE`] bytes (16 KB).
///
/// # Example
///
/// ```rust
/// use nexus_common::types::Key;
///
/// let key = Key::from_bytes(b"user:1234");
/// assert_eq!(key.len(), 9);
/// ```
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key(Bytes);

impl Key {
    /// Creates an empty key.
    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self(Bytes::new())
    }

    /// Creates a key from a byte slice.
    #[inline]
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }

    /// Creates a key from owned bytes.
    #[inline]
    #[must_use]
    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self(Bytes::from(vec))
    }

    /// Creates a key from a `Bytes` instance.
    #[inline]
    #[must_use]
    pub const fn from_raw(bytes: Bytes) -> Self {
        Self(bytes)
    }

    /// Creates a key from a string.
    #[inline]
    #[must_use]
    pub fn from_str(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }

    /// Returns the length of the key in bytes.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the key is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the key as a byte slice.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the underlying `Bytes`.
    #[inline]
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// Returns a reference to the underlying `Bytes`.
    #[inline]
    #[must_use]
    pub fn as_raw(&self) -> &Bytes {
        &self.0
    }

    /// Checks if this key starts with the given prefix.
    #[inline]
    #[must_use]
    pub fn starts_with(&self, prefix: &[u8]) -> bool {
        self.0.starts_with(prefix)
    }

    /// Checks if this key ends with the given suffix.
    #[inline]
    #[must_use]
    pub fn ends_with(&self, suffix: &[u8]) -> bool {
        self.0.ends_with(suffix)
    }

    /// Returns a successor key (for range scans).
    ///
    /// The successor is the smallest key that is greater than this key.
    #[must_use]
    pub fn successor(&self) -> Self {
        let mut bytes = self.0.to_vec();

        // Find the rightmost byte that is not 0xFF
        for i in (0..bytes.len()).rev() {
            if bytes[i] < 0xFF {
                bytes[i] += 1;
                bytes.truncate(i + 1);
                return Self::from_vec(bytes);
            }
        }

        // All bytes are 0xFF, append 0x00
        bytes.push(0x00);
        Self::from_vec(bytes)
    }
}

impl Deref for Key {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for Key {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Ord for Key {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Key {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Try to display as UTF-8 string if valid, otherwise show hex
        match std::str::from_utf8(&self.0) {
            Ok(s) if s.chars().all(|c| !c.is_control() || c == ' ') => {
                write!(f, "Key({:?})", s)
            }
            _ => {
                write!(f, "Key(0x")?;
                for byte in &self.0[..self.0.len().min(32)] {
                    write!(f, "{byte:02x}")?;
                }
                if self.0.len() > 32 {
                    write!(f, "...")?;
                }
                write!(f, ")")
            }
        }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => {
                for byte in &self.0[..self.0.len().min(32)] {
                    write!(f, "{byte:02x}")?;
                }
                if self.0.len() > 32 {
                    write!(f, "...")?;
                }
                Ok(())
            }
        }
    }
}

impl From<&[u8]> for Key {
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        Self::from_bytes(bytes)
    }
}

impl From<Vec<u8>> for Key {
    #[inline]
    fn from(vec: Vec<u8>) -> Self {
        Self::from_vec(vec)
    }
}

impl From<&str> for Key {
    #[inline]
    fn from(s: &str) -> Self {
        Self::from_str(s)
    }
}

impl From<String> for Key {
    #[inline]
    fn from(s: String) -> Self {
        Self::from_vec(s.into_bytes())
    }
}

impl From<Bytes> for Key {
    #[inline]
    fn from(bytes: Bytes) -> Self {
        Self::from_raw(bytes)
    }
}

/// A database value.
///
/// Values are variable-length byte sequences stored alongside keys.
/// They can represent any serializable data.
///
/// # Size Limits
///
/// Values are limited to [`MAX_VALUE_SIZE`] bytes (1 MB).
///
/// # Example
///
/// ```rust
/// use nexus_common::types::Value;
///
/// let value = Value::from_bytes(b"Hello, World!");
/// assert_eq!(value.len(), 13);
/// ```
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Value(Bytes);

impl Value {
    /// Creates an empty value.
    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self(Bytes::new())
    }

    /// Creates a value from a byte slice.
    #[inline]
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }

    /// Creates a value from owned bytes.
    #[inline]
    #[must_use]
    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self(Bytes::from(vec))
    }

    /// Creates a value from a `Bytes` instance.
    #[inline]
    #[must_use]
    pub const fn from_raw(bytes: Bytes) -> Self {
        Self(bytes)
    }

    /// Creates a value from a string.
    #[inline]
    #[must_use]
    pub fn from_str(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }

    /// Returns the length of the value in bytes.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the value is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the value as a byte slice.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the underlying `Bytes`.
    #[inline]
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// Returns a reference to the underlying `Bytes`.
    #[inline]
    #[must_use]
    pub fn as_raw(&self) -> &Bytes {
        &self.0
    }

    /// Tries to convert the value to a UTF-8 string.
    #[must_use]
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.0)
    }
}

impl Deref for Value {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for Value {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Value({} bytes)", self.0.len())
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} bytes", self.0.len())
    }
}

impl From<&[u8]> for Value {
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        Self::from_bytes(bytes)
    }
}

impl From<Vec<u8>> for Value {
    #[inline]
    fn from(vec: Vec<u8>) -> Self {
        Self::from_vec(vec)
    }
}

impl From<&str> for Value {
    #[inline]
    fn from(s: &str) -> Self {
        Self::from_str(s)
    }
}

impl From<String> for Value {
    #[inline]
    fn from(s: String) -> Self {
        Self::from_vec(s.into_bytes())
    }
}

impl From<Bytes> for Value {
    #[inline]
    fn from(bytes: Bytes) -> Self {
        Self::from_raw(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_creation() {
        let key = Key::from_bytes(b"test");
        assert_eq!(key.len(), 4);
        assert_eq!(key.as_bytes(), b"test");

        let key2 = Key::from_str("test");
        assert_eq!(key, key2);

        let key3: Key = "test".into();
        assert_eq!(key, key3);
    }

    #[test]
    fn test_key_ordering() {
        let a = Key::from_bytes(b"aaa");
        let b = Key::from_bytes(b"bbb");
        let aa = Key::from_bytes(b"aa");

        assert!(a < b);
        assert!(aa < a);
    }

    #[test]
    fn test_key_successor() {
        let key = Key::from_bytes(b"abc");
        let succ = key.successor();
        assert_eq!(succ.as_bytes(), b"abd");

        let key = Key::from_bytes(&[0xFF, 0xFF]);
        let succ = key.successor();
        assert_eq!(succ.as_bytes(), &[0xFF, 0xFF, 0x00]);
    }

    #[test]
    fn test_key_prefix() {
        let key = Key::from_bytes(b"user:1234:profile");
        assert!(key.starts_with(b"user:"));
        assert!(key.ends_with(b":profile"));
        assert!(!key.starts_with(b"admin:"));
    }

    #[test]
    fn test_value_creation() {
        let value = Value::from_bytes(b"hello world");
        assert_eq!(value.len(), 11);
        assert!(!value.is_empty());

        let empty = Value::empty();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_value_string_conversion() {
        let value = Value::from_str("hello");
        assert_eq!(value.to_string_lossy(), "hello");
    }
}
