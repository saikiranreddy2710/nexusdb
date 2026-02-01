//! Core identifier types for NexusDB.
//!
//! These types provide type-safe wrappers around numeric identifiers,
//! preventing accidental misuse of different ID types.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Page identifier - uniquely identifies a page in the database.
///
/// Pages are the fundamental unit of storage in NexusDB. Each page is
/// identified by a unique 64-bit identifier.
///
/// # Example
///
/// ```rust
/// use nexus_common::types::PageId;
///
/// let page = PageId::new(42);
/// assert_eq!(page.as_u64(), 42);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct PageId(u64);

impl PageId {
    /// Invalid page ID constant, used as a sentinel value.
    pub const INVALID: Self = Self(u64::MAX);

    /// First valid page ID (page 0 is typically reserved for metadata).
    pub const FIRST: Self = Self(0);

    /// Creates a new `PageId` from a raw u64 value.
    #[inline]
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw u64 value.
    #[inline]
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the next page ID.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Checks if this is a valid page ID.
    #[inline]
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0 != Self::INVALID.0
    }

    /// Creates a PageId from bytes (big-endian).
    #[inline]
    #[must_use]
    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    /// Converts to bytes (big-endian).
    #[inline]
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl fmt::Debug for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "PageId(INVALID)")
        } else {
            write!(f, "PageId({})", self.0)
        }
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for PageId {
    #[inline]
    fn from(id: u64) -> Self {
        Self::new(id)
    }
}

impl From<PageId> for u64 {
    #[inline]
    fn from(id: PageId) -> Self {
        id.0
    }
}

/// Transaction identifier - uniquely identifies a transaction.
///
/// Transaction IDs are monotonically increasing and are used to:
/// - Track transaction state
/// - Determine visibility in MVCC
/// - Order transactions for serializability
///
/// # Example
///
/// ```rust
/// use nexus_common::types::TxnId;
///
/// let txn = TxnId::new(1);
/// assert!(txn.is_valid());
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct TxnId(u64);

impl TxnId {
    /// Invalid transaction ID, used as a sentinel value.
    pub const INVALID: Self = Self(0);

    /// Minimum valid transaction ID.
    pub const MIN: Self = Self(1);

    /// Maximum transaction ID.
    pub const MAX: Self = Self(u64::MAX - 1);

    /// Creates a new `TxnId` from a raw u64 value.
    #[inline]
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw u64 value.
    #[inline]
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the next transaction ID.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Checks if this is a valid transaction ID.
    #[inline]
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0 != Self::INVALID.0
    }

    /// Creates a TxnId from bytes (big-endian).
    #[inline]
    #[must_use]
    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    /// Converts to bytes (big-endian).
    #[inline]
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl fmt::Debug for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "TxnId(INVALID)")
        } else {
            write!(f, "TxnId({})", self.0)
        }
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for TxnId {
    #[inline]
    fn from(id: u64) -> Self {
        Self::new(id)
    }
}

impl From<TxnId> for u64 {
    #[inline]
    fn from(id: TxnId) -> Self {
        id.0
    }
}

/// Log Sequence Number - uniquely identifies a position in the WAL.
///
/// LSNs are monotonically increasing and are used to:
/// - Order log records
/// - Track recovery progress
/// - Implement checkpointing
///
/// # Example
///
/// ```rust
/// use nexus_common::types::Lsn;
///
/// let lsn = Lsn::new(1000);
/// assert!(lsn > Lsn::INVALID);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Lsn(u64);

impl Lsn {
    /// Invalid LSN, used as a sentinel value.
    pub const INVALID: Self = Self(0);

    /// First valid LSN.
    pub const FIRST: Self = Self(1);

    /// Maximum LSN value.
    pub const MAX: Self = Self(u64::MAX);

    /// Creates a new `Lsn` from a raw u64 value.
    #[inline]
    #[must_use]
    pub const fn new(lsn: u64) -> Self {
        Self(lsn)
    }

    /// Returns the raw u64 value.
    #[inline]
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the LSN offset by the given amount.
    #[inline]
    #[must_use]
    pub const fn offset(self, delta: u64) -> Self {
        Self(self.0.saturating_add(delta))
    }

    /// Checks if this is a valid LSN.
    #[inline]
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0 != Self::INVALID.0
    }

    /// Returns the difference between two LSNs.
    #[inline]
    #[must_use]
    pub const fn diff(self, other: Self) -> u64 {
        self.0.saturating_sub(other.0)
    }

    /// Creates an Lsn from bytes (big-endian).
    #[inline]
    #[must_use]
    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    /// Converts to bytes (big-endian).
    #[inline]
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "Lsn(INVALID)")
        } else {
            write!(f, "Lsn({})", self.0)
        }
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Lsn {
    #[inline]
    fn from(lsn: u64) -> Self {
        Self::new(lsn)
    }
}

impl From<Lsn> for u64 {
    #[inline]
    fn from(lsn: Lsn) -> Self {
        lsn.0
    }
}

/// Node identifier - uniquely identifies a node in the cluster.
///
/// Node IDs are assigned when a node joins the cluster and remain
/// stable for the lifetime of that node.
///
/// # Example
///
/// ```rust
/// use nexus_common::types::NodeId;
///
/// let node = NodeId::new(1);
/// assert!(node.is_valid());
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct NodeId(u32);

impl NodeId {
    /// Invalid node ID, used as a sentinel value.
    pub const INVALID: Self = Self(0);

    /// First valid node ID.
    pub const FIRST: Self = Self(1);

    /// Creates a new `NodeId` from a raw u32 value.
    #[inline]
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Returns the raw u32 value.
    #[inline]
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Checks if this is a valid node ID.
    #[inline]
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0 != Self::INVALID.0
    }

    /// Creates a NodeId from bytes (big-endian).
    #[inline]
    #[must_use]
    pub fn from_be_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_be_bytes(bytes))
    }

    /// Converts to bytes (big-endian).
    #[inline]
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 4] {
        self.0.to_be_bytes()
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "NodeId(INVALID)")
        } else {
            write!(f, "NodeId({})", self.0)
        }
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for NodeId {
    #[inline]
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<NodeId> for u32 {
    #[inline]
    fn from(id: NodeId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_id() {
        let page = PageId::new(42);
        assert_eq!(page.as_u64(), 42);
        assert!(page.is_valid());
        assert!(!PageId::INVALID.is_valid());

        let next = page.next();
        assert_eq!(next.as_u64(), 43);

        // Byte conversion
        let bytes = page.to_be_bytes();
        assert_eq!(PageId::from_be_bytes(bytes), page);
    }

    #[test]
    fn test_txn_id() {
        let txn = TxnId::new(100);
        assert_eq!(txn.as_u64(), 100);
        assert!(txn.is_valid());
        assert!(!TxnId::INVALID.is_valid());

        let next = txn.next();
        assert_eq!(next.as_u64(), 101);
    }

    #[test]
    fn test_lsn() {
        let lsn = Lsn::new(1000);
        assert_eq!(lsn.as_u64(), 1000);
        assert!(lsn.is_valid());
        assert!(!Lsn::INVALID.is_valid());

        let offset = lsn.offset(500);
        assert_eq!(offset.as_u64(), 1500);

        let diff = offset.diff(lsn);
        assert_eq!(diff, 500);
    }

    #[test]
    fn test_node_id() {
        let node = NodeId::new(5);
        assert_eq!(node.as_u32(), 5);
        assert!(node.is_valid());
        assert!(!NodeId::INVALID.is_valid());
    }

    #[test]
    fn test_ordering() {
        assert!(PageId::new(1) < PageId::new(2));
        assert!(TxnId::new(1) < TxnId::new(2));
        assert!(Lsn::new(1) < Lsn::new(2));
        assert!(NodeId::new(1) < NodeId::new(2));
    }
}
