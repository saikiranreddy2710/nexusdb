//! Page types and flags.

use std::fmt;

/// Types of pages in NexusDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PageType {
    /// Free page (available for allocation).
    Free = 0,
    /// Data page (stores key-value records).
    Data = 1,
    /// Internal B+tree node (stores separators and child pointers).
    Internal = 2,
    /// Leaf B+tree node (stores actual records).
    Leaf = 3,
    /// Overflow page (stores large values).
    Overflow = 4,
    /// Metadata page (stores database/table metadata).
    Metadata = 5,
    /// Free list page (tracks free pages).
    FreeList = 6,
    /// Delta page (stores delta records for SageTree).
    Delta = 7,
}

impl PageType {
    /// Creates a PageType from a raw byte value.
    #[inline]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Free),
            1 => Some(Self::Data),
            2 => Some(Self::Internal),
            3 => Some(Self::Leaf),
            4 => Some(Self::Overflow),
            5 => Some(Self::Metadata),
            6 => Some(Self::FreeList),
            7 => Some(Self::Delta),
            _ => None,
        }
    }

    /// Returns true if this is a B+tree page (internal or leaf).
    #[inline]
    pub const fn is_btree_page(self) -> bool {
        matches!(self, Self::Internal | Self::Leaf)
    }

    /// Returns true if this page can contain user data.
    #[inline]
    pub const fn is_data_page(self) -> bool {
        matches!(self, Self::Data | Self::Leaf | Self::Overflow)
    }

    /// Returns true if this is a free page.
    #[inline]
    pub const fn is_free(self) -> bool {
        matches!(self, Self::Free)
    }
}

impl Default for PageType {
    fn default() -> Self {
        Self::Free
    }
}

impl fmt::Display for PageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Free => write!(f, "Free"),
            Self::Data => write!(f, "Data"),
            Self::Internal => write!(f, "Internal"),
            Self::Leaf => write!(f, "Leaf"),
            Self::Overflow => write!(f, "Overflow"),
            Self::Metadata => write!(f, "Metadata"),
            Self::FreeList => write!(f, "FreeList"),
            Self::Delta => write!(f, "Delta"),
        }
    }
}

/// Flags for page state and properties.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PageFlags(u16);

impl PageFlags {
    /// Page is dirty (modified since last flush).
    pub const DIRTY: u16 = 1 << 0;
    /// Page is pinned in buffer pool.
    pub const PINNED: u16 = 1 << 1;
    /// Page has overflow data (record spans multiple pages).
    pub const HAS_OVERFLOW: u16 = 1 << 2;
    /// Page is compressed.
    pub const COMPRESSED: u16 = 1 << 3;
    /// Page uses prefix compression for keys.
    pub const PREFIX_COMPRESSED: u16 = 1 << 4;
    /// Page has deleted records (tombstones).
    pub const HAS_TOMBSTONES: u16 = 1 << 5;
    /// Page needs compaction.
    pub const NEEDS_COMPACTION: u16 = 1 << 6;
    /// Page is being written (WAL protection).
    pub const WRITE_IN_PROGRESS: u16 = 1 << 7;
    /// Page is a root page.
    pub const IS_ROOT: u16 = 1 << 8;
    /// Page is rightmost at its level.
    pub const IS_RIGHTMOST: u16 = 1 << 9;
    /// Page contains variable-length records.
    pub const VARIABLE_LENGTH: u16 = 1 << 10;

    /// Creates empty flags.
    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Creates flags from raw bits.
    #[inline]
    #[must_use]
    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    /// Returns the raw bits.
    #[inline]
    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Sets a flag.
    #[inline]
    pub fn set(&mut self, flag: u16) {
        self.0 |= flag;
    }

    /// Clears a flag.
    #[inline]
    pub fn clear(&mut self, flag: u16) {
        self.0 &= !flag;
    }

    /// Checks if a flag is set.
    #[inline]
    #[must_use]
    pub const fn is_set(self, flag: u16) -> bool {
        (self.0 & flag) != 0
    }

    /// Returns true if the page is dirty.
    #[inline]
    #[must_use]
    pub const fn is_dirty(self) -> bool {
        self.is_set(Self::DIRTY)
    }

    /// Returns true if the page is pinned.
    #[inline]
    #[must_use]
    pub const fn is_pinned(self) -> bool {
        self.is_set(Self::PINNED)
    }

    /// Returns true if the page has overflow data.
    #[inline]
    #[must_use]
    pub const fn has_overflow(self) -> bool {
        self.is_set(Self::HAS_OVERFLOW)
    }

    /// Returns true if the page is compressed.
    #[inline]
    #[must_use]
    pub const fn is_compressed(self) -> bool {
        self.is_set(Self::COMPRESSED)
    }

    /// Returns true if the page has tombstones.
    #[inline]
    #[must_use]
    pub const fn has_tombstones(self) -> bool {
        self.is_set(Self::HAS_TOMBSTONES)
    }

    /// Returns true if the page needs compaction.
    #[inline]
    #[must_use]
    pub const fn needs_compaction(self) -> bool {
        self.is_set(Self::NEEDS_COMPACTION)
    }

    /// Returns true if this is a root page.
    #[inline]
    #[must_use]
    pub const fn is_root(self) -> bool {
        self.is_set(Self::IS_ROOT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_type_from_u8() {
        assert_eq!(PageType::from_u8(0), Some(PageType::Free));
        assert_eq!(PageType::from_u8(1), Some(PageType::Data));
        assert_eq!(PageType::from_u8(2), Some(PageType::Internal));
        assert_eq!(PageType::from_u8(3), Some(PageType::Leaf));
        assert_eq!(PageType::from_u8(4), Some(PageType::Overflow));
        assert_eq!(PageType::from_u8(255), None);
    }

    #[test]
    fn test_page_type_predicates() {
        assert!(PageType::Internal.is_btree_page());
        assert!(PageType::Leaf.is_btree_page());
        assert!(!PageType::Data.is_btree_page());

        assert!(PageType::Data.is_data_page());
        assert!(PageType::Leaf.is_data_page());
        assert!(PageType::Overflow.is_data_page());
        assert!(!PageType::Internal.is_data_page());

        assert!(PageType::Free.is_free());
        assert!(!PageType::Data.is_free());
    }

    #[test]
    fn test_page_flags() {
        let mut flags = PageFlags::empty();
        assert!(!flags.is_dirty());
        assert!(!flags.is_pinned());

        flags.set(PageFlags::DIRTY);
        assert!(flags.is_dirty());
        assert!(!flags.is_pinned());

        flags.set(PageFlags::PINNED);
        assert!(flags.is_dirty());
        assert!(flags.is_pinned());

        flags.clear(PageFlags::DIRTY);
        assert!(!flags.is_dirty());
        assert!(flags.is_pinned());
    }

    #[test]
    fn test_page_flags_from_bits() {
        let flags = PageFlags::from_bits(PageFlags::DIRTY | PageFlags::HAS_OVERFLOW);
        assert!(flags.is_dirty());
        assert!(flags.has_overflow());
        assert!(!flags.is_pinned());
    }
}
