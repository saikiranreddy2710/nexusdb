//! Node types for the SageTree storage engine.
//!
//! This module defines the B-tree node types:
//! - Internal nodes: Store keys and child pointers
//! - Leaf nodes: Store key-value pairs with MVCC metadata
//!
//! SageTree uses a Bw-tree inspired design where updates can be applied as
//! delta records to avoid in-place modifications.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use nexus_common::types::{Key, Lsn, PageId, TxnId, Value};

use super::error::{SageTreeError, SageTreeResult};

/// Magic number for node validation.
pub const NODE_MAGIC: u16 = 0x5354; // "ST" for SageTree

/// Node type discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    /// Internal node with keys and child pointers.
    Internal = 1,
    /// Leaf node with key-value pairs.
    Leaf = 2,
    /// Overflow page for large values.
    Overflow = 3,
    /// Delta record (for Bw-tree style updates).
    Delta = 4,
}

impl NodeType {
    /// Converts a byte to NodeType.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Internal),
            2 => Some(Self::Leaf),
            3 => Some(Self::Overflow),
            4 => Some(Self::Delta),
            _ => None,
        }
    }

    /// Converts NodeType to byte.
    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Flags for node state.
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeFlags(u8);

impl NodeFlags {
    /// No flags set.
    pub const NONE: Self = Self(0);
    /// Node is dirty and needs to be written.
    pub const DIRTY: Self = Self(1 << 0);
    /// Node is being consolidated.
    pub const CONSOLIDATING: Self = Self(1 << 1);
    /// Node is the root of the tree.
    pub const IS_ROOT: Self = Self(1 << 2);
    /// Node has been deleted (tombstone).
    pub const DELETED: Self = Self(1 << 3);
    /// Node has delta records attached.
    pub const HAS_DELTA: Self = Self(1 << 4);

    /// Checks if a flag is set.
    #[inline]
    pub fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Sets a flag.
    #[inline]
    pub fn set(&mut self, flag: Self) {
        self.0 |= flag.0;
    }

    /// Clears a flag.
    #[inline]
    pub fn clear(&mut self, flag: Self) {
        self.0 &= !flag.0;
    }

    /// Returns the raw byte value.
    #[inline]
    pub fn as_byte(self) -> u8 {
        self.0
    }

    /// Creates from a byte value.
    #[inline]
    pub fn from_byte(b: u8) -> Self {
        Self(b)
    }
}

/// Header common to all node types.
///
/// Layout (40 bytes):
/// - magic: u16 (2 bytes)
/// - node_type: u8 (1 byte)
/// - flags: u8 (1 byte)
/// - level: u16 (2 bytes) - 0 for leaf, >0 for internal
/// - key_count: u16 (2 bytes)
/// - delta_count: u16 (2 bytes)
/// - reserved: u16 (2 bytes)
/// - page_id: u64 (8 bytes)
/// - next_page: u64 (8 bytes) - for leaf node chaining
/// - prev_page: u64 (8 bytes) - for leaf node chaining
/// - checksum: u32 (4 bytes)
#[derive(Debug, Clone)]
pub struct NodeHeader {
    /// Magic number for validation.
    pub magic: u16,
    /// Type of this node.
    pub node_type: NodeType,
    /// Node flags.
    pub flags: NodeFlags,
    /// Level in the tree (0 = leaf, increases toward root).
    pub level: u16,
    /// Number of keys in this node.
    pub key_count: u16,
    /// Number of delta records attached.
    pub delta_count: u16,
    /// Page ID of this node.
    pub page_id: PageId,
    /// Next sibling page (for leaf nodes).
    pub next_page: PageId,
    /// Previous sibling page (for leaf nodes).
    pub prev_page: PageId,
    /// Checksum of node data.
    pub checksum: u32,
}

/// Size of the node header in bytes.
pub const NODE_HEADER_SIZE: usize = 40;

impl NodeHeader {
    /// Creates a new header for a leaf node.
    pub fn new_leaf(page_id: PageId) -> Self {
        Self {
            magic: NODE_MAGIC,
            node_type: NodeType::Leaf,
            flags: NodeFlags::NONE,
            level: 0,
            key_count: 0,
            delta_count: 0,
            page_id,
            next_page: PageId::INVALID,
            prev_page: PageId::INVALID,
            checksum: 0,
        }
    }

    /// Creates a new header for an internal node.
    pub fn new_internal(page_id: PageId, level: u16) -> Self {
        Self {
            magic: NODE_MAGIC,
            node_type: NodeType::Internal,
            flags: NodeFlags::NONE,
            level,
            key_count: 0,
            delta_count: 0,
            page_id,
            next_page: PageId::INVALID,
            prev_page: PageId::INVALID,
            checksum: 0,
        }
    }

    /// Serializes the header to bytes.
    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u16_le(self.magic);
        buf.put_u8(self.node_type.as_byte());
        buf.put_u8(self.flags.as_byte());
        buf.put_u16_le(self.level);
        buf.put_u16_le(self.key_count);
        buf.put_u16_le(self.delta_count);
        buf.put_u16_le(0); // reserved
        buf.put_u64_le(self.page_id.as_u64());
        buf.put_u64_le(self.next_page.as_u64());
        buf.put_u64_le(self.prev_page.as_u64());
        buf.put_u32_le(self.checksum);
    }

    /// Deserializes a header from bytes.
    pub fn deserialize(buf: &mut impl Buf) -> SageTreeResult<Self> {
        if buf.remaining() < NODE_HEADER_SIZE {
            return Err(SageTreeError::deserialization(
                "buffer too small for header",
            ));
        }

        let magic = buf.get_u16_le();
        if magic != NODE_MAGIC {
            return Err(SageTreeError::deserialization(format!(
                "invalid node magic: expected 0x{:04X}, got 0x{:04X}",
                NODE_MAGIC, magic
            )));
        }

        let node_type = NodeType::from_byte(buf.get_u8())
            .ok_or_else(|| SageTreeError::deserialization("invalid node type"))?;
        let flags = NodeFlags::from_byte(buf.get_u8());
        let level = buf.get_u16_le();
        let key_count = buf.get_u16_le();
        let delta_count = buf.get_u16_le();
        let _reserved = buf.get_u16_le();
        let page_id = PageId::new(buf.get_u64_le());
        let next_page = PageId::new(buf.get_u64_le());
        let prev_page = PageId::new(buf.get_u64_le());
        let checksum = buf.get_u32_le();

        Ok(Self {
            magic,
            node_type,
            flags,
            level,
            key_count,
            delta_count,
            page_id,
            next_page,
            prev_page,
            checksum,
        })
    }
}

/// Version information for MVCC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInfo {
    /// Transaction that created this version.
    pub created_by: TxnId,
    /// Transaction that deleted this version (if any).
    pub deleted_by: TxnId,
    /// LSN of the creation.
    pub created_lsn: Lsn,
    /// LSN of the deletion (if any).
    pub deleted_lsn: Lsn,
}

impl Default for VersionInfo {
    fn default() -> Self {
        Self {
            created_by: TxnId::INVALID,
            deleted_by: TxnId::INVALID,
            created_lsn: Lsn::INVALID,
            deleted_lsn: Lsn::INVALID,
        }
    }
}

impl VersionInfo {
    /// Creates version info for a new record.
    pub fn new(txn_id: TxnId, lsn: Lsn) -> Self {
        Self {
            created_by: txn_id,
            deleted_by: TxnId::INVALID,
            created_lsn: lsn,
            deleted_lsn: Lsn::INVALID,
        }
    }

    /// Marks this version as deleted.
    pub fn mark_deleted(&mut self, txn_id: TxnId, lsn: Lsn) {
        self.deleted_by = txn_id;
        self.deleted_lsn = lsn;
    }

    /// Checks if this version is deleted.
    pub fn is_deleted(&self) -> bool {
        self.deleted_by.is_valid()
    }

    /// Checks if this version is visible to the given transaction.
    ///
    /// A version is visible if:
    /// 1. It was created by a committed transaction before this one, and
    /// 2. It was not deleted, or was deleted by a transaction after this one
    pub fn is_visible_to(&self, txn_id: TxnId) -> bool {
        // Simple visibility check: created before txn_id and not deleted
        // Full MVCC would check commit timestamps
        self.created_by < txn_id && (!self.is_deleted() || self.deleted_by > txn_id)
    }

    /// Serializes the version info to bytes (32 bytes).
    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.created_by.as_u64());
        buf.put_u64_le(self.deleted_by.as_u64());
        buf.put_u64_le(self.created_lsn.as_u64());
        buf.put_u64_le(self.deleted_lsn.as_u64());
    }

    /// Deserializes version info from bytes.
    pub fn deserialize(buf: &mut impl Buf) -> SageTreeResult<Self> {
        if buf.remaining() < 32 {
            return Err(SageTreeError::deserialization(
                "buffer too small for version info",
            ));
        }

        Ok(Self {
            created_by: TxnId::new(buf.get_u64_le()),
            deleted_by: TxnId::new(buf.get_u64_le()),
            created_lsn: Lsn::new(buf.get_u64_le()),
            deleted_lsn: Lsn::new(buf.get_u64_le()),
        })
    }
}

/// Size of version info in bytes.
pub const VERSION_INFO_SIZE: usize = 32;

/// Entry in a leaf node.
#[derive(Debug, Clone)]
pub struct LeafEntry {
    /// The key.
    pub key: Key,
    /// The value.
    pub value: Value,
    /// Version information for MVCC.
    pub version: VersionInfo,
}

impl LeafEntry {
    /// Creates a new leaf entry.
    pub fn new(key: Key, value: Value, version: VersionInfo) -> Self {
        Self {
            key,
            value,
            version,
        }
    }

    /// Creates a new leaf entry without versioning (for non-MVCC use).
    pub fn simple(key: Key, value: Value) -> Self {
        Self {
            key,
            value,
            version: VersionInfo::default(),
        }
    }

    /// Returns the serialized size of this entry.
    pub fn serialized_size(&self) -> usize {
        // key_len(4) + key + value_len(4) + value + version_info(32)
        4 + self.key.len() + 4 + self.value.len() + VERSION_INFO_SIZE
    }

    /// Serializes the entry to bytes.
    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.key.len() as u32);
        buf.put_slice(self.key.as_bytes());
        buf.put_u32_le(self.value.len() as u32);
        buf.put_slice(self.value.as_bytes());
        self.version.serialize(buf);
    }

    /// Deserializes an entry from bytes.
    pub fn deserialize(buf: &mut impl Buf) -> SageTreeResult<Self> {
        if buf.remaining() < 8 {
            return Err(SageTreeError::deserialization(
                "buffer too small for entry header",
            ));
        }

        let key_len = buf.get_u32_le() as usize;
        if buf.remaining() < key_len {
            return Err(SageTreeError::deserialization("buffer too small for key"));
        }
        let mut key_bytes = vec![0u8; key_len];
        buf.copy_to_slice(&mut key_bytes);
        let key = Key::from_vec(key_bytes);

        if buf.remaining() < 4 {
            return Err(SageTreeError::deserialization(
                "buffer too small for value length",
            ));
        }
        let value_len = buf.get_u32_le() as usize;
        if buf.remaining() < value_len {
            return Err(SageTreeError::deserialization("buffer too small for value"));
        }
        let mut value_bytes = vec![0u8; value_len];
        buf.copy_to_slice(&mut value_bytes);
        let value = Value::from_vec(value_bytes);

        let version = VersionInfo::deserialize(buf)?;

        Ok(Self {
            key,
            value,
            version,
        })
    }
}

/// Entry in an internal node (separator key + child pointer).
#[derive(Debug, Clone)]
pub struct InternalEntry {
    /// Separator key.
    pub key: Key,
    /// Page ID of the child node.
    pub child: PageId,
}

impl InternalEntry {
    /// Creates a new internal entry.
    pub fn new(key: Key, child: PageId) -> Self {
        Self { key, child }
    }

    /// Returns the serialized size of this entry.
    pub fn serialized_size(&self) -> usize {
        // key_len(4) + key + child_page_id(8)
        4 + self.key.len() + 8
    }

    /// Serializes the entry to bytes.
    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.key.len() as u32);
        buf.put_slice(self.key.as_bytes());
        buf.put_u64_le(self.child.as_u64());
    }

    /// Deserializes an entry from bytes.
    pub fn deserialize(buf: &mut impl Buf) -> SageTreeResult<Self> {
        if buf.remaining() < 4 {
            return Err(SageTreeError::deserialization(
                "buffer too small for key length",
            ));
        }

        let key_len = buf.get_u32_le() as usize;
        if buf.remaining() < key_len + 8 {
            return Err(SageTreeError::deserialization(
                "buffer too small for internal entry",
            ));
        }

        let mut key_bytes = vec![0u8; key_len];
        buf.copy_to_slice(&mut key_bytes);
        let key = Key::from_vec(key_bytes);

        let child = PageId::new(buf.get_u64_le());

        Ok(Self { key, child })
    }
}

/// A leaf node containing key-value pairs.
#[derive(Debug, Clone)]
pub struct LeafNode {
    /// Node header.
    pub header: NodeHeader,
    /// Sorted list of entries.
    pub entries: Vec<LeafEntry>,
}

impl LeafNode {
    /// Creates a new empty leaf node.
    pub fn new(page_id: PageId) -> Self {
        Self {
            header: NodeHeader::new_leaf(page_id),
            entries: Vec::new(),
        }
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the node is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Finds the position for a key using binary search.
    pub fn search(&self, key: &Key) -> Result<usize, usize> {
        self.entries.binary_search_by(|e| e.key.cmp(key))
    }

    /// Gets the value for a key.
    pub fn get(&self, key: &Key) -> Option<&LeafEntry> {
        match self.search(key) {
            Ok(idx) => Some(&self.entries[idx]),
            Err(_) => None,
        }
    }

    /// Inserts a new entry, maintaining sorted order.
    /// Returns Err if key already exists.
    pub fn insert(&mut self, entry: LeafEntry) -> SageTreeResult<()> {
        match self.search(&entry.key) {
            Ok(_) => Err(SageTreeError::DuplicateKey),
            Err(idx) => {
                self.entries.insert(idx, entry);
                self.header.key_count = self.entries.len() as u16;
                Ok(())
            }
        }
    }

    /// Updates an existing entry.
    /// Returns Err if key doesn't exist.
    pub fn update(&mut self, key: &Key, value: Value, version: VersionInfo) -> SageTreeResult<()> {
        match self.search(key) {
            Ok(idx) => {
                self.entries[idx].value = value;
                self.entries[idx].version = version;
                Ok(())
            }
            Err(_) => Err(SageTreeError::KeyNotFound),
        }
    }

    /// Upserts an entry (insert or update).
    pub fn upsert(&mut self, entry: LeafEntry) {
        match self.search(&entry.key) {
            Ok(idx) => {
                self.entries[idx] = entry;
            }
            Err(idx) => {
                self.entries.insert(idx, entry);
                self.header.key_count = self.entries.len() as u16;
            }
        }
    }

    /// Removes an entry by key.
    /// Returns the removed entry or Err if not found.
    pub fn remove(&mut self, key: &Key) -> SageTreeResult<LeafEntry> {
        match self.search(key) {
            Ok(idx) => {
                let entry = self.entries.remove(idx);
                self.header.key_count = self.entries.len() as u16;
                Ok(entry)
            }
            Err(_) => Err(SageTreeError::KeyNotFound),
        }
    }

    /// Returns the first key in the node.
    pub fn first_key(&self) -> Option<&Key> {
        self.entries.first().map(|e| &e.key)
    }

    /// Returns the last key in the node.
    pub fn last_key(&self) -> Option<&Key> {
        self.entries.last().map(|e| &e.key)
    }

    /// Splits the node at the middle, returning the new right node.
    pub fn split(&mut self, new_page_id: PageId) -> LeafNode {
        let mid = self.entries.len() / 2;
        let right_entries: Vec<_> = self.entries.drain(mid..).collect();

        let mut right = LeafNode::new(new_page_id);
        right.entries = right_entries;
        right.header.key_count = right.entries.len() as u16;
        self.header.key_count = self.entries.len() as u16;

        // Update sibling links
        right.header.prev_page = self.header.page_id;
        right.header.next_page = self.header.next_page;
        self.header.next_page = new_page_id;

        right
    }

    /// Merges another leaf node into this one.
    pub fn merge(&mut self, other: LeafNode) {
        self.entries.extend(other.entries);
        self.header.key_count = self.entries.len() as u16;
        self.header.next_page = other.header.next_page;
    }

    /// Calculates the total serialized size of all entries.
    pub fn entries_size(&self) -> usize {
        self.entries.iter().map(|e| e.serialized_size()).sum()
    }

    /// Serializes the leaf node to bytes.
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(NODE_HEADER_SIZE + self.entries_size());
        self.header.serialize(&mut buf);
        for entry in &self.entries {
            entry.serialize(&mut buf);
        }
        buf.freeze()
    }

    /// Deserializes a leaf node from bytes.
    pub fn deserialize(data: &[u8]) -> SageTreeResult<Self> {
        let mut buf = Bytes::copy_from_slice(data);
        let header = NodeHeader::deserialize(&mut buf)?;

        if header.node_type != NodeType::Leaf {
            return Err(SageTreeError::InvalidNodeType {
                expected: "Leaf",
                found: format!("{:?}", header.node_type),
            });
        }

        let mut entries = Vec::with_capacity(header.key_count as usize);
        for _ in 0..header.key_count {
            entries.push(LeafEntry::deserialize(&mut buf)?);
        }

        Ok(Self { header, entries })
    }
}

/// An internal node containing keys and child pointers.
#[derive(Debug, Clone)]
pub struct InternalNode {
    /// Node header.
    pub header: NodeHeader,
    /// Leftmost child (before first key).
    pub leftmost_child: PageId,
    /// Sorted list of entries (key, right_child).
    /// For key[i], children less than key[i] go to children[i],
    /// children >= key[i] go to children[i+1].
    pub entries: Vec<InternalEntry>,
}

impl InternalNode {
    /// Creates a new empty internal node.
    pub fn new(page_id: PageId, level: u16) -> Self {
        Self {
            header: NodeHeader::new_internal(page_id, level),
            leftmost_child: PageId::INVALID,
            entries: Vec::new(),
        }
    }

    /// Returns the number of keys.
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the node is empty (no keys).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the total number of children.
    pub fn child_count(&self) -> usize {
        if self.leftmost_child.is_valid() {
            self.entries.len() + 1
        } else {
            0
        }
    }

    /// Finds the child page for a key.
    pub fn find_child(&self, key: &Key) -> PageId {
        for entry in &self.entries {
            if key < &entry.key {
                // Key is less than this separator, go to left child
                // For the first entry, this is leftmost_child
                // For subsequent entries, this is the previous entry's child
                if let Some(prev_idx) = self.entries.iter().position(|e| &e.key == &entry.key) {
                    if prev_idx == 0 {
                        return self.leftmost_child;
                    } else {
                        return self.entries[prev_idx - 1].child;
                    }
                }
            }
        }
        // Key is >= all separators, go to rightmost child
        self.entries
            .last()
            .map(|e| e.child)
            .unwrap_or(self.leftmost_child)
    }

    /// Finds the child page for a key using binary search.
    pub fn find_child_binary(&self, key: &Key) -> PageId {
        match self.entries.binary_search_by(|e| e.key.cmp(key)) {
            Ok(idx) => {
                // Exact match: go to the child of this entry (right subtree)
                self.entries[idx].child
            }
            Err(idx) => {
                // No exact match: idx is where the key would be inserted
                if idx == 0 {
                    self.leftmost_child
                } else {
                    self.entries[idx - 1].child
                }
            }
        }
    }

    /// Inserts a new separator key and child.
    /// The child is placed to the right of the key.
    pub fn insert(&mut self, key: Key, child: PageId) {
        let entry = InternalEntry::new(key.clone(), child);
        match self.entries.binary_search_by(|e| e.key.cmp(&key)) {
            Ok(_) => {
                // Key already exists, update child
                if let Some(pos) = self.entries.iter().position(|e| e.key == key) {
                    self.entries[pos].child = child;
                }
            }
            Err(idx) => {
                self.entries.insert(idx, entry);
                self.header.key_count = self.entries.len() as u16;
            }
        }
    }

    /// Removes a separator key and returns its child.
    pub fn remove(&mut self, key: &Key) -> Option<PageId> {
        if let Some(pos) = self.entries.iter().position(|e| &e.key == key) {
            let entry = self.entries.remove(pos);
            self.header.key_count = self.entries.len() as u16;
            Some(entry.child)
        } else {
            None
        }
    }

    /// Returns the first separator key.
    pub fn first_key(&self) -> Option<&Key> {
        self.entries.first().map(|e| &e.key)
    }

    /// Returns the last separator key.
    pub fn last_key(&self) -> Option<&Key> {
        self.entries.last().map(|e| &e.key)
    }

    /// Gets all children page IDs.
    pub fn children(&self) -> Vec<PageId> {
        let mut result = Vec::with_capacity(self.entries.len() + 1);
        if self.leftmost_child.is_valid() {
            result.push(self.leftmost_child);
        }
        for entry in &self.entries {
            result.push(entry.child);
        }
        result
    }

    /// Splits the node at the middle, returning (middle_key, new_right_node).
    pub fn split(&mut self, new_page_id: PageId) -> (Key, InternalNode) {
        let mid = self.entries.len() / 2;

        // The middle key becomes the separator in the parent
        let middle_key = self.entries[mid].key.clone();

        // Right node gets entries after the middle
        let right_entries: Vec<_> = self.entries.drain(mid + 1..).collect();

        let mut right = InternalNode::new(new_page_id, self.header.level);
        right.leftmost_child = self
            .entries
            .pop()
            .map(|e| e.child)
            .unwrap_or(PageId::INVALID);
        right.entries = right_entries;
        right.header.key_count = right.entries.len() as u16;
        self.header.key_count = self.entries.len() as u16;

        (middle_key, right)
    }

    /// Merges another internal node into this one with a separator key.
    pub fn merge(&mut self, separator: Key, other: InternalNode) {
        self.entries
            .push(InternalEntry::new(separator, other.leftmost_child));
        self.entries.extend(other.entries);
        self.header.key_count = self.entries.len() as u16;
    }

    /// Calculates the total serialized size of all entries.
    pub fn entries_size(&self) -> usize {
        8 + self
            .entries
            .iter()
            .map(|e| e.serialized_size())
            .sum::<usize>()
    }

    /// Serializes the internal node to bytes.
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(NODE_HEADER_SIZE + self.entries_size());
        self.header.serialize(&mut buf);
        buf.put_u64_le(self.leftmost_child.as_u64());
        for entry in &self.entries {
            entry.serialize(&mut buf);
        }
        buf.freeze()
    }

    /// Deserializes an internal node from bytes.
    pub fn deserialize(data: &[u8]) -> SageTreeResult<Self> {
        let mut buf = Bytes::copy_from_slice(data);
        let header = NodeHeader::deserialize(&mut buf)?;

        if header.node_type != NodeType::Internal {
            return Err(SageTreeError::InvalidNodeType {
                expected: "Internal",
                found: format!("{:?}", header.node_type),
            });
        }

        let leftmost_child = PageId::new(buf.get_u64_le());

        let mut entries = Vec::with_capacity(header.key_count as usize);
        for _ in 0..header.key_count {
            entries.push(InternalEntry::deserialize(&mut buf)?);
        }

        Ok(Self {
            header,
            leftmost_child,
            entries,
        })
    }
}

/// Unified node type that can be either internal or leaf.
#[derive(Debug, Clone)]
pub enum Node {
    /// Internal node.
    Internal(InternalNode),
    /// Leaf node.
    Leaf(LeafNode),
}

impl Node {
    /// Returns the page ID of this node.
    pub fn page_id(&self) -> PageId {
        match self {
            Node::Internal(n) => n.header.page_id,
            Node::Leaf(n) => n.header.page_id,
        }
    }

    /// Returns the level of this node.
    pub fn level(&self) -> u16 {
        match self {
            Node::Internal(n) => n.header.level,
            Node::Leaf(_) => 0,
        }
    }

    /// Returns true if this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf(_))
    }

    /// Returns true if this is an internal node.
    pub fn is_internal(&self) -> bool {
        matches!(self, Node::Internal(_))
    }

    /// Returns the key count.
    pub fn key_count(&self) -> usize {
        match self {
            Node::Internal(n) => n.key_count(),
            Node::Leaf(n) => n.len(),
        }
    }

    /// Serializes the node to bytes.
    pub fn serialize(&self) -> Bytes {
        match self {
            Node::Internal(n) => n.serialize(),
            Node::Leaf(n) => n.serialize(),
        }
    }

    /// Deserializes a node from bytes, determining type automatically.
    pub fn deserialize(data: &[u8]) -> SageTreeResult<Self> {
        if data.len() < 3 {
            return Err(SageTreeError::deserialization("buffer too small"));
        }

        // Peek at the node type (offset 2 in header)
        let node_type = NodeType::from_byte(data[2])
            .ok_or_else(|| SageTreeError::deserialization("invalid node type"))?;

        match node_type {
            NodeType::Internal => Ok(Node::Internal(InternalNode::deserialize(data)?)),
            NodeType::Leaf => Ok(Node::Leaf(LeafNode::deserialize(data)?)),
            _ => Err(SageTreeError::deserialization(format!(
                "unsupported node type: {:?}",
                node_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_type() {
        assert_eq!(NodeType::from_byte(1), Some(NodeType::Internal));
        assert_eq!(NodeType::from_byte(2), Some(NodeType::Leaf));
        assert_eq!(NodeType::from_byte(0), None);
    }

    #[test]
    fn test_node_flags() {
        let mut flags = NodeFlags::NONE;
        assert!(!flags.contains(NodeFlags::DIRTY));

        flags.set(NodeFlags::DIRTY);
        assert!(flags.contains(NodeFlags::DIRTY));

        flags.set(NodeFlags::IS_ROOT);
        assert!(flags.contains(NodeFlags::DIRTY));
        assert!(flags.contains(NodeFlags::IS_ROOT));

        flags.clear(NodeFlags::DIRTY);
        assert!(!flags.contains(NodeFlags::DIRTY));
        assert!(flags.contains(NodeFlags::IS_ROOT));
    }

    #[test]
    fn test_node_header_serialization() {
        let header = NodeHeader::new_leaf(PageId::new(42));

        let mut buf = BytesMut::new();
        header.serialize(&mut buf);
        assert_eq!(buf.len(), NODE_HEADER_SIZE);

        let mut read_buf = buf.freeze();
        let deserialized = NodeHeader::deserialize(&mut read_buf).unwrap();

        assert_eq!(deserialized.magic, NODE_MAGIC);
        assert_eq!(deserialized.node_type, NodeType::Leaf);
        assert_eq!(deserialized.page_id.as_u64(), 42);
    }

    #[test]
    fn test_version_info() {
        let mut version = VersionInfo::new(TxnId::new(1), Lsn::new(100));
        assert!(!version.is_deleted());
        assert!(version.is_visible_to(TxnId::new(2)));

        version.mark_deleted(TxnId::new(3), Lsn::new(200));
        assert!(version.is_deleted());
        assert!(version.is_visible_to(TxnId::new(2)));
        assert!(!version.is_visible_to(TxnId::new(4)));
    }

    #[test]
    fn test_leaf_entry_serialization() {
        let entry = LeafEntry::simple(Key::from_str("hello"), Value::from_str("world"));

        let mut buf = BytesMut::new();
        entry.serialize(&mut buf);

        let mut read_buf = buf.freeze();
        let deserialized = LeafEntry::deserialize(&mut read_buf).unwrap();

        assert_eq!(deserialized.key.as_bytes(), b"hello");
        assert_eq!(deserialized.value.as_bytes(), b"world");
    }

    #[test]
    fn test_leaf_node_operations() {
        let mut leaf = LeafNode::new(PageId::new(1));

        // Insert entries
        leaf.insert(LeafEntry::simple(Key::from_str("c"), Value::from_str("3")))
            .unwrap();
        leaf.insert(LeafEntry::simple(Key::from_str("a"), Value::from_str("1")))
            .unwrap();
        leaf.insert(LeafEntry::simple(Key::from_str("b"), Value::from_str("2")))
            .unwrap();

        assert_eq!(leaf.len(), 3);

        // Check ordering
        assert_eq!(leaf.entries[0].key.as_bytes(), b"a");
        assert_eq!(leaf.entries[1].key.as_bytes(), b"b");
        assert_eq!(leaf.entries[2].key.as_bytes(), b"c");

        // Get
        let entry = leaf.get(&Key::from_str("b")).unwrap();
        assert_eq!(entry.value.as_bytes(), b"2");

        // Duplicate key
        let result = leaf.insert(LeafEntry::simple(
            Key::from_str("a"),
            Value::from_str("dup"),
        ));
        assert!(matches!(result, Err(SageTreeError::DuplicateKey)));

        // Update
        leaf.update(
            &Key::from_str("b"),
            Value::from_str("updated"),
            VersionInfo::default(),
        )
        .unwrap();
        assert_eq!(
            leaf.get(&Key::from_str("b")).unwrap().value.as_bytes(),
            b"updated"
        );

        // Remove
        let removed = leaf.remove(&Key::from_str("a")).unwrap();
        assert_eq!(removed.key.as_bytes(), b"a");
        assert_eq!(leaf.len(), 2);
    }

    #[test]
    fn test_leaf_node_serialization() {
        let mut leaf = LeafNode::new(PageId::new(1));
        leaf.insert(LeafEntry::simple(
            Key::from_str("key1"),
            Value::from_str("value1"),
        ))
        .unwrap();
        leaf.insert(LeafEntry::simple(
            Key::from_str("key2"),
            Value::from_str("value2"),
        ))
        .unwrap();

        let serialized = leaf.serialize();
        let deserialized = LeafNode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized.entries[0].key.as_bytes(), b"key1");
        assert_eq!(deserialized.entries[1].key.as_bytes(), b"key2");
    }

    #[test]
    fn test_leaf_node_split() {
        let mut leaf = LeafNode::new(PageId::new(1));
        for i in 0..10 {
            leaf.insert(LeafEntry::simple(
                Key::from_str(&format!("key{:02}", i)),
                Value::from_str(&format!("val{}", i)),
            ))
            .unwrap();
        }

        let right = leaf.split(PageId::new(2));

        assert_eq!(leaf.len(), 5);
        assert_eq!(right.len(), 5);
        assert!(leaf.last_key().unwrap() < right.first_key().unwrap());
        assert_eq!(leaf.header.next_page, right.header.page_id);
        assert_eq!(right.header.prev_page, leaf.header.page_id);
    }

    #[test]
    fn test_internal_node_operations() {
        let mut node = InternalNode::new(PageId::new(1), 1);
        node.leftmost_child = PageId::new(10);

        node.insert(Key::from_str("m"), PageId::new(20));
        node.insert(Key::from_str("g"), PageId::new(15));
        node.insert(Key::from_str("t"), PageId::new(25));

        assert_eq!(node.key_count(), 3);
        assert_eq!(node.child_count(), 4);

        // Find child for different keys
        assert_eq!(node.find_child_binary(&Key::from_str("a")), PageId::new(10)); // < g
        assert_eq!(node.find_child_binary(&Key::from_str("g")), PageId::new(15)); // = g
        assert_eq!(node.find_child_binary(&Key::from_str("h")), PageId::new(15)); // > g, < m
        assert_eq!(node.find_child_binary(&Key::from_str("p")), PageId::new(20)); // > m, < t
        assert_eq!(node.find_child_binary(&Key::from_str("z")), PageId::new(25));
        // > t
    }

    #[test]
    fn test_internal_node_serialization() {
        let mut node = InternalNode::new(PageId::new(1), 1);
        node.leftmost_child = PageId::new(10);
        node.insert(Key::from_str("mid"), PageId::new(20));

        let serialized = node.serialize();
        let deserialized = InternalNode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.leftmost_child, PageId::new(10));
        assert_eq!(deserialized.key_count(), 1);
        assert_eq!(deserialized.entries[0].key.as_bytes(), b"mid");
    }

    #[test]
    fn test_internal_node_split() {
        let mut node = InternalNode::new(PageId::new(1), 1);
        node.leftmost_child = PageId::new(100);
        for i in 0..9 {
            node.insert(
                Key::from_str(&format!("key{}", i)),
                PageId::new(100 + i as u64 + 1),
            );
        }

        let (mid_key, right) = node.split(PageId::new(2));

        assert!(node.key_count() > 0);
        assert!(right.key_count() > 0);
        assert!(node.last_key().unwrap() < &mid_key);
        assert!(right.first_key().is_none() || &mid_key < right.first_key().unwrap());
    }

    #[test]
    fn test_node_enum() {
        let leaf = Node::Leaf(LeafNode::new(PageId::new(1)));
        assert!(leaf.is_leaf());
        assert!(!leaf.is_internal());
        assert_eq!(leaf.level(), 0);

        let internal = Node::Internal(InternalNode::new(PageId::new(2), 1));
        assert!(internal.is_internal());
        assert!(!internal.is_leaf());
        assert_eq!(internal.level(), 1);
    }

    #[test]
    fn test_node_unified_serialization() {
        let mut leaf = LeafNode::new(PageId::new(1));
        leaf.insert(LeafEntry::simple(Key::from_str("k"), Value::from_str("v")))
            .unwrap();

        let node = Node::Leaf(leaf);
        let serialized = node.serialize();
        let deserialized = Node::deserialize(&serialized).unwrap();

        assert!(deserialized.is_leaf());
        if let Node::Leaf(n) = deserialized {
            assert_eq!(n.len(), 1);
        }
    }
}
