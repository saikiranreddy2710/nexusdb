//! Delta chain implementation for Bw-tree style updates.
//!
//! Delta chains allow updates to be applied without modifying the base page,
//! reducing write amplification and enabling lock-free reads. The chain is
//! periodically consolidated when it exceeds a threshold length.
//!
//! # Design
//!
//! Each delta record represents a single modification:
//! - Insert: Add a new key-value pair
//! - Update: Modify an existing value
//! - Delete: Remove a key
//! - Split: Node was split (structural modification marker)
//! - Merge: Node was merged (structural modification marker)
//!
//! Delta records are prepended to the chain, so the most recent changes
//! are at the head. When reading, the chain is traversed from head to tail,
//! applying deltas to reconstruct the current state.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use nexus_common::types::{Key, Lsn, PageId, TxnId, Value};

use super::error::{SageTreeError, SageTreeResult};
use super::node::{LeafEntry, LeafNode, VersionInfo};

/// Magic number for delta record validation.
pub const DELTA_MAGIC: u16 = 0x4454; // "DT" for Delta

/// Type of delta operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DeltaType {
    /// Insert a new key-value pair.
    Insert = 1,
    /// Update an existing value.
    Update = 2,
    /// Delete a key.
    Delete = 3,
    /// Node was split (structural modification).
    Split = 4,
    /// Node was merged (structural modification).
    Merge = 5,
    /// Consolidation boundary marker.
    Consolidate = 6,
}

impl DeltaType {
    /// Converts a byte to DeltaType.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Insert),
            2 => Some(Self::Update),
            3 => Some(Self::Delete),
            4 => Some(Self::Split),
            5 => Some(Self::Merge),
            6 => Some(Self::Consolidate),
            _ => None,
        }
    }

    /// Converts DeltaType to byte.
    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// A single delta record in the chain.
#[derive(Debug, Clone)]
pub struct DeltaRecord {
    /// Type of this delta.
    pub delta_type: DeltaType,
    /// Transaction that created this delta.
    pub txn_id: TxnId,
    /// LSN of this delta.
    pub lsn: Lsn,
    /// Key affected by this delta.
    pub key: Key,
    /// New value (for Insert/Update).
    pub value: Option<Value>,
    /// Old value (for Update/Delete, for rollback).
    pub old_value: Option<Value>,
    /// Split/merge information.
    pub split_info: Option<SplitInfo>,
}

/// Information about a split operation.
#[derive(Debug, Clone)]
pub struct SplitInfo {
    /// Page ID of the new sibling.
    pub sibling_page: PageId,
    /// Separator key.
    pub separator_key: Key,
    /// Whether this node is the left (true) or right (false) side.
    pub is_left: bool,
}

impl SplitInfo {
    /// Creates split info for a left node after split.
    pub fn left(sibling_page: PageId, separator_key: Key) -> Self {
        Self {
            sibling_page,
            separator_key,
            is_left: true,
        }
    }

    /// Creates split info for a right node after split.
    pub fn right(sibling_page: PageId, separator_key: Key) -> Self {
        Self {
            sibling_page,
            separator_key,
            is_left: false,
        }
    }

    /// Serializes the split info to bytes.
    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.sibling_page.as_u64());
        buf.put_u32_le(self.separator_key.len() as u32);
        buf.put_slice(self.separator_key.as_bytes());
        buf.put_u8(if self.is_left { 1 } else { 0 });
    }

    /// Deserializes split info from bytes.
    pub fn deserialize(buf: &mut impl Buf) -> SageTreeResult<Self> {
        if buf.remaining() < 13 {
            return Err(SageTreeError::deserialization(
                "buffer too small for split info",
            ));
        }

        let sibling_page = PageId::new(buf.get_u64_le());
        let key_len = buf.get_u32_le() as usize;

        if buf.remaining() < key_len + 1 {
            return Err(SageTreeError::deserialization(
                "buffer too small for split key",
            ));
        }

        let mut key_bytes = vec![0u8; key_len];
        buf.copy_to_slice(&mut key_bytes);
        let separator_key = Key::from_vec(key_bytes);
        let is_left = buf.get_u8() != 0;

        Ok(Self {
            sibling_page,
            separator_key,
            is_left,
        })
    }
}

impl DeltaRecord {
    /// Creates an insert delta.
    pub fn insert(txn_id: TxnId, lsn: Lsn, key: Key, value: Value) -> Self {
        Self {
            delta_type: DeltaType::Insert,
            txn_id,
            lsn,
            key,
            value: Some(value),
            old_value: None,
            split_info: None,
        }
    }

    /// Creates an update delta.
    pub fn update(txn_id: TxnId, lsn: Lsn, key: Key, new_value: Value, old_value: Value) -> Self {
        Self {
            delta_type: DeltaType::Update,
            txn_id,
            lsn,
            key,
            value: Some(new_value),
            old_value: Some(old_value),
            split_info: None,
        }
    }

    /// Creates a delete delta.
    pub fn delete(txn_id: TxnId, lsn: Lsn, key: Key, old_value: Value) -> Self {
        Self {
            delta_type: DeltaType::Delete,
            txn_id,
            lsn,
            key,
            value: None,
            old_value: Some(old_value),
            split_info: None,
        }
    }

    /// Creates a split delta.
    pub fn split(txn_id: TxnId, lsn: Lsn, info: SplitInfo) -> Self {
        Self {
            delta_type: DeltaType::Split,
            txn_id,
            lsn,
            key: info.separator_key.clone(),
            value: None,
            old_value: None,
            split_info: Some(info),
        }
    }

    /// Creates a merge delta.
    pub fn merge(txn_id: TxnId, lsn: Lsn, sibling_page: PageId, separator_key: Key) -> Self {
        Self {
            delta_type: DeltaType::Merge,
            txn_id,
            lsn,
            key: separator_key.clone(),
            value: None,
            old_value: None,
            split_info: Some(SplitInfo {
                sibling_page,
                separator_key,
                is_left: true,
            }),
        }
    }

    /// Returns the serialized size of this delta.
    pub fn serialized_size(&self) -> usize {
        let mut size = 2 + 1 + 8 + 8; // magic + type + txn_id + lsn
        size += 4 + self.key.len(); // key_len + key
        size += 1; // has_value flag
        if let Some(ref v) = self.value {
            size += 4 + v.len();
        }
        size += 1; // has_old_value flag
        if let Some(ref v) = self.old_value {
            size += 4 + v.len();
        }
        size += 1; // has_split_info flag
        if let Some(ref info) = self.split_info {
            size += 8 + 4 + info.separator_key.len() + 1;
        }
        size
    }

    /// Serializes the delta record to bytes.
    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u16_le(DELTA_MAGIC);
        buf.put_u8(self.delta_type.as_byte());
        buf.put_u64_le(self.txn_id.as_u64());
        buf.put_u64_le(self.lsn.as_u64());

        buf.put_u32_le(self.key.len() as u32);
        buf.put_slice(self.key.as_bytes());

        if let Some(ref value) = self.value {
            buf.put_u8(1);
            buf.put_u32_le(value.len() as u32);
            buf.put_slice(value.as_bytes());
        } else {
            buf.put_u8(0);
        }

        if let Some(ref old_value) = self.old_value {
            buf.put_u8(1);
            buf.put_u32_le(old_value.len() as u32);
            buf.put_slice(old_value.as_bytes());
        } else {
            buf.put_u8(0);
        }

        if let Some(ref info) = self.split_info {
            buf.put_u8(1);
            info.serialize(buf);
        } else {
            buf.put_u8(0);
        }
    }

    /// Deserializes a delta record from bytes.
    pub fn deserialize(buf: &mut impl Buf) -> SageTreeResult<Self> {
        if buf.remaining() < 19 {
            return Err(SageTreeError::deserialization(
                "buffer too small for delta header",
            ));
        }

        let magic = buf.get_u16_le();
        if magic != DELTA_MAGIC {
            return Err(SageTreeError::deserialization(format!(
                "invalid delta magic: expected 0x{:04X}, got 0x{:04X}",
                DELTA_MAGIC, magic
            )));
        }

        let delta_type = DeltaType::from_byte(buf.get_u8())
            .ok_or_else(|| SageTreeError::deserialization("invalid delta type"))?;
        let txn_id = TxnId::new(buf.get_u64_le());
        let lsn = Lsn::new(buf.get_u64_le());

        let key_len = buf.get_u32_le() as usize;
        if buf.remaining() < key_len {
            return Err(SageTreeError::deserialization("buffer too small for key"));
        }
        let mut key_bytes = vec![0u8; key_len];
        buf.copy_to_slice(&mut key_bytes);
        let key = Key::from_vec(key_bytes);

        let value = if buf.get_u8() != 0 {
            let len = buf.get_u32_le() as usize;
            if buf.remaining() < len {
                return Err(SageTreeError::deserialization("buffer too small for value"));
            }
            let mut bytes = vec![0u8; len];
            buf.copy_to_slice(&mut bytes);
            Some(Value::from_vec(bytes))
        } else {
            None
        };

        let old_value = if buf.get_u8() != 0 {
            let len = buf.get_u32_le() as usize;
            if buf.remaining() < len {
                return Err(SageTreeError::deserialization(
                    "buffer too small for old_value",
                ));
            }
            let mut bytes = vec![0u8; len];
            buf.copy_to_slice(&mut bytes);
            Some(Value::from_vec(bytes))
        } else {
            None
        };

        let split_info = if buf.get_u8() != 0 {
            Some(SplitInfo::deserialize(buf)?)
        } else {
            None
        };

        Ok(Self {
            delta_type,
            txn_id,
            lsn,
            key,
            value,
            old_value,
            split_info,
        })
    }
}

/// A chain of delta records attached to a node.
///
/// The chain maintains deltas in reverse chronological order (newest first).
/// When the chain exceeds a threshold, it should be consolidated with the
/// base node to form a new base.
#[derive(Debug, Clone)]
pub struct DeltaChain {
    /// The delta records (newest first).
    deltas: Vec<DeltaRecord>,
    /// Maximum chain length before consolidation.
    max_length: usize,
}

impl Default for DeltaChain {
    fn default() -> Self {
        Self::new(8)
    }
}

impl DeltaChain {
    /// Creates a new empty delta chain.
    pub fn new(max_length: usize) -> Self {
        Self {
            deltas: Vec::new(),
            max_length,
        }
    }

    /// Returns the number of deltas in the chain.
    pub fn len(&self) -> usize {
        self.deltas.len()
    }

    /// Returns true if the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    /// Returns true if the chain needs consolidation.
    pub fn needs_consolidation(&self) -> bool {
        self.deltas.len() >= self.max_length
    }

    /// Prepends a delta to the chain (newest deltas first).
    pub fn push(&mut self, delta: DeltaRecord) {
        self.deltas.insert(0, delta);
    }

    /// Returns an iterator over the deltas (newest first).
    pub fn iter(&self) -> impl Iterator<Item = &DeltaRecord> {
        self.deltas.iter()
    }

    /// Returns an iterator over the deltas in reverse order (oldest first).
    pub fn iter_reverse(&self) -> impl Iterator<Item = &DeltaRecord> {
        self.deltas.iter().rev()
    }

    /// Clears all deltas from the chain.
    pub fn clear(&mut self) {
        self.deltas.clear();
    }

    /// Gets the current value for a key by traversing the chain.
    /// Returns (value, was_deleted).
    pub fn get(&self, key: &Key) -> Option<(Option<&Value>, bool)> {
        for delta in &self.deltas {
            if &delta.key == key {
                match delta.delta_type {
                    DeltaType::Insert | DeltaType::Update => {
                        return Some((delta.value.as_ref(), false));
                    }
                    DeltaType::Delete => {
                        return Some((None, true));
                    }
                    _ => {}
                }
            }
        }
        None
    }

    /// Applies all deltas to a base leaf node, producing a consolidated node.
    pub fn apply_to_leaf(&self, mut base: LeafNode) -> LeafNode {
        // Apply deltas in reverse order (oldest first)
        for delta in self.deltas.iter().rev() {
            match delta.delta_type {
                DeltaType::Insert => {
                    if let Some(ref value) = delta.value {
                        let entry = LeafEntry::new(
                            delta.key.clone(),
                            value.clone(),
                            VersionInfo::new(delta.txn_id, delta.lsn),
                        );
                        // Ignore errors for duplicate keys (may be from replay)
                        let _ = base.insert(entry);
                    }
                }
                DeltaType::Update => {
                    if let Some(ref value) = delta.value {
                        let _ = base.update(
                            &delta.key,
                            value.clone(),
                            VersionInfo::new(delta.txn_id, delta.lsn),
                        );
                    }
                }
                DeltaType::Delete => {
                    let _ = base.remove(&delta.key);
                }
                _ => {
                    // Split/Merge are handled at the tree level
                }
            }
        }
        base
    }

    /// Collects all affected keys in the chain.
    pub fn affected_keys(&self) -> Vec<&Key> {
        let mut keys = Vec::new();
        for delta in &self.deltas {
            match delta.delta_type {
                DeltaType::Insert | DeltaType::Update | DeltaType::Delete => {
                    if !keys.contains(&&delta.key) {
                        keys.push(&delta.key);
                    }
                }
                _ => {}
            }
        }
        keys
    }

    /// Returns the highest LSN in the chain.
    pub fn max_lsn(&self) -> Lsn {
        self.deltas
            .iter()
            .map(|d| d.lsn)
            .max()
            .unwrap_or(Lsn::INVALID)
    }

    /// Serializes the chain to bytes.
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32_le(self.deltas.len() as u32);
        for delta in &self.deltas {
            delta.serialize(&mut buf);
        }
        buf.freeze()
    }

    /// Deserializes a chain from bytes.
    pub fn deserialize(data: &[u8], max_length: usize) -> SageTreeResult<Self> {
        let mut buf = Bytes::copy_from_slice(data);

        if buf.remaining() < 4 {
            return Err(SageTreeError::deserialization(
                "buffer too small for chain length",
            ));
        }

        let count = buf.get_u32_le() as usize;
        let mut deltas = Vec::with_capacity(count);

        for _ in 0..count {
            deltas.push(DeltaRecord::deserialize(&mut buf)?);
        }

        Ok(Self { deltas, max_length })
    }
}

/// A node with an attached delta chain.
///
/// This represents the logical state of a node, combining the base node
/// with any pending delta modifications.
#[derive(Debug, Clone)]
pub struct DeltaNode {
    /// Base leaf node.
    pub base: LeafNode,
    /// Delta chain with pending modifications.
    pub chain: DeltaChain,
}

impl DeltaNode {
    /// Creates a new delta node with an empty chain.
    pub fn new(base: LeafNode, max_chain_length: usize) -> Self {
        Self {
            base,
            chain: DeltaChain::new(max_chain_length),
        }
    }

    /// Gets a value by key, checking chain first, then base.
    pub fn get(&self, key: &Key) -> Option<&Value> {
        // Check delta chain first
        if let Some((value, deleted)) = self.chain.get(key) {
            if deleted {
                return None;
            }
            return value;
        }

        // Fall back to base node
        self.base.get(key).map(|e| &e.value)
    }

    /// Inserts a key-value pair.
    pub fn insert(
        &mut self,
        txn_id: TxnId,
        lsn: Lsn,
        key: Key,
        value: Value,
    ) -> SageTreeResult<()> {
        // Check if key already exists
        if self.get(&key).is_some() {
            return Err(SageTreeError::DuplicateKey);
        }

        let delta = DeltaRecord::insert(txn_id, lsn, key, value);
        self.chain.push(delta);
        Ok(())
    }

    /// Updates an existing key-value pair.
    pub fn update(
        &mut self,
        txn_id: TxnId,
        lsn: Lsn,
        key: Key,
        value: Value,
    ) -> SageTreeResult<()> {
        // Get the old value for the delta record
        let old_value = self.get(&key).ok_or(SageTreeError::KeyNotFound)?.clone();

        let delta = DeltaRecord::update(txn_id, lsn, key, value, old_value);
        self.chain.push(delta);
        Ok(())
    }

    /// Deletes a key.
    pub fn delete(&mut self, txn_id: TxnId, lsn: Lsn, key: Key) -> SageTreeResult<()> {
        // Get the old value for the delta record
        let old_value = self.get(&key).ok_or(SageTreeError::KeyNotFound)?.clone();

        let delta = DeltaRecord::delete(txn_id, lsn, key, old_value);
        self.chain.push(delta);
        Ok(())
    }

    /// Returns true if consolidation is needed.
    pub fn needs_consolidation(&self) -> bool {
        self.chain.needs_consolidation()
    }

    /// Consolidates the delta chain into the base node.
    pub fn consolidate(&mut self) {
        if !self.chain.is_empty() {
            self.base = self.chain.apply_to_leaf(self.base.clone());
            self.chain.clear();
        }
    }

    /// Returns the page ID of the base node.
    pub fn page_id(&self) -> PageId {
        self.base.header.page_id
    }

    /// Returns the total number of logical entries.
    pub fn len(&self) -> usize {
        // This is approximate; for exact count we'd need to apply deltas
        let base_count = self.base.len();
        let inserts = self
            .chain
            .iter()
            .filter(|d| d.delta_type == DeltaType::Insert)
            .count();
        let deletes = self
            .chain
            .iter()
            .filter(|d| d.delta_type == DeltaType::Delete)
            .count();
        base_count + inserts - deletes
    }

    /// Returns true if the node is logically empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_type() {
        assert_eq!(DeltaType::from_byte(1), Some(DeltaType::Insert));
        assert_eq!(DeltaType::from_byte(2), Some(DeltaType::Update));
        assert_eq!(DeltaType::from_byte(3), Some(DeltaType::Delete));
        assert_eq!(DeltaType::from_byte(0), None);
    }

    #[test]
    fn test_delta_record_serialization() {
        let delta = DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(100),
            Key::from_str("key1"),
            Value::from_str("value1"),
        );

        let mut buf = BytesMut::new();
        delta.serialize(&mut buf);

        let mut read_buf = buf.freeze();
        let deserialized = DeltaRecord::deserialize(&mut read_buf).unwrap();

        assert_eq!(deserialized.delta_type, DeltaType::Insert);
        assert_eq!(deserialized.key.as_bytes(), b"key1");
        assert_eq!(deserialized.value.as_ref().unwrap().as_bytes(), b"value1");
    }

    #[test]
    fn test_delta_chain_operations() {
        let mut chain = DeltaChain::new(4);
        assert!(chain.is_empty());

        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(100),
            Key::from_str("a"),
            Value::from_str("1"),
        ));
        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(101),
            Key::from_str("b"),
            Value::from_str("2"),
        ));

        assert_eq!(chain.len(), 2);

        let (val, deleted) = chain.get(&Key::from_str("a")).unwrap();
        assert!(!deleted);
        assert_eq!(val.unwrap().as_bytes(), b"1");
    }

    #[test]
    fn test_delta_chain_delete() {
        let mut chain = DeltaChain::new(4);

        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(100),
            Key::from_str("a"),
            Value::from_str("1"),
        ));
        chain.push(DeltaRecord::delete(
            TxnId::new(1),
            Lsn::new(101),
            Key::from_str("a"),
            Value::from_str("1"),
        ));

        let (val, deleted) = chain.get(&Key::from_str("a")).unwrap();
        assert!(deleted);
        assert!(val.is_none());
    }

    #[test]
    fn test_delta_chain_apply() {
        let mut chain = DeltaChain::new(8);

        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(100),
            Key::from_str("a"),
            Value::from_str("1"),
        ));
        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(101),
            Key::from_str("b"),
            Value::from_str("2"),
        ));
        chain.push(DeltaRecord::update(
            TxnId::new(1),
            Lsn::new(102),
            Key::from_str("a"),
            Value::from_str("1-updated"),
            Value::from_str("1"),
        ));

        let base = LeafNode::new(PageId::new(1));
        let consolidated = chain.apply_to_leaf(base);

        assert_eq!(consolidated.len(), 2);
        assert_eq!(
            consolidated
                .get(&Key::from_str("a"))
                .unwrap()
                .value
                .as_bytes(),
            b"1-updated"
        );
        assert_eq!(
            consolidated
                .get(&Key::from_str("b"))
                .unwrap()
                .value
                .as_bytes(),
            b"2"
        );
    }

    #[test]
    fn test_delta_chain_serialization() {
        let mut chain = DeltaChain::new(4);
        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(100),
            Key::from_str("x"),
            Value::from_str("y"),
        ));

        let serialized = chain.serialize();
        let deserialized = DeltaChain::deserialize(&serialized, 4).unwrap();

        assert_eq!(deserialized.len(), 1);
    }

    #[test]
    fn test_delta_chain_consolidation_threshold() {
        let mut chain = DeltaChain::new(3);

        for i in 0..2 {
            chain.push(DeltaRecord::insert(
                TxnId::new(1),
                Lsn::new(i as u64),
                Key::from_str(&format!("k{}", i)),
                Value::from_str(&format!("v{}", i)),
            ));
        }

        assert!(!chain.needs_consolidation());

        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(2),
            Key::from_str("k2"),
            Value::from_str("v2"),
        ));

        assert!(chain.needs_consolidation());
    }

    #[test]
    fn test_delta_node_operations() {
        let base = LeafNode::new(PageId::new(1));
        let mut node = DeltaNode::new(base, 8);

        // Insert
        node.insert(
            TxnId::new(1),
            Lsn::new(100),
            Key::from_str("a"),
            Value::from_str("1"),
        )
        .unwrap();
        node.insert(
            TxnId::new(1),
            Lsn::new(101),
            Key::from_str("b"),
            Value::from_str("2"),
        )
        .unwrap();

        assert_eq!(node.get(&Key::from_str("a")).unwrap().as_bytes(), b"1");
        assert_eq!(node.get(&Key::from_str("b")).unwrap().as_bytes(), b"2");

        // Update
        node.update(
            TxnId::new(1),
            Lsn::new(102),
            Key::from_str("a"),
            Value::from_str("1-new"),
        )
        .unwrap();
        assert_eq!(node.get(&Key::from_str("a")).unwrap().as_bytes(), b"1-new");

        // Delete
        node.delete(TxnId::new(1), Lsn::new(103), Key::from_str("b"))
            .unwrap();
        assert!(node.get(&Key::from_str("b")).is_none());

        // Duplicate insert fails
        let result = node.insert(
            TxnId::new(1),
            Lsn::new(104),
            Key::from_str("a"),
            Value::from_str("dup"),
        );
        assert!(matches!(result, Err(SageTreeError::DuplicateKey)));
    }

    #[test]
    fn test_delta_node_consolidation() {
        let base = LeafNode::new(PageId::new(1));
        let mut node = DeltaNode::new(base, 4);

        node.insert(
            TxnId::new(1),
            Lsn::new(100),
            Key::from_str("a"),
            Value::from_str("1"),
        )
        .unwrap();
        node.insert(
            TxnId::new(1),
            Lsn::new(101),
            Key::from_str("b"),
            Value::from_str("2"),
        )
        .unwrap();

        assert_eq!(node.chain.len(), 2);
        assert!(node.base.is_empty());

        node.consolidate();

        assert_eq!(node.chain.len(), 0);
        assert_eq!(node.base.len(), 2);
        assert_eq!(node.get(&Key::from_str("a")).unwrap().as_bytes(), b"1");
    }

    #[test]
    fn test_split_info() {
        let info = SplitInfo::left(PageId::new(42), Key::from_str("split_key"));

        let mut buf = BytesMut::new();
        info.serialize(&mut buf);

        let mut read_buf = buf.freeze();
        let deserialized = SplitInfo::deserialize(&mut read_buf).unwrap();

        assert_eq!(deserialized.sibling_page, PageId::new(42));
        assert_eq!(deserialized.separator_key.as_bytes(), b"split_key");
        assert!(deserialized.is_left);
    }

    #[test]
    fn test_delta_max_lsn() {
        let mut chain = DeltaChain::new(8);

        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(100),
            Key::from_str("a"),
            Value::from_str("1"),
        ));
        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(300),
            Key::from_str("b"),
            Value::from_str("2"),
        ));
        chain.push(DeltaRecord::insert(
            TxnId::new(1),
            Lsn::new(200),
            Key::from_str("c"),
            Value::from_str("3"),
        ));

        assert_eq!(chain.max_lsn(), Lsn::new(300));
    }
}
