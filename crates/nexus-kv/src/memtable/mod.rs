//! MemTable: in-memory write buffer for the LSM-tree.
//!
//! The MemTable accepts writes (puts and deletes) and stores them in a
//! concurrent skip list, sorted by internal key. When the memtable exceeds
//! the configured size threshold, it is frozen into an immutable memtable
//! and scheduled for flushing to an SSTable on disk.
//!
//! ## Concurrency Model
//!
//! - **Writes**: Serialized by the engine's write lock. Only one writer
//!   at a time inserts into the skip list.
//! - **Reads**: Lock-free. Multiple readers can scan the skip list
//!   concurrently with a single writer.

pub mod skiplist;

use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub use skiplist::{InternalKey, SkipList, SkipListIterator, ValueType};

use crate::error::{KvError, KvResult};

/// A write entry to be applied to the memtable.
#[derive(Debug, Clone)]
pub struct WriteBatch {
    entries: Vec<WriteBatchEntry>,
    approximate_size: usize,
}

/// A single entry in a write batch.
#[derive(Debug, Clone)]
pub struct WriteBatchEntry {
    pub key: Bytes,
    pub value: Bytes,
    pub value_type: ValueType,
}

impl WriteBatch {
    /// Create a new empty write batch.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            approximate_size: 0,
        }
    }

    /// Create a batch with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            approximate_size: 0,
        }
    }

    /// Add a put operation to the batch.
    pub fn put(&mut self, key: Bytes, value: Bytes) {
        self.approximate_size += key.len() + value.len() + 16;
        self.entries.push(WriteBatchEntry {
            key,
            value,
            value_type: ValueType::Value,
        });
    }

    /// Add a delete operation to the batch.
    pub fn delete(&mut self, key: Bytes) {
        self.approximate_size += key.len() + 16;
        self.entries.push(WriteBatchEntry {
            key,
            value: Bytes::new(),
            value_type: ValueType::Deletion,
        });
    }

    /// Number of operations in the batch.
    pub fn count(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Approximate size in bytes of this batch.
    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }

    /// Iterate over the entries in the batch.
    pub fn iter(&self) -> impl Iterator<Item = &WriteBatchEntry> {
        self.entries.iter()
    }

    /// Consume and return the entries.
    pub fn into_entries(self) -> Vec<WriteBatchEntry> {
        self.entries
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// An active, mutable memtable that accepts writes.
///
/// Thread-safe: writes are serialized, reads are lock-free.
pub struct MemTable {
    /// The underlying skip list storing sorted key-value pairs.
    table: SkipList,
    /// Maximum allowed size in bytes before the memtable should be frozen.
    max_size: usize,
    /// Next sequence number to assign.
    next_sequence: AtomicU64,
    /// Minimum sequence number in this memtable.
    min_sequence: AtomicU64,
    /// Maximum sequence number in this memtable.
    max_sequence: AtomicU64,
    /// Unique identifier for this memtable.
    id: u64,
    /// Write lock for serializing mutations.
    write_lock: RwLock<()>,
}

impl MemTable {
    /// Create a new memtable with the given size limit and starting sequence.
    pub fn new(id: u64, max_size: usize, start_sequence: u64) -> Self {
        Self {
            table: SkipList::new(),
            max_size,
            next_sequence: AtomicU64::new(start_sequence),
            min_sequence: AtomicU64::new(u64::MAX),
            max_sequence: AtomicU64::new(0),
            id,
            write_lock: RwLock::new(()),
        }
    }

    /// Returns the unique ID of this memtable.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Insert a key-value pair.
    ///
    /// Returns the assigned sequence number.
    pub fn put(&self, key: Bytes, value: Bytes) -> KvResult<u64> {
        self.check_capacity(key.len() + value.len())?;
        let seq = self.allocate_sequence();
        let _guard = self.write_lock.write();
        self.table.insert(InternalKey::put(key, seq), value);
        self.update_sequence_bounds(seq);
        Ok(seq)
    }

    /// Mark a key as deleted.
    ///
    /// Returns the assigned sequence number.
    pub fn delete(&self, key: Bytes) -> KvResult<u64> {
        self.check_capacity(key.len())?;
        let seq = self.allocate_sequence();
        let _guard = self.write_lock.write();
        self.table
            .insert(InternalKey::delete(key, seq), Bytes::new());
        self.update_sequence_bounds(seq);
        Ok(seq)
    }

    /// Apply a write batch atomically.
    ///
    /// All entries in the batch are assigned consecutive sequence numbers.
    pub fn apply_batch(&self, batch: &WriteBatch) -> KvResult<u64> {
        if batch.is_empty() {
            return Ok(self.next_sequence.load(Ordering::Relaxed));
        }
        self.check_capacity(batch.approximate_size())?;

        let _guard = self.write_lock.write();
        let mut first_seq = 0;

        for entry in batch.iter() {
            let seq = self.allocate_sequence();
            if first_seq == 0 {
                first_seq = seq;
            }
            let ikey = InternalKey::new(entry.key.clone(), seq, entry.value_type);
            self.table.insert(ikey, entry.value.clone());
            self.update_sequence_bounds(seq);
        }

        Ok(first_seq)
    }

    /// Look up the value for a user key at the given sequence number.
    ///
    /// Returns `None` if the key is not found, `Some((ValueType, value))` otherwise.
    /// If `ValueType::Deletion` is returned, the key was deleted.
    pub fn get(&self, user_key: &[u8], sequence: u64) -> Option<(ValueType, Bytes)> {
        self.table
            .get(user_key, sequence)
            .map(|(vt, val)| (vt, val.clone()))
    }

    /// Check if the memtable contains the given user key.
    pub fn contains_key(&self, user_key: &[u8]) -> bool {
        self.table.contains_key(user_key)
    }

    /// Create an iterator over all entries in the memtable.
    pub fn iter(&self) -> SkipListIterator<'_> {
        self.table.iter()
    }

    /// Create an iterator starting from the given internal key.
    pub fn iter_from(&self, target: &InternalKey) -> SkipListIterator<'_> {
        self.table.iter_from(target)
    }

    /// Returns true if the memtable should be frozen (size threshold exceeded).
    pub fn should_freeze(&self) -> bool {
        self.table.approximate_memory_usage() as usize >= self.max_size
    }

    /// Returns the approximate memory usage in bytes.
    pub fn approximate_memory_usage(&self) -> usize {
        self.table.approximate_memory_usage() as usize
    }

    /// Returns the number of entries in the memtable.
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Returns true if the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Returns the sequence number range [min, max] stored in this memtable.
    pub fn sequence_range(&self) -> (u64, u64) {
        (
            self.min_sequence.load(Ordering::Relaxed),
            self.max_sequence.load(Ordering::Relaxed),
        )
    }

    /// Returns the next sequence number that will be assigned.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Relaxed)
    }

    // ── Internal Helpers ────────────────────────────────────────

    fn allocate_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn update_sequence_bounds(&self, seq: u64) {
        // Update min (using fetch_min semantics via compare_exchange loop)
        let mut current = self.min_sequence.load(Ordering::Relaxed);
        while seq < current {
            match self.min_sequence.compare_exchange_weak(
                current,
                seq,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
        // Update max
        let mut current = self.max_sequence.load(Ordering::Relaxed);
        while seq > current {
            match self.max_sequence.compare_exchange_weak(
                current,
                seq,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    fn check_capacity(&self, additional_bytes: usize) -> KvResult<()> {
        let current = self.table.approximate_memory_usage() as usize;
        if current + additional_bytes > self.max_size * 2 {
            // Hard limit: reject writes if way over capacity
            return Err(KvError::MemTableFull {
                size: current,
                max: self.max_size,
            });
        }
        Ok(())
    }
}

/// An immutable memtable that has been frozen and is pending flush to disk.
///
/// Once frozen, no more writes are accepted. Reads continue to work
/// via lock-free skip list traversal. The flush process iterates over
/// all entries to produce an SSTable.
pub struct ImmutableMemTable {
    /// The frozen memtable (held in an Arc so freeze can succeed even
    /// when concurrent readers still hold references to the old Arc).
    inner: Arc<MemTable>,
    /// Whether this memtable has been flushed to disk.
    flushed: AtomicBool,
}

impl ImmutableMemTable {
    /// Create an immutable memtable from a mutable one.
    pub fn from(memtable: MemTable) -> Self {
        Self {
            inner: Arc::new(memtable),
            flushed: AtomicBool::new(false),
        }
    }

    /// Create an immutable memtable directly from an `Arc<MemTable>`,
    /// avoiding the need to unwrap or clone the inner data.
    pub fn from_arc(memtable: Arc<MemTable>) -> Self {
        Self {
            inner: memtable,
            flushed: AtomicBool::new(false),
        }
    }

    /// Look up a value by user key and sequence number.
    pub fn get(&self, user_key: &[u8], sequence: u64) -> Option<(ValueType, Bytes)> {
        self.inner.get(user_key, sequence)
    }

    /// Create an iterator over all entries.
    pub fn iter(&self) -> SkipListIterator<'_> {
        self.inner.iter()
    }

    /// Returns the memtable's ID.
    pub fn id(&self) -> u64 {
        self.inner.id()
    }

    /// Returns the sequence number range.
    pub fn sequence_range(&self) -> (u64, u64) {
        self.inner.sequence_range()
    }

    /// Returns approximate memory usage.
    pub fn approximate_memory_usage(&self) -> usize {
        self.inner.approximate_memory_usage()
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Mark this memtable as flushed.
    pub fn mark_flushed(&self) {
        self.flushed.store(true, Ordering::Release);
    }

    /// Check if this memtable has been flushed.
    pub fn is_flushed(&self) -> bool {
        self.flushed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_put_get() {
        let mt = MemTable::new(1, 1024 * 1024, 1);
        mt.put(Bytes::from("key1"), Bytes::from("val1")).unwrap();
        mt.put(Bytes::from("key2"), Bytes::from("val2")).unwrap();

        let (vt, val) = mt.get(b"key1", u64::MAX).unwrap();
        assert_eq!(vt, ValueType::Value);
        assert_eq!(val.as_ref(), b"val1");

        let (vt, val) = mt.get(b"key2", u64::MAX).unwrap();
        assert_eq!(vt, ValueType::Value);
        assert_eq!(val.as_ref(), b"val2");
    }

    #[test]
    fn test_memtable_delete() {
        let mt = MemTable::new(1, 1024 * 1024, 1);
        mt.put(Bytes::from("key"), Bytes::from("value")).unwrap();
        mt.delete(Bytes::from("key")).unwrap();

        // At latest sequence, the key is deleted
        let (vt, _) = mt.get(b"key", u64::MAX).unwrap();
        assert_eq!(vt, ValueType::Deletion);
    }

    #[test]
    fn test_write_batch() {
        let mt = MemTable::new(1, 1024 * 1024, 1);
        let mut batch = WriteBatch::new();
        batch.put(Bytes::from("a"), Bytes::from("1"));
        batch.put(Bytes::from("b"), Bytes::from("2"));
        batch.put(Bytes::from("c"), Bytes::from("3"));

        mt.apply_batch(&batch).unwrap();
        assert_eq!(mt.len(), 3);

        let (_, val) = mt.get(b"b", u64::MAX).unwrap();
        assert_eq!(val.as_ref(), b"2");
    }

    #[test]
    fn test_should_freeze() {
        let mt = MemTable::new(1, 256, 1); // Very small limit
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let val = format!("value_{:04}", i);
            let _ = mt.put(Bytes::from(key), Bytes::from(val));
        }
        assert!(mt.should_freeze());
    }

    #[test]
    fn test_immutable_memtable() {
        let mt = MemTable::new(1, 1024 * 1024, 1);
        mt.put(Bytes::from("key"), Bytes::from("value")).unwrap();

        let imm = ImmutableMemTable::from(mt);
        assert!(!imm.is_flushed());

        let (vt, val) = imm.get(b"key", u64::MAX).unwrap();
        assert_eq!(vt, ValueType::Value);
        assert_eq!(val.as_ref(), b"value");

        imm.mark_flushed();
        assert!(imm.is_flushed());
    }

    #[test]
    fn test_sequence_numbers() {
        let mt = MemTable::new(1, 1024 * 1024, 100);
        let s1 = mt.put(Bytes::from("k1"), Bytes::from("v1")).unwrap();
        let s2 = mt.put(Bytes::from("k2"), Bytes::from("v2")).unwrap();
        let s3 = mt.put(Bytes::from("k3"), Bytes::from("v3")).unwrap();

        assert_eq!(s1, 100);
        assert_eq!(s2, 101);
        assert_eq!(s3, 102);
        assert_eq!(mt.sequence_range(), (100, 102));
    }
}
