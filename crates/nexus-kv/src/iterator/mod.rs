//! Merge iterator for combining multiple sorted iterators.
//!
//! The merge iterator is the key abstraction for LSM-tree reads. It merges
//! entries from the active memtable, immutable memtables, and all SSTable
//! levels into a single sorted stream. For duplicate keys (same user key),
//! the entry with the highest sequence number wins.

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::memtable::skiplist::InternalKey;

/// A key-value entry produced by an iterator.
#[derive(Debug, Clone)]
pub struct IteratorEntry {
    /// The internal key (user_key + sequence + type).
    pub key: InternalKey,
    /// The value (empty for deletions).
    pub value: Bytes,
}

impl IteratorEntry {
    pub fn new(key: InternalKey, value: Bytes) -> Self {
        Self { key, value }
    }
}

/// Trait for iterators that produce sorted key-value entries.
pub trait KvIterator {
    /// Returns true if the iterator is at a valid entry.
    fn valid(&self) -> bool;

    /// Get the current entry.
    fn current(&self) -> Option<IteratorEntry>;

    /// Advance to the next entry.
    fn advance(&mut self);

    /// Seek to the first entry >= the given key.
    fn seek(&mut self, target: &InternalKey);

    /// Seek to the first entry.
    fn seek_to_first(&mut self);
}

/// A wrapper for heap-based merging. We use `Reverse`-style ordering
/// because `BinaryHeap` is a max-heap, but we want min-first.
struct HeapEntry {
    entry: IteratorEntry,
    /// Index of the source iterator (lower = higher priority for same key).
    source_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key == other.entry.key && self.source_idx == other.source_idx
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior
        match other.entry.key.cmp(&self.entry.key) {
            Ordering::Equal => {
                // For same key, prefer lower source_idx (newer data source)
                other.source_idx.cmp(&self.source_idx)
            }
            ord => ord,
        }
    }
}

/// Merges multiple sorted iterators into a single sorted stream.
///
/// Sources are ordered by priority: source 0 (memtable) has the highest
/// priority, followed by immutable memtables, then L0 SSTables (newest
/// first), then L1+ SSTables.
///
/// For entries with the same user key, the entry from the highest-priority
/// source (lowest index) is returned, and duplicates from lower-priority
/// sources are skipped.
pub struct MergeIterator {
    /// Source iterators, ordered by priority (index 0 = highest priority).
    sources: Vec<Box<dyn KvIterator>>,
    /// Min-heap for k-way merging.
    heap: BinaryHeap<HeapEntry>,
    /// Current entry.
    current: Option<IteratorEntry>,
}

impl MergeIterator {
    /// Create a new merge iterator from the given sources.
    ///
    /// Sources should be ordered by priority: index 0 is the highest
    /// priority (e.g., active memtable), and later indices are lower
    /// priority (older data).
    pub fn new(sources: Vec<Box<dyn KvIterator>>) -> Self {
        let mut iter = Self {
            sources,
            heap: BinaryHeap::new(),
            current: None,
        };
        iter.build_heap();
        iter.advance_to_next();
        iter
    }

    /// Build the initial heap from all valid source iterators.
    fn build_heap(&mut self) {
        self.heap.clear();
        for (idx, source) in self.sources.iter().enumerate() {
            if source.valid() {
                if let Some(entry) = source.current() {
                    self.heap.push(HeapEntry {
                        entry,
                        source_idx: idx,
                    });
                }
            }
        }
    }

    /// Advance to the next unique user key, skipping duplicates.
    fn advance_to_next(&mut self) {
        loop {
            match self.heap.pop() {
                None => {
                    self.current = None;
                    return;
                }
                Some(heap_entry) => {
                    // Advance the source that produced this entry
                    self.sources[heap_entry.source_idx].advance();
                    if self.sources[heap_entry.source_idx].valid() {
                        if let Some(next_entry) =
                            self.sources[heap_entry.source_idx].current()
                        {
                            self.heap.push(HeapEntry {
                                entry: next_entry,
                                source_idx: heap_entry.source_idx,
                            });
                        }
                    }

                    // Skip entries with the same user key from other sources
                    // (they have lower priority / older data)
                    self.skip_same_user_key(&heap_entry.entry.key.user_key);

                    self.current = Some(heap_entry.entry);
                    return;
                }
            }
        }
    }

    /// Remove and discard heap entries with the same user key.
    fn skip_same_user_key(&mut self, user_key: &Bytes) {
        while let Some(top) = self.heap.peek() {
            if top.entry.key.user_key.as_ref() == user_key.as_ref() {
                let popped = self.heap.pop().unwrap();
                // Advance the source
                self.sources[popped.source_idx].advance();
                if self.sources[popped.source_idx].valid() {
                    if let Some(next_entry) =
                        self.sources[popped.source_idx].current()
                    {
                        self.heap.push(HeapEntry {
                            entry: next_entry,
                            source_idx: popped.source_idx,
                        });
                    }
                }
            } else {
                break;
            }
        }
    }
}

impl KvIterator for MergeIterator {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn current(&self) -> Option<IteratorEntry> {
        self.current.clone()
    }

    fn advance(&mut self) {
        self.advance_to_next();
    }

    fn seek(&mut self, target: &InternalKey) {
        // Seek all sources, then rebuild the heap
        for source in &mut self.sources {
            source.seek(target);
        }
        self.build_heap();
        self.advance_to_next();
    }

    fn seek_to_first(&mut self) {
        for source in &mut self.sources {
            source.seek_to_first();
        }
        self.build_heap();
        self.advance_to_next();
    }
}

/// A simple in-memory iterator over a vector of entries.
/// Useful for wrapping memtable scan results.
pub struct VecIterator {
    entries: Vec<IteratorEntry>,
    position: usize,
}

impl VecIterator {
    /// Create a new vector iterator.
    pub fn new(entries: Vec<IteratorEntry>) -> Self {
        Self {
            entries,
            position: 0,
        }
    }

    /// Create an empty iterator.
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }
}

impl KvIterator for VecIterator {
    fn valid(&self) -> bool {
        self.position < self.entries.len()
    }

    fn current(&self) -> Option<IteratorEntry> {
        self.entries.get(self.position).cloned()
    }

    fn advance(&mut self) {
        if self.position < self.entries.len() {
            self.position += 1;
        }
    }

    fn seek(&mut self, target: &InternalKey) {
        self.position = self
            .entries
            .partition_point(|e| e.key < *target);
    }

    fn seek_to_first(&mut self) {
        self.position = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(user_key: &str, seq: u64, value: &str) -> IteratorEntry {
        IteratorEntry::new(
            InternalKey::put(Bytes::from(user_key.to_string()), seq),
            Bytes::from(value.to_string()),
        )
    }

    fn make_delete(user_key: &str, seq: u64) -> IteratorEntry {
        IteratorEntry::new(
            InternalKey::delete(Bytes::from(user_key.to_string()), seq),
            Bytes::new(),
        )
    }

    use crate::memtable::skiplist::ValueType;

    #[test]
    fn test_vec_iterator() {
        let entries = vec![
            make_entry("a", 1, "v1"),
            make_entry("b", 2, "v2"),
            make_entry("c", 3, "v3"),
        ];
        let mut iter = VecIterator::new(entries);

        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().key.user_key.as_ref(), b"a");

        iter.advance();
        assert_eq!(iter.current().unwrap().key.user_key.as_ref(), b"b");

        iter.advance();
        assert_eq!(iter.current().unwrap().key.user_key.as_ref(), b"c");

        iter.advance();
        assert!(!iter.valid());
    }

    #[test]
    fn test_merge_two_sources() {
        let source1 = Box::new(VecIterator::new(vec![
            make_entry("a", 3, "newer_a"),
            make_entry("c", 3, "newer_c"),
        ])) as Box<dyn KvIterator>;

        let source2 = Box::new(VecIterator::new(vec![
            make_entry("a", 1, "older_a"),
            make_entry("b", 2, "b_val"),
            make_entry("c", 1, "older_c"),
        ])) as Box<dyn KvIterator>;

        let mut merge = MergeIterator::new(vec![source1, source2]);

        // "a" should come from source1 (higher priority, seq 3)
        assert!(merge.valid());
        let entry = merge.current().unwrap();
        assert_eq!(entry.key.user_key.as_ref(), b"a");
        assert_eq!(entry.value.as_ref(), b"newer_a");

        merge.advance();
        let entry = merge.current().unwrap();
        assert_eq!(entry.key.user_key.as_ref(), b"b");
        assert_eq!(entry.value.as_ref(), b"b_val");

        merge.advance();
        let entry = merge.current().unwrap();
        assert_eq!(entry.key.user_key.as_ref(), b"c");
        assert_eq!(entry.value.as_ref(), b"newer_c");

        merge.advance();
        assert!(!merge.valid());
    }

    #[test]
    fn test_merge_with_deletions() {
        let source1 = Box::new(VecIterator::new(vec![
            make_delete("a", 3), // Delete at seq 3
        ])) as Box<dyn KvIterator>;

        let source2 = Box::new(VecIterator::new(vec![
            make_entry("a", 1, "old_value"), // Put at seq 1
        ])) as Box<dyn KvIterator>;

        let mut merge = MergeIterator::new(vec![source1, source2]);

        assert!(merge.valid());
        let entry = merge.current().unwrap();
        assert_eq!(entry.key.user_key.as_ref(), b"a");
        assert_eq!(entry.key.value_type, ValueType::Deletion);

        merge.advance();
        assert!(!merge.valid());
    }

    #[test]
    fn test_vec_iterator_seek() {
        let entries = vec![
            make_entry("a", 1, "1"),
            make_entry("c", 1, "3"),
            make_entry("e", 1, "5"),
            make_entry("g", 1, "7"),
        ];
        let mut iter = VecIterator::new(entries);

        // Seek to "c" (exact match)
        iter.seek(&InternalKey::put(Bytes::from("c"), u64::MAX));
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().key.user_key.as_ref(), b"c");

        // Seek to "d" (between entries)
        iter.seek(&InternalKey::put(Bytes::from("d"), u64::MAX));
        assert!(iter.valid());
        assert_eq!(iter.current().unwrap().key.user_key.as_ref(), b"e");
    }
}
