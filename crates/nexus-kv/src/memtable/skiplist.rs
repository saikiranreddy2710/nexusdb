//! Lock-free concurrent skip list for the MemTable.
//!
//! Implements a skip list following the LevelDB/RocksDB approach:
//! - Single-writer, multiple-readers (no CAS contention)
//! - Atomic pointer loads for lock-free reads
//! - Geometric random height distribution
//! - Arena-style memory tracking
//!
//! The skip list stores `InternalKey → Value` pairs sorted by
//! (user_key ASC, sequence DESC) so that newer versions appear first.

use bytes::Bytes;
use rand::Rng;
use std::cmp::Ordering;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering as MemOrder};

/// Maximum height of the skip list. With branching factor 4,
/// this supports ~4^20 = 10^12 entries efficiently.
const MAX_HEIGHT: usize = 20;

/// Inverse probability of height increase. A value of 4 means
/// each level has 1/4 the nodes of the level below.
const BRANCHING_FACTOR: u32 = 4;

/// Type of entry in the memtable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ValueType {
    /// Tombstone: marks a key as deleted.
    Deletion = 0,
    /// Normal value insertion.
    Value = 1,
}

/// Internal key format used within the LSM-tree.
///
/// Encodes `(user_key, sequence, value_type)` with ordering:
/// - Primary: user_key in ascending byte order
/// - Secondary: sequence number in descending order (newer first)
/// - Tertiary: value_type in descending order
#[derive(Debug, Clone)]
pub struct InternalKey {
    /// The user-visible key.
    pub user_key: Bytes,
    /// Monotonically increasing sequence number.
    pub sequence: u64,
    /// Whether this is a put or delete.
    pub value_type: ValueType,
}

impl InternalKey {
    /// Create a new internal key for a value insertion.
    pub fn new(user_key: Bytes, sequence: u64, value_type: ValueType) -> Self {
        Self {
            user_key,
            sequence,
            value_type,
        }
    }

    /// Create an internal key for a put operation.
    pub fn put(user_key: Bytes, sequence: u64) -> Self {
        Self::new(user_key, sequence, ValueType::Value)
    }

    /// Create an internal key for a delete operation.
    pub fn delete(user_key: Bytes, sequence: u64) -> Self {
        Self::new(user_key, sequence, ValueType::Deletion)
    }

    /// Encode the internal key to bytes for storage.
    /// Format: [user_key_len (4 bytes)] [user_key] [sequence (8 bytes)] [value_type (1 byte)]
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::with_capacity(4 + self.user_key.len() + 8 + 1);
        buf.extend_from_slice(&(self.user_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.user_key);
        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.push(self.value_type as u8);
        Bytes::from(buf)
    }

    /// Decode an internal key from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 13 {
            return None;
        }
        let key_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + key_len + 9 {
            return None;
        }
        let user_key = Bytes::copy_from_slice(&data[4..4 + key_len]);
        let seq_start = 4 + key_len;
        let sequence = u64::from_le_bytes([
            data[seq_start],
            data[seq_start + 1],
            data[seq_start + 2],
            data[seq_start + 3],
            data[seq_start + 4],
            data[seq_start + 5],
            data[seq_start + 6],
            data[seq_start + 7],
        ]);
        let vt = match data[seq_start + 8] {
            0 => ValueType::Deletion,
            1 => ValueType::Value,
            _ => return None,
        };
        Some(Self {
            user_key,
            sequence,
            value_type: vt,
        })
    }

    /// Estimated memory usage of this key.
    pub fn approximate_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.user_key.len()
    }
}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key
            && self.sequence == other.sequence
            && self.value_type == other.value_type
    }
}

impl Eq for InternalKey {}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.as_ref().cmp(other.user_key.as_ref()) {
            Ordering::Equal => {
                // Reverse ordering: newer sequence numbers first
                match other.sequence.cmp(&self.sequence) {
                    Ordering::Equal => other.value_type.cmp(&self.value_type),
                    ord => ord,
                }
            }
            ord => ord,
        }
    }
}

// ── Skip List Node ──────────────────────────────────────────────

/// A node in the skip list. Each node stores a key-value pair and
/// an array of forward pointers (one per level).
struct Node {
    key: InternalKey,
    value: Bytes,
    /// Height of this node (1-indexed: a height-1 node has only next[0]).
    height: usize,
    /// Forward pointers. Index 0 is the bottom level.
    /// Only indices 0..height are valid.
    next: [AtomicPtr<Node>; MAX_HEIGHT],
}

impl Node {
    /// Create a new node with the given key, value, and height.
    fn new(key: InternalKey, value: Bytes, height: usize) -> Box<Self> {
        debug_assert!(height >= 1 && height <= MAX_HEIGHT);
        Box::new(Node {
            key,
            value,
            height,
            next: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
        })
    }

    /// Create the sentinel head node.
    fn head() -> Box<Self> {
        // Head node has dummy key/value and maximum height
        Box::new(Node {
            key: InternalKey::new(Bytes::new(), 0, ValueType::Value),
            value: Bytes::new(),
            height: MAX_HEIGHT,
            next: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
        })
    }

    /// Get the next node at the given level (lock-free read).
    #[inline]
    fn get_next(&self, level: usize) -> *mut Node {
        self.next[level].load(MemOrder::Acquire)
    }

    /// Set the next node at the given level (writer only).
    #[inline]
    fn set_next(&self, level: usize, node: *mut Node) {
        self.next[level].store(node, MemOrder::Release);
    }

    /// Approximate memory usage of this node.
    fn approximate_memory(&self) -> usize {
        std::mem::size_of::<Self>() + self.key.user_key.len() + self.value.len()
    }
}

// ── Skip List ───────────────────────────────────────────────────

/// A concurrent skip list supporting lock-free reads and serialized writes.
///
/// This implementation follows the single-writer / multiple-reader model
/// used in LevelDB and RocksDB. The writer is serialized externally (by
/// the MemTable's write lock), while readers can proceed without any
/// synchronization.
pub struct SkipList {
    /// Sentinel head node. Never deallocated while the list exists.
    head: Box<Node>,
    /// Current maximum height of any node in the list.
    max_height: AtomicUsize,
    /// Number of entries in the list.
    len: AtomicUsize,
    /// Approximate memory usage in bytes.
    memory_usage: AtomicU64,
}

impl SkipList {
    /// Create a new empty skip list.
    pub fn new() -> Self {
        Self {
            head: Node::head(),
            max_height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
            memory_usage: AtomicU64::new(0),
        }
    }

    /// Returns the number of entries in the skip list.
    pub fn len(&self) -> usize {
        self.len.load(MemOrder::Relaxed)
    }

    /// Returns true if the skip list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the approximate memory usage in bytes.
    pub fn approximate_memory_usage(&self) -> u64 {
        self.memory_usage.load(MemOrder::Relaxed)
    }

    /// Generate a random height for a new node using geometric distribution.
    fn random_height() -> usize {
        let mut rng = rand::thread_rng();
        let mut height = 1;
        while height < MAX_HEIGHT && rng.gen_range(0..BRANCHING_FACTOR) == 0 {
            height += 1;
        }
        height
    }

    /// Get the current maximum height.
    fn get_max_height(&self) -> usize {
        self.max_height.load(MemOrder::Relaxed)
    }

    /// Insert a key-value pair into the skip list.
    ///
    /// # Safety
    /// Must be called with external write serialization (only one
    /// concurrent writer). Multiple readers are safe.
    pub fn insert(&self, key: InternalKey, value: Bytes) {
        let height = Self::random_height();
        let mem = std::mem::size_of::<Node>() + key.user_key.len() + value.len();

        // Find the position to insert at each level
        let mut prev = [ptr::null_mut::<Node>(); MAX_HEIGHT];
        let head_ptr = &*self.head as *const Node as *mut Node;
        let mut current = head_ptr;

        let max_h = self.get_max_height();
        for level in (0..max_h).rev() {
            loop {
                let next = unsafe { &*current }.get_next(level);
                if next.is_null() || unsafe { &*next }.key > key {
                    break;
                }
                current = next;
            }
            prev[level] = current;
        }

        // If new node is taller than current max, set prev for upper levels to head
        if height > max_h {
            for item in prev.iter_mut().take(height).skip(max_h) {
                *item = head_ptr;
            }
            self.max_height.store(height, MemOrder::Relaxed);
        }

        // Create the new node
        let new_node = Node::new(key, value, height);
        let new_ptr = Box::into_raw(new_node);

        // Link the new node into the list at each level.
        // We insert bottom-up so that concurrent readers on higher levels
        // never see a node that isn't fully linked at lower levels.
        for level in 0..height {
            unsafe {
                let prev_node = &*prev[level];
                let next = prev_node.get_next(level);
                (*new_ptr).set_next(level, next);
                prev_node.set_next(level, new_ptr);
            }
        }

        self.len.fetch_add(1, MemOrder::Relaxed);
        self.memory_usage.fetch_add(mem as u64, MemOrder::Relaxed);
    }

    /// Look up the first entry whose key is >= the given key.
    ///
    /// Returns a reference to the matching node's key and value, or `None`
    /// if no such entry exists.
    pub fn seek(&self, target: &InternalKey) -> Option<(&InternalKey, &Bytes)> {
        let node = self.find_ge(target);
        if node.is_null() {
            None
        } else {
            unsafe { Some((&(*node).key, &(*node).value)) }
        }
    }

    /// Get the value associated with the given user key at or before the
    /// specified sequence number.
    ///
    /// This finds the newest version of the key with sequence <= `sequence`.
    pub fn get(&self, user_key: &[u8], sequence: u64) -> Option<(ValueType, &Bytes)> {
        // Create a lookup key with the target sequence
        let lookup = InternalKey::new(Bytes::copy_from_slice(user_key), sequence, ValueType::Value);
        let node = self.find_ge(&lookup);
        if node.is_null() {
            return None;
        }
        let entry = unsafe { &*node };
        // Check if the user key matches
        if entry.key.user_key.as_ref() == user_key && entry.key.sequence <= sequence {
            Some((entry.key.value_type, &entry.value))
        } else {
            None
        }
    }

    /// Find the first node whose key is >= target.
    fn find_ge(&self, target: &InternalKey) -> *mut Node {
        let mut current = &*self.head as *const Node as *mut Node;
        let max_h = self.get_max_height();

        for level in (0..max_h).rev() {
            loop {
                let next = unsafe { &*current }.get_next(level);
                if next.is_null() {
                    break;
                }
                let next_key = unsafe { &(*next).key };
                if *next_key >= *target {
                    break;
                }
                current = next;
            }
        }

        // Move to the next node at level 0 (first node >= target)
        unsafe { &*current }.get_next(0)
    }

    /// Check if the skip list contains any entry with the given user key.
    pub fn contains_key(&self, user_key: &[u8]) -> bool {
        let lookup = InternalKey::new(Bytes::copy_from_slice(user_key), u64::MAX, ValueType::Value);
        let node = self.find_ge(&lookup);
        if node.is_null() {
            return false;
        }
        unsafe { (*node).key.user_key.as_ref() == user_key }
    }

    /// Create an iterator over all entries in the skip list.
    pub fn iter(&self) -> SkipListIterator<'_> {
        SkipListIterator {
            list: self,
            current: self.head.get_next(0),
        }
    }

    /// Create an iterator starting from the first entry >= target.
    pub fn iter_from(&self, target: &InternalKey) -> SkipListIterator<'_> {
        let node = self.find_ge(target);
        SkipListIterator {
            list: self,
            current: node,
        }
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        // Walk the bottom level and free all nodes
        let mut current = self.head.get_next(0);
        while !current.is_null() {
            let next = unsafe { (*current).get_next(0) };
            unsafe {
                drop(Box::from_raw(current));
            }
            current = next;
        }
    }
}

// Safety: The skip list is safe to share across threads because:
// - Reads use atomic loads (Acquire ordering)
// - Writes use atomic stores (Release ordering)
// - Write serialization is guaranteed externally by the MemTable lock
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

// ── Skip List Iterator ──────────────────────────────────────────

/// Iterator over skip list entries.
///
/// Iterates in internal key order (user_key ASC, sequence DESC).
/// Lock-free: can proceed concurrently with writes.
pub struct SkipListIterator<'a> {
    #[allow(dead_code)]
    list: &'a SkipList,
    current: *mut Node,
}

impl<'a> SkipListIterator<'a> {
    /// Returns true if the iterator is pointing to a valid entry.
    pub fn valid(&self) -> bool {
        !self.current.is_null()
    }

    /// Get the current entry's key and value.
    pub fn current(&self) -> Option<(&'a InternalKey, &'a Bytes)> {
        if self.current.is_null() {
            None
        } else {
            unsafe { Some((&(*self.current).key, &(*self.current).value)) }
        }
    }

    /// Get the current entry's key.
    pub fn key(&self) -> Option<&'a InternalKey> {
        if self.current.is_null() {
            None
        } else {
            unsafe { Some(&(*self.current).key) }
        }
    }

    /// Get the current entry's value.
    pub fn value(&self) -> Option<&'a Bytes> {
        if self.current.is_null() {
            None
        } else {
            unsafe { Some(&(*self.current).value) }
        }
    }

    /// Advance to the next entry.
    pub fn next(&mut self) {
        if !self.current.is_null() {
            self.current = unsafe { (*self.current).get_next(0) };
        }
    }
}

// Safety: SkipListIterator borrows the SkipList and only reads via
// atomic loads, so it's safe to send across threads.
unsafe impl Send for SkipListIterator<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_skiplist() {
        let sl = SkipList::new();
        assert!(sl.is_empty());
        assert_eq!(sl.len(), 0);
        assert!(sl.get(b"key", u64::MAX).is_none());
    }

    #[test]
    fn test_insert_and_get() {
        let sl = SkipList::new();
        sl.insert(
            InternalKey::put(Bytes::from("hello"), 1),
            Bytes::from("world"),
        );
        assert_eq!(sl.len(), 1);

        let (vt, val) = sl.get(b"hello", 1).unwrap();
        assert_eq!(vt, ValueType::Value);
        assert_eq!(val.as_ref(), b"world");
    }

    #[test]
    fn test_multiple_versions() {
        let sl = SkipList::new();
        sl.insert(InternalKey::put(Bytes::from("key"), 1), Bytes::from("v1"));
        sl.insert(InternalKey::put(Bytes::from("key"), 2), Bytes::from("v2"));
        sl.insert(InternalKey::put(Bytes::from("key"), 3), Bytes::from("v3"));

        // Reading at sequence 3 should return v3
        let (_, val) = sl.get(b"key", 3).unwrap();
        assert_eq!(val.as_ref(), b"v3");

        // Reading at sequence 2 should return v2
        let (_, val) = sl.get(b"key", 2).unwrap();
        assert_eq!(val.as_ref(), b"v2");

        // Reading at sequence 1 should return v1
        let (_, val) = sl.get(b"key", 1).unwrap();
        assert_eq!(val.as_ref(), b"v1");
    }

    #[test]
    fn test_delete_marker() {
        let sl = SkipList::new();
        sl.insert(
            InternalKey::put(Bytes::from("key"), 1),
            Bytes::from("value"),
        );
        sl.insert(InternalKey::delete(Bytes::from("key"), 2), Bytes::new());

        // At sequence 2, key is deleted
        let (vt, _) = sl.get(b"key", 2).unwrap();
        assert_eq!(vt, ValueType::Deletion);

        // At sequence 1, key still has value
        let (vt, val) = sl.get(b"key", 1).unwrap();
        assert_eq!(vt, ValueType::Value);
        assert_eq!(val.as_ref(), b"value");
    }

    #[test]
    fn test_ordering() {
        let sl = SkipList::new();
        sl.insert(InternalKey::put(Bytes::from("c"), 1), Bytes::from("3"));
        sl.insert(InternalKey::put(Bytes::from("a"), 1), Bytes::from("1"));
        sl.insert(InternalKey::put(Bytes::from("b"), 1), Bytes::from("2"));

        let mut iter = sl.iter();
        let (k, v) = iter.current().unwrap();
        assert_eq!(k.user_key.as_ref(), b"a");
        assert_eq!(v.as_ref(), b"1");

        iter.next();
        let (k, v) = iter.current().unwrap();
        assert_eq!(k.user_key.as_ref(), b"b");
        assert_eq!(v.as_ref(), b"2");

        iter.next();
        let (k, v) = iter.current().unwrap();
        assert_eq!(k.user_key.as_ref(), b"c");
        assert_eq!(v.as_ref(), b"3");

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn test_many_entries() {
        let sl = SkipList::new();
        for i in 0..1000u64 {
            let key = format!("{:08}", i);
            let value = format!("value_{}", i);
            sl.insert(
                InternalKey::put(Bytes::from(key), i + 1),
                Bytes::from(value),
            );
        }
        assert_eq!(sl.len(), 1000);

        // Verify all entries are retrievable
        for i in 0..1000u64 {
            let key = format!("{:08}", i);
            let (vt, _) = sl.get(key.as_bytes(), i + 1).unwrap();
            assert_eq!(vt, ValueType::Value);
        }

        // Verify iterator produces entries in order
        let mut iter = sl.iter();
        let mut count = 0;
        let mut last_key = Vec::new();
        while iter.valid() {
            let (k, _) = iter.current().unwrap();
            if !last_key.is_empty() {
                assert!(k.user_key.as_ref() >= last_key.as_slice());
            }
            last_key = k.user_key.to_vec();
            iter.next();
            count += 1;
        }
        assert_eq!(count, 1000);
    }

    #[test]
    fn test_contains_key() {
        let sl = SkipList::new();
        sl.insert(
            InternalKey::put(Bytes::from("exists"), 1),
            Bytes::from("yes"),
        );

        assert!(sl.contains_key(b"exists"));
        assert!(!sl.contains_key(b"nope"));
    }

    #[test]
    fn test_memory_tracking() {
        let sl = SkipList::new();
        assert_eq!(sl.approximate_memory_usage(), 0);

        sl.insert(
            InternalKey::put(Bytes::from("key"), 1),
            Bytes::from("value"),
        );
        assert!(sl.approximate_memory_usage() > 0);
    }
}
