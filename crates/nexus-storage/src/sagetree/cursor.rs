//! Cursor implementation for SageTree traversal and range scans.
//!
//! The cursor provides iteration over key-value pairs in the tree, supporting:
//! - Point lookups (seek to a specific key)
//! - Range scans (iterate over a range of keys)
//! - Forward and backward iteration
//! - Fractional cascading hints for efficient range queries

use nexus_common::types::{Key, PageId, Value};
use std::ops::Bound;

use super::node::{LeafEntry, LeafNode};

/// Direction of cursor movement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Move forward (ascending key order).
    Forward,
    /// Move backward (descending key order).
    Backward,
}

/// Position of the cursor within a leaf node.
#[derive(Debug, Clone)]
pub struct CursorPosition {
    /// Page ID of the current leaf node.
    pub page_id: PageId,
    /// Index within the leaf's entries.
    pub index: usize,
    /// Total number of entries in the current leaf.
    pub entry_count: usize,
}

impl CursorPosition {
    /// Creates a new cursor position.
    pub fn new(page_id: PageId, index: usize, entry_count: usize) -> Self {
        Self {
            page_id,
            index,
            entry_count,
        }
    }

    /// Returns true if the position is valid (within bounds).
    pub fn is_valid(&self) -> bool {
        self.page_id.is_valid() && self.index < self.entry_count
    }

    /// Returns true if at the beginning of the leaf.
    pub fn at_start(&self) -> bool {
        self.index == 0
    }

    /// Returns true if at the end of the leaf.
    pub fn at_end(&self) -> bool {
        self.index >= self.entry_count
    }
}

/// A key-value pair returned by the cursor.
#[derive(Debug, Clone)]
pub struct CursorEntry {
    /// The key.
    pub key: Key,
    /// The value.
    pub value: Value,
}

impl CursorEntry {
    /// Creates a new cursor entry.
    pub fn new(key: Key, value: Value) -> Self {
        Self { key, value }
    }

    /// Creates from a leaf entry.
    pub fn from_leaf_entry(entry: &LeafEntry) -> Self {
        Self {
            key: entry.key.clone(),
            value: entry.value.clone(),
        }
    }
}

/// Range bounds for cursor iteration.
#[derive(Debug, Clone)]
pub struct KeyRange {
    /// Start bound.
    pub start: Bound<Key>,
    /// End bound.
    pub end: Bound<Key>,
}

impl KeyRange {
    /// Creates a range covering all keys.
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// Creates a range from start (inclusive) to end (exclusive).
    pub fn new(start: Key, end: Key) -> Self {
        Self {
            start: Bound::Included(start),
            end: Bound::Excluded(end),
        }
    }

    /// Creates a range from start (inclusive) to end (inclusive).
    pub fn inclusive(start: Key, end: Key) -> Self {
        Self {
            start: Bound::Included(start),
            end: Bound::Included(end),
        }
    }

    /// Creates a range starting from a key (inclusive).
    pub fn from(start: Key) -> Self {
        Self {
            start: Bound::Included(start),
            end: Bound::Unbounded,
        }
    }

    /// Creates a range up to a key (exclusive).
    pub fn until(end: Key) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Excluded(end),
        }
    }

    /// Creates a range with a prefix (all keys starting with the prefix).
    pub fn prefix(prefix: Key) -> Self {
        let start = prefix.clone();
        let end = prefix.successor();
        Self {
            start: Bound::Included(start),
            end: Bound::Excluded(end),
        }
    }

    /// Checks if a key is within the range.
    pub fn contains(&self, key: &Key) -> bool {
        let after_start = match &self.start {
            Bound::Included(start) => key >= start,
            Bound::Excluded(start) => key > start,
            Bound::Unbounded => true,
        };

        let before_end = match &self.end {
            Bound::Included(end) => key <= end,
            Bound::Excluded(end) => key < end,
            Bound::Unbounded => true,
        };

        after_start && before_end
    }

    /// Checks if a key is before the start of the range.
    pub fn is_before_start(&self, key: &Key) -> bool {
        match &self.start {
            Bound::Included(start) => key < start,
            Bound::Excluded(start) => key <= start,
            Bound::Unbounded => false,
        }
    }

    /// Checks if a key is after the end of the range.
    pub fn is_after_end(&self, key: &Key) -> bool {
        match &self.end {
            Bound::Included(end) => key > end,
            Bound::Excluded(end) => key >= end,
            Bound::Unbounded => false,
        }
    }

    /// Returns the start key for seeking, if bounded.
    pub fn seek_key(&self) -> Option<&Key> {
        match &self.start {
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
            Bound::Unbounded => None,
        }
    }
}

impl Default for KeyRange {
    fn default() -> Self {
        Self::all()
    }
}

/// Cursor state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorState {
    /// Cursor has not been positioned yet.
    Uninitialized,
    /// Cursor is positioned on a valid entry.
    Valid,
    /// Cursor is at the end (past all entries).
    AtEnd,
    /// Cursor is before the start.
    BeforeStart,
    /// Cursor is invalid (e.g., tree modified).
    Invalid,
}

/// A cursor for iterating over a leaf node's entries.
///
/// This is a simple cursor that operates on a single leaf node.
/// For tree-wide iteration, use `TreeCursor` which handles
/// navigation between leaf nodes.
#[derive(Debug, Clone)]
pub struct LeafCursor {
    /// The leaf node being iterated.
    entries: Vec<LeafEntry>,
    /// Current position in the entries.
    position: usize,
    /// Current state.
    state: CursorState,
    /// Range to iterate over.
    range: KeyRange,
    /// Direction of iteration.
    direction: Direction,
}

impl LeafCursor {
    /// Creates a new cursor over a leaf node.
    pub fn new(leaf: &LeafNode, range: KeyRange, direction: Direction) -> Self {
        let entries = leaf.entries.clone();
        Self {
            entries,
            position: 0,
            state: CursorState::Uninitialized,
            range,
            direction,
        }
    }

    /// Returns the current state.
    pub fn state(&self) -> CursorState {
        self.state
    }

    /// Returns true if the cursor is valid (positioned on an entry).
    pub fn is_valid(&self) -> bool {
        self.state == CursorState::Valid
    }

    /// Seeks to the first entry in range.
    pub fn seek_first(&mut self) {
        if self.entries.is_empty() {
            self.state = CursorState::AtEnd;
            return;
        }

        match self.direction {
            Direction::Forward => {
                // Find first entry in range
                self.position = match &self.range.start {
                    Bound::Unbounded => 0,
                    Bound::Included(key) => self.entries.partition_point(|e| &e.key < key),
                    Bound::Excluded(key) => self.entries.partition_point(|e| &e.key <= key),
                };

                if self.position >= self.entries.len() {
                    self.state = CursorState::AtEnd;
                } else if self.range.is_after_end(&self.entries[self.position].key) {
                    self.state = CursorState::AtEnd;
                } else {
                    self.state = CursorState::Valid;
                }
            }
            Direction::Backward => {
                // Find last entry in range
                let end_pos = match &self.range.end {
                    Bound::Unbounded => self.entries.len(),
                    Bound::Included(key) => self.entries.partition_point(|e| &e.key <= key),
                    Bound::Excluded(key) => self.entries.partition_point(|e| &e.key < key),
                };

                if end_pos == 0 {
                    self.state = CursorState::BeforeStart;
                } else {
                    self.position = end_pos - 1;
                    if self.range.is_before_start(&self.entries[self.position].key) {
                        self.state = CursorState::BeforeStart;
                    } else {
                        self.state = CursorState::Valid;
                    }
                }
            }
        }
    }

    /// Seeks to a specific key.
    pub fn seek(&mut self, key: &Key) {
        if self.entries.is_empty() {
            self.state = CursorState::AtEnd;
            return;
        }

        match self.entries.binary_search_by(|e| e.key.cmp(key)) {
            Ok(idx) => {
                self.position = idx;
                if self.range.contains(&self.entries[idx].key) {
                    self.state = CursorState::Valid;
                } else {
                    self.state = CursorState::AtEnd;
                }
            }
            Err(idx) => {
                if idx >= self.entries.len() {
                    self.state = CursorState::AtEnd;
                } else {
                    self.position = idx;
                    if self.range.contains(&self.entries[idx].key) {
                        self.state = CursorState::Valid;
                    } else {
                        self.state = CursorState::AtEnd;
                    }
                }
            }
        }
    }

    /// Moves to the next entry.
    pub fn next(&mut self) -> Option<CursorEntry> {
        if self.state != CursorState::Valid {
            return None;
        }

        let entry = CursorEntry::from_leaf_entry(&self.entries[self.position]);

        match self.direction {
            Direction::Forward => {
                self.position += 1;
                if self.position >= self.entries.len() {
                    self.state = CursorState::AtEnd;
                } else if self.range.is_after_end(&self.entries[self.position].key) {
                    self.state = CursorState::AtEnd;
                }
            }
            Direction::Backward => {
                if self.position == 0 {
                    self.state = CursorState::BeforeStart;
                } else {
                    self.position -= 1;
                    if self.range.is_before_start(&self.entries[self.position].key) {
                        self.state = CursorState::BeforeStart;
                    }
                }
            }
        }

        Some(entry)
    }

    /// Returns the current entry without advancing.
    pub fn current(&self) -> Option<CursorEntry> {
        if self.state != CursorState::Valid {
            return None;
        }
        Some(CursorEntry::from_leaf_entry(&self.entries[self.position]))
    }

    /// Returns the current key without advancing.
    pub fn current_key(&self) -> Option<&Key> {
        if self.state != CursorState::Valid {
            return None;
        }
        Some(&self.entries[self.position].key)
    }

    /// Collects all remaining entries into a vector.
    pub fn collect_all(&mut self) -> Vec<CursorEntry> {
        let mut results = Vec::new();
        while let Some(entry) = self.next() {
            results.push(entry);
        }
        results
    }
}

/// A stack frame for tree traversal.
#[derive(Debug, Clone)]
pub struct TraversalFrame {
    /// Page ID of the node.
    pub page_id: PageId,
    /// Index of the child we descended into (for internal nodes).
    pub child_index: usize,
}

impl TraversalFrame {
    /// Creates a new traversal frame.
    pub fn new(page_id: PageId, child_index: usize) -> Self {
        Self {
            page_id,
            child_index,
        }
    }
}

/// A stack-based path for tree traversal.
///
/// This tracks the path from root to current leaf, enabling
/// navigation to sibling leaves without re-traversing from root.
#[derive(Debug, Clone)]
pub struct TraversalPath {
    /// Stack of frames from root to current position.
    frames: Vec<TraversalFrame>,
}

impl TraversalPath {
    /// Creates an empty path.
    pub fn new() -> Self {
        Self { frames: Vec::new() }
    }

    /// Pushes a new frame onto the path.
    pub fn push(&mut self, page_id: PageId, child_index: usize) {
        self.frames.push(TraversalFrame::new(page_id, child_index));
    }

    /// Pops the top frame from the path.
    pub fn pop(&mut self) -> Option<TraversalFrame> {
        self.frames.pop()
    }

    /// Returns the current (top) frame.
    pub fn current(&self) -> Option<&TraversalFrame> {
        self.frames.last()
    }

    /// Returns the depth of the path.
    pub fn depth(&self) -> usize {
        self.frames.len()
    }

    /// Returns true if the path is empty.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Clears the path.
    pub fn clear(&mut self) {
        self.frames.clear();
    }
}

impl Default for TraversalPath {
    fn default() -> Self {
        Self::new()
    }
}

/// Hint for fractional cascading optimization.
///
/// When performing range scans, we can use information from
/// one node to skip ahead in sibling nodes.
#[derive(Debug, Clone)]
pub struct CascadingHint {
    /// Estimated position in the next leaf.
    pub estimated_index: usize,
    /// Key at the hint position.
    pub hint_key: Option<Key>,
    /// Confidence level (0.0 to 1.0).
    pub confidence: f64,
}

impl CascadingHint {
    /// Creates a new hint.
    pub fn new(estimated_index: usize, hint_key: Option<Key>, confidence: f64) -> Self {
        Self {
            estimated_index,
            hint_key,
            confidence,
        }
    }

    /// Creates an empty (no hint) value.
    pub fn none() -> Self {
        Self {
            estimated_index: 0,
            hint_key: None,
            confidence: 0.0,
        }
    }

    /// Returns true if this hint is useful.
    pub fn is_useful(&self) -> bool {
        self.confidence > 0.5
    }
}

impl Default for CascadingHint {
    fn default() -> Self {
        Self::none()
    }
}

/// Statistics collected during cursor iteration.
#[derive(Debug, Clone, Default)]
pub struct CursorStats {
    /// Number of entries visited.
    pub entries_visited: usize,
    /// Number of leaf nodes visited.
    pub leaves_visited: usize,
    /// Number of internal nodes visited.
    pub internal_nodes_visited: usize,
    /// Number of times we used fractional cascading hint.
    pub hints_used: usize,
    /// Number of times the hint was accurate.
    pub hints_accurate: usize,
}

impl CursorStats {
    /// Creates new empty stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records visiting an entry.
    pub fn visit_entry(&mut self) {
        self.entries_visited += 1;
    }

    /// Records visiting a leaf node.
    pub fn visit_leaf(&mut self) {
        self.leaves_visited += 1;
    }

    /// Records visiting an internal node.
    pub fn visit_internal(&mut self) {
        self.internal_nodes_visited += 1;
    }

    /// Records using a hint.
    pub fn use_hint(&mut self, accurate: bool) {
        self.hints_used += 1;
        if accurate {
            self.hints_accurate += 1;
        }
    }

    /// Returns the hint accuracy ratio.
    pub fn hint_accuracy(&self) -> f64 {
        if self.hints_used == 0 {
            0.0
        } else {
            self.hints_accurate as f64 / self.hints_used as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sagetree::node::LeafEntry;

    fn make_leaf_with_entries(count: usize) -> LeafNode {
        let mut leaf = LeafNode::new(PageId::new(1));
        for i in 0..count {
            leaf.insert(LeafEntry::simple(
                Key::from_str(&format!("key{:03}", i)),
                Value::from_str(&format!("val{}", i)),
            ))
            .unwrap();
        }
        leaf
    }

    #[test]
    fn test_key_range_contains() {
        let range = KeyRange::new(Key::from_str("b"), Key::from_str("e"));

        assert!(!range.contains(&Key::from_str("a")));
        assert!(range.contains(&Key::from_str("b")));
        assert!(range.contains(&Key::from_str("c")));
        assert!(range.contains(&Key::from_str("d")));
        assert!(!range.contains(&Key::from_str("e")));
        assert!(!range.contains(&Key::from_str("f")));
    }

    #[test]
    fn test_key_range_inclusive() {
        let range = KeyRange::inclusive(Key::from_str("b"), Key::from_str("d"));

        assert!(!range.contains(&Key::from_str("a")));
        assert!(range.contains(&Key::from_str("b")));
        assert!(range.contains(&Key::from_str("c")));
        assert!(range.contains(&Key::from_str("d")));
        assert!(!range.contains(&Key::from_str("e")));
    }

    #[test]
    fn test_key_range_prefix() {
        let range = KeyRange::prefix(Key::from_str("user:"));

        assert!(!range.contains(&Key::from_str("admin:1")));
        assert!(range.contains(&Key::from_str("user:")));
        assert!(range.contains(&Key::from_str("user:1")));
        assert!(range.contains(&Key::from_str("user:999")));
        assert!(!range.contains(&Key::from_str("userz")));
    }

    #[test]
    fn test_leaf_cursor_forward() {
        let leaf = make_leaf_with_entries(5);
        let mut cursor = LeafCursor::new(&leaf, KeyRange::all(), Direction::Forward);

        cursor.seek_first();
        assert!(cursor.is_valid());

        let entries = cursor.collect_all();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].key.as_bytes(), b"key000");
        assert_eq!(entries[4].key.as_bytes(), b"key004");
    }

    #[test]
    fn test_leaf_cursor_backward() {
        let leaf = make_leaf_with_entries(5);
        let mut cursor = LeafCursor::new(&leaf, KeyRange::all(), Direction::Backward);

        cursor.seek_first();
        assert!(cursor.is_valid());

        let entries = cursor.collect_all();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].key.as_bytes(), b"key004");
        assert_eq!(entries[4].key.as_bytes(), b"key000");
    }

    #[test]
    fn test_leaf_cursor_with_range() {
        let leaf = make_leaf_with_entries(10);
        let range = KeyRange::new(Key::from_str("key002"), Key::from_str("key007"));
        let mut cursor = LeafCursor::new(&leaf, range, Direction::Forward);

        cursor.seek_first();
        let entries = cursor.collect_all();

        assert_eq!(entries.len(), 5); // key002, key003, key004, key005, key006
        assert_eq!(entries[0].key.as_bytes(), b"key002");
        assert_eq!(entries[4].key.as_bytes(), b"key006");
    }

    #[test]
    fn test_leaf_cursor_seek() {
        let leaf = make_leaf_with_entries(10);
        let mut cursor = LeafCursor::new(&leaf, KeyRange::all(), Direction::Forward);

        cursor.seek(&Key::from_str("key005"));
        assert!(cursor.is_valid());
        assert_eq!(cursor.current_key().unwrap().as_bytes(), b"key005");
    }

    #[test]
    fn test_leaf_cursor_empty() {
        let leaf = LeafNode::new(PageId::new(1));
        let mut cursor = LeafCursor::new(&leaf, KeyRange::all(), Direction::Forward);

        cursor.seek_first();
        assert_eq!(cursor.state(), CursorState::AtEnd);
        assert!(!cursor.is_valid());
    }

    #[test]
    fn test_traversal_path() {
        let mut path = TraversalPath::new();
        assert!(path.is_empty());

        path.push(PageId::new(1), 0);
        path.push(PageId::new(2), 1);
        path.push(PageId::new(3), 2);

        assert_eq!(path.depth(), 3);
        assert_eq!(path.current().unwrap().page_id, PageId::new(3));

        let frame = path.pop().unwrap();
        assert_eq!(frame.page_id, PageId::new(3));
        assert_eq!(frame.child_index, 2);

        assert_eq!(path.depth(), 2);
    }

    #[test]
    fn test_cursor_stats() {
        let mut stats = CursorStats::new();

        stats.visit_entry();
        stats.visit_entry();
        stats.visit_leaf();
        stats.visit_internal();
        stats.use_hint(true);
        stats.use_hint(false);

        assert_eq!(stats.entries_visited, 2);
        assert_eq!(stats.leaves_visited, 1);
        assert_eq!(stats.internal_nodes_visited, 1);
        assert_eq!(stats.hints_used, 2);
        assert_eq!(stats.hint_accuracy(), 0.5);
    }

    #[test]
    fn test_cascading_hint() {
        let hint = CascadingHint::new(5, Some(Key::from_str("key")), 0.8);
        assert!(hint.is_useful());

        let no_hint = CascadingHint::none();
        assert!(!no_hint.is_useful());
    }

    #[test]
    fn test_cursor_position() {
        let pos = CursorPosition::new(PageId::new(1), 3, 10);
        assert!(pos.is_valid());
        assert!(!pos.at_start());
        assert!(!pos.at_end());

        let start_pos = CursorPosition::new(PageId::new(1), 0, 10);
        assert!(start_pos.at_start());

        let end_pos = CursorPosition::new(PageId::new(1), 10, 10);
        assert!(end_pos.at_end());
        assert!(!end_pos.is_valid());
    }
}
