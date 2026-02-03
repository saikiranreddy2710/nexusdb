//! Main SageTree implementation.
//!
//! SageTree is a novel B-tree variant that combines:
//! - Bw-tree style delta chains for reduced write amplification
//! - Fractional cascading for efficient range queries  
//! - MVCC support for snapshot isolation
//!
//! This module provides the main tree structure and operations.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::RwLock;

use nexus_cache::bloom::BloomFilter;
use nexus_common::types::{Key, Lsn, PageId, TxnId, Value};

use super::config::SageTreeConfig;
use super::cursor::{CursorEntry, Direction, KeyRange, LeafCursor};
use super::delta::DeltaNode;
use super::error::{SageTreeError, SageTreeResult};
use super::node::{InternalNode, LeafNode, NodeFlags};

/// Page allocator for the tree.
#[derive(Debug)]
pub struct PageAllocator {
    /// Next page ID to allocate.
    next_page_id: AtomicU64,
    /// Free page list.
    free_pages: RwLock<Vec<PageId>>,
}

impl PageAllocator {
    /// Creates a new allocator starting at page 1.
    pub fn new() -> Self {
        Self {
            next_page_id: AtomicU64::new(1),
            free_pages: RwLock::new(Vec::new()),
        }
    }

    /// Creates an allocator starting at a specific page.
    pub fn starting_at(page_id: u64) -> Self {
        Self {
            next_page_id: AtomicU64::new(page_id),
            free_pages: RwLock::new(Vec::new()),
        }
    }

    /// Allocates a new page ID.
    pub fn allocate(&self) -> PageId {
        // Try to reuse a free page first
        if let Ok(mut free) = self.free_pages.write() {
            if let Some(page_id) = free.pop() {
                return page_id;
            }
        }
        // Allocate new page
        PageId::new(self.next_page_id.fetch_add(1, AtomicOrdering::SeqCst))
    }

    /// Returns a page to the free list.
    pub fn free(&self, page_id: PageId) {
        if let Ok(mut free) = self.free_pages.write() {
            free.push(page_id);
        }
    }

    /// Returns the next page ID that will be allocated.
    pub fn peek_next(&self) -> PageId {
        PageId::new(self.next_page_id.load(AtomicOrdering::SeqCst))
    }
}

impl Default for PageAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the tree.
#[derive(Debug, Clone, Default)]
pub struct TreeStats {
    /// Total number of entries.
    pub entry_count: usize,
    /// Number of leaf nodes.
    pub leaf_count: usize,
    /// Number of internal nodes.
    pub internal_count: usize,
    /// Height of the tree.
    pub height: usize,
    /// Total delta records pending consolidation.
    pub pending_deltas: usize,
    /// Number of split operations.
    pub splits: usize,
    /// Number of merge operations.
    pub merges: usize,
}

/// An in-memory SageTree implementation.
///
/// This is a simplified implementation that stores all nodes in memory.
/// A production implementation would integrate with the BufferPool for
/// disk-based storage.
#[derive(Debug)]
pub struct SageTree {
    /// Configuration.
    config: SageTreeConfig,
    /// Page allocator.
    allocator: PageAllocator,
    /// Root page ID.
    root: RwLock<Option<PageId>>,
    /// Height of the tree (0 = single leaf, increases as tree grows).
    height: AtomicU64,
    /// Node storage (page_id -> node with delta chain).
    nodes: RwLock<HashMap<PageId, DeltaNode>>,
    /// Internal nodes (page_id -> internal node).
    internal_nodes: RwLock<HashMap<PageId, InternalNode>>,
    /// Statistics.
    stats: RwLock<TreeStats>,
    /// Bloom filter for fast negative lookups.
    bloom_filter: RwLock<BloomFilter>,
}

impl SageTree {
    /// Creates a new empty SageTree with default configuration.
    pub fn new() -> Self {
        Self::with_config(SageTreeConfig::default())
    }

    /// Creates a new empty SageTree with the given configuration.
    pub fn with_config(config: SageTreeConfig) -> Self {
        // Create a bloom filter sized for expected items
        // Using 1% false positive rate for 100K expected items initially
        let bloom_filter = BloomFilter::with_rate(100_000, 0.01);

        Self {
            config,
            allocator: PageAllocator::new(),
            root: RwLock::new(None),
            height: AtomicU64::new(0),
            nodes: RwLock::new(HashMap::new()),
            internal_nodes: RwLock::new(HashMap::new()),
            stats: RwLock::new(TreeStats::default()),
            bloom_filter: RwLock::new(bloom_filter),
        }
    }

    /// Returns the configuration.
    pub fn config(&self) -> &SageTreeConfig {
        &self.config
    }

    /// Returns the current height of the tree.
    pub fn height(&self) -> usize {
        self.height.load(AtomicOrdering::SeqCst) as usize
    }

    /// Returns true if the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.root.read().unwrap().is_none()
    }

    /// Returns the number of entries in the tree.
    pub fn len(&self) -> usize {
        self.stats.read().unwrap().entry_count
    }

    /// Returns tree statistics.
    pub fn stats(&self) -> TreeStats {
        self.stats.read().unwrap().clone()
    }

    // =========================================================================
    // Core Operations
    // =========================================================================

    /// Gets a value by key.
    ///
    /// Uses a Bloom filter for fast negative lookups - if the key is
    /// definitely not in the tree, we can return early without traversing.
    pub fn get(&self, key: &Key) -> SageTreeResult<Option<Value>> {
        // Fast path: check bloom filter for definite negative
        {
            let bloom = self.bloom_filter.read().unwrap();
            if !bloom.contains(key) {
                // Key is definitely not in the tree
                return Ok(None);
            }
        }
        // Bloom filter says key might exist, so we need to check the tree

        let root = self.root.read().unwrap();
        let root_id = match *root {
            Some(id) => id,
            None => return Ok(None),
        };
        drop(root);

        // Traverse to leaf
        let leaf_id = self.find_leaf(root_id, key)?;

        // Get from delta node
        let nodes = self.nodes.read().unwrap();
        let node = nodes
            .get(&leaf_id)
            .ok_or(SageTreeError::PageNotFound(leaf_id))?;

        Ok(node.get(key).cloned())
    }

    /// Inserts a key-value pair.
    pub fn insert(&self, key: Key, value: Value) -> SageTreeResult<()> {
        self.insert_with_txn(TxnId::new(1), Lsn::new(0), key, value)
    }

    /// Inserts a key-value pair with transaction info.
    pub fn insert_with_txn(
        &self,
        txn_id: TxnId,
        lsn: Lsn,
        key: Key,
        value: Value,
    ) -> SageTreeResult<()> {
        // Ensure root exists
        self.ensure_root()?;

        let root = self.root.read().unwrap().unwrap();
        drop(self.root.read());

        // Find the leaf node
        let leaf_id = self.find_leaf(root, &key)?;

        // Insert into the delta node
        {
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes
                .get_mut(&leaf_id)
                .ok_or(SageTreeError::PageNotFound(leaf_id))?;

            node.insert(txn_id, lsn, key.clone(), value)?;

            // Check if consolidation is needed
            if node.needs_consolidation() {
                node.consolidate();
            }
        }

        // Add key to bloom filter for fast negative lookups
        {
            let mut bloom = self.bloom_filter.write().unwrap();
            bloom.insert(&key);
        }

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.entry_count += 1;
        }

        // Check if split is needed
        self.maybe_split_leaf(leaf_id)?;

        Ok(())
    }

    /// Updates an existing key-value pair.
    pub fn update(&self, key: &Key, value: Value) -> SageTreeResult<()> {
        self.update_with_txn(TxnId::new(1), Lsn::new(0), key.clone(), value)
    }

    /// Updates an existing key-value pair with transaction info.
    pub fn update_with_txn(
        &self,
        txn_id: TxnId,
        lsn: Lsn,
        key: Key,
        value: Value,
    ) -> SageTreeResult<()> {
        let root = self.root.read().unwrap();
        let root_id = match *root {
            Some(id) => id,
            None => return Err(SageTreeError::KeyNotFound),
        };
        drop(root);

        // Find the leaf node
        let leaf_id = self.find_leaf(root_id, &key)?;

        // Update in the delta node
        {
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes
                .get_mut(&leaf_id)
                .ok_or(SageTreeError::PageNotFound(leaf_id))?;

            node.update(txn_id, lsn, key, value)?;

            // Check if consolidation is needed
            if node.needs_consolidation() {
                node.consolidate();
            }
        }

        Ok(())
    }

    /// Deletes a key.
    pub fn delete(&self, key: &Key) -> SageTreeResult<()> {
        self.delete_with_txn(TxnId::new(1), Lsn::new(0), key.clone())
    }

    /// Deletes a key with transaction info.
    pub fn delete_with_txn(&self, txn_id: TxnId, lsn: Lsn, key: Key) -> SageTreeResult<()> {
        let root = self.root.read().unwrap();
        let root_id = match *root {
            Some(id) => id,
            None => return Err(SageTreeError::KeyNotFound),
        };
        drop(root);

        // Find the leaf node
        let leaf_id = self.find_leaf(root_id, &key)?;

        // Delete from the delta node
        {
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes
                .get_mut(&leaf_id)
                .ok_or(SageTreeError::PageNotFound(leaf_id))?;

            node.delete(txn_id, lsn, key)?;

            // Check if consolidation is needed
            if node.needs_consolidation() {
                node.consolidate();
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.entry_count = stats.entry_count.saturating_sub(1);
        }

        // Note: In a full implementation, we would check for underflow
        // and potentially merge nodes here.

        Ok(())
    }

    /// Upserts a key-value pair (insert or update).
    pub fn upsert(&self, key: Key, value: Value) -> SageTreeResult<()> {
        match self.get(&key)? {
            Some(_) => self.update(&key, value),
            None => self.insert(key, value),
        }
    }

    // =========================================================================
    // Range Operations
    // =========================================================================

    /// Scans a range of keys.
    pub fn scan(&self, range: KeyRange) -> SageTreeResult<Vec<CursorEntry>> {
        self.scan_with_limit(range, usize::MAX)
    }

    /// Scans a range of keys with a limit.
    pub fn scan_with_limit(
        &self,
        range: KeyRange,
        limit: usize,
    ) -> SageTreeResult<Vec<CursorEntry>> {
        let root = self.root.read().unwrap();
        let root_id = match *root {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };
        drop(root);

        // Find the starting leaf
        let start_key = range.seek_key();
        let start_leaf_id = if let Some(key) = start_key {
            self.find_leaf(root_id, key)?
        } else {
            self.find_leftmost_leaf(root_id)?
        };

        let mut results = Vec::new();
        let mut current_leaf_id = Some(start_leaf_id);

        // Iterate through leaves
        while let Some(leaf_id) = current_leaf_id {
            if results.len() >= limit {
                break;
            }

            let nodes = self.nodes.read().unwrap();
            let node = match nodes.get(&leaf_id) {
                Some(n) => n,
                None => break,
            };

            // Create a consolidated view of the leaf
            let mut consolidated = node.base.clone();
            consolidated = node.chain.apply_to_leaf(consolidated);

            // Create cursor over this leaf
            let mut cursor = LeafCursor::new(&consolidated, range.clone(), Direction::Forward);
            cursor.seek_first();

            while cursor.is_valid() && results.len() < limit {
                if let Some(entry) = cursor.next() {
                    results.push(entry);
                }
            }

            // Move to next leaf
            current_leaf_id = if consolidated.header.next_page.is_valid() {
                Some(consolidated.header.next_page)
            } else {
                None
            };

            drop(nodes);

            // Check if we've passed the range end
            if let Some(last) = results.last() {
                if range.is_after_end(&last.key) {
                    results.pop();
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Returns all keys with a given prefix.
    pub fn scan_prefix(&self, prefix: &Key) -> SageTreeResult<Vec<CursorEntry>> {
        self.scan(KeyRange::prefix(prefix.clone()))
    }

    /// Returns the first entry in the tree.
    pub fn first(&self) -> SageTreeResult<Option<CursorEntry>> {
        let mut results = self.scan_with_limit(KeyRange::all(), 1)?;
        Ok(results.pop())
    }

    /// Returns the last entry in the tree.
    pub fn last(&self) -> SageTreeResult<Option<CursorEntry>> {
        let root = self.root.read().unwrap();
        let root_id = match *root {
            Some(id) => id,
            None => return Ok(None),
        };
        drop(root);

        // Find rightmost leaf
        let leaf_id = self.find_rightmost_leaf(root_id)?;

        let nodes = self.nodes.read().unwrap();
        let node = nodes
            .get(&leaf_id)
            .ok_or(SageTreeError::PageNotFound(leaf_id))?;

        // Consolidate and get last entry
        let mut consolidated = node.base.clone();
        consolidated = node.chain.apply_to_leaf(consolidated);

        if let Some(entry) = consolidated.entries.last() {
            Ok(Some(CursorEntry::from_leaf_entry(entry)))
        } else {
            Ok(None)
        }
    }

    // =========================================================================
    // Internal Tree Operations
    // =========================================================================

    /// Ensures the root node exists.
    fn ensure_root(&self) -> SageTreeResult<()> {
        let mut root = self.root.write().unwrap();
        if root.is_none() {
            // Create initial leaf node
            let page_id = self.allocator.allocate();
            let leaf = LeafNode::new(page_id);
            let delta_node = DeltaNode::new(leaf, self.config.max_delta_chain_length);

            let mut nodes = self.nodes.write().unwrap();
            nodes.insert(page_id, delta_node);

            *root = Some(page_id);

            let mut stats = self.stats.write().unwrap();
            stats.leaf_count = 1;
        }
        Ok(())
    }

    /// Finds the leaf node containing (or that would contain) a key.
    fn find_leaf(&self, root_id: PageId, key: &Key) -> SageTreeResult<PageId> {
        let height = self.height();

        if height == 0 {
            // Root is a leaf
            return Ok(root_id);
        }

        // Traverse internal nodes
        let mut current_id = root_id;
        let internal_nodes = self.internal_nodes.read().unwrap();

        for _ in 0..height {
            let node = internal_nodes
                .get(&current_id)
                .ok_or(SageTreeError::PageNotFound(current_id))?;

            current_id = node.find_child_binary(key);
        }

        Ok(current_id)
    }

    /// Finds the leftmost leaf node.
    fn find_leftmost_leaf(&self, root_id: PageId) -> SageTreeResult<PageId> {
        let height = self.height();

        if height == 0 {
            return Ok(root_id);
        }

        let mut current_id = root_id;
        let internal_nodes = self.internal_nodes.read().unwrap();

        for _ in 0..height {
            let node = internal_nodes
                .get(&current_id)
                .ok_or(SageTreeError::PageNotFound(current_id))?;

            current_id = node.leftmost_child;
        }

        Ok(current_id)
    }

    /// Finds the rightmost leaf node.
    fn find_rightmost_leaf(&self, root_id: PageId) -> SageTreeResult<PageId> {
        let height = self.height();

        if height == 0 {
            return Ok(root_id);
        }

        let mut current_id = root_id;
        let internal_nodes = self.internal_nodes.read().unwrap();

        for _ in 0..height {
            let node = internal_nodes
                .get(&current_id)
                .ok_or(SageTreeError::PageNotFound(current_id))?;

            current_id = node
                .entries
                .last()
                .map(|e| e.child)
                .unwrap_or(node.leftmost_child);
        }

        Ok(current_id)
    }

    /// Checks if a leaf node needs splitting and performs the split if needed.
    fn maybe_split_leaf(&self, leaf_id: PageId) -> SageTreeResult<()> {
        let needs_split = {
            let nodes = self.nodes.read().unwrap();
            let node = nodes
                .get(&leaf_id)
                .ok_or(SageTreeError::PageNotFound(leaf_id))?;

            // Consolidate first to get accurate count
            let mut consolidated = node.base.clone();
            consolidated = node.chain.apply_to_leaf(consolidated);

            consolidated.len() > self.config.max_leaf_keys()
        };

        if needs_split {
            self.split_leaf(leaf_id)?;
        }

        Ok(())
    }

    /// Splits a leaf node.
    fn split_leaf(&self, leaf_id: PageId) -> SageTreeResult<()> {
        let (new_leaf_id, separator_key) = {
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes
                .get_mut(&leaf_id)
                .ok_or(SageTreeError::PageNotFound(leaf_id))?;

            // Consolidate before splitting
            node.consolidate();

            // Allocate new page
            let new_page_id = self.allocator.allocate();

            // Split the base leaf
            let right_leaf = node.base.split(new_page_id);
            let separator = right_leaf
                .first_key()
                .ok_or_else(|| SageTreeError::structure_error("split produced empty right node"))?
                .clone();

            // Create new delta node for right leaf
            let right_node = DeltaNode::new(right_leaf, self.config.max_delta_chain_length);
            nodes.insert(new_page_id, right_node);

            (new_page_id, separator)
        };

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.leaf_count += 1;
            stats.splits += 1;
        }

        // Insert separator into parent
        self.insert_into_parent(leaf_id, separator_key, new_leaf_id)?;

        Ok(())
    }

    /// Inserts a separator key and child pointer into the parent node.
    fn insert_into_parent(
        &self,
        left_child: PageId,
        key: Key,
        right_child: PageId,
    ) -> SageTreeResult<()> {
        let root_id = self.root.read().unwrap().unwrap();

        if root_id == left_child {
            // Need to create new root
            let new_root_id = self.allocator.allocate();
            let mut new_root = InternalNode::new(new_root_id, 1);
            new_root.leftmost_child = left_child;
            new_root.insert(key, right_child);
            new_root.header.flags.set(NodeFlags::IS_ROOT);

            {
                let mut internal_nodes = self.internal_nodes.write().unwrap();
                internal_nodes.insert(new_root_id, new_root);
            }

            {
                let mut root = self.root.write().unwrap();
                *root = Some(new_root_id);
            }

            self.height.fetch_add(1, AtomicOrdering::SeqCst);

            {
                let mut stats = self.stats.write().unwrap();
                stats.internal_count += 1;
                stats.height = self.height();
            }
        } else {
            // Find parent and insert
            // For simplicity, we search from root
            // A production implementation would maintain parent pointers
            let parent_id = self.find_parent(root_id, left_child)?;

            let needs_split = {
                let mut internal_nodes = self.internal_nodes.write().unwrap();
                let parent = internal_nodes
                    .get_mut(&parent_id)
                    .ok_or(SageTreeError::PageNotFound(parent_id))?;

                parent.insert(key.clone(), right_child);
                parent.key_count() > self.config.max_internal_keys()
            };

            if needs_split {
                self.split_internal(parent_id)?;
            }
        }

        Ok(())
    }

    /// Finds the parent of a node.
    fn find_parent(&self, root_id: PageId, child_id: PageId) -> SageTreeResult<PageId> {
        let height = self.height();
        if height == 0 {
            return Err(SageTreeError::structure_error("leaf has no parent"));
        }

        let internal_nodes = self.internal_nodes.read().unwrap();

        fn find_recursive(
            internal_nodes: &HashMap<PageId, InternalNode>,
            current: PageId,
            target: PageId,
            remaining_levels: usize,
        ) -> Option<PageId> {
            if remaining_levels == 0 {
                return None;
            }

            let node = internal_nodes.get(&current)?;

            // Check if any child is the target
            if node.leftmost_child == target {
                return Some(current);
            }
            for entry in &node.entries {
                if entry.child == target {
                    return Some(current);
                }
            }

            // Recurse into children
            if remaining_levels > 1 {
                if let Some(result) = find_recursive(
                    internal_nodes,
                    node.leftmost_child,
                    target,
                    remaining_levels - 1,
                ) {
                    return Some(result);
                }
                for entry in &node.entries {
                    if let Some(result) =
                        find_recursive(internal_nodes, entry.child, target, remaining_levels - 1)
                    {
                        return Some(result);
                    }
                }
            }

            None
        }

        find_recursive(&internal_nodes, root_id, child_id, height)
            .ok_or_else(|| SageTreeError::structure_error("parent not found"))
    }

    /// Splits an internal node.
    fn split_internal(&self, node_id: PageId) -> SageTreeResult<()> {
        let (new_node_id, separator_key) = {
            let mut internal_nodes = self.internal_nodes.write().unwrap();
            let node = internal_nodes
                .get_mut(&node_id)
                .ok_or(SageTreeError::PageNotFound(node_id))?;

            let new_page_id = self.allocator.allocate();
            let (separator, right_node) = node.split(new_page_id);

            internal_nodes.insert(new_page_id, right_node);

            (new_page_id, separator)
        };

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.internal_count += 1;
            stats.splits += 1;
        }

        // Insert separator into parent
        self.insert_into_parent(node_id, separator_key, new_node_id)?;

        Ok(())
    }

    // =========================================================================
    // Maintenance Operations
    // =========================================================================

    /// Consolidates all delta chains in the tree.
    pub fn consolidate_all(&self) {
        let mut nodes = self.nodes.write().unwrap();
        for node in nodes.values_mut() {
            node.consolidate();
        }
    }

    /// Returns the number of pending delta records across all nodes.
    pub fn pending_deltas(&self) -> usize {
        let nodes = self.nodes.read().unwrap();
        nodes.values().map(|n| n.chain.len()).sum()
    }

    /// Clears all entries from the tree.
    pub fn clear(&self) {
        {
            let mut root = self.root.write().unwrap();
            *root = None;
        }
        {
            let mut nodes = self.nodes.write().unwrap();
            nodes.clear();
        }
        {
            let mut internal_nodes = self.internal_nodes.write().unwrap();
            internal_nodes.clear();
        }
        self.height.store(0, AtomicOrdering::SeqCst);
        {
            let mut stats = self.stats.write().unwrap();
            *stats = TreeStats::default();
        }
    }
}

impl Default for SageTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_creation() {
        let tree = SageTree::new();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        assert_eq!(tree.height(), 0);
    }

    #[test]
    fn test_single_insert_get() {
        let tree = SageTree::new();

        tree.insert(Key::from_str("key1"), Value::from_str("value1"))
            .unwrap();

        assert!(!tree.is_empty());
        assert_eq!(tree.len(), 1);

        let result = tree.get(&Key::from_str("key1")).unwrap();
        assert_eq!(result.unwrap().as_bytes(), b"value1");
    }

    #[test]
    fn test_multiple_inserts() {
        let tree = SageTree::new();

        for i in 0..100 {
            tree.insert(
                Key::from_str(&format!("key{:03}", i)),
                Value::from_str(&format!("value{}", i)),
            )
            .unwrap();
        }

        assert_eq!(tree.len(), 100);

        for i in 0..100 {
            let result = tree.get(&Key::from_str(&format!("key{:03}", i))).unwrap();
            assert!(result.is_some());
        }
    }

    #[test]
    fn test_get_nonexistent() {
        let tree = SageTree::new();
        tree.insert(Key::from_str("key1"), Value::from_str("value1"))
            .unwrap();

        let result = tree.get(&Key::from_str("nonexistent")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_update() {
        let tree = SageTree::new();

        tree.insert(Key::from_str("key1"), Value::from_str("original"))
            .unwrap();
        tree.update(&Key::from_str("key1"), Value::from_str("updated"))
            .unwrap();

        let result = tree.get(&Key::from_str("key1")).unwrap();
        assert_eq!(result.unwrap().as_bytes(), b"updated");
    }

    #[test]
    fn test_update_nonexistent() {
        let tree = SageTree::new();

        let result = tree.update(&Key::from_str("key1"), Value::from_str("value"));
        assert!(matches!(result, Err(SageTreeError::KeyNotFound)));
    }

    #[test]
    fn test_delete() {
        let tree = SageTree::new();

        tree.insert(Key::from_str("key1"), Value::from_str("value1"))
            .unwrap();
        tree.insert(Key::from_str("key2"), Value::from_str("value2"))
            .unwrap();

        tree.delete(&Key::from_str("key1")).unwrap();

        assert!(tree.get(&Key::from_str("key1")).unwrap().is_none());
        assert!(tree.get(&Key::from_str("key2")).unwrap().is_some());
    }

    #[test]
    fn test_upsert() {
        let tree = SageTree::new();

        // Insert new
        tree.upsert(Key::from_str("key1"), Value::from_str("value1"))
            .unwrap();
        assert_eq!(
            tree.get(&Key::from_str("key1"))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"value1"
        );

        // Update existing
        tree.upsert(Key::from_str("key1"), Value::from_str("updated"))
            .unwrap();
        assert_eq!(
            tree.get(&Key::from_str("key1"))
                .unwrap()
                .unwrap()
                .as_bytes(),
            b"updated"
        );
    }

    #[test]
    fn test_scan_all() {
        let tree = SageTree::new();

        for i in 0..10 {
            tree.insert(
                Key::from_str(&format!("key{:02}", i)),
                Value::from_str(&format!("val{}", i)),
            )
            .unwrap();
        }

        let results = tree.scan(KeyRange::all()).unwrap();
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].key.as_bytes(), b"key00");
        assert_eq!(results[9].key.as_bytes(), b"key09");
    }

    #[test]
    fn test_scan_range() {
        let tree = SageTree::new();

        for i in 0..10 {
            tree.insert(
                Key::from_str(&format!("key{:02}", i)),
                Value::from_str(&format!("val{}", i)),
            )
            .unwrap();
        }

        let range = KeyRange::new(Key::from_str("key03"), Key::from_str("key07"));
        let results = tree.scan(range).unwrap();

        assert_eq!(results.len(), 4); // key03, key04, key05, key06
        assert_eq!(results[0].key.as_bytes(), b"key03");
        assert_eq!(results[3].key.as_bytes(), b"key06");
    }

    #[test]
    fn test_scan_with_limit() {
        let tree = SageTree::new();

        for i in 0..100 {
            tree.insert(
                Key::from_str(&format!("key{:03}", i)),
                Value::from_str(&format!("val{}", i)),
            )
            .unwrap();
        }

        let results = tree.scan_with_limit(KeyRange::all(), 10).unwrap();
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_first_last() {
        let tree = SageTree::new();

        tree.insert(Key::from_str("b"), Value::from_str("2"))
            .unwrap();
        tree.insert(Key::from_str("a"), Value::from_str("1"))
            .unwrap();
        tree.insert(Key::from_str("c"), Value::from_str("3"))
            .unwrap();

        let first = tree.first().unwrap().unwrap();
        assert_eq!(first.key.as_bytes(), b"a");

        let last = tree.last().unwrap().unwrap();
        assert_eq!(last.key.as_bytes(), b"c");
    }

    #[test]
    fn test_first_last_empty() {
        let tree = SageTree::new();

        assert!(tree.first().unwrap().is_none());
        assert!(tree.last().unwrap().is_none());
    }

    #[test]
    fn test_consolidation() {
        let config = SageTreeConfig::for_testing().with_max_delta_chain_length(3);
        let tree = SageTree::with_config(config);

        // Insert enough to trigger consolidation
        for i in 0..10 {
            tree.insert(
                Key::from_str(&format!("key{}", i)),
                Value::from_str(&format!("val{}", i)),
            )
            .unwrap();
        }

        // Force consolidation
        tree.consolidate_all();
        assert_eq!(tree.pending_deltas(), 0);
    }

    #[test]
    fn test_clear() {
        let tree = SageTree::new();

        for i in 0..10 {
            tree.insert(
                Key::from_str(&format!("key{}", i)),
                Value::from_str(&format!("val{}", i)),
            )
            .unwrap();
        }

        assert!(!tree.is_empty());

        tree.clear();

        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        assert_eq!(tree.height(), 0);
    }

    #[test]
    fn test_page_allocator() {
        let allocator = PageAllocator::new();

        let p1 = allocator.allocate();
        let p2 = allocator.allocate();
        let p3 = allocator.allocate();

        assert_eq!(p1.as_u64(), 1);
        assert_eq!(p2.as_u64(), 2);
        assert_eq!(p3.as_u64(), 3);

        allocator.free(p2);
        let p4 = allocator.allocate();
        assert_eq!(p4, p2); // Reused
    }

    #[test]
    fn test_tree_stats() {
        let tree = SageTree::new();

        for i in 0..5 {
            tree.insert(
                Key::from_str(&format!("key{}", i)),
                Value::from_str(&format!("val{}", i)),
            )
            .unwrap();
        }

        let stats = tree.stats();
        assert_eq!(stats.entry_count, 5);
        assert!(stats.leaf_count >= 1);
    }

    #[test]
    fn test_duplicate_key() {
        let tree = SageTree::new();

        tree.insert(Key::from_str("key1"), Value::from_str("value1"))
            .unwrap();
        let result = tree.insert(Key::from_str("key1"), Value::from_str("value2"));

        assert!(matches!(result, Err(SageTreeError::DuplicateKey)));
    }

    #[test]
    fn test_many_inserts_splits() {
        // Use small max_leaf_keys to trigger splits
        let config = SageTreeConfig::for_testing().with_branching_factor(8);
        let tree = SageTree::with_config(config);

        for i in 0..100 {
            tree.insert(
                Key::from_str(&format!("key{:03}", i)),
                Value::from_str(&format!("val{}", i)),
            )
            .unwrap();
        }

        // Verify all entries exist
        for i in 0..100 {
            let result = tree.get(&Key::from_str(&format!("key{:03}", i))).unwrap();
            assert!(result.is_some(), "Missing key{:03}", i);
        }

        let stats = tree.stats();
        assert!(stats.splits > 0, "Expected some splits to occur");
    }
}
