//! Main SageTree implementation.
//!
//! SageTree is a novel B-tree variant that combines:
//! - Bw-tree style delta chains for reduced write amplification
//! - Fractional cascading for efficient range queries  
//! - MVCC support for snapshot isolation
//!
//! This module provides the main tree structure and operations.

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::RwLock;

use nexus_cache::bloom::BloomFilter;
use nexus_common::types::{Key, Lsn, PageId, TxnId, Value};

use super::config::SageTreeConfig;
use super::cursor::{CursorEntry, Direction, KeyRange, LeafCursor};
use super::delta::DeltaNode;
use super::error::{SageTreeError, SageTreeResult};
use super::node::{InternalNode, LeafNode, NodeFlags};
use super::pager::{MemoryPager, Pager};

/// Page allocator for the tree (delegates to the Pager).
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
        if let Ok(mut free) = self.free_pages.write() {
            if let Some(page_id) = free.pop() {
                return page_id;
            }
        }
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

/// SageTree: a B-tree variant with delta chains and MVCC support.
///
/// Storage is abstracted through the [`Pager`] trait, allowing the tree
/// to operate against in-memory HashMaps (default) or a disk-backed file.
///
/// Use [`SageTree::new()`] for a purely in-memory tree, or
/// [`SageTree::with_pager()`] to supply a custom [`Pager`] (e.g.
/// [`FilePager`](super::pager::FilePager) for disk persistence).
#[derive(Debug)]
pub struct SageTree {
    /// Configuration.
    config: SageTreeConfig,
    /// Page-level storage backend.
    pager: Box<dyn Pager>,
    /// Root page ID.
    root: RwLock<Option<PageId>>,
    /// Height of the tree (0 = single leaf, increases as tree grows).
    height: AtomicU64,
    /// Statistics.
    stats: RwLock<TreeStats>,
    /// Bloom filter for fast negative lookups.
    bloom_filter: RwLock<BloomFilter>,
}

impl SageTree {
    /// Creates a new empty SageTree with default configuration (in-memory).
    pub fn new() -> Self {
        Self::with_config(SageTreeConfig::default())
    }

    /// Creates a new empty SageTree with the given configuration (in-memory).
    pub fn with_config(config: SageTreeConfig) -> Self {
        Self::with_pager(config, Box::new(MemoryPager::new()))
    }

    /// Creates a new SageTree backed by the given [`Pager`].
    ///
    /// If the pager already contains pages (e.g. loaded from disk by
    /// [`FilePager::open`]), call [`rebuild_from_pager`](Self::rebuild_from_pager)
    /// afterwards to reconstruct the in-memory tree state (root pointer,
    /// height, stats, bloom filter).
    pub fn with_pager(config: SageTreeConfig, pager: Box<dyn Pager>) -> Self {
        let bloom_filter = BloomFilter::with_rate(100_000, 0.01);

        let mut tree = Self {
            config,
            pager,
            root: RwLock::new(None),
            height: AtomicU64::new(0),
            stats: RwLock::new(TreeStats::default()),
            bloom_filter: RwLock::new(bloom_filter),
        };

        // Auto-rebuild if the pager already has pages loaded
        let has_leaves = !tree.pager.leaf_page_ids().is_empty();
        let has_internals = !tree.pager.internal_page_ids().is_empty();
        if has_leaves || has_internals {
            let _ = tree.rebuild_from_pager();
        }

        tree
    }

    /// Reconstructs tree state (root, height, stats, bloom filter) from
    /// pages already present in the pager.
    ///
    /// This is called automatically by [`with_pager`](Self::with_pager) when
    /// the pager has pre-loaded pages (e.g. from a [`FilePager`]).
    pub fn rebuild_from_pager(&mut self) -> SageTreeResult<()> {
        let leaf_ids = self.pager.leaf_page_ids();
        let internal_ids = self.pager.internal_page_ids();

        if leaf_ids.is_empty() && internal_ids.is_empty() {
            return Ok(());
        }

        // Find the root: the internal node with the highest level,
        // or the single leaf if there are no internal nodes.
        let mut max_level: u16 = 0;
        let mut root_id: Option<PageId> = None;

        for &pid in &internal_ids {
            if let Ok(node) = self.pager.get_internal(pid) {
                if node.header.level >= max_level {
                    max_level = node.header.level;
                    root_id = Some(pid);
                }
            }
        }

        // If no internal nodes, root is a leaf
        if root_id.is_none() {
            if leaf_ids.len() == 1 {
                root_id = Some(leaf_ids[0]);
            } else if !leaf_ids.is_empty() {
                // Multiple leaves but no internal nodes — pick the one that
                // has no prev_page (the leftmost leaf is the root at height 0
                // only when there is exactly one leaf; multiple leaves without
                // an internal parent shouldn't happen, but handle gracefully).
                for &lid in &leaf_ids {
                    if let Ok(node) = self.pager.get_leaf(lid) {
                        if !node.base.header.prev_page.is_valid() {
                            root_id = Some(lid);
                            break;
                        }
                    }
                }
                if root_id.is_none() {
                    root_id = Some(leaf_ids[0]);
                }
            }
        }

        // Count entries and rebuild bloom filter
        let mut entry_count = 0usize;
        let mut bloom = BloomFilter::with_rate(
            std::cmp::max(100_000, leaf_ids.len() * 256),
            0.01,
        );

        for &lid in &leaf_ids {
            if let Ok(node) = self.pager.get_leaf(lid) {
                let mut consolidated = node.base.clone();
                consolidated = node.chain.apply_to_leaf(consolidated);
                for entry in &consolidated.entries {
                    bloom.insert(&entry.key);
                    entry_count += 1;
                }
            }
        }

        // Set tree state
        *self.root.write().unwrap() = root_id;
        self.height.store(max_level as u64, AtomicOrdering::SeqCst);
        *self.bloom_filter.write().unwrap() = bloom;
        {
            let mut stats = self.stats.write().unwrap();
            stats.entry_count = entry_count;
            stats.leaf_count = leaf_ids.len();
            stats.internal_count = internal_ids.len();
            stats.height = max_level as usize;
        }

        Ok(())
    }

    /// Flush the underlying pager to disk.
    pub fn flush(&self) -> SageTreeResult<()> {
        self.pager.flush()
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
                return Ok(None);
            }
        }

        let root = self.root.read().unwrap();
        let root_id = match *root {
            Some(id) => id,
            None => return Ok(None),
        };
        drop(root);

        let leaf_id = self.find_leaf(root_id, key)?;
        let node = self.pager.get_leaf(leaf_id)?;

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

        // Find the leaf node
        let leaf_id = self.find_leaf(root, &key)?;

        // Insert into the delta node
        {
            let mut node = self.pager.get_leaf(leaf_id)?;
            node.insert(txn_id, lsn, key.clone(), value)?;

            if node.needs_consolidation() {
                node.consolidate();
            }
            self.pager.put_leaf(leaf_id, &node)?;
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
            let mut node = self.pager.get_leaf(leaf_id)?;
            node.update(txn_id, lsn, key, value)?;

            if node.needs_consolidation() {
                node.consolidate();
            }
            self.pager.put_leaf(leaf_id, &node)?;
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

        let leaf_id = self.find_leaf(root_id, &key)?;

        {
            let mut node = self.pager.get_leaf(leaf_id)?;
            node.delete(txn_id, lsn, key)?;

            if node.needs_consolidation() {
                node.consolidate();
            }
            self.pager.put_leaf(leaf_id, &node)?;
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

            let node = match self.pager.get_leaf(leaf_id) {
                Ok(n) => n,
                Err(_) => break,
            };

            // Create a consolidated view of the leaf
            let mut consolidated = node.base.clone();
            consolidated = node.chain.apply_to_leaf(consolidated);

            let mut cursor = LeafCursor::new(&consolidated, range.clone(), Direction::Forward);
            cursor.seek_first();

            while cursor.is_valid() && results.len() < limit {
                if let Some(entry) = cursor.next() {
                    results.push(entry);
                }
            }

            current_leaf_id = if consolidated.header.next_page.is_valid() {
                Some(consolidated.header.next_page)
            } else {
                None
            };

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

        let node = self.pager.get_leaf(leaf_id)?;

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
            let page_id = self.pager.allocate_page();
            let leaf = LeafNode::new(page_id);
            let delta_node = DeltaNode::new(leaf, self.config.max_delta_chain_length);

            self.pager.put_leaf(page_id, &delta_node)?;
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
            return Ok(root_id);
        }

        let mut current_id = root_id;
        for _ in 0..height {
            let node = self.pager.get_internal(current_id)?;
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
        for _ in 0..height {
            let node = self.pager.get_internal(current_id)?;
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
        for _ in 0..height {
            let node = self.pager.get_internal(current_id)?;
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
            let node = self.pager.get_leaf(leaf_id)?;
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
            let mut node = self.pager.get_leaf(leaf_id)?;

            // Consolidate before splitting
            node.consolidate();

            let new_page_id = self.pager.allocate_page();

            // Split the base leaf
            let right_leaf = node.base.split(new_page_id);
            let separator = right_leaf
                .first_key()
                .ok_or_else(|| SageTreeError::structure_error("split produced empty right node"))?
                .clone();

            let right_node = DeltaNode::new(right_leaf, self.config.max_delta_chain_length);

            // Persist both halves
            self.pager.put_leaf(leaf_id, &node)?;
            self.pager.put_leaf(new_page_id, &right_node)?;

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
            let new_root_id = self.pager.allocate_page();
            let mut new_root = InternalNode::new(new_root_id, 1);
            new_root.leftmost_child = left_child;
            new_root.insert(key, right_child);
            new_root.header.flags.set(NodeFlags::IS_ROOT);

            self.pager.put_internal(new_root_id, &new_root)?;

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
            let parent_id = self.find_parent(root_id, left_child)?;

            let needs_split = {
                let mut parent = self.pager.get_internal(parent_id)?;
                parent.insert(key.clone(), right_child);
                let over = parent.key_count() > self.config.max_internal_keys();
                self.pager.put_internal(parent_id, &parent)?;
                over
            };

            if needs_split {
                self.split_internal(parent_id)?;
            }
        }

        Ok(())
    }

    /// Finds the parent of a node by searching from the root.
    fn find_parent(&self, root_id: PageId, child_id: PageId) -> SageTreeResult<PageId> {
        let height = self.height();
        if height == 0 {
            return Err(SageTreeError::structure_error("leaf has no parent"));
        }

        self.find_parent_recursive(root_id, child_id, height)
            .ok_or_else(|| SageTreeError::structure_error("parent not found"))
    }

    fn find_parent_recursive(
        &self,
        current: PageId,
        target: PageId,
        remaining: usize,
    ) -> Option<PageId> {
        if remaining == 0 {
            return None;
        }

        let node = self.pager.get_internal(current).ok()?;

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
        if remaining > 1 {
            if let Some(result) =
                self.find_parent_recursive(node.leftmost_child, target, remaining - 1)
            {
                return Some(result);
            }
            for entry in &node.entries {
                if let Some(result) = self.find_parent_recursive(entry.child, target, remaining - 1)
                {
                    return Some(result);
                }
            }
        }

        None
    }

    /// Splits an internal node.
    fn split_internal(&self, node_id: PageId) -> SageTreeResult<()> {
        let (new_node_id, separator_key) = {
            let mut node = self.pager.get_internal(node_id)?;

            let new_page_id = self.pager.allocate_page();
            let (separator, right_node) = node.split(new_page_id);

            self.pager.put_internal(node_id, &node)?;
            self.pager.put_internal(new_page_id, &right_node)?;

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
    ///
    /// Note: This only works efficiently with the MemoryPager. For the
    /// FilePager, consolidation happens on `put_leaf()` automatically.
    pub fn consolidate_all(&self) {
        // Collect all leaf pages by scanning from leftmost leaf
        let root = self.root.read().unwrap();
        let root_id = match *root {
            Some(id) => id,
            None => return,
        };
        drop(root);

        if let Ok(start) = self.find_leftmost_leaf(root_id) {
            let mut current = Some(start);
            while let Some(leaf_id) = current {
                if let Ok(mut node) = self.pager.get_leaf(leaf_id) {
                    let next = if node.base.header.next_page.is_valid() {
                        Some(node.base.header.next_page)
                    } else {
                        None
                    };
                    if !node.chain.is_empty() {
                        node.consolidate();
                        let _ = self.pager.put_leaf(leaf_id, &node);
                    }
                    current = next;
                } else {
                    break;
                }
            }
        }
    }

    /// Returns the number of pending delta records across all nodes.
    pub fn pending_deltas(&self) -> usize {
        let root = self.root.read().unwrap();
        let root_id = match *root {
            Some(id) => id,
            None => return 0,
        };
        drop(root);

        let mut count = 0;
        if let Ok(start) = self.find_leftmost_leaf(root_id) {
            let mut current = Some(start);
            while let Some(leaf_id) = current {
                if let Ok(node) = self.pager.get_leaf(leaf_id) {
                    count += node.chain.len();
                    current = if node.base.header.next_page.is_valid() {
                        Some(node.base.header.next_page)
                    } else {
                        None
                    };
                } else {
                    break;
                }
            }
        }
        count
    }

    /// Clears all entries from the tree, freeing all pager pages.
    pub fn clear(&self) {
        // Free all pages from the pager before resetting tree state
        let _ = self.pager.clear();
        {
            let mut root = self.root.write().unwrap();
            *root = None;
        }
        self.height.store(0, AtomicOrdering::SeqCst);
        {
            let mut stats = self.stats.write().unwrap();
            *stats = TreeStats::default();
        }
        {
            let mut bloom = self.bloom_filter.write().unwrap();
            bloom.clear();
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
