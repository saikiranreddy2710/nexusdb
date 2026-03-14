//! Page-level storage abstraction for the SageTree.
//!
//! The `Pager` trait abstracts how tree nodes are read and written,
//! enabling the SageTree to work with both in-memory storage (for
//! testing) and disk-backed storage via the BufferPool.
//!
//! ## Implementations
//!
//! - [`MemoryPager`]: Stores nodes in `HashMap`s. Zero I/O overhead,
//!   no persistence. Used by tests and in-memory databases.
//!
//! - [`FilePager`]: Serializes nodes to fixed-size pages in a data file
//!   with an in-memory cache. Provides crash recovery by reading the
//!   serialized pages back on open.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use nexus_common::types::PageId;

use super::delta::DeltaNode;
use super::error::{SageTreeError, SageTreeResult};
use super::node::{InternalNode, LeafNode, NodeType};

// ── Page Layout Constants ───────────────────────────────────────────────────

/// Page size for the file pager (8 KB, matching the buffer pool).
const FILE_PAGE_SIZE: usize = 8192;

/// Per-page envelope: [node_type: 1][data_len: 4][data: N][padding]
const ENVELOPE_HEADER: usize = 9; // 1 (type) + 4 (len) + 4 (crc32)

/// Magic indicating an unused (free) page on disk.
const FREE_PAGE_MARKER: u8 = 0x00;

// ── Pager Trait ─────────────────────────────────────────────────────────────

/// Abstraction for reading/writing SageTree nodes.
///
/// All methods are synchronous. Implementations must be `Send + Sync`
/// because the `SageTree` holds a `dyn Pager` behind a trait object
/// shared across threads.
pub trait Pager: Send + Sync + std::fmt::Debug {
    // ── Leaf nodes ──────────────────────────────────────────────

    /// Load a leaf node (with its delta chain) by page ID.
    fn get_leaf(&self, page_id: PageId) -> SageTreeResult<DeltaNode>;

    /// Store a leaf node (with its delta chain) at the given page ID.
    fn put_leaf(&self, page_id: PageId, node: &DeltaNode) -> SageTreeResult<()>;

    /// Remove a leaf node.
    fn remove_leaf(&self, page_id: PageId) -> SageTreeResult<()>;

    // ── Internal nodes ──────────────────────────────────────────

    /// Load an internal node by page ID.
    fn get_internal(&self, page_id: PageId) -> SageTreeResult<InternalNode>;

    /// Store an internal node at the given page ID.
    fn put_internal(&self, page_id: PageId, node: &InternalNode) -> SageTreeResult<()>;

    /// Remove an internal node.
    fn remove_internal(&self, page_id: PageId) -> SageTreeResult<()>;

    // ── Page allocation ─────────────────────────────────────────

    /// Allocate a new page ID.
    fn allocate_page(&self) -> PageId;

    /// Return a page ID to the free list.
    fn free_page(&self, page_id: PageId);

    // ── Enumeration ──────────────────────────────────────────────

    /// Returns all page IDs that currently hold leaf nodes.
    fn leaf_page_ids(&self) -> Vec<PageId>;

    /// Returns all page IDs that currently hold internal nodes.
    fn internal_page_ids(&self) -> Vec<PageId>;

    // ── Lifecycle ───────────────────────────────────────────────

    /// Flush all dirty data to disk (no-op for memory pager).
    fn flush(&self) -> SageTreeResult<()>;

    /// Clear all pages, releasing resources. Resets the pager to an empty state.
    fn clear(&self) -> SageTreeResult<()>;
}

// ── MemoryPager ─────────────────────────────────────────────────────────────

/// In-memory pager using HashMaps. No disk I/O, no persistence.
///
/// Uses `parking_lot::RwLock` instead of `std::sync::RwLock` to avoid
/// lock poisoning and improve performance under contention.
#[derive(Debug)]
pub struct MemoryPager {
    leaves: parking_lot::RwLock<HashMap<PageId, DeltaNode>>,
    internals: parking_lot::RwLock<HashMap<PageId, InternalNode>>,
    next_page: std::sync::atomic::AtomicU64,
    free_pages: parking_lot::RwLock<Vec<PageId>>,
}

impl MemoryPager {
    /// Create a new empty memory pager.
    pub fn new() -> Self {
        Self {
            leaves: parking_lot::RwLock::new(HashMap::new()),
            internals: parking_lot::RwLock::new(HashMap::new()),
            next_page: std::sync::atomic::AtomicU64::new(1),
            free_pages: parking_lot::RwLock::new(Vec::new()),
        }
    }
}

impl Default for MemoryPager {
    fn default() -> Self {
        Self::new()
    }
}

impl Pager for MemoryPager {
    fn get_leaf(&self, page_id: PageId) -> SageTreeResult<DeltaNode> {
        let leaves = self.leaves.read();
        leaves
            .get(&page_id)
            .cloned()
            .ok_or(SageTreeError::PageNotFound(page_id))
    }

    fn put_leaf(&self, page_id: PageId, node: &DeltaNode) -> SageTreeResult<()> {
        let mut leaves = self.leaves.write();
        leaves.insert(page_id, node.clone());
        Ok(())
    }

    fn remove_leaf(&self, page_id: PageId) -> SageTreeResult<()> {
        let mut leaves = self.leaves.write();
        leaves.remove(&page_id);
        Ok(())
    }

    fn get_internal(&self, page_id: PageId) -> SageTreeResult<InternalNode> {
        let internals = self.internals.read();
        internals
            .get(&page_id)
            .cloned()
            .ok_or(SageTreeError::PageNotFound(page_id))
    }

    fn put_internal(&self, page_id: PageId, node: &InternalNode) -> SageTreeResult<()> {
        let mut internals = self.internals.write();
        internals.insert(page_id, node.clone());
        Ok(())
    }

    fn remove_internal(&self, page_id: PageId) -> SageTreeResult<()> {
        let mut internals = self.internals.write();
        internals.remove(&page_id);
        Ok(())
    }

    fn allocate_page(&self) -> PageId {
        let mut free = self.free_pages.write();
        if let Some(page) = free.pop() {
            return page;
        }
        drop(free);
        PageId::new(
            self.next_page
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        )
    }

    fn free_page(&self, page_id: PageId) {
        let mut free = self.free_pages.write();
        free.push(page_id);
    }

    fn leaf_page_ids(&self) -> Vec<PageId> {
        self.leaves.read().keys().copied().collect()
    }

    fn internal_page_ids(&self) -> Vec<PageId> {
        self.internals.read().keys().copied().collect()
    }

    fn flush(&self) -> SageTreeResult<()> {
        Ok(()) // No-op for memory
    }

    fn clear(&self) -> SageTreeResult<()> {
        self.leaves.write().clear();
        self.internals.write().clear();
        self.free_pages.write().clear();
        self.next_page.store(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

// ── FilePager ───────────────────────────────────────────────────────────────

/// Disk-backed pager that serializes nodes to fixed-size pages in a file.
///
/// Each page is `FILE_PAGE_SIZE` bytes at file offset `page_id * FILE_PAGE_SIZE`.
/// Format per page: `[node_type: u8][data_len: u32-LE][serialized_data][zero-padding]`
///
/// An in-memory write-back cache avoids redundant disk reads. Dirty pages
/// are written on `put_*` calls immediately (write-through).
#[derive(Debug)]
pub struct FilePager {
    /// Path to the data file.
    path: PathBuf,
    /// File handle (protected by Mutex for positioned writes).
    file: std::sync::Mutex<File>,
    /// In-memory cache for leaf nodes.
    leaf_cache: std::sync::RwLock<HashMap<PageId, DeltaNode>>,
    /// In-memory cache for internal nodes.
    internal_cache: std::sync::RwLock<HashMap<PageId, InternalNode>>,
    /// Page allocator state.
    next_page: std::sync::atomic::AtomicU64,
    free_pages: std::sync::RwLock<Vec<PageId>>,
}

impl FilePager {
    /// Returns the path to the data file.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Create or open a file pager at the given path.
    ///
    /// If the file exists, all pages are loaded into the cache. If it
    /// doesn't exist, a new empty file is created.
    pub fn open(path: impl Into<PathBuf>) -> SageTreeResult<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let mut pager = Self {
            path,
            file: std::sync::Mutex::new(file),
            leaf_cache: std::sync::RwLock::new(HashMap::new()),
            internal_cache: std::sync::RwLock::new(HashMap::new()),
            next_page: std::sync::atomic::AtomicU64::new(1),
            free_pages: std::sync::RwLock::new(Vec::new()),
        };

        // Load existing pages from the file into cache
        pager.load_all()?;

        Ok(pager)
    }

    /// Load all pages from the data file into the in-memory cache.
    fn load_all(&mut self) -> SageTreeResult<()> {
        let file = self.file.get_mut().unwrap();
        let file_len = file.metadata()?.len();

        if file_len == 0 {
            return Ok(());
        }

        let num_pages = file_len / FILE_PAGE_SIZE as u64;
        let mut buf = vec![0u8; FILE_PAGE_SIZE];
        let mut max_page_id = 0u64;

        for page_idx in 0..num_pages {
            let offset = page_idx * FILE_PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset))?;

            if let Err(_) = file.read_exact(&mut buf) {
                // Short read (partial page at end of file), skip
                continue;
            }

            let node_type_byte = buf[0];
            if node_type_byte == FREE_PAGE_MARKER {
                // Free page, skip
                self.free_pages
                    .get_mut()
                    .unwrap()
                    .push(PageId::new(page_idx));
                continue;
            }

            let data_len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
            if data_len == 0 || ENVELOPE_HEADER + data_len > FILE_PAGE_SIZE {
                continue; // Corrupted or empty, skip
            }

            let stored_crc = u32::from_le_bytes(buf[5..9].try_into().unwrap());
            let data = &buf[ENVELOPE_HEADER..ENVELOPE_HEADER + data_len];
            let computed_crc = crc32fast::hash(data);
            if stored_crc != computed_crc {
                // CRC mismatch — page is corrupt, treat as free
                self.free_pages
                    .get_mut()
                    .unwrap()
                    .push(PageId::new(page_idx));
                continue;
            }
            let page_id = PageId::new(page_idx);

            match NodeType::from_byte(node_type_byte) {
                Some(NodeType::Leaf) => {
                    if let Ok(leaf) = LeafNode::deserialize(data) {
                        let delta = DeltaNode::new(leaf, 8);
                        self.leaf_cache.get_mut().unwrap().insert(page_id, delta);
                    }
                }
                Some(NodeType::Internal) => {
                    if let Ok(internal) = InternalNode::deserialize(data) {
                        self.internal_cache
                            .get_mut()
                            .unwrap()
                            .insert(page_id, internal);
                    }
                }
                _ => {
                    // Unknown type, treat as free
                    self.free_pages.get_mut().unwrap().push(page_id);
                }
            }

            max_page_id = max_page_id.max(page_idx);
        }

        // Set the next page allocator past the highest page seen
        if num_pages > 0 {
            *self.next_page.get_mut() = num_pages;
        }

        Ok(())
    }

    /// Write a single page to the data file.
    fn write_page(&self, page_id: PageId, node_type: NodeType, data: &[u8]) -> SageTreeResult<()> {
        if ENVELOPE_HEADER + data.len() > FILE_PAGE_SIZE {
            return Err(SageTreeError::serialization(format!(
                "serialized node too large for page: {} bytes (max {})",
                data.len(),
                FILE_PAGE_SIZE - ENVELOPE_HEADER
            )));
        }

        let mut page_buf = vec![0u8; FILE_PAGE_SIZE];
        page_buf[0] = node_type.as_byte();
        page_buf[1..5].copy_from_slice(&(data.len() as u32).to_le_bytes());
        let crc = crc32fast::hash(data);
        page_buf[5..9].copy_from_slice(&crc.to_le_bytes());
        page_buf[ENVELOPE_HEADER..ENVELOPE_HEADER + data.len()].copy_from_slice(data);

        let offset = page_id.as_u64() * FILE_PAGE_SIZE as u64;

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&page_buf)?;
        file.sync_data()?;

        Ok(())
    }

    /// Mark a page as free on disk.
    fn zero_page(&self, page_id: PageId) -> SageTreeResult<()> {
        let page_buf = vec![FREE_PAGE_MARKER; FILE_PAGE_SIZE];
        let offset = page_id.as_u64() * FILE_PAGE_SIZE as u64;

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&page_buf)?;
        file.sync_data()?;

        Ok(())
    }
}

impl Pager for FilePager {
    fn get_leaf(&self, page_id: PageId) -> SageTreeResult<DeltaNode> {
        let cache = self.leaf_cache.read().unwrap();
        cache
            .get(&page_id)
            .cloned()
            .ok_or(SageTreeError::PageNotFound(page_id))
    }

    fn put_leaf(&self, page_id: PageId, node: &DeltaNode) -> SageTreeResult<()> {
        // Consolidate before serializing (write the base leaf, not deltas)
        let mut consolidated = node.clone();
        consolidated.consolidate();
        let data = consolidated.base.serialize();

        // Write to disk first (write-through)
        self.write_page(page_id, NodeType::Leaf, &data)?;

        // Then update cache
        let mut cache = self.leaf_cache.write().unwrap();
        cache.insert(page_id, node.clone());

        Ok(())
    }

    fn remove_leaf(&self, page_id: PageId) -> SageTreeResult<()> {
        let mut cache = self.leaf_cache.write().unwrap();
        cache.remove(&page_id);
        let _ = self.zero_page(page_id);
        Ok(())
    }

    fn get_internal(&self, page_id: PageId) -> SageTreeResult<InternalNode> {
        let cache = self.internal_cache.read().unwrap();
        cache
            .get(&page_id)
            .cloned()
            .ok_or(SageTreeError::PageNotFound(page_id))
    }

    fn put_internal(&self, page_id: PageId, node: &InternalNode) -> SageTreeResult<()> {
        let data = node.serialize();

        // Write to disk first
        self.write_page(page_id, NodeType::Internal, &data)?;

        // Then update cache
        let mut cache = self.internal_cache.write().unwrap();
        cache.insert(page_id, node.clone());

        Ok(())
    }

    fn remove_internal(&self, page_id: PageId) -> SageTreeResult<()> {
        let mut cache = self.internal_cache.write().unwrap();
        cache.remove(&page_id);
        let _ = self.zero_page(page_id);
        Ok(())
    }

    fn allocate_page(&self) -> PageId {
        if let Ok(mut free) = self.free_pages.write() {
            if let Some(page) = free.pop() {
                return page;
            }
        }
        PageId::new(
            self.next_page
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        )
    }

    fn free_page(&self, page_id: PageId) {
        // Remove from caches
        {
            let mut lc = self.leaf_cache.write().unwrap();
            lc.remove(&page_id);
        }
        {
            let mut ic = self.internal_cache.write().unwrap();
            ic.remove(&page_id);
        }
        let _ = self.zero_page(page_id);
        if let Ok(mut free) = self.free_pages.write() {
            free.push(page_id);
        }
    }

    fn leaf_page_ids(&self) -> Vec<PageId> {
        self.leaf_cache.read().unwrap().keys().copied().collect()
    }

    fn internal_page_ids(&self) -> Vec<PageId> {
        self.internal_cache.read().unwrap().keys().copied().collect()
    }

    fn flush(&self) -> SageTreeResult<()> {
        let mut file = self.file.lock().unwrap();
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    fn clear(&self) -> SageTreeResult<()> {
        self.leaf_cache.write().unwrap().clear();
        self.internal_cache.write().unwrap().clear();
        self.free_pages.write().unwrap().clear();
        self.next_page.store(1, std::sync::atomic::Ordering::SeqCst);
        // Truncate the data file
        let file = self.file.lock().unwrap();
        file.set_len(0)?;
        file.sync_all()?;
        Ok(())
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_common::types::{Key, Value};

    fn make_leaf(page_id: PageId, keys: &[&str]) -> DeltaNode {
        let mut leaf = LeafNode::new(page_id);
        for k in keys {
            leaf.insert(super::super::node::LeafEntry::simple(
                Key::from_str(k),
                Value::from_str(&format!("val_{}", k)),
            ))
            .unwrap();
        }
        DeltaNode::new(leaf, 8)
    }

    fn make_internal(page_id: PageId) -> InternalNode {
        let mut node = InternalNode::new(page_id, 1);
        node.leftmost_child = PageId::new(100);
        node.insert(Key::from_str("mid"), PageId::new(200));
        node
    }

    #[test]
    fn test_memory_pager_leaf_roundtrip() {
        let pager = MemoryPager::new();
        let pid = pager.allocate_page();
        let node = make_leaf(pid, &["a", "b", "c"]);

        pager.put_leaf(pid, &node).unwrap();
        let loaded = pager.get_leaf(pid).unwrap();
        assert_eq!(loaded.base.len(), 3);
    }

    #[test]
    fn test_memory_pager_internal_roundtrip() {
        let pager = MemoryPager::new();
        let pid = pager.allocate_page();
        let node = make_internal(pid);

        pager.put_internal(pid, &node).unwrap();
        let loaded = pager.get_internal(pid).unwrap();
        assert_eq!(loaded.key_count(), 1);
    }

    #[test]
    fn test_memory_pager_not_found() {
        let pager = MemoryPager::new();
        assert!(pager.get_leaf(PageId::new(999)).is_err());
        assert!(pager.get_internal(PageId::new(999)).is_err());
    }

    #[test]
    fn test_file_pager_leaf_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("sage.db");

        let pid;
        // Write
        {
            let pager = FilePager::open(&path).unwrap();
            pid = pager.allocate_page();
            let node = make_leaf(pid, &["x", "y", "z"]);
            pager.put_leaf(pid, &node).unwrap();
            pager.flush().unwrap();
        }

        // Re-open and read
        {
            let pager = FilePager::open(&path).unwrap();
            let loaded = pager.get_leaf(pid).unwrap();
            assert_eq!(loaded.base.len(), 3);
        }
    }

    #[test]
    fn test_file_pager_internal_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("sage.db");

        let pid;
        {
            let pager = FilePager::open(&path).unwrap();
            pid = pager.allocate_page();
            let node = make_internal(pid);
            pager.put_internal(pid, &node).unwrap();
            pager.flush().unwrap();
        }

        {
            let pager = FilePager::open(&path).unwrap();
            let loaded = pager.get_internal(pid).unwrap();
            assert_eq!(loaded.key_count(), 1);
            assert_eq!(loaded.leftmost_child, PageId::new(100));
        }
    }

    #[test]
    fn test_file_pager_persistence_across_reopens() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("sage.db");

        // Write leaf 1
        let p1;
        {
            let pager = FilePager::open(&path).unwrap();
            p1 = pager.allocate_page();
            pager.put_leaf(p1, &make_leaf(p1, &["a", "b"])).unwrap();
            pager.flush().unwrap();
        }

        // Add leaf 2
        let p2;
        {
            let pager = FilePager::open(&path).unwrap();
            // Leaf 1 should still exist
            assert!(pager.get_leaf(p1).is_ok());

            p2 = pager.allocate_page();
            pager
                .put_leaf(p2, &make_leaf(p2, &["c", "d", "e"]))
                .unwrap();
            pager.flush().unwrap();
        }

        // Both should survive
        {
            let pager = FilePager::open(&path).unwrap();
            assert_eq!(pager.get_leaf(p1).unwrap().base.len(), 2);
            assert_eq!(pager.get_leaf(p2).unwrap().base.len(), 3);
        }
    }

    #[test]
    fn test_file_pager_free_page() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("sage.db");

        let pager = FilePager::open(&path).unwrap();
        let p1 = pager.allocate_page();
        pager.put_leaf(p1, &make_leaf(p1, &["a"])).unwrap();

        pager.free_page(p1);
        assert!(pager.get_leaf(p1).is_err());

        // Freed page should be reused
        let p2 = pager.allocate_page();
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_file_pager_crc32_corruption_detection() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("corrupt.db");

        let pid;
        // Step 1: Create a FilePager and write a leaf node
        {
            let pager = FilePager::open(&path).unwrap();
            pid = pager.allocate_page();
            let node = make_leaf(pid, &["alpha", "beta", "gamma"]);
            pager.put_leaf(pid, &node).unwrap();
            pager.flush().unwrap();
        }

        // Verify the page can be read before corruption
        {
            let pager = FilePager::open(&path).unwrap();
            let loaded = pager.get_leaf(pid).unwrap();
            assert_eq!(loaded.base.len(), 3);
        }

        // Step 2: Open the raw file and corrupt a byte in the data portion
        {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();

            let page_offset = pid.as_u64() * FILE_PAGE_SIZE as u64;
            // The data starts at ENVELOPE_HEADER (9 bytes) into the page.
            // Corrupt a byte in the data area (e.g., offset +20 from page start).
            let corrupt_offset = page_offset + 20;
            file.seek(SeekFrom::Start(corrupt_offset)).unwrap();

            let mut byte = [0u8; 1];
            file.read_exact(&mut byte).unwrap();
            // Flip bits to ensure the byte changes
            byte[0] ^= 0xFF;
            file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            file.write_all(&byte).unwrap();
            file.sync_all().unwrap();
        }

        // Step 3: Re-open the FilePager — the corrupted page should be
        // treated as free (CRC mismatch), so get_leaf should return an error
        {
            let pager = FilePager::open(&path).unwrap();
            let result = pager.get_leaf(pid);
            assert!(
                result.is_err(),
                "corrupted page should not be loadable, but got: {:?}",
                result.unwrap().base.len()
            );
        }
    }
}
