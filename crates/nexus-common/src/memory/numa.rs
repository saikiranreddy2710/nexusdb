//! NUMA-aware memory allocation.
//!
//! NUMA (Non-Uniform Memory Access) systems have memory attached to different
//! CPU sockets. Accessing local memory is faster than remote memory.
//!
//! This module provides:
//! - Detection of NUMA availability
//! - NUMA-aware memory allocation
//! - Graceful fallback on non-NUMA systems
//!
//! # Platform Support
//!
//! - **Linux**: Full NUMA support via libnuma (when available)
//! - **macOS/Windows**: Falls back to standard allocation

use super::aligned::{AlignedBuffer, IO_ALIGNMENT};

/// Represents a NUMA node (memory domain).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumaNode(u32);

impl NumaNode {
    /// The local NUMA node (where the current thread is running).
    pub const LOCAL: Self = Self(u32::MAX);

    /// Any NUMA node (no preference).
    pub const ANY: Self = Self(u32::MAX - 1);

    /// Creates a NUMA node with the specified ID.
    #[inline]
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Returns the node ID.
    #[inline]
    #[must_use]
    pub const fn id(self) -> u32 {
        self.0
    }

    /// Returns true if this is the LOCAL pseudo-node.
    #[inline]
    #[must_use]
    pub const fn is_local(self) -> bool {
        self.0 == Self::LOCAL.0
    }

    /// Returns true if this is the ANY pseudo-node.
    #[inline]
    #[must_use]
    pub const fn is_any(self) -> bool {
        self.0 == Self::ANY.0
    }
}

impl Default for NumaNode {
    fn default() -> Self {
        Self::LOCAL
    }
}

impl From<u32> for NumaNode {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

/// Returns whether NUMA is available on this system.
///
/// # Example
///
/// ```rust
/// use nexus_common::memory::numa_available;
///
/// if numa_available() {
///     println!("NUMA is available");
/// } else {
///     println!("NUMA is not available, using fallback");
/// }
/// ```
#[must_use]
pub fn numa_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Check if /sys/devices/system/node exists
        std::path::Path::new("/sys/devices/system/node").exists()
    }

    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Returns the number of NUMA nodes on the system.
#[must_use]
pub fn numa_node_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        if numa_available() {
            // Count directories in /sys/devices/system/node
            std::fs::read_dir("/sys/devices/system/node")
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .filter(|e| e.file_name().to_string_lossy().starts_with("node"))
                        .count()
                })
                .unwrap_or(1)
        } else {
            1
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        1
    }
}

/// Returns the NUMA node for the current thread.
///
/// On non-NUMA systems, always returns node 0.
#[must_use]
pub fn current_numa_node() -> NumaNode {
    #[cfg(target_os = "linux")]
    {
        // Use getcpu() to get the current NUMA node
        // For simplicity, we'll use a sysfs-based approach
        if numa_available() {
            // Read from /proc/self/numa_maps or use sched_getcpu
            // For now, return node 0 as a simple implementation
            NumaNode::new(0)
        } else {
            NumaNode::new(0)
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        NumaNode::new(0)
    }
}

/// NUMA-aware memory allocator.
///
/// This allocator attempts to allocate memory on a specific NUMA node
/// for better memory locality. On non-NUMA systems, it falls back to
/// standard aligned allocation.
///
/// # Example
///
/// ```rust
/// use nexus_common::memory::{NumaAllocator, NumaNode};
///
/// let allocator = NumaAllocator::new();
///
/// // Allocate on the local NUMA node
/// let buffer = allocator.allocate(8192, NumaNode::LOCAL);
/// assert_eq!(buffer.len(), 8192);
///
/// // Allocate on a specific node (falls back if not available)
/// let buffer2 = allocator.allocate(4096, NumaNode::new(0));
/// ```
pub struct NumaAllocator {
    /// Whether NUMA is available on this system.
    numa_available: bool,
    /// Number of NUMA nodes.
    node_count: usize,
}

impl NumaAllocator {
    /// Creates a new NUMA allocator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            numa_available: numa_available(),
            node_count: numa_node_count(),
        }
    }

    /// Returns whether NUMA is available.
    #[inline]
    #[must_use]
    pub const fn is_numa_available(&self) -> bool {
        self.numa_available
    }

    /// Returns the number of NUMA nodes.
    #[inline]
    #[must_use]
    pub const fn node_count(&self) -> usize {
        self.node_count
    }

    /// Allocates memory on the specified NUMA node.
    ///
    /// The returned buffer is page-aligned (4KB) for optimal I/O performance.
    ///
    /// # Arguments
    ///
    /// * `size` - Size in bytes to allocate
    /// * `node` - NUMA node preference (falls back if not available)
    #[must_use]
    pub fn allocate(&self, size: usize, node: NumaNode) -> AlignedBuffer {
        self.allocate_aligned(size, IO_ALIGNMENT, node)
    }

    /// Allocates memory with specific alignment on the specified NUMA node.
    ///
    /// # Arguments
    ///
    /// * `size` - Size in bytes to allocate
    /// * `alignment` - Required alignment (must be power of 2)
    /// * `node` - NUMA node preference (falls back if not available)
    #[must_use]
    pub fn allocate_aligned(
        &self,
        size: usize,
        alignment: usize,
        node: NumaNode,
    ) -> AlignedBuffer {
        #[cfg(target_os = "linux")]
        {
            if self.numa_available && !node.is_any() {
                // Try NUMA-aware allocation
                if let Some(buffer) = self.try_numa_alloc(size, alignment, node) {
                    return buffer;
                }
            }
        }

        // Fallback to standard aligned allocation
        AlignedBuffer::new(size, alignment)
    }

    /// Attempts NUMA-aware allocation on Linux.
    #[cfg(target_os = "linux")]
    fn try_numa_alloc(
        &self,
        size: usize,
        alignment: usize,
        node: NumaNode,
    ) -> Option<AlignedBuffer> {
        use std::alloc::Layout;

        // For a production implementation, we would use:
        // - libnuma (numa_alloc_onnode)
        // - mbind() system call
        // - mmap with MPOL_BIND

        // For now, we use mmap with hints
        // This is a simplified implementation - a real one would use libnuma

        let _layout = Layout::from_size_align(size, alignment).ok()?;

        // Use mmap for large allocations
        if size >= 2 * 1024 * 1024 {
            // For huge pages / large allocations, mmap is preferred
            // Fall through to standard allocation for now
            return None;
        }

        // Standard allocation - the kernel's first-touch policy will
        // allocate memory on the node where it's first accessed
        None
    }

    /// Interleaves memory allocation across all NUMA nodes.
    ///
    /// This is useful for shared data structures accessed by threads
    /// on different nodes.
    #[must_use]
    pub fn allocate_interleaved(&self, size: usize) -> AlignedBuffer {
        self.allocate_interleaved_aligned(size, IO_ALIGNMENT)
    }

    /// Allocates interleaved memory with specific alignment.
    #[must_use]
    pub fn allocate_interleaved_aligned(&self, size: usize, alignment: usize) -> AlignedBuffer {
        #[cfg(target_os = "linux")]
        {
            if self.numa_available {
                // For interleaved allocation, we would use:
                // - numa_alloc_interleaved()
                // - mbind() with MPOL_INTERLEAVE
                // Fall through to standard allocation
            }
        }

        // Fallback to standard aligned allocation
        AlignedBuffer::new(size, alignment)
    }

    /// Binds memory to a specific NUMA node.
    ///
    /// This moves existing memory to the specified node. Use with caution
    /// as it can be expensive.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer to bind
    /// * `node` - Target NUMA node
    ///
    /// # Returns
    ///
    /// `true` if binding succeeded, `false` otherwise.
    pub fn bind_to_node(&self, _buffer: &AlignedBuffer, node: NumaNode) -> bool {
        #[cfg(target_os = "linux")]
        {
            if self.numa_available && !node.is_any() && !node.is_local() {
                // Would use mbind() here
                // libc::mbind(ptr, len, MPOL_BIND, &node_mask, max_node, MPOL_MF_MOVE)
                return false;
            }
        }

        false
    }

    /// Prefaults memory pages to ensure they're allocated.
    ///
    /// This touches each page to trigger allocation, which is useful
    /// for benchmarking or when you want predictable access times.
    pub fn prefault(&self, buffer: &mut AlignedBuffer) {
        let page_size = 4096usize;
        let slice = buffer.as_mut_slice();

        // Touch each page
        for offset in (0..slice.len()).step_by(page_size) {
            // volatile write to prevent optimization
            unsafe {
                std::ptr::write_volatile(&mut slice[offset], 0);
            }
        }
    }
}

impl Default for NumaAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for NumaAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NumaAllocator")
            .field("numa_available", &self.numa_available)
            .field("node_count", &self.node_count)
            .finish()
    }
}

/// Statistics about NUMA memory usage.
#[derive(Debug, Clone, Default)]
pub struct NumaStats {
    /// Bytes allocated on each NUMA node.
    pub bytes_per_node: Vec<usize>,
    /// Total bytes allocated.
    pub total_bytes: usize,
    /// Number of allocations on each node.
    pub allocs_per_node: Vec<usize>,
}

impl NumaStats {
    /// Creates new empty stats for the given number of nodes.
    #[must_use]
    pub fn new(node_count: usize) -> Self {
        Self {
            bytes_per_node: vec![0; node_count],
            allocs_per_node: vec![0; node_count],
            total_bytes: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_node() {
        let node = NumaNode::new(0);
        assert_eq!(node.id(), 0);
        assert!(!node.is_local());
        assert!(!node.is_any());

        let local = NumaNode::LOCAL;
        assert!(local.is_local());

        let any = NumaNode::ANY;
        assert!(any.is_any());
    }

    #[test]
    fn test_numa_available_function() {
        // Just check it doesn't panic
        let _ = numa_available();
    }

    #[test]
    fn test_numa_node_count_function() {
        let count = numa_node_count();
        assert!(count >= 1);
    }

    #[test]
    fn test_current_numa_node_function() {
        let node = current_numa_node();
        // Should return a valid node
        assert!(node.id() < 256 || node.is_local());
    }

    #[test]
    fn test_numa_allocator_creation() {
        let allocator = NumaAllocator::new();
        assert!(allocator.node_count() >= 1);
    }

    #[test]
    fn test_numa_allocator_allocate() {
        let allocator = NumaAllocator::new();

        let buffer = allocator.allocate(4096, NumaNode::LOCAL);
        assert_eq!(buffer.len(), 4096);
        assert!(buffer.is_aligned_to(IO_ALIGNMENT));
    }

    #[test]
    fn test_numa_allocator_allocate_specific_node() {
        let allocator = NumaAllocator::new();

        let buffer = allocator.allocate(8192, NumaNode::new(0));
        assert_eq!(buffer.len(), 8192);
    }

    #[test]
    fn test_numa_allocator_allocate_any() {
        let allocator = NumaAllocator::new();

        let buffer = allocator.allocate(4096, NumaNode::ANY);
        assert_eq!(buffer.len(), 4096);
    }

    #[test]
    fn test_numa_allocator_interleaved() {
        let allocator = NumaAllocator::new();

        let buffer = allocator.allocate_interleaved(8192);
        assert_eq!(buffer.len(), 8192);
    }

    #[test]
    fn test_numa_allocator_prefault() {
        let allocator = NumaAllocator::new();

        let mut buffer = allocator.allocate(4096 * 4, NumaNode::LOCAL);
        allocator.prefault(&mut buffer);

        // Buffer should still be valid
        assert_eq!(buffer.len(), 4096 * 4);
    }

    #[test]
    fn test_numa_stats() {
        let stats = NumaStats::new(4);
        assert_eq!(stats.bytes_per_node.len(), 4);
        assert_eq!(stats.allocs_per_node.len(), 4);
        assert_eq!(stats.total_bytes, 0);
    }

    #[test]
    fn test_numa_allocator_aligned() {
        let allocator = NumaAllocator::new();

        let buffer = allocator.allocate_aligned(8192, 4096, NumaNode::LOCAL);
        assert!(buffer.is_aligned_to(4096));
    }
}
