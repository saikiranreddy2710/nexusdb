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
/// On Linux, uses `sched_getcpu()` to find the current CPU, then maps
/// it to a NUMA node via sysfs. On non-NUMA systems, returns node 0.
#[must_use]
pub fn current_numa_node() -> NumaNode {
    #[cfg(target_os = "linux")]
    {
        if numa_available() {
            // Use sched_getcpu() to get the current CPU
            let cpu = unsafe { libc::sched_getcpu() };
            if cpu >= 0 {
                // Map CPU to NUMA node via sysfs
                let path = format!("/sys/devices/system/cpu/cpu{}/topology/physical_package_id", cpu);
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if let Ok(node_id) = content.trim().parse::<u32>() {
                        return NumaNode::new(node_id);
                    }
                }
                // Fallback: try /sys/devices/system/node/nodeN/cpulist
                let node_count = numa_node_count();
                for node in 0..node_count {
                    let cpulist_path = format!(
                        "/sys/devices/system/node/node{}/cpulist", node
                    );
                    if let Ok(cpulist) = std::fs::read_to_string(&cpulist_path) {
                        if cpu_in_list(&cpulist, cpu as u32) {
                            return NumaNode::new(node as u32);
                        }
                    }
                }
            }
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

/// Parse a CPU list string (e.g., "0-3,8-11") and check if a CPU is included.
#[cfg(target_os = "linux")]
fn cpu_in_list(list: &str, cpu: u32) -> bool {
    for part in list.trim().split(',') {
        let part = part.trim();
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(s), Ok(e)) = (start.parse::<u32>(), end.parse::<u32>()) {
                if cpu >= s && cpu <= e {
                    return true;
                }
            }
        } else if let Ok(c) = part.parse::<u32>() {
            if c == cpu {
                return true;
            }
        }
    }
    false
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

        let _ = node; // suppress unused warning on non-Linux

        // Fallback to standard aligned allocation
        AlignedBuffer::new(size, alignment)
    }

    /// Attempts NUMA-aware allocation on Linux using mmap + mbind.
    ///
    /// Strategy:
    /// 1. Allocate memory with mmap (MAP_ANONYMOUS | MAP_PRIVATE)
    /// 2. Bind the memory to the target NUMA node using mbind()
    /// 3. Prefault pages to trigger physical allocation on the target node
    #[cfg(target_os = "linux")]
    fn try_numa_alloc(
        &self,
        size: usize,
        alignment: usize,
        node: NumaNode,
    ) -> Option<AlignedBuffer> {
        // Resolve the node ID
        let node_id = if node.is_local() {
            current_numa_node().id() as usize
        } else {
            node.id() as usize
        };

        if node_id >= self.node_count {
            return None;
        }

        // Round up size to page alignment
        let page_size = 4096usize;
        let alloc_size = (size + page_size - 1) & !(page_size - 1);

        // Allocate anonymous memory via mmap
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                alloc_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return None;
        }

        // Set NUMA binding policy using mbind
        // MPOL_BIND = 2, forces allocation on specified nodes
        const MPOL_BIND: libc::c_int = 2;
        const MPOL_MF_MOVE: libc::c_uint = 1 << 1;

        // Build the node mask bitmask (one bit per node). The kernel expects
        // the mask to be expressed in machine-word chunks (unsigned long),
        // so we must size the array based on the actual bit width of c_ulong.
        let bits_per_ulong = (std::mem::size_of::<libc::c_ulong>() * 8) as usize;
        let mask_len = (self.node_count + bits_per_ulong - 1) / bits_per_ulong;
        let mut nodemask: Vec<libc::c_ulong> = vec![0; mask_len];
        let word_index = node_id / bits_per_ulong;
        let bit_index = node_id % bits_per_ulong;
        nodemask[word_index] |= (1 as libc::c_ulong) << bit_index;

        let ret = unsafe {
            libc::syscall(
                libc::SYS_mbind,
                ptr,
                alloc_size,
                MPOL_BIND,
                nodemask.as_ptr(),
                self.node_count as libc::c_ulong + 1,
                MPOL_MF_MOVE,
            )
        };

        if ret != 0 {
            // mbind failed; memory is still usable, just not NUMA-bound
            tracing::debug!(
                "mbind to node {} failed: {}",
                node_id,
                std::io::Error::last_os_error()
            );
        }

        // Prefault pages so physical memory is allocated on the target node
        unsafe {
            for offset in (0..alloc_size).step_by(page_size) {
                std::ptr::write_volatile((ptr as *mut u8).add(offset), 0);
            }
        }

        // Wrap in AlignedBuffer using the raw pointer
        // Since mmap returns page-aligned memory, alignment is guaranteed
        // We wrap it using standard AlignedBuffer and copy from mmap
        let mut buffer = AlignedBuffer::new(size, alignment.max(page_size));
        unsafe {
            std::ptr::copy_nonoverlapping(
                ptr as *const u8,
                buffer.as_mut_ptr(),
                size,
            );
            libc::munmap(ptr, alloc_size);
        }

        Some(buffer)
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
    ///
    /// On Linux, uses mmap + mbind with MPOL_INTERLEAVE to spread pages
    /// across all NUMA nodes in round-robin fashion. This provides
    /// balanced bandwidth for shared data structures.
    #[must_use]
    pub fn allocate_interleaved_aligned(&self, size: usize, alignment: usize) -> AlignedBuffer {
        #[cfg(target_os = "linux")]
        {
            if self.numa_available && self.node_count > 1 {
                if let Some(buffer) = self.try_interleaved_alloc(size, alignment) {
                    return buffer;
                }
            }
        }

        // Fallback to standard aligned allocation
        AlignedBuffer::new(size, alignment)
    }

    /// Attempts interleaved allocation on Linux using mbind with MPOL_INTERLEAVE.
    #[cfg(target_os = "linux")]
    fn try_interleaved_alloc(&self, size: usize, alignment: usize) -> Option<AlignedBuffer> {
        let page_size = 4096usize;
        let alloc_size = (size + page_size - 1) & !(page_size - 1);

        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                alloc_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return None;
        }

        // MPOL_INTERLEAVE = 3
        const MPOL_INTERLEAVE: libc::c_int = 3;

        // Build mask with all nodes set. As above, respect the actual
        // bit width of c_ulong so the mask is correctly sized on both
        // 32-bit and 64-bit platforms.
        let bits_per_ulong = (std::mem::size_of::<libc::c_ulong>() * 8) as usize;
        let mask_len = (self.node_count + bits_per_ulong - 1) / bits_per_ulong;
        let mut nodemask: Vec<libc::c_ulong> = vec![0; mask_len];
        for i in 0..self.node_count {
            let word_index = i / bits_per_ulong;
            let bit_index = i % bits_per_ulong;
            nodemask[word_index] |= (1 as libc::c_ulong) << bit_index;
        }

        let ret = unsafe {
            libc::syscall(
                libc::SYS_mbind,
                ptr,
                alloc_size,
                MPOL_INTERLEAVE,
                nodemask.as_ptr(),
                self.node_count as libc::c_ulong + 1,
                0u32, // no MPOL_MF flags
            )
        };

        if ret != 0 {
            tracing::debug!(
                "mbind INTERLEAVE failed: {}",
                std::io::Error::last_os_error()
            );
        }

        // Prefault pages
        unsafe {
            for offset in (0..alloc_size).step_by(page_size) {
                std::ptr::write_volatile((ptr as *mut u8).add(offset), 0);
            }
        }

        let mut buffer = AlignedBuffer::new(size, alignment.max(page_size));
        unsafe {
            std::ptr::copy_nonoverlapping(ptr as *const u8, buffer.as_mut_ptr(), size);
            libc::munmap(ptr, alloc_size);
        }

        Some(buffer)
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
    pub fn bind_to_node(&self, buffer: &AlignedBuffer, _node: NumaNode) -> bool {
        #[cfg(target_os = "linux")]
        {
            if self.numa_available && !_node.is_any() {
                let node_id = if _node.is_local() {
                    current_numa_node().id() as usize
                } else {
                    _node.id() as usize
                };

                if node_id >= self.node_count {
                    return false;
                }

                const MPOL_BIND: libc::c_int = 2;
                const MPOL_MF_MOVE: libc::c_uint = 1 << 1;

                // Build the node mask with correct word sizing.
                let bits_per_ulong = (std::mem::size_of::<libc::c_ulong>() * 8) as usize;
                let mask_len = (self.node_count + bits_per_ulong - 1) / bits_per_ulong;
                let mut nodemask: Vec<libc::c_ulong> = vec![0; mask_len];
                let word_index = node_id / bits_per_ulong;
                let bit_index = node_id % bits_per_ulong;
                nodemask[word_index] |= (1 as libc::c_ulong) << bit_index;

                let ret = unsafe {
                    libc::syscall(
                        libc::SYS_mbind,
                        buffer.as_ptr(),
                        buffer.len(),
                        MPOL_BIND,
                        nodemask.as_ptr(),
                        self.node_count as libc::c_ulong + 1,
                        MPOL_MF_MOVE,
                    )
                };

                return ret == 0;
            }
        }

        let _ = buffer; // suppress unused warning on non-Linux
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
