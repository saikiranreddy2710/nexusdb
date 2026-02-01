//! Memory management utilities for NexusDB.
//!
//! This module provides efficient memory allocation primitives:
//!
//! - **Aligned allocation**: Memory aligned to page/cache boundaries for efficient I/O
//! - **Arena allocator**: Fast bump allocation for temporary/scratch memory
//! - **NUMA-aware allocation**: Locality-aware memory placement (with fallback)
//! - **Memory pools**: Fixed-size block allocation for common object sizes
//!
//! # Design Principles
//!
//! 1. **Zero-copy where possible**: Aligned buffers can be used directly with O_DIRECT
//! 2. **Cache-friendly**: Allocations respect cache line boundaries
//! 3. **Lock-free fast paths**: Arena allocation is single-threaded bump allocation
//! 4. **Graceful degradation**: NUMA features fall back on non-NUMA systems

mod aligned;
mod arena;
mod numa;
mod pool;

pub use aligned::{
    allocate_aligned, AlignedBuffer, AlignedVec, CacheLineAligned, CACHE_LINE_SIZE, IO_ALIGNMENT,
    SIMD_ALIGNMENT,
};
pub use arena::{Arena, ArenaChunk, ScopedArena};
pub use numa::{current_numa_node, numa_available, NumaAllocator, NumaNode, NumaStats};
pub use pool::{MemoryPool, PooledBuffer, ThreadLocalPoolCache};
