//! Aligned memory allocation utilities.
//!
//! Provides memory buffers aligned to specific boundaries for:
//! - Direct I/O (O_DIRECT requires page-aligned buffers)
//! - SIMD operations (require 16/32/64-byte alignment)
//! - Cache efficiency (64-byte cache line alignment)

use std::alloc::{self, Layout};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::slice;

/// Default alignment for I/O operations (4KB page alignment).
pub const IO_ALIGNMENT: usize = 4096;

/// Cache line size on most modern CPUs.
pub const CACHE_LINE_SIZE: usize = 64;

/// SIMD alignment for AVX-512.
pub const SIMD_ALIGNMENT: usize = 64;

/// Allocates aligned memory with the specified size and alignment.
///
/// # Safety
///
/// The caller must ensure that:
/// - `size` is not zero
/// - `alignment` is a power of two
/// - `alignment` is not zero
///
/// # Panics
///
/// Panics if the allocation fails or if the alignment/size requirements
/// cannot be satisfied.
///
/// # Example
///
/// ```rust
/// use nexus_common::memory::allocate_aligned;
///
/// let buffer = allocate_aligned(4096, 4096);
/// assert_eq!(buffer.as_ptr() as usize % 4096, 0);
/// ```
#[must_use]
pub fn allocate_aligned(size: usize, alignment: usize) -> AlignedBuffer {
    assert!(size > 0, "size must be greater than 0");
    assert!(alignment.is_power_of_two(), "alignment must be power of 2");
    assert!(alignment > 0, "alignment must be greater than 0");

    let layout = Layout::from_size_align(size, alignment).expect("invalid layout");

    // SAFETY: Layout is valid (checked above)
    let ptr = unsafe { alloc::alloc_zeroed(layout) };

    let ptr = NonNull::new(ptr).expect("allocation failed");

    AlignedBuffer {
        ptr,
        size,
        alignment,
    }
}

/// A buffer with guaranteed memory alignment.
///
/// This is useful for:
/// - Direct I/O operations (O_DIRECT)
/// - Memory-mapped I/O
/// - SIMD vectorized operations
/// - Avoiding false sharing in concurrent code
///
/// # Example
///
/// ```rust
/// use nexus_common::memory::AlignedBuffer;
///
/// let mut buffer = AlignedBuffer::new(8192, 4096);
/// buffer[0] = 42;
/// assert_eq!(buffer[0], 42);
/// assert!(buffer.is_aligned_to(4096));
/// ```
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    size: usize,
    alignment: usize,
}

// SAFETY: AlignedBuffer owns its memory and doesn't share it
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Creates a new aligned buffer with the specified size and alignment.
    ///
    /// The buffer is zero-initialized.
    #[must_use]
    pub fn new(size: usize, alignment: usize) -> Self {
        allocate_aligned(size, alignment)
    }

    /// Creates a page-aligned buffer (4KB alignment).
    #[must_use]
    pub fn page_aligned(size: usize) -> Self {
        Self::new(size, IO_ALIGNMENT)
    }

    /// Creates a cache-line aligned buffer (64-byte alignment).
    #[must_use]
    pub fn cache_aligned(size: usize) -> Self {
        Self::new(size, CACHE_LINE_SIZE)
    }

    /// Returns the size of the buffer in bytes.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.size
    }

    /// Returns true if the buffer is empty.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Returns the alignment of the buffer.
    #[inline]
    #[must_use]
    pub const fn alignment(&self) -> usize {
        self.alignment
    }

    /// Returns a raw pointer to the buffer.
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns a mutable raw pointer to the buffer.
    #[inline]
    #[must_use]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Returns the buffer as a byte slice.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid and size is correct
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    /// Returns the buffer as a mutable byte slice.
    #[inline]
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid, size is correct, and we have exclusive access
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    /// Checks if the buffer is aligned to the specified alignment.
    #[inline]
    #[must_use]
    pub fn is_aligned_to(&self, alignment: usize) -> bool {
        (self.ptr.as_ptr() as usize) % alignment == 0
    }

    /// Fills the entire buffer with zeros.
    pub fn zero(&mut self) {
        // SAFETY: ptr is valid and size is correct
        unsafe {
            std::ptr::write_bytes(self.ptr.as_ptr(), 0, self.size);
        }
    }

    /// Fills the entire buffer with the specified byte value.
    pub fn fill(&mut self, value: u8) {
        // SAFETY: ptr is valid and size is correct
        unsafe {
            std::ptr::write_bytes(self.ptr.as_ptr(), value, self.size);
        }
    }

    /// Copies data from a slice into the buffer.
    ///
    /// # Panics
    ///
    /// Panics if the source slice is larger than the buffer.
    pub fn copy_from_slice(&mut self, src: &[u8]) {
        assert!(src.len() <= self.size, "source slice too large");
        self.as_mut_slice()[..src.len()].copy_from_slice(src);
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        let layout =
            Layout::from_size_align(self.size, self.alignment).expect("invalid layout in drop");

        // SAFETY: ptr was allocated with this layout
        unsafe {
            alloc::dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl Clone for AlignedBuffer {
    fn clone(&self) -> Self {
        let mut new_buffer = Self::new(self.size, self.alignment);
        new_buffer.as_mut_slice().copy_from_slice(self.as_slice());
        new_buffer
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for AlignedBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl AsRef<[u8]> for AlignedBuffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for AlignedBuffer {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl fmt::Debug for AlignedBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlignedBuffer")
            .field("size", &self.size)
            .field("alignment", &self.alignment)
            .field("ptr", &self.ptr)
            .finish()
    }
}

/// A Vec-like container with guaranteed alignment.
///
/// Unlike `AlignedBuffer`, this supports dynamic resizing while
/// maintaining alignment guarantees.
///
/// # Example
///
/// ```rust
/// use nexus_common::memory::AlignedVec;
///
/// let mut vec = AlignedVec::with_capacity(1024, 64);
/// vec.extend_from_slice(&[1, 2, 3, 4]);
/// assert_eq!(vec.len(), 4);
/// ```
pub struct AlignedVec {
    buffer: AlignedBuffer,
    len: usize,
}

impl AlignedVec {
    /// Creates a new empty aligned vector with the specified capacity and alignment.
    #[must_use]
    pub fn with_capacity(capacity: usize, alignment: usize) -> Self {
        let capacity = capacity.max(1); // Ensure non-zero capacity
        Self {
            buffer: AlignedBuffer::new(capacity, alignment),
            len: 0,
        }
    }

    /// Creates a page-aligned vector with the specified capacity.
    #[must_use]
    pub fn page_aligned(capacity: usize) -> Self {
        Self::with_capacity(capacity, IO_ALIGNMENT)
    }

    /// Returns the current length.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the vector is empty.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the current capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the alignment.
    #[inline]
    #[must_use]
    pub fn alignment(&self) -> usize {
        self.buffer.alignment()
    }

    /// Returns a slice of the used portion.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer.as_slice()[..self.len]
    }

    /// Returns a mutable slice of the used portion.
    #[inline]
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer.as_mut_slice()[..self.len]
    }

    /// Clears the vector, setting length to 0.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Extends the vector with bytes from a slice.
    ///
    /// # Panics
    ///
    /// Panics if there isn't enough capacity.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let new_len = self.len + data.len();
        assert!(new_len <= self.buffer.len(), "insufficient capacity");

        self.buffer.as_mut_slice()[self.len..new_len].copy_from_slice(data);
        self.len = new_len;
    }

    /// Resizes the vector to the specified length.
    ///
    /// If the new length is greater, the new bytes are filled with `value`.
    ///
    /// # Panics
    ///
    /// Panics if the new length exceeds capacity.
    pub fn resize(&mut self, new_len: usize, value: u8) {
        assert!(new_len <= self.buffer.len(), "insufficient capacity");

        if new_len > self.len {
            self.buffer.as_mut_slice()[self.len..new_len].fill(value);
        }
        self.len = new_len;
    }

    /// Sets the length without initializing.
    ///
    /// # Safety
    ///
    /// The caller must ensure that all bytes in the new range are initialized.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        assert!(new_len <= self.buffer.len(), "length exceeds capacity");
        self.len = new_len;
    }

    /// Returns a raw pointer to the data.
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.buffer.as_ptr()
    }

    /// Returns a mutable raw pointer to the data.
    #[inline]
    #[must_use]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buffer.as_mut_ptr()
    }
}

impl Deref for AlignedVec {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for AlignedVec {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl fmt::Debug for AlignedVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlignedVec")
            .field("len", &self.len)
            .field("capacity", &self.buffer.len())
            .field("alignment", &self.buffer.alignment())
            .finish()
    }
}

/// A wrapper to ensure a value is aligned to a cache line.
///
/// This is useful for avoiding false sharing in concurrent data structures.
///
/// # Example
///
/// ```rust
/// use nexus_common::memory::CacheLineAligned;
/// use std::sync::atomic::AtomicU64;
///
/// // Each counter is on its own cache line
/// struct Counters {
///     counter1: CacheLineAligned<AtomicU64>,
///     counter2: CacheLineAligned<AtomicU64>,
/// }
/// ```
#[repr(align(64))]
pub struct CacheLineAligned<T> {
    value: T,
}

impl<T> CacheLineAligned<T> {
    /// Creates a new cache-line aligned value.
    #[inline]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// Returns a reference to the inner value.
    #[inline]
    pub const fn get(&self) -> &T {
        &self.value
    }

    /// Returns a mutable reference to the inner value.
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }

    /// Consumes the wrapper and returns the inner value.
    #[inline]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Deref for CacheLineAligned<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for CacheLineAligned<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<T: Default> Default for CacheLineAligned<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone> Clone for CacheLineAligned<T> {
    fn clone(&self) -> Self {
        Self::new(self.value.clone())
    }
}

impl<T: fmt::Debug> fmt::Debug for CacheLineAligned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn test_aligned_buffer_creation() {
        let buffer = AlignedBuffer::new(4096, 4096);
        assert_eq!(buffer.len(), 4096);
        assert!(buffer.is_aligned_to(4096));
    }

    #[test]
    fn test_aligned_buffer_page_aligned() {
        let buffer = AlignedBuffer::page_aligned(8192);
        assert_eq!(buffer.len(), 8192);
        assert!(buffer.is_aligned_to(IO_ALIGNMENT));
    }

    #[test]
    fn test_aligned_buffer_cache_aligned() {
        let buffer = AlignedBuffer::cache_aligned(1024);
        assert!(buffer.is_aligned_to(CACHE_LINE_SIZE));
    }

    #[test]
    fn test_aligned_buffer_read_write() {
        let mut buffer = AlignedBuffer::new(1024, 64);

        // Write some data
        buffer[0] = 0xDE;
        buffer[1] = 0xAD;
        buffer[2] = 0xBE;
        buffer[3] = 0xEF;

        assert_eq!(buffer[0], 0xDE);
        assert_eq!(buffer[1], 0xAD);
        assert_eq!(buffer[2], 0xBE);
        assert_eq!(buffer[3], 0xEF);
    }

    #[test]
    fn test_aligned_buffer_zero() {
        let mut buffer = AlignedBuffer::new(1024, 64);
        buffer.fill(0xFF);
        buffer.zero();

        for byte in buffer.as_slice() {
            assert_eq!(*byte, 0);
        }
    }

    #[test]
    fn test_aligned_buffer_fill() {
        let mut buffer = AlignedBuffer::new(1024, 64);
        buffer.fill(0xAB);

        for byte in buffer.as_slice() {
            assert_eq!(*byte, 0xAB);
        }
    }

    #[test]
    fn test_aligned_buffer_clone() {
        let mut buffer = AlignedBuffer::new(1024, 64);
        buffer[0] = 42;
        buffer[100] = 100;

        let cloned = buffer.clone();
        assert_eq!(cloned[0], 42);
        assert_eq!(cloned[100], 100);
        assert_eq!(cloned.len(), buffer.len());
        assert_eq!(cloned.alignment(), buffer.alignment());
    }

    #[test]
    fn test_aligned_vec() {
        let mut vec = AlignedVec::with_capacity(1024, 64);
        assert_eq!(vec.len(), 0);
        assert!(vec.is_empty());

        vec.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(vec.len(), 4);
        assert_eq!(vec.as_slice(), &[1, 2, 3, 4]);

        vec.extend_from_slice(&[5, 6]);
        assert_eq!(vec.len(), 6);
        assert_eq!(vec.as_slice(), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_aligned_vec_resize() {
        let mut vec = AlignedVec::with_capacity(1024, 64);
        vec.resize(100, 0xAB);
        assert_eq!(vec.len(), 100);

        for byte in vec.as_slice() {
            assert_eq!(*byte, 0xAB);
        }
    }

    #[test]
    fn test_aligned_vec_clear() {
        let mut vec = AlignedVec::with_capacity(1024, 64);
        vec.extend_from_slice(&[1, 2, 3, 4]);
        vec.clear();
        assert!(vec.is_empty());
    }

    #[test]
    fn test_cache_line_aligned() {
        let aligned = CacheLineAligned::new(42u64);
        assert_eq!(*aligned, 42);

        // Check alignment
        let ptr = &aligned as *const _ as usize;
        assert_eq!(ptr % CACHE_LINE_SIZE, 0);
    }

    #[test]
    fn test_cache_line_aligned_size() {
        // Ensure the struct is at least 64 bytes
        assert!(mem::size_of::<CacheLineAligned<u64>>() >= CACHE_LINE_SIZE);
    }

    #[test]
    fn test_allocate_aligned_function() {
        let buffer = allocate_aligned(8192, 4096);
        assert!(buffer.is_aligned_to(4096));
        assert_eq!(buffer.len(), 8192);
    }

    #[test]
    #[should_panic(expected = "size must be greater than 0")]
    fn test_allocate_aligned_zero_size() {
        let _ = allocate_aligned(0, 64);
    }

    #[test]
    #[should_panic(expected = "alignment must be power of 2")]
    fn test_allocate_aligned_non_power_of_two() {
        let _ = allocate_aligned(1024, 63);
    }
}
