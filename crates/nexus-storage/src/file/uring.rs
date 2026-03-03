//! io_uring-based file I/O implementation for Linux.
//!
//! Provides a high-performance file I/O backend using Linux's io_uring
//! subsystem (kernel 5.1+). Key advantages over traditional async I/O:
//!
//! - **Batched submissions**: Multiple I/O operations submitted in a single syscall
//! - **Registered buffers**: Pre-registered memory for zero-copy I/O
//! - **SQPOLL mode**: Kernel-side polling eliminates submission syscalls
//! - **True async**: No thread pool needed (unlike tokio::fs)
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────┐
//! │ UringFile                                   │
//! │  ├── fd: RawFd (O_DIRECT optional)         │
//! │  ├── ring: IoUring (shared per-file)       │
//! │  └── registered_buffers: Option<Vec<u8>>   │
//! │                                             │
//! │ Operations:                                 │
//! │  read_at  → Read SQE  → submit → wait CQE │
//! │  write_at → Write SQE → submit → wait CQE │
//! │  sync     → Fsync SQE → submit → wait CQE │
//! └────────────────────────────────────────────┘
//! ```

#![cfg(all(target_os = "linux", feature = "io-uring"))]

use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use io_uring::{opcode, types, IoUring};
use parking_lot::Mutex;

use super::error::{IoError, IoResult};
use super::handle::{FileAdvice, FileHandle};
use super::options::OpenOptions;

/// Default io_uring queue depth.
const DEFAULT_QUEUE_DEPTH: u32 = 256;

/// Pre-registered buffer pool for zero-copy io_uring I/O.
///
/// Registering buffers with `IORING_REGISTER_BUFFERS` tells the kernel
/// to pin the memory and map it into kernel address space once. Subsequent
/// `ReadFixed`/`WriteFixed` operations avoid per-I/O copy and page-table walks.
///
/// Call [`UringFile::register_buffers`] to enable this, then use
/// [`UringFile::read_fixed`] / [`UringFile::write_fixed`].
pub struct RegisteredBufferPool {
    /// Each buffer is a page-aligned slab.
    buffers: Vec<Vec<u8>>,
    /// Size of each individual buffer.
    buf_size: usize,
    /// Index of next free buffer (simple bump allocator + free list).
    free_list: Vec<u16>,
}

impl RegisteredBufferPool {
    /// Allocate `count` buffers of `buf_size` bytes each.
    pub fn new(count: usize, buf_size: usize) -> Self {
        let mut buffers = Vec::with_capacity(count);
        let mut free_list = Vec::with_capacity(count);
        for i in 0..count {
            buffers.push(vec![0u8; buf_size]);
            free_list.push(i as u16);
        }
        Self {
            buffers,
            buf_size,
            free_list,
        }
    }

    /// Acquire a buffer index from the pool. Returns `None` if exhausted.
    pub fn acquire(&mut self) -> Option<u16> {
        self.free_list.pop()
    }

    /// Return a buffer index to the pool.
    pub fn release(&mut self, idx: u16) {
        self.free_list.push(idx);
    }

    /// Get a mutable reference to the buffer at `idx`.
    pub fn get_mut(&mut self, idx: u16) -> &mut [u8] {
        &mut self.buffers[idx as usize][..self.buf_size]
    }

    /// Get an immutable reference to the buffer at `idx`.
    pub fn get(&self, idx: u16) -> &[u8] {
        &self.buffers[idx as usize][..self.buf_size]
    }

    /// Returns `iovec` array suitable for `io_uring_register_buffers`.
    fn as_iovecs(&self) -> Vec<libc::iovec> {
        self.buffers
            .iter()
            .map(|b| libc::iovec {
                iov_base: b.as_ptr() as *mut libc::c_void,
                iov_len: self.buf_size,
            })
            .collect()
    }

    /// Number of buffers in the pool.
    pub fn count(&self) -> usize {
        self.buffers.len()
    }

    /// Size of each buffer.
    pub fn buffer_size(&self) -> usize {
        self.buf_size
    }
}

/// io_uring-based file implementation.
///
/// Provides high-performance async I/O on Linux using io_uring.
/// Each file instance owns a shared io_uring ring for submitting operations.
///
/// ## Registered Buffers (Zero-Copy I/O)
///
/// Call [`register_buffers`](Self::register_buffers) after opening the
/// file, then use [`read_fixed`](Self::read_fixed) /
/// [`write_fixed`](Self::write_fixed) for zero-copy operations. The kernel
/// pins the registered memory and avoids per-I/O page-table walks.
///
/// ## SQPOLL Mode
///
/// Set `sqpoll = true` in the builder to enable kernel-side submission
/// queue polling. This eliminates the `io_uring_enter` syscall for
/// submissions (the kernel thread polls the SQ automatically). Use this
/// for latency-critical workloads at the cost of one kernel thread per
/// ring spinning at 100% CPU.
///
/// ## Fixed File Descriptors
///
/// Call [`register_files`](Self::register_files) to register fds with
/// the ring. Subsequent operations using the *registered* index avoid
/// the per-operation fd lookup in the kernel.
pub struct UringFile {
    /// The raw file descriptor.
    fd: RawFd,
    /// The file path.
    path: PathBuf,
    /// Whether the file is writable.
    writable: bool,
    /// Whether the file was opened with O_DIRECT.
    direct_io: bool,
    /// The io_uring instance (shared, mutex-protected for thread safety).
    ring: Arc<Mutex<IoUring>>,
    /// Pre-registered buffer pool for zero-copy I/O.
    registered_buffers: Mutex<Option<RegisteredBufferPool>>,
    /// Whether files have been registered (fixed fd mode).
    fixed_fd_index: Option<u32>,
}

impl UringFile {
    /// Opens a file with io_uring support.
    ///
    /// The file is opened using `libc::open` with appropriate flags,
    /// and an io_uring instance is created for I/O operations.
    pub async fn open(path: impl AsRef<Path>, options: OpenOptions) -> IoResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Build open flags from options
        let mut flags: libc::c_int = 0;

        if options.is_read() && options.is_write() {
            flags |= libc::O_RDWR;
        } else if options.is_write() {
            flags |= libc::O_WRONLY;
        } else {
            flags |= libc::O_RDONLY;
        }

        if options.is_create() {
            flags |= libc::O_CREAT;
        }
        if options.is_create_new() {
            flags |= libc::O_CREAT | libc::O_EXCL;
        }
        if options.is_truncate() {
            flags |= libc::O_TRUNC;
        }
        if options.is_append() {
            flags |= libc::O_APPEND;
        }
        if options.is_direct_io() {
            flags |= libc::O_DIRECT;
        }
        if options.is_sync() {
            flags |= libc::O_SYNC;
        }
        if options.is_dsync() {
            flags |= libc::O_DSYNC;
        }

        let mode: libc::mode_t = options.get_mode().unwrap_or(0o644);

        // Open the file
        let c_path = std::ffi::CString::new(
            path.to_str()
                .ok_or_else(|| IoError::UringError {
                    message: "invalid path encoding".into(),
                })?,
        )
        .map_err(|_| IoError::UringError {
            message: "path contains null byte".into(),
        })?;

        let fd = unsafe { libc::open(c_path.as_ptr(), flags, mode) };
        if fd < 0 {
            let err = std::io::Error::last_os_error();
            return Err(IoError::from_io_with_path(err, &path));
        }

        // Pre-allocate if requested
        if let Some(prealloc) = options.preallocate_size() {
            if prealloc > 0 {
                let ret = unsafe {
                    libc::fallocate(fd, 0, 0, prealloc as libc::off_t)
                };
                if ret != 0 {
                    // Non-fatal: fallocate may not be supported
                    tracing::debug!("fallocate failed (non-fatal): {}", std::io::Error::last_os_error());
                }
            }
        }

        // Create io_uring instance
        let ring = Self::create_ring(DEFAULT_QUEUE_DEPTH)?;

        Ok(Self {
            fd,
            path,
            writable: options.is_write(),
            direct_io: options.is_direct_io(),
            ring: Arc::new(Mutex::new(ring)),
            registered_buffers: Mutex::new(None),
            fixed_fd_index: None,
        })
    }

    /// Open with SQPOLL mode enabled.
    ///
    /// In SQPOLL mode the kernel spawns a thread that continuously polls
    /// the submission queue, eliminating the `io_uring_enter` syscall for
    /// submissions. The `idle_ms` parameter controls how long the kernel
    /// thread idles before going to sleep (woken on next submit).
    pub async fn open_sqpoll(
        path: impl AsRef<Path>,
        options: OpenOptions,
        idle_ms: u32,
    ) -> IoResult<Self> {
        let mut file = Self::open(path, options).await?;

        // Replace the ring with an SQPOLL-enabled one
        let ring = io_uring::IoUring::builder()
            .setup_sqpoll(idle_ms)
            .build(DEFAULT_QUEUE_DEPTH)
            .map_err(|e| IoError::UringError {
                message: format!("failed to create SQPOLL io_uring: {}", e),
            })?;

        file.ring = Arc::new(Mutex::new(ring));
        Ok(file)
    }

    /// Creates a new io_uring instance with the given queue depth.
    fn create_ring(depth: u32) -> IoResult<IoUring> {
        IoUring::new(depth).map_err(|e| IoError::UringError {
            message: format!("failed to create io_uring: {}", e),
        })
    }

    // ─── Registered Buffers (Zero-Copy) ─────────────────────────

    /// Register a pool of buffers with the io_uring instance for zero-copy I/O.
    ///
    /// After registration, use [`read_fixed`](Self::read_fixed) and
    /// [`write_fixed`](Self::write_fixed) with buffer indices for maximum
    /// performance. The kernel pins the memory once and avoids per-I/O copies.
    pub fn register_buffers(&self, count: usize, buf_size: usize) -> IoResult<()> {
        let pool = RegisteredBufferPool::new(count, buf_size);
        let iovecs = pool.as_iovecs();

        let mut ring = self.ring.lock();

        // SAFETY: iovecs point to valid, pinned memory in `pool.buffers`.
        // The buffers must remain alive as long as they are registered.
        let submitter = ring.submitter();
        submitter
            .register_buffers(&iovecs)
            .map_err(|e| IoError::UringError {
                message: format!("register_buffers failed: {}", e),
            })?;

        drop(ring);

        *self.registered_buffers.lock() = Some(pool);
        Ok(())
    }

    /// Unregister previously registered buffers.
    pub fn unregister_buffers(&self) -> IoResult<()> {
        let mut ring = self.ring.lock();
        let submitter = ring.submitter();
        submitter
            .unregister_buffers()
            .map_err(|e| IoError::UringError {
                message: format!("unregister_buffers failed: {}", e),
            })?;
        drop(ring);

        *self.registered_buffers.lock() = None;
        Ok(())
    }

    /// Read using a pre-registered buffer (zero-copy).
    ///
    /// Returns the number of bytes read. The data is placed into the
    /// registered buffer at `buf_idx`; access it via the
    /// [`RegisteredBufferPool`] returned by [`get_registered_buffers`].
    pub async fn read_fixed(
        &self,
        buf_idx: u16,
        len: u32,
        offset: u64,
    ) -> IoResult<usize> {
        let ptr = {
            let pool_guard = self.registered_buffers.lock();
            let pool = pool_guard.as_ref().ok_or(IoError::UringError {
                message: "no registered buffers".into(),
            })?;
            pool.buffers[buf_idx as usize].as_ptr() as *mut u8
        };

        let fd = self.target_fd();
        let mut ring = self.ring.lock();

        let sqe = opcode::ReadFixed::new(fd, ptr, len, buf_idx)
            .offset(offset)
            .build()
            .user_data(0x10);

        unsafe {
            ring.submission()
                .push(&sqe)
                .map_err(|_| IoError::UringError {
                    message: "submission queue full".into(),
                })?;
        }

        let result = Self::submit_and_wait_one(&mut ring)?;
        Ok(result as usize)
    }

    /// Write using a pre-registered buffer (zero-copy).
    ///
    /// Writes `len` bytes from the registered buffer at `buf_idx`.
    pub async fn write_fixed(
        &self,
        buf_idx: u16,
        len: u32,
        offset: u64,
    ) -> IoResult<usize> {
        if !self.writable {
            return Err(IoError::InvalidOperation {
                operation: "write_fixed",
                mode: "read-only",
            });
        }

        let ptr = {
            let pool_guard = self.registered_buffers.lock();
            let pool = pool_guard.as_ref().ok_or(IoError::UringError {
                message: "no registered buffers".into(),
            })?;
            pool.buffers[buf_idx as usize].as_ptr() as *const u8
        };

        let fd = self.target_fd();
        let mut ring = self.ring.lock();

        let sqe = opcode::WriteFixed::new(fd, ptr, len, buf_idx)
            .offset(offset)
            .build()
            .user_data(0x11);

        unsafe {
            ring.submission()
                .push(&sqe)
                .map_err(|_| IoError::UringError {
                    message: "submission queue full".into(),
                })?;
        }

        let result = Self::submit_and_wait_one(&mut ring)?;
        Ok(result as usize)
    }

    /// Access the registered buffer pool (e.g. to read data after `read_fixed`).
    pub fn with_registered_buffers<F, R>(&self, f: F) -> IoResult<R>
    where
        F: FnOnce(&mut RegisteredBufferPool) -> R,
    {
        let mut guard = self.registered_buffers.lock();
        let pool = guard.as_mut().ok_or(IoError::UringError {
            message: "no registered buffers".into(),
        })?;
        Ok(f(pool))
    }

    // ─── Fixed File Descriptors ─────────────────────────────────

    /// Register this file's fd with the io_uring for fixed-fd operations.
    ///
    /// After registration, all SQEs use the registered index instead of
    /// the raw fd, avoiding per-operation fd lookup in the kernel.
    pub fn register_files(&mut self) -> IoResult<()> {
        let fds = [self.fd];
        let mut ring = self.ring.lock();
        let submitter = ring.submitter();
        submitter.register_files(&fds).map_err(|e| IoError::UringError {
            message: format!("register_files failed: {}", e),
        })?;
        drop(ring);

        self.fixed_fd_index = Some(0);
        Ok(())
    }

    /// Unregister fixed file descriptors.
    pub fn unregister_files(&mut self) -> IoResult<()> {
        let mut ring = self.ring.lock();
        let submitter = ring.submitter();
        submitter
            .unregister_files()
            .map_err(|e| IoError::UringError {
                message: format!("unregister_files failed: {}", e),
            })?;
        drop(ring);

        self.fixed_fd_index = None;
        Ok(())
    }

    /// Returns the target `types::Fd` - either fixed or raw.
    fn target_fd(&self) -> types::Fd {
        if let Some(idx) = self.fixed_fd_index {
            types::Fd(idx as RawFd)
        } else {
            types::Fd(self.fd)
        }
    }

    /// Submit a single SQE and wait for its completion.
    /// Returns the result value from the CQE.
    fn submit_and_wait_one(ring: &mut IoUring) -> IoResult<i32> {
        // Submit all pending entries
        ring.submit_and_wait(1).map_err(|e| IoError::UringError {
            message: format!("io_uring submit failed: {}", e),
        })?;

        // Get the completion
        let cqe = ring.completion().next().ok_or(IoError::UringError {
            message: "no completion entry available".into(),
        })?;

        let result = cqe.result();
        if result < 0 {
            return Err(IoError::Io {
                source: std::io::Error::from_raw_os_error(-result),
            });
        }

        Ok(result)
    }

    /// Submit a batch of operations and collect all results.
    ///
    /// This is more efficient than submitting one at a time because
    /// it uses a single `io_uring_enter` syscall for all operations.
    pub fn submit_batch(
        ring: &mut IoUring,
        count: usize,
    ) -> IoResult<Vec<i32>> {
        ring.submit_and_wait(count).map_err(|e| IoError::UringError {
            message: format!("batch submit failed: {}", e),
        })?;

        let mut results = Vec::with_capacity(count);
        for cqe in ring.completion() {
            let result = cqe.result();
            if result < 0 {
                results.push(result);
            } else {
                results.push(result);
            }
        }

        Ok(results)
    }

    /// Check if the buffer meets alignment requirements for O_DIRECT.
    fn check_direct_io_alignment(&self, buf: &[u8], offset: u64) -> IoResult<()> {
        if !self.direct_io {
            return Ok(());
        }

        const ALIGNMENT: usize = 4096;

        if buf.as_ptr() as usize % ALIGNMENT != 0 {
            return Err(IoError::InvalidAlignment {
                expected: ALIGNMENT,
                actual: buf.as_ptr() as usize % ALIGNMENT,
            });
        }
        if buf.len() % ALIGNMENT != 0 {
            return Err(IoError::InvalidAlignment {
                expected: ALIGNMENT,
                actual: buf.len() % ALIGNMENT,
            });
        }
        if offset as usize % ALIGNMENT != 0 {
            return Err(IoError::InvalidAlignment {
                expected: ALIGNMENT,
                actual: offset as usize % ALIGNMENT,
            });
        }

        Ok(())
    }
}

impl FileHandle for UringFile {
    fn path(&self) -> &Path {
        &self.path
    }

    async fn size(&self) -> IoResult<u64> {
        // Use fstat via libc (no io_uring op for stat)
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        let ret = unsafe { libc::fstat(self.fd, &mut stat) };
        if ret != 0 {
            return Err(IoError::Io {
                source: std::io::Error::last_os_error(),
            });
        }
        Ok(stat.st_size as u64)
    }

    async fn read_at(&self, buf: &mut [u8], offset: u64) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        self.check_direct_io_alignment(buf, offset)?;

        let mut ring = self.ring.lock();

        // Build the Read SQE
        let read_e = opcode::Read::new(
            types::Fd(self.fd),
            buf.as_mut_ptr(),
            buf.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(0x01);

        // Push to submission queue
        unsafe {
            ring.submission()
                .push(&read_e)
                .map_err(|_| IoError::UringError {
                    message: "submission queue full".into(),
                })?;
        }

        let result = Self::submit_and_wait_one(&mut ring)?;
        Ok(result as usize)
    }

    async fn write_at(&self, buf: &[u8], offset: u64) -> IoResult<usize> {
        if !self.writable {
            return Err(IoError::InvalidOperation {
                operation: "write",
                mode: "read-only",
            });
        }
        if buf.is_empty() {
            return Ok(0);
        }
        self.check_direct_io_alignment(buf, offset)?;

        let mut ring = self.ring.lock();

        // Build the Write SQE
        let write_e = opcode::Write::new(
            types::Fd(self.fd),
            buf.as_ptr(),
            buf.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(0x02);

        unsafe {
            ring.submission()
                .push(&write_e)
                .map_err(|_| IoError::UringError {
                    message: "submission queue full".into(),
                })?;
        }

        let result = Self::submit_and_wait_one(&mut ring)?;
        Ok(result as usize)
    }

    async fn sync(&self) -> IoResult<()> {
        let mut ring = self.ring.lock();

        // Fsync SQE - syncs both data and metadata
        let fsync_e = opcode::Fsync::new(types::Fd(self.fd))
            .build()
            .user_data(0x03);

        unsafe {
            ring.submission()
                .push(&fsync_e)
                .map_err(|_| IoError::UringError {
                    message: "submission queue full".into(),
                })?;
        }

        Self::submit_and_wait_one(&mut ring)?;
        Ok(())
    }

    async fn datasync(&self) -> IoResult<()> {
        let mut ring = self.ring.lock();

        // Fsync with DATASYNC flag - syncs data only (not metadata)
        let fsync_e = opcode::Fsync::new(types::Fd(self.fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(0x04);

        unsafe {
            ring.submission()
                .push(&fsync_e)
                .map_err(|_| IoError::UringError {
                    message: "submission queue full".into(),
                })?;
        }

        Self::submit_and_wait_one(&mut ring)?;
        Ok(())
    }

    async fn set_len(&self, size: u64) -> IoResult<()> {
        if !self.writable {
            return Err(IoError::InvalidOperation {
                operation: "truncate",
                mode: "read-only",
            });
        }
        // ftruncate doesn't have an io_uring opcode, use libc directly
        let ret = unsafe { libc::ftruncate(self.fd, size as libc::off_t) };
        if ret != 0 {
            return Err(IoError::Io {
                source: std::io::Error::last_os_error(),
            });
        }
        Ok(())
    }

    async fn allocate(&self, offset: u64, len: u64) -> IoResult<()> {
        if !self.writable {
            return Err(IoError::InvalidOperation {
                operation: "allocate",
                mode: "read-only",
            });
        }
        // fallocate via libc
        let ret = unsafe {
            libc::fallocate(self.fd, 0, offset as libc::off_t, len as libc::off_t)
        };
        if ret != 0 {
            return Err(IoError::Io {
                source: std::io::Error::last_os_error(),
            });
        }
        Ok(())
    }

    async fn advise(&self, offset: u64, len: u64, advice: FileAdvice) -> IoResult<()> {
        let posix_advice = match advice {
            FileAdvice::Normal => libc::POSIX_FADV_NORMAL,
            FileAdvice::Sequential => libc::POSIX_FADV_SEQUENTIAL,
            FileAdvice::Random => libc::POSIX_FADV_RANDOM,
            FileAdvice::NoReuse => libc::POSIX_FADV_NOREUSE,
            FileAdvice::WillNeed => libc::POSIX_FADV_WILLNEED,
            FileAdvice::DontNeed => libc::POSIX_FADV_DONTNEED,
        };

        let ret = unsafe {
            libc::posix_fadvise(
                self.fd,
                offset as libc::off_t,
                len as libc::off_t,
                posix_advice,
            )
        };
        if ret != 0 {
            return Err(IoError::Io {
                source: std::io::Error::from_raw_os_error(ret),
            });
        }
        Ok(())
    }

    async fn close(self) -> IoResult<()> {
        let mut ring = self.ring.lock();

        // Submit a Close SQE via io_uring
        let close_e = opcode::Close::new(types::Fd(self.fd))
            .build()
            .user_data(0x05);

        unsafe {
            ring.submission()
                .push(&close_e)
                .map_err(|_| IoError::UringError {
                    message: "submission queue full".into(),
                })?;
        }

        Self::submit_and_wait_one(&mut ring)?;

        // Prevent Drop from closing the fd again
        std::mem::forget(self);

        Ok(())
    }
}

impl Drop for UringFile {
    fn drop(&mut self) {
        // Close the file descriptor
        unsafe {
            libc::close(self.fd);
        }
    }
}

/// Batched I/O operation for submitting multiple operations at once.
///
/// This provides significant throughput improvements by amortizing
/// the cost of `io_uring_enter` syscalls across multiple operations.
///
/// ```ignore
/// let mut batch = UringBatch::new(&ring);
/// batch.add_read(fd, buf1, offset1);
/// batch.add_read(fd, buf2, offset2);
/// batch.add_write(fd, data, offset3);
/// let results = batch.submit()?;
/// ```
pub struct UringBatch {
    ring: Arc<Mutex<IoUring>>,
    count: usize,
}

impl UringBatch {
    /// Create a new batch operation context.
    pub fn new(ring: Arc<Mutex<IoUring>>) -> Self {
        Self { ring, count: 0 }
    }

    /// Add a read operation to the batch.
    pub fn add_read(
        &mut self,
        fd: RawFd,
        buf: &mut [u8],
        offset: u64,
    ) -> IoResult<()> {
        let mut ring = self.ring.lock();
        let read_e = opcode::Read::new(
            types::Fd(fd),
            buf.as_mut_ptr(),
            buf.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(self.count as u64);

        unsafe {
            ring.submission()
                .push(&read_e)
                .map_err(|_| IoError::UringError {
                    message: "submission queue full".into(),
                })?;
        }
        self.count += 1;
        Ok(())
    }

    /// Add a write operation to the batch.
    pub fn add_write(
        &mut self,
        fd: RawFd,
        buf: &[u8],
        offset: u64,
    ) -> IoResult<()> {
        let mut ring = self.ring.lock();
        let write_e = opcode::Write::new(
            types::Fd(fd),
            buf.as_ptr(),
            buf.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(self.count as u64);

        unsafe {
            ring.submission()
                .push(&write_e)
                .map_err(|_| IoError::UringError {
                    message: "submission queue full".into(),
                })?;
        }
        self.count += 1;
        Ok(())
    }

    /// Submit all queued operations and wait for completion.
    ///
    /// Returns results indexed by submission order.
    pub fn submit(self) -> IoResult<Vec<i32>> {
        if self.count == 0 {
            return Ok(Vec::new());
        }
        let mut ring = self.ring.lock();
        UringFile::submit_batch(&mut ring, self.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_ring() {
        // Verify we can create an io_uring instance
        let ring = UringFile::create_ring(32);
        assert!(ring.is_ok());
    }

    #[test]
    fn test_create_ring_large_depth() {
        let ring = UringFile::create_ring(DEFAULT_QUEUE_DEPTH);
        assert!(ring.is_ok());
    }
}
