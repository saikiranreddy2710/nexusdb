//! Standard async file I/O implementation using tokio.
//!
//! This is the default cross-platform implementation that works on all
//! supported operating systems. It uses tokio's async file operations.

use std::fs::File as StdFile;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::task;

use super::error::{IoError, IoResult};
use super::handle::{FileAdvice, FileHandle};
use super::options::OpenOptions;

/// Standard file implementation using tokio.
///
/// This implementation wraps a standard file and uses tokio's spawn_blocking
/// for async operations. While not as efficient as io_uring, it provides
/// reliable cross-platform async I/O.
pub struct StandardFile {
    /// The underlying file, wrapped in a mutex for thread-safe access.
    file: Arc<Mutex<StdFile>>,
    /// The file path.
    path: PathBuf,
    /// Whether the file was opened with write access.
    writable: bool,
    /// Whether direct I/O is enabled.
    #[allow(dead_code)]
    direct_io: bool,
}

impl StandardFile {
    /// Opens a file with the specified options.
    pub async fn open(path: impl AsRef<Path>, options: OpenOptions) -> IoResult<Self> {
        let path = path.as_ref().to_path_buf();
        let path_clone = path.clone();
        let direct_io = options.direct_io;
        let writable = options.write;

        let file = task::spawn_blocking(move || Self::open_sync(&path_clone, &options))
            .await
            .map_err(|e| IoError::Io {
                source: std::io::Error::new(std::io::ErrorKind::Other, e),
            })??;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            path,
            writable,
            direct_io,
        })
    }

    /// Synchronously opens a file with the specified options.
    fn open_sync(path: &Path, options: &OpenOptions) -> IoResult<StdFile> {
        let mut std_opts = options.to_std_options();

        // Apply platform-specific direct I/O flags
        #[cfg(target_os = "linux")]
        if options.direct_io {
            use std::os::unix::fs::OpenOptionsExt;
            // O_DIRECT = 0x4000 on Linux
            std_opts.custom_flags(libc::O_DIRECT);
        }

        #[cfg(target_os = "macos")]
        if options.direct_io {
            // macOS doesn't support O_DIRECT, we'll use F_NOCACHE after opening
        }

        let file = std_opts
            .open(path)
            .map_err(|e| IoError::from_io_with_path(e, path))?;

        // macOS: Set F_NOCACHE flag to disable caching
        #[cfg(target_os = "macos")]
        if options.direct_io {
            use std::os::unix::io::AsRawFd;
            unsafe {
                libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
            }
        }

        // Pre-allocate if requested
        if let Some(size) = options.preallocate {
            Self::preallocate_sync(&file, 0, size)?;
        }

        Ok(file)
    }

    /// Synchronously pre-allocates file space.
    #[cfg(target_os = "linux")]
    fn preallocate_sync(file: &StdFile, offset: u64, len: u64) -> IoResult<()> {
        use std::os::unix::io::AsRawFd;
        let ret = unsafe { libc::posix_fallocate(file.as_raw_fd(), offset as i64, len as i64) };
        if ret != 0 {
            return Err(IoError::Io {
                source: std::io::Error::from_raw_os_error(ret),
            });
        }
        Ok(())
    }

    #[cfg(target_os = "macos")]
    fn preallocate_sync(file: &StdFile, _offset: u64, len: u64) -> IoResult<()> {
        use std::os::unix::io::AsRawFd;

        // macOS uses F_PREALLOCATE
        #[repr(C)]
        struct FStore {
            fst_flags: u32,
            fst_posmode: i32,
            fst_offset: i64,
            fst_length: i64,
            fst_bytesalloc: i64,
        }

        const F_ALLOCATECONTIG: u32 = 0x02;
        const F_ALLOCATEALL: u32 = 0x04;
        const F_PEOFPOSMODE: i32 = 3;
        const F_PREALLOCATE: i32 = 42;

        let mut fstore = FStore {
            fst_flags: F_ALLOCATECONTIG | F_ALLOCATEALL,
            fst_posmode: F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: len as i64,
            fst_bytesalloc: 0,
        };

        let ret = unsafe { libc::fcntl(file.as_raw_fd(), F_PREALLOCATE, &mut fstore) };

        if ret == -1 {
            // Try without contiguous allocation
            fstore.fst_flags = F_ALLOCATEALL;
            let ret = unsafe { libc::fcntl(file.as_raw_fd(), F_PREALLOCATE, &mut fstore) };
            if ret == -1 {
                return Err(IoError::Io {
                    source: std::io::Error::last_os_error(),
                });
            }
        }

        Ok(())
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn preallocate_sync(file: &StdFile, _offset: u64, len: u64) -> IoResult<()> {
        // Fallback: just set the file length
        file.set_len(len).map_err(|e| IoError::Io { source: e })
    }

    /// Returns the underlying file for testing.
    #[cfg(test)]
    fn inner(&self) -> &Arc<Mutex<StdFile>> {
        &self.file
    }
}

impl FileHandle for StandardFile {
    fn path(&self) -> &Path {
        &self.path
    }

    async fn size(&self) -> IoResult<u64> {
        let file = Arc::clone(&self.file);
        task::spawn_blocking(move || {
            let file = file.lock();
            file.metadata()
                .map(|m| m.len())
                .map_err(|e| IoError::Io { source: e })
        })
        .await
        .map_err(|e| IoError::Io {
            source: std::io::Error::new(std::io::ErrorKind::Other, e),
        })?
    }

    async fn read_at(&self, buf: &mut [u8], offset: u64) -> IoResult<usize> {
        let file = Arc::clone(&self.file);
        let len = buf.len();

        // Create a buffer we can send to the blocking task
        let mut owned_buf = vec![0u8; len];

        let (result, read_buf) = task::spawn_blocking(move || {
            let mut file = file.lock();
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| IoError::Io { source: e })?;
            let n = file
                .read(&mut owned_buf)
                .map_err(|e| IoError::Io { source: e })?;
            Ok::<_, IoError>((n, owned_buf))
        })
        .await
        .map_err(|e| IoError::Io {
            source: std::io::Error::new(std::io::ErrorKind::Other, e),
        })??;

        buf[..result].copy_from_slice(&read_buf[..result]);
        Ok(result)
    }

    async fn write_at(&self, buf: &[u8], offset: u64) -> IoResult<usize> {
        if !self.writable {
            return Err(IoError::InvalidOperation {
                operation: "write",
                mode: "read-only",
            });
        }

        let file = Arc::clone(&self.file);
        let owned_buf = buf.to_vec();

        task::spawn_blocking(move || {
            let mut file = file.lock();
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| IoError::Io { source: e })?;
            file.write(&owned_buf)
                .map_err(|e| IoError::Io { source: e })
        })
        .await
        .map_err(|e| IoError::Io {
            source: std::io::Error::new(std::io::ErrorKind::Other, e),
        })?
    }

    async fn sync(&self) -> IoResult<()> {
        let file = Arc::clone(&self.file);
        task::spawn_blocking(move || {
            let file = file.lock();
            file.sync_all().map_err(|e| IoError::Io { source: e })
        })
        .await
        .map_err(|e| IoError::Io {
            source: std::io::Error::new(std::io::ErrorKind::Other, e),
        })?
    }

    async fn datasync(&self) -> IoResult<()> {
        let file = Arc::clone(&self.file);
        task::spawn_blocking(move || {
            let file = file.lock();
            file.sync_data().map_err(|e| IoError::Io { source: e })
        })
        .await
        .map_err(|e| IoError::Io {
            source: std::io::Error::new(std::io::ErrorKind::Other, e),
        })?
    }

    async fn set_len(&self, size: u64) -> IoResult<()> {
        if !self.writable {
            return Err(IoError::InvalidOperation {
                operation: "set_len",
                mode: "read-only",
            });
        }

        let file = Arc::clone(&self.file);
        task::spawn_blocking(move || {
            let file = file.lock();
            file.set_len(size).map_err(|e| IoError::Io { source: e })
        })
        .await
        .map_err(|e| IoError::Io {
            source: std::io::Error::new(std::io::ErrorKind::Other, e),
        })?
    }

    async fn allocate(&self, offset: u64, len: u64) -> IoResult<()> {
        if !self.writable {
            return Err(IoError::InvalidOperation {
                operation: "allocate",
                mode: "read-only",
            });
        }

        let file = Arc::clone(&self.file);
        task::spawn_blocking(move || {
            let file = file.lock();
            Self::preallocate_sync(&file, offset, len)
        })
        .await
        .map_err(|e| IoError::Io {
            source: std::io::Error::new(std::io::ErrorKind::Other, e),
        })?
    }

    async fn advise(&self, offset: u64, len: u64, advice: FileAdvice) -> IoResult<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;

            let posix_advice = match advice {
                FileAdvice::Normal => libc::POSIX_FADV_NORMAL,
                FileAdvice::Sequential => libc::POSIX_FADV_SEQUENTIAL,
                FileAdvice::Random => libc::POSIX_FADV_RANDOM,
                FileAdvice::NoReuse => libc::POSIX_FADV_NOREUSE,
                FileAdvice::WillNeed => libc::POSIX_FADV_WILLNEED,
                FileAdvice::DontNeed => libc::POSIX_FADV_DONTNEED,
            };

            let file = Arc::clone(&self.file);
            task::spawn_blocking(move || {
                let file = file.lock();
                let ret = unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), offset as i64, len as i64, posix_advice)
                };
                if ret != 0 {
                    Err(IoError::Io {
                        source: std::io::Error::from_raw_os_error(ret),
                    })
                } else {
                    Ok(())
                }
            })
            .await
            .map_err(|e| IoError::Io {
                source: std::io::Error::new(std::io::ErrorKind::Other, e),
            })?
        }

        #[cfg(not(target_os = "linux"))]
        {
            // fadvise is not available on macOS/Windows, just ignore
            let _ = (offset, len, advice);
            Ok(())
        }
    }

    async fn close(self) -> IoResult<()>
    where
        Self: Sized,
    {
        // File will be closed when dropped
        // We could explicitly sync here if needed
        Ok(())
    }
}

impl std::fmt::Debug for StandardFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandardFile")
            .field("path", &self.path)
            .field("writable", &self.writable)
            .field("direct_io", &self.direct_io)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_open_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let options = OpenOptions::for_create();
        let file = StandardFile::open(&path, options).await.unwrap();

        assert_eq!(file.path(), path);
        assert!(file.writable);
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("rw.db");

        let file = StandardFile::open(&path, OpenOptions::for_create())
            .await
            .unwrap();

        // Write data
        let data = b"Hello, World!";
        let written = file.write_at(data, 0).await.unwrap();
        assert_eq!(written, data.len());

        // Sync
        file.sync().await.unwrap();

        // Read back
        let mut buf = vec![0u8; data.len()];
        let read = file.read_at(&mut buf, 0).await.unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_write_at_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("offset.db");

        let file = StandardFile::open(&path, OpenOptions::for_create())
            .await
            .unwrap();

        // Write at offset 100
        let data = b"Test data";
        file.write_at(data, 100).await.unwrap();
        file.sync().await.unwrap();

        // Verify file size
        let size = file.size().await.unwrap();
        assert_eq!(size, 100 + data.len() as u64);

        // Read back
        let mut buf = vec![0u8; data.len()];
        file.read_at(&mut buf, 100).await.unwrap();
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_read_exact() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("exact.db");

        let file = StandardFile::open(&path, OpenOptions::for_create())
            .await
            .unwrap();

        let data = b"0123456789";
        file.write_at(data, 0).await.unwrap();
        file.sync().await.unwrap();

        // Read exact
        let mut buf = vec![0u8; 10];
        file.read_exact_at(&mut buf, 0).await.unwrap();
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_read_exact_short() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("short.db");

        let file = StandardFile::open(&path, OpenOptions::for_create())
            .await
            .unwrap();

        let data = b"Short";
        file.write_at(data, 0).await.unwrap();
        file.sync().await.unwrap();

        // Try to read more than available
        let mut buf = vec![0u8; 100];
        let result = file.read_exact_at(&mut buf, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_len() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("len.db");

        let file = StandardFile::open(&path, OpenOptions::for_create())
            .await
            .unwrap();

        // Extend file
        file.set_len(1024).await.unwrap();
        assert_eq!(file.size().await.unwrap(), 1024);

        // Truncate
        file.set_len(512).await.unwrap();
        assert_eq!(file.size().await.unwrap(), 512);
    }

    #[tokio::test]
    async fn test_sync_and_datasync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sync.db");

        let file = StandardFile::open(&path, OpenOptions::for_create())
            .await
            .unwrap();

        file.write_at(b"data", 0).await.unwrap();
        file.sync().await.unwrap();
        file.datasync().await.unwrap();
    }

    #[tokio::test]
    async fn test_advise() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("advise.db");

        let file = StandardFile::open(&path, OpenOptions::for_create())
            .await
            .unwrap();
        file.set_len(4096).await.unwrap();

        // These should not error even if fadvise is not available
        file.advise(0, 4096, FileAdvice::Sequential).await.unwrap();
        file.advise(0, 4096, FileAdvice::Random).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_only_write_fails() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ro.db");

        // Create the file first
        {
            let file = StandardFile::open(&path, OpenOptions::for_create())
                .await
                .unwrap();
            file.write_at(b"data", 0).await.unwrap();
        }

        // Open read-only
        let file = StandardFile::open(&path, OpenOptions::for_read())
            .await
            .unwrap();
        let result = file.write_at(b"new data", 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_allocate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("alloc.db");

        let file = StandardFile::open(&path, OpenOptions::for_create())
            .await
            .unwrap();

        // Allocate space
        file.allocate(0, 1024 * 1024).await.unwrap();

        // Size might not change on all platforms, but it shouldn't error
    }

    #[tokio::test]
    async fn test_concurrent_reads() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("concurrent.db");

        let file = StandardFile::open(&path, OpenOptions::for_create())
            .await
            .unwrap();

        // Write some data
        for i in 0..10u8 {
            file.write_at(&[i; 100], i as u64 * 100).await.unwrap();
        }
        file.sync().await.unwrap();

        // Wrap in Arc for sharing
        let file = Arc::new(file);

        // Spawn multiple concurrent reads
        let mut handles = vec![];
        for i in 0..10u8 {
            let file = Arc::clone(&file);
            handles.push(tokio::spawn(async move {
                let mut buf = vec![0u8; 100];
                file.read_exact_at(&mut buf, i as u64 * 100).await.unwrap();
                assert!(buf.iter().all(|&b| b == i));
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}
