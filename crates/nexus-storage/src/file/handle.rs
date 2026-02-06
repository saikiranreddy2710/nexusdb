//! File handle trait and file manager.

use std::path::Path;
use std::sync::Arc;

use super::error::{IoError, IoResult};
use super::options::OpenOptions;
use super::std_io::StandardFile;

/// Trait for asynchronous file operations.
///
/// This trait provides a unified interface for file I/O that can be
/// implemented by different backends (standard async I/O, io_uring, etc.).
///
/// All operations are position-based (pread/pwrite style) for thread-safety.
#[allow(async_fn_in_trait)]
pub trait FileHandle: Send + Sync {
    /// Returns the file path.
    fn path(&self) -> &Path;

    /// Returns the current file size.
    async fn size(&self) -> IoResult<u64>;

    /// Reads data from the file at the specified offset.
    ///
    /// Returns the number of bytes read. May return less than the buffer
    /// size if EOF is reached.
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> IoResult<usize>;

    /// Reads exactly `buf.len()` bytes from the file at the specified offset.
    ///
    /// Returns an error if EOF is reached before the buffer is filled.
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> IoResult<()> {
        let mut total_read = 0;
        while total_read < buf.len() {
            let n = self
                .read_at(&mut buf[total_read..], offset + total_read as u64)
                .await?;
            if n == 0 {
                return Err(IoError::short_read(buf.len(), total_read));
            }
            total_read += n;
        }
        Ok(())
    }

    /// Writes data to the file at the specified offset.
    ///
    /// Returns the number of bytes written.
    async fn write_at(&self, buf: &[u8], offset: u64) -> IoResult<usize>;

    /// Writes all bytes to the file at the specified offset.
    ///
    /// Returns an error if not all bytes could be written.
    async fn write_all_at(&self, buf: &[u8], offset: u64) -> IoResult<()> {
        let mut total_written = 0;
        while total_written < buf.len() {
            let n = self
                .write_at(&buf[total_written..], offset + total_written as u64)
                .await?;
            if n == 0 {
                return Err(IoError::short_write(buf.len(), total_written));
            }
            total_written += n;
        }
        Ok(())
    }

    /// Syncs all data and metadata to disk.
    async fn sync(&self) -> IoResult<()>;

    /// Syncs only data (not metadata) to disk.
    async fn datasync(&self) -> IoResult<()>;

    /// Truncates or extends the file to the specified size.
    async fn set_len(&self, size: u64) -> IoResult<()>;

    /// Pre-allocates space for the file.
    ///
    /// This is a hint to the filesystem to allocate contiguous space.
    async fn allocate(&self, offset: u64, len: u64) -> IoResult<()>;

    /// Advises the kernel about the access pattern.
    async fn advise(&self, offset: u64, len: u64, advice: FileAdvice) -> IoResult<()>;

    /// Closes the file.
    async fn close(self) -> IoResult<()>
    where
        Self: Sized;
}

/// Advice hints for file access patterns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileAdvice {
    /// No specific advice.
    Normal,
    /// Data will be accessed sequentially.
    Sequential,
    /// Data will be accessed randomly.
    Random,
    /// Data will be accessed once.
    NoReuse,
    /// Data will be accessed soon.
    WillNeed,
    /// Data will not be accessed soon.
    DontNeed,
}

/// File manager for creating and managing file handles.
///
/// The file manager provides a unified interface for opening files
/// with different backends based on platform and configuration.
pub struct FileManager {
    /// Whether to prefer io_uring when available.
    #[allow(dead_code)]
    prefer_uring: bool,
    /// Default I/O alignment.
    #[allow(dead_code)]
    io_alignment: usize,
}

impl FileManager {
    /// Creates a new file manager with default settings.
    pub fn new() -> IoResult<Self> {
        Ok(Self {
            prefer_uring: cfg!(all(target_os = "linux", feature = "io-uring")),
            io_alignment: super::IO_ALIGNMENT,
        })
    }

    /// Creates a file manager that prefers io_uring on Linux.
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    pub fn with_uring() -> IoResult<Self> {
        Ok(Self {
            prefer_uring: true,
            io_alignment: super::IO_ALIGNMENT,
        })
    }

    /// Creates a file manager that uses standard I/O only.
    pub fn with_std_io() -> IoResult<Self> {
        Ok(Self {
            prefer_uring: false,
            io_alignment: super::IO_ALIGNMENT,
        })
    }

    /// Opens a file with the specified options.
    pub async fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
    ) -> IoResult<Arc<StandardFile>> {
        let path = path.as_ref();

        // For now, always use StandardFile
        // io_uring support can be added later
        let file = StandardFile::open(path, options).await?;
        Ok(Arc::new(file))
    }

    /// Opens a file for reading.
    pub async fn open_read(&self, path: impl AsRef<Path>) -> IoResult<Arc<StandardFile>> {
        self.open(path, OpenOptions::for_read()).await
    }

    /// Opens a file for reading and writing, creating if necessary.
    pub async fn open_write(&self, path: impl AsRef<Path>) -> IoResult<Arc<StandardFile>> {
        self.open(path, OpenOptions::for_create()).await
    }

    /// Opens a database file with optimal settings.
    pub async fn open_database(&self, path: impl AsRef<Path>) -> IoResult<Arc<StandardFile>> {
        self.open(path, OpenOptions::for_database()).await
    }

    /// Creates a new file, failing if it already exists.
    pub async fn create(&self, path: impl AsRef<Path>) -> IoResult<Arc<StandardFile>> {
        self.open(
            path,
            OpenOptions::new().read(true).write(true).create_new(true),
        )
        .await
    }

    /// Deletes a file.
    pub async fn remove(&self, path: impl AsRef<Path>) -> IoResult<()> {
        let path = path.as_ref();
        tokio::fs::remove_file(path)
            .await
            .map_err(|e| IoError::from_io_with_path(e, path))
    }

    /// Checks if a file exists.
    pub async fn exists(&self, path: impl AsRef<Path>) -> bool {
        tokio::fs::metadata(path.as_ref()).await.is_ok()
    }

    /// Returns metadata for a file.
    pub async fn metadata(&self, path: impl AsRef<Path>) -> IoResult<FileMetadata> {
        let path = path.as_ref();
        let meta = tokio::fs::metadata(path)
            .await
            .map_err(|e| IoError::from_io_with_path(e, path))?;

        Ok(FileMetadata {
            size: meta.len(),
            is_file: meta.is_file(),
            is_dir: meta.is_dir(),
        })
    }

    /// Renames a file.
    pub async fn rename(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) -> IoResult<()> {
        let from = from.as_ref();
        let to = to.as_ref();
        tokio::fs::rename(from, to)
            .await
            .map_err(|e| IoError::from_io_with_path(e, from))
    }

    /// Creates a directory and all parent directories.
    pub async fn create_dir_all(&self, path: impl AsRef<Path>) -> IoResult<()> {
        let path = path.as_ref();
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| IoError::from_io_with_path(e, path))
    }
}

impl Default for FileManager {
    fn default() -> Self {
        Self::new().expect("failed to create file manager")
    }
}

/// Basic file metadata.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// File size in bytes.
    pub size: u64,
    /// Whether this is a regular file.
    pub is_file: bool,
    /// Whether this is a directory.
    pub is_dir: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_manager_creation() {
        let manager = FileManager::new().unwrap();
        assert!(manager.io_alignment > 0);
    }

    #[tokio::test]
    async fn test_file_not_found() {
        let manager = FileManager::new().unwrap();
        let result = manager.open_read("/nonexistent/file.db").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_and_read_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let manager = FileManager::new().unwrap();

        // Create and write
        {
            let file = manager.create(&path).await.unwrap();
            let data = b"Hello, NexusDB!";
            file.write_all_at(data, 0).await.unwrap();
            file.sync().await.unwrap();
        }

        // Read back
        {
            let file = manager.open_read(&path).await.unwrap();
            let mut buf = vec![0u8; 15];
            file.read_exact_at(&mut buf, 0).await.unwrap();
            assert_eq!(&buf, b"Hello, NexusDB!");
        }
    }

    #[tokio::test]
    async fn test_file_metadata() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("meta.db");

        let manager = FileManager::new().unwrap();
        let file = manager.create(&path).await.unwrap();
        file.write_all_at(&[0u8; 1024], 0).await.unwrap();
        file.sync().await.unwrap();
        drop(file);

        let meta = manager.metadata(&path).await.unwrap();
        assert_eq!(meta.size, 1024);
        assert!(meta.is_file);
        assert!(!meta.is_dir);
    }

    #[tokio::test]
    async fn test_exists() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("exists.db");

        let manager = FileManager::new().unwrap();

        assert!(!manager.exists(&path).await);

        let file = manager.create(&path).await.unwrap();
        drop(file);

        assert!(manager.exists(&path).await);
    }

    #[tokio::test]
    async fn test_remove() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("remove.db");

        let manager = FileManager::new().unwrap();
        let file = manager.create(&path).await.unwrap();
        drop(file);

        assert!(manager.exists(&path).await);
        manager.remove(&path).await.unwrap();
        assert!(!manager.exists(&path).await);
    }

    #[tokio::test]
    async fn test_rename() {
        let dir = tempdir().unwrap();
        let path1 = dir.path().join("old.db");
        let path2 = dir.path().join("new.db");

        let manager = FileManager::new().unwrap();
        let file = manager.create(&path1).await.unwrap();
        drop(file);

        manager.rename(&path1, &path2).await.unwrap();

        assert!(!manager.exists(&path1).await);
        assert!(manager.exists(&path2).await);
    }
}
