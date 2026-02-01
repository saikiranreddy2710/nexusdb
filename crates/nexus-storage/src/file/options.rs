//! File open options.

use std::fs;

/// Options for opening files.
///
/// This is similar to `std::fs::OpenOptions` but with additional
/// options for direct I/O and other database-specific features.
///
/// # Example
///
/// ```rust
/// use nexus_storage::file::OpenOptions;
///
/// let options = OpenOptions::new()
///     .read(true)
///     .write(true)
///     .create(true)
///     .direct_io(true);
/// ```
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// Open for reading.
    pub(crate) read: bool,
    /// Open for writing.
    pub(crate) write: bool,
    /// Append mode.
    pub(crate) append: bool,
    /// Truncate existing file.
    pub(crate) truncate: bool,
    /// Create file if it doesn't exist.
    pub(crate) create: bool,
    /// Create file, fail if it exists.
    pub(crate) create_new: bool,
    /// Use direct I/O (bypass OS cache).
    pub(crate) direct_io: bool,
    /// Sync all writes to disk.
    pub(crate) sync: bool,
    /// Data sync only (not metadata).
    pub(crate) dsync: bool,
    /// Pre-allocate file space.
    pub(crate) preallocate: Option<u64>,
    /// Custom mode (Unix permissions).
    pub(crate) mode: Option<u32>,
}

impl OpenOptions {
    /// Creates a new set of options with default values.
    ///
    /// All options are initially set to `false` or `None`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            direct_io: false,
            sync: false,
            dsync: false,
            preallocate: None,
            mode: None,
        }
    }

    /// Sets the option for read access.
    #[must_use]
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Sets the option for write access.
    #[must_use]
    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Sets the option for append mode.
    #[must_use]
    pub fn append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }

    /// Sets the option for truncating an existing file.
    #[must_use]
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// Sets the option to create a new file if it doesn't exist.
    #[must_use]
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Sets the option to create a new file, failing if it exists.
    #[must_use]
    pub fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Sets the option for direct I/O (bypass OS page cache).
    ///
    /// This is useful for database workloads where the database
    /// has its own buffer pool and doesn't benefit from OS caching.
    ///
    /// # Platform Support
    ///
    /// - **Linux**: Uses O_DIRECT flag
    /// - **macOS**: Uses F_NOCACHE fcntl
    /// - **Windows**: Uses FILE_FLAG_NO_BUFFERING
    #[must_use]
    pub fn direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }

    /// Sets the option for synchronous I/O.
    ///
    /// When enabled, all writes are immediately flushed to disk.
    #[must_use]
    pub fn sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// Sets the option for data-only synchronous I/O.
    ///
    /// Like `sync`, but only syncs data, not metadata.
    #[must_use]
    pub fn dsync(mut self, dsync: bool) -> Self {
        self.dsync = dsync;
        self
    }

    /// Sets pre-allocation size for the file.
    ///
    /// The file will be pre-allocated to at least this size
    /// when opened for writing.
    #[must_use]
    pub fn preallocate(mut self, size: u64) -> Self {
        self.preallocate = Some(size);
        self
    }

    /// Sets the Unix file mode (permissions).
    #[cfg(unix)]
    #[must_use]
    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Returns true if read access is enabled.
    #[inline]
    pub fn is_read(&self) -> bool {
        self.read
    }

    /// Returns true if write access is enabled.
    #[inline]
    pub fn is_write(&self) -> bool {
        self.write
    }

    /// Returns true if direct I/O is enabled.
    #[inline]
    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }

    /// Converts to std::fs::OpenOptions.
    ///
    /// Note: This does not include direct I/O flags, which must
    /// be set platform-specifically.
    pub fn to_std_options(&self) -> fs::OpenOptions {
        let mut opts = fs::OpenOptions::new();
        opts.read(self.read)
            .write(self.write)
            .append(self.append)
            .truncate(self.truncate)
            .create(self.create)
            .create_new(self.create_new);

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            if let Some(mode) = self.mode {
                opts.mode(mode);
            }
        }

        opts
    }

    /// Creates a builder for reading files.
    #[must_use]
    pub fn for_read() -> Self {
        Self::new().read(true)
    }

    /// Creates a builder for writing files.
    #[must_use]
    pub fn for_write() -> Self {
        Self::new().read(true).write(true)
    }

    /// Creates a builder for creating new files.
    #[must_use]
    pub fn for_create() -> Self {
        Self::new().read(true).write(true).create(true)
    }

    /// Creates a builder for database files.
    ///
    /// This enables direct I/O and sync for durability.
    #[must_use]
    pub fn for_database() -> Self {
        Self::new()
            .read(true)
            .write(true)
            .create(true)
            .direct_io(true)
            .dsync(true)
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = OpenOptions::new();
        assert!(!opts.read);
        assert!(!opts.write);
        assert!(!opts.create);
        assert!(!opts.direct_io);
    }

    #[test]
    fn test_builder() {
        let opts = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .direct_io(true);

        assert!(opts.read);
        assert!(opts.write);
        assert!(opts.create);
        assert!(opts.direct_io);
    }

    #[test]
    fn test_for_database() {
        let opts = OpenOptions::for_database();
        assert!(opts.read);
        assert!(opts.write);
        assert!(opts.create);
        assert!(opts.direct_io);
        assert!(opts.dsync);
    }

    #[test]
    fn test_to_std_options() {
        let opts = OpenOptions::for_create();
        let std_opts = opts.to_std_options();
        // Just verify it doesn't panic
        drop(std_opts);
    }

    #[test]
    fn test_preallocate() {
        let opts = OpenOptions::new().preallocate(1024 * 1024);
        assert_eq!(opts.preallocate, Some(1024 * 1024));
    }
}
