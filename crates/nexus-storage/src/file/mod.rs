//! Async file I/O layer for NexusDB.
//!
//! This module provides an abstraction layer for file operations with:
//!
//! - **Async I/O**: Non-blocking file operations using tokio
//! - **Direct I/O**: Bypass OS page cache for predictable latency
//! - **io_uring support**: High-performance Linux-specific backend (optional)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │           FileHandle Trait              │
//! │  (read, write, sync, allocate, etc.)    │
//! └─────────────────────────────────────────┘
//!              │                   │
//!              ▼                   ▼
//! ┌─────────────────────┐  ┌─────────────────────┐
//! │   StandardFile      │  │   UringFile         │
//! │   (tokio-based)     │  │   (io_uring)        │
//! │   All platforms     │  │   Linux only        │
//! └─────────────────────┘  └─────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use nexus_storage::file::{FileManager, FileHandle, OpenOptions};
//!
//! async fn example() -> std::io::Result<()> {
//!     let manager = FileManager::new()?;
//!     
//!     let options = OpenOptions::new()
//!         .read(true)
//!         .write(true)
//!         .create(true);
//!     
//!     let file = manager.open("data.db", options).await?;
//!     
//!     // Write data at offset 0
//!     let data = vec![0u8; 8192];
//!     file.write_at(&data, 0).await?;
//!     
//!     // Sync to disk
//!     file.sync().await?;
//!     
//!     Ok(())
//! }
//! ```

mod error;
mod handle;
mod options;
mod std_io;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod uring;

pub use error::{IoError, IoResult};
pub use handle::{FileHandle, FileManager};
pub use options::OpenOptions;
pub use std_io::StandardFile;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use uring::UringFile;

/// Default I/O alignment for direct I/O (4 KB).
pub const IO_ALIGNMENT: usize = 4096;

/// Maximum number of concurrent I/O operations.
pub const MAX_CONCURRENT_IOS: usize = 256;

/// Default I/O depth for io_uring.
pub const DEFAULT_IO_DEPTH: u32 = 128;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert!(IO_ALIGNMENT.is_power_of_two());
        assert!(MAX_CONCURRENT_IOS > 0);
        assert!(DEFAULT_IO_DEPTH > 0);
    }
}
