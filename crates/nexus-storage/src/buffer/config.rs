//! Buffer pool configuration.

use nexus_common::constants::{
    BUFFER_POOL_PARTITIONS, DEFAULT_BUFFER_POOL_SIZE, DEFAULT_PAGE_SIZE, EVICTION_BATCH_SIZE,
    MIN_BUFFER_POOL_SIZE,
};

/// Configuration for the buffer pool.
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Number of page frames in the buffer pool.
    pub num_frames: usize,
    /// Page size in bytes.
    pub page_size: usize,
    /// Number of partitions to reduce lock contention.
    pub num_partitions: usize,
    /// Batch size for eviction sweeps.
    pub eviction_batch_size: usize,
    /// Whether to use direct I/O.
    pub direct_io: bool,
    /// Prefetch distance (pages to read ahead).
    pub prefetch_distance: usize,
}

impl BufferPoolConfig {
    /// Creates a new configuration with the specified number of frames.
    pub fn new(num_frames: usize) -> Self {
        Self {
            num_frames,
            page_size: DEFAULT_PAGE_SIZE,
            num_partitions: BUFFER_POOL_PARTITIONS,
            eviction_batch_size: EVICTION_BATCH_SIZE,
            direct_io: false,
            prefetch_distance: 8,
        }
    }

    /// Creates a configuration from a memory size in bytes.
    pub fn from_memory_size(memory_bytes: usize) -> Self {
        let memory_bytes = memory_bytes.max(MIN_BUFFER_POOL_SIZE);
        let num_frames = memory_bytes / DEFAULT_PAGE_SIZE;
        Self::new(num_frames)
    }

    /// Creates a default configuration (1 GB buffer pool).
    pub fn default_config() -> Self {
        Self::from_memory_size(DEFAULT_BUFFER_POOL_SIZE)
    }

    /// Sets the page size.
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        assert!(page_size.is_power_of_two());
        assert!(page_size >= 4096);
        self.page_size = page_size;
        self
    }

    /// Sets the number of partitions.
    pub fn with_partitions(mut self, num_partitions: usize) -> Self {
        self.num_partitions = num_partitions;
        self
    }

    /// Enables or disables direct I/O.
    pub fn with_direct_io(mut self, enabled: bool) -> Self {
        self.direct_io = enabled;
        self
    }

    /// Sets the prefetch distance.
    pub fn with_prefetch(mut self, distance: usize) -> Self {
        self.prefetch_distance = distance;
        self
    }

    /// Returns the total memory used by the buffer pool.
    pub fn memory_usage(&self) -> usize {
        self.num_frames * self.page_size
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.num_frames == 0 {
            return Err("num_frames must be > 0");
        }
        if !self.page_size.is_power_of_two() {
            return Err("page_size must be a power of 2");
        }
        if self.page_size < 4096 {
            return Err("page_size must be >= 4096");
        }
        if self.num_partitions == 0 {
            return Err("num_partitions must be > 0");
        }
        Ok(())
    }
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self::default_config()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_new() {
        let config = BufferPoolConfig::new(1000);
        assert_eq!(config.num_frames, 1000);
        assert_eq!(config.page_size, DEFAULT_PAGE_SIZE);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_from_memory() {
        let config = BufferPoolConfig::from_memory_size(64 * 1024 * 1024); // 64 MB
        assert_eq!(config.num_frames, 64 * 1024 * 1024 / DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn test_config_builder() {
        let config = BufferPoolConfig::new(100)
            .with_page_size(16384)
            .with_partitions(8)
            .with_direct_io(true)
            .with_prefetch(16);

        assert_eq!(config.page_size, 16384);
        assert_eq!(config.num_partitions, 8);
        assert!(config.direct_io);
        assert_eq!(config.prefetch_distance, 16);
    }

    #[test]
    fn test_memory_usage() {
        let config = BufferPoolConfig::new(1000);
        assert_eq!(config.memory_usage(), 1000 * DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn test_validation() {
        let config = BufferPoolConfig::new(0);
        assert!(config.validate().is_err());

        let config = BufferPoolConfig::new(100).with_partitions(0);
        assert!(config.validate().is_err());
    }
}
