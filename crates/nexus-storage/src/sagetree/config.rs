//! Configuration for the SageTree storage engine.
//!
//! This module defines configuration options that control SageTree behavior.

use nexus_common::constants::{
    DEFAULT_PAGE_FILL_FACTOR, MAX_BRANCHING_FACTOR, MAX_DELTA_CHAIN_LENGTH, MIN_PAGE_FILL_FACTOR,
};

/// Configuration for a SageTree instance.
#[derive(Debug, Clone)]
pub struct SageTreeConfig {
    /// Page size in bytes (default: 8KB).
    pub page_size: usize,

    /// Maximum number of children in internal nodes (branching factor).
    pub max_branching_factor: usize,

    /// Target fill factor for pages (default: 0.7).
    pub fill_factor: f64,

    /// Minimum fill factor before merge/rebalance (default: 0.4).
    pub min_fill_factor: f64,

    /// Maximum delta chain length before consolidation.
    pub max_delta_chain_length: usize,

    /// Whether to use delta chains (Bw-tree style) for updates.
    pub enable_delta_chains: bool,

    /// Whether to enable fractional cascading for range queries.
    pub enable_fractional_cascading: bool,

    /// Whether to prefetch sibling nodes during traversal.
    pub enable_sibling_prefetch: bool,

    /// Number of nodes to prefetch (if enabled).
    pub prefetch_count: usize,

    /// Whether to enable node consolidation in background.
    pub background_consolidation: bool,
}

impl Default for SageTreeConfig {
    fn default() -> Self {
        Self {
            page_size: nexus_common::constants::DEFAULT_PAGE_SIZE,
            max_branching_factor: MAX_BRANCHING_FACTOR,
            fill_factor: DEFAULT_PAGE_FILL_FACTOR,
            min_fill_factor: MIN_PAGE_FILL_FACTOR,
            max_delta_chain_length: MAX_DELTA_CHAIN_LENGTH,
            enable_delta_chains: true,
            enable_fractional_cascading: true,
            enable_sibling_prefetch: true,
            prefetch_count: 4,
            background_consolidation: true,
        }
    }
}

impl SageTreeConfig {
    /// Creates a new SageTree configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the page size.
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    /// Sets the maximum branching factor.
    pub fn with_branching_factor(mut self, factor: usize) -> Self {
        self.max_branching_factor = factor;
        self
    }

    /// Sets the fill factor.
    pub fn with_fill_factor(mut self, factor: f64) -> Self {
        self.fill_factor = factor.clamp(0.5, 1.0);
        self
    }

    /// Sets the minimum fill factor.
    pub fn with_min_fill_factor(mut self, factor: f64) -> Self {
        self.min_fill_factor = factor.clamp(0.1, 0.5);
        self
    }

    /// Sets the maximum delta chain length.
    pub fn with_max_delta_chain_length(mut self, length: usize) -> Self {
        self.max_delta_chain_length = length;
        self
    }

    /// Enables or disables delta chains.
    pub fn with_delta_chains(mut self, enable: bool) -> Self {
        self.enable_delta_chains = enable;
        self
    }

    /// Enables or disables fractional cascading.
    pub fn with_fractional_cascading(mut self, enable: bool) -> Self {
        self.enable_fractional_cascading = enable;
        self
    }

    /// Enables or disables sibling prefetch.
    pub fn with_sibling_prefetch(mut self, enable: bool) -> Self {
        self.enable_sibling_prefetch = enable;
        self
    }

    /// Sets the prefetch count.
    pub fn with_prefetch_count(mut self, count: usize) -> Self {
        self.prefetch_count = count;
        self
    }

    /// Enables or disables background consolidation.
    pub fn with_background_consolidation(mut self, enable: bool) -> Self {
        self.background_consolidation = enable;
        self
    }

    /// Calculates the minimum number of keys in a node.
    pub fn min_keys(&self) -> usize {
        ((self.max_branching_factor as f64 * self.min_fill_factor) as usize).max(1)
    }

    /// Calculates the target number of keys in a node.
    pub fn target_keys(&self) -> usize {
        (self.max_branching_factor as f64 * self.fill_factor) as usize
    }

    /// Calculates the maximum number of keys in a leaf node.
    /// Leaf nodes store key-value pairs, so they can hold fewer entries.
    pub fn max_leaf_keys(&self) -> usize {
        // Estimate based on average key+value size
        // This is a rough estimate; actual limit depends on data size
        self.max_branching_factor / 2
    }

    /// Calculates the maximum number of keys in an internal node.
    /// Internal nodes only store keys and child pointers.
    pub fn max_internal_keys(&self) -> usize {
        self.max_branching_factor - 1
    }

    /// Returns the header size for nodes.
    pub fn node_header_size(&self) -> usize {
        // Node header: type(1) + flags(1) + key_count(2) + level(2) +
        //              delta_count(2) + next_page(8) + prev_page(8) +
        //              parent_page(8) + checksum(4) = 36 bytes, round to 40
        40
    }

    /// Returns the usable space in a page for node data.
    pub fn usable_space(&self) -> usize {
        self.page_size - self.node_header_size()
    }
}

/// Preset configuration for testing with small pages.
impl SageTreeConfig {
    /// Creates a test configuration with small parameters.
    pub fn for_testing() -> Self {
        Self {
            page_size: 4096,
            max_branching_factor: 16,
            fill_factor: 0.5,
            min_fill_factor: 0.25,
            max_delta_chain_length: 4,
            enable_delta_chains: true,
            enable_fractional_cascading: false,
            enable_sibling_prefetch: false,
            prefetch_count: 0,
            background_consolidation: false,
        }
    }

    /// Creates a high-performance configuration.
    pub fn high_performance() -> Self {
        Self {
            page_size: 16384, // 16KB pages
            max_branching_factor: 512,
            fill_factor: 0.8,
            min_fill_factor: 0.4,
            max_delta_chain_length: 16,
            enable_delta_chains: true,
            enable_fractional_cascading: true,
            enable_sibling_prefetch: true,
            prefetch_count: 8,
            background_consolidation: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SageTreeConfig::default();
        assert_eq!(config.page_size, 8192);
        assert_eq!(config.max_branching_factor, 256);
        assert_eq!(config.fill_factor, 0.7);
        assert!(config.enable_delta_chains);
    }

    #[test]
    fn test_builder_pattern() {
        let config = SageTreeConfig::new()
            .with_page_size(16384)
            .with_branching_factor(128)
            .with_fill_factor(0.8)
            .with_delta_chains(false);

        assert_eq!(config.page_size, 16384);
        assert_eq!(config.max_branching_factor, 128);
        assert_eq!(config.fill_factor, 0.8);
        assert!(!config.enable_delta_chains);
    }

    #[test]
    fn test_fill_factor_clamping() {
        let config = SageTreeConfig::new().with_fill_factor(1.5);
        assert_eq!(config.fill_factor, 1.0);

        let config = SageTreeConfig::new().with_fill_factor(0.3);
        assert_eq!(config.fill_factor, 0.5);
    }

    #[test]
    fn test_key_calculations() {
        let config = SageTreeConfig::new()
            .with_branching_factor(100)
            .with_fill_factor(0.7)
            .with_min_fill_factor(0.4);

        assert_eq!(config.target_keys(), 70);
        assert_eq!(config.min_keys(), 40);
        assert_eq!(config.max_internal_keys(), 99);
    }

    #[test]
    fn test_usable_space() {
        let config = SageTreeConfig::default();
        assert!(config.usable_space() > 0);
        assert!(config.usable_space() < config.page_size);
    }

    #[test]
    fn test_preset_configs() {
        let test_config = SageTreeConfig::for_testing();
        assert_eq!(test_config.max_branching_factor, 16);
        assert!(!test_config.enable_fractional_cascading);

        let perf_config = SageTreeConfig::high_performance();
        assert_eq!(perf_config.page_size, 16384);
        assert!(perf_config.enable_fractional_cascading);
    }
}
