//! Bloom Filter implementation for probabilistic set membership.
//!
//! A Bloom filter is a space-efficient probabilistic data structure that
//! can tell you with certainty that an element is NOT in a set, but may
//! have false positives when checking if an element IS in a set.
//!
//! This is useful for:
//! - Avoiding unnecessary disk reads for non-existent keys
//! - Reducing network requests in distributed systems
//! - Quick negative lookups before expensive operations

use std::hash::{Hash, Hasher};

use siphasher::sip::SipHasher13;

/// A Bloom filter for probabilistic set membership testing.
///
/// # Example
///
/// ```
/// use nexus_cache::bloom::BloomFilter;
///
/// let mut filter = BloomFilter::with_rate(1000, 0.01); // 1% false positive rate
/// filter.insert(&"hello");
/// assert!(filter.contains(&"hello")); // Definitely true
/// assert!(!filter.contains(&"world")); // Definitely not present
/// ```
pub struct BloomFilter {
    /// Bit array.
    bits: Vec<u64>,
    /// Number of bits.
    num_bits: usize,
    /// Number of hash functions.
    num_hashes: usize,
    /// Number of items inserted.
    count: usize,
    /// Hash seeds for k hash functions.
    seeds: Vec<(u64, u64)>,
}

impl BloomFilter {
    /// Creates a new Bloom filter with the given number of bits and hash functions.
    pub fn new(num_bits: usize, num_hashes: usize) -> Self {
        let num_bits = num_bits.max(64);
        let num_hashes = num_hashes.max(1);
        let num_words = num_bits.div_ceil(64);

        // Generate random seeds for hash functions
        let seeds: Vec<(u64, u64)> = (0..num_hashes)
            .map(|i| {
                // Use deterministic seeds based on index for reproducibility
                let a = 0x517cc1b727220a95u64.wrapping_add(i as u64);
                let b = 0x0fc94dc6e6eb8a5fu64.wrapping_add(i as u64 * 2);
                (a, b)
            })
            .collect();

        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
            count: 0,
            seeds,
        }
    }

    /// Creates a Bloom filter optimized for the expected number of items
    /// and desired false positive rate.
    ///
    /// # Arguments
    ///
    /// * `expected_items` - Expected number of items to insert
    /// * `false_positive_rate` - Desired false positive rate (e.g., 0.01 for 1%)
    pub fn with_rate(expected_items: usize, false_positive_rate: f64) -> Self {
        let expected_items = expected_items.max(1);
        let fp_rate = false_positive_rate.clamp(0.0001, 0.5);

        // Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let m = -((expected_items as f64) * fp_rate.ln() / (2.0_f64.ln().powi(2)));
        let num_bits = (m.ceil() as usize).max(64);

        // Optimal number of hash functions: k = (m/n) * ln(2)
        let k = ((num_bits as f64 / expected_items as f64) * 2.0_f64.ln()).round() as usize;
        let num_hashes = k.clamp(1, 30);

        Self::new(num_bits, num_hashes)
    }

    /// Inserts an item into the filter.
    pub fn insert<T: Hash>(&mut self, item: &T) {
        // Collect bit indices first to avoid borrow conflicts
        let bit_indices: Vec<usize> = self
            .seeds
            .iter()
            .map(|(seed_a, seed_b)| self.hash_item(item, *seed_a, *seed_b))
            .collect();

        for bit_index in bit_indices {
            self.set_bit(bit_index);
        }
        self.count += 1;
    }

    /// Checks if an item might be in the filter.
    ///
    /// Returns `true` if the item might be in the set (could be false positive).
    /// Returns `false` if the item is definitely not in the set.
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        for (seed_a, seed_b) in &self.seeds {
            let bit_index = self.hash_item(item, *seed_a, *seed_b);
            if !self.get_bit(bit_index) {
                return false;
            }
        }
        true
    }

    /// Checks if an item is definitely not in the filter.
    ///
    /// This is the inverse of `contains()` - returns `true` if the item
    /// is definitely not present, `false` if it might be present.
    pub fn definitely_not_contains<T: Hash>(&self, item: &T) -> bool {
        !self.contains(item)
    }

    /// Returns the number of items inserted.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Returns the number of bits in the filter.
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Returns the number of hash functions.
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Returns the approximate false positive rate based on current fill.
    pub fn estimated_false_positive_rate(&self) -> f64 {
        let ones = self.count_ones();
        let fill_ratio = ones as f64 / self.num_bits as f64;
        fill_ratio.powi(self.num_hashes as i32)
    }

    /// Returns the fill ratio (percentage of bits set to 1).
    pub fn fill_ratio(&self) -> f64 {
        self.count_ones() as f64 / self.num_bits as f64
    }

    /// Clears the filter.
    pub fn clear(&mut self) {
        self.bits.fill(0);
        self.count = 0;
    }

    /// Merges another Bloom filter into this one.
    ///
    /// Both filters must have the same size and number of hash functions.
    pub fn merge(&mut self, other: &BloomFilter) -> Result<(), &'static str> {
        if self.num_bits != other.num_bits || self.num_hashes != other.num_hashes {
            return Err("Bloom filters must have same parameters to merge");
        }

        for (i, word) in other.bits.iter().enumerate() {
            self.bits[i] |= word;
        }
        self.count += other.count;
        Ok(())
    }

    // Private helper methods

    fn hash_item<T: Hash>(&self, item: &T, seed_a: u64, seed_b: u64) -> usize {
        let mut hasher = SipHasher13::new_with_keys(seed_a, seed_b);
        item.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_bits
    }

    fn set_bit(&mut self, index: usize) {
        let word_index = index / 64;
        let bit_index = index % 64;
        self.bits[word_index] |= 1u64 << bit_index;
    }

    fn get_bit(&self, index: usize) -> bool {
        let word_index = index / 64;
        let bit_index = index % 64;
        (self.bits[word_index] >> bit_index) & 1 == 1
    }

    fn count_ones(&self) -> usize {
        self.bits.iter().map(|w| w.count_ones() as usize).sum()
    }
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter")
            .field("num_bits", &self.num_bits)
            .field("num_hashes", &self.num_hashes)
            .field("count", &self.count)
            .field("fill_ratio", &format!("{:.2}%", self.fill_ratio() * 100.0))
            .finish()
    }
}

/// A counting Bloom filter that supports deletion.
///
/// Instead of single bits, uses counters so items can be removed.
/// Uses more memory than a standard Bloom filter.
pub struct CountingBloomFilter {
    /// Counter array.
    counters: Vec<u8>,
    /// Number of counters.
    num_counters: usize,
    /// Number of hash functions.
    num_hashes: usize,
    /// Number of items inserted.
    count: usize,
    /// Hash seeds.
    seeds: Vec<(u64, u64)>,
}

impl CountingBloomFilter {
    /// Creates a new counting Bloom filter.
    pub fn new(num_counters: usize, num_hashes: usize) -> Self {
        let num_counters = num_counters.max(1);
        let num_hashes = num_hashes.max(1);

        let seeds: Vec<(u64, u64)> = (0..num_hashes)
            .map(|i| {
                let a = 0x517cc1b727220a95u64.wrapping_add(i as u64);
                let b = 0x0fc94dc6e6eb8a5fu64.wrapping_add(i as u64 * 2);
                (a, b)
            })
            .collect();

        Self {
            counters: vec![0u8; num_counters],
            num_counters,
            num_hashes,
            count: 0,
            seeds,
        }
    }

    /// Creates a counting Bloom filter with optimized parameters.
    pub fn with_rate(expected_items: usize, false_positive_rate: f64) -> Self {
        let expected_items = expected_items.max(1);
        let fp_rate = false_positive_rate.clamp(0.0001, 0.5);

        let m = -((expected_items as f64) * fp_rate.ln() / (2.0_f64.ln().powi(2)));
        let num_counters = (m.ceil() as usize).max(1);

        let k = ((num_counters as f64 / expected_items as f64) * 2.0_f64.ln()).round() as usize;
        let num_hashes = k.clamp(1, 30);

        Self::new(num_counters, num_hashes)
    }

    /// Inserts an item into the filter.
    pub fn insert<T: Hash>(&mut self, item: &T) {
        for (seed_a, seed_b) in &self.seeds {
            let index = self.hash_item(item, *seed_a, *seed_b);
            self.counters[index] = self.counters[index].saturating_add(1);
        }
        self.count += 1;
    }

    /// Removes an item from the filter.
    ///
    /// Note: Removing an item that wasn't inserted can cause false negatives.
    pub fn remove<T: Hash>(&mut self, item: &T) {
        if self.contains(item) {
            for (seed_a, seed_b) in &self.seeds {
                let index = self.hash_item(item, *seed_a, *seed_b);
                self.counters[index] = self.counters[index].saturating_sub(1);
            }
            self.count = self.count.saturating_sub(1);
        }
    }

    /// Checks if an item might be in the filter.
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        for (seed_a, seed_b) in &self.seeds {
            let index = self.hash_item(item, *seed_a, *seed_b);
            if self.counters[index] == 0 {
                return false;
            }
        }
        true
    }

    /// Returns the number of items inserted.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Returns the number of counters.
    pub fn num_counters(&self) -> usize {
        self.num_counters
    }

    /// Returns the number of hash functions.
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Clears the filter.
    pub fn clear(&mut self) {
        self.counters.fill(0);
        self.count = 0;
    }

    fn hash_item<T: Hash>(&self, item: &T, seed_a: u64, seed_b: u64) -> usize {
        let mut hasher = SipHasher13::new_with_keys(seed_a, seed_b);
        item.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_counters
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut filter = BloomFilter::new(1000, 5);

        filter.insert(&"hello");
        filter.insert(&"world");

        assert!(filter.contains(&"hello"));
        assert!(filter.contains(&"world"));
        assert_eq!(filter.count(), 2);
    }

    #[test]
    fn test_negative_lookup() {
        let mut filter = BloomFilter::new(10000, 7);

        for i in 0..100 {
            filter.insert(&format!("key_{}", i));
        }

        // These should definitely not be in the filter
        // (with very high probability given our parameters)
        let mut false_positives = 0;
        for i in 1000..1100 {
            if filter.contains(&format!("key_{}", i)) {
                false_positives += 1;
            }
        }

        // Should have very few false positives
        assert!(
            false_positives < 10,
            "Too many false positives: {}",
            false_positives
        );
    }

    #[test]
    fn test_with_rate() {
        let filter = BloomFilter::with_rate(1000, 0.01);

        // Should have reasonable parameters
        assert!(filter.num_bits() >= 1000);
        assert!(filter.num_hashes() >= 1);
    }

    #[test]
    fn test_clear() {
        let mut filter = BloomFilter::new(1000, 5);

        filter.insert(&"hello");
        assert!(filter.contains(&"hello"));

        filter.clear();
        assert!(!filter.contains(&"hello"));
        assert_eq!(filter.count(), 0);
    }

    #[test]
    fn test_merge() {
        let mut filter1 = BloomFilter::new(1000, 5);
        let mut filter2 = BloomFilter::new(1000, 5);

        filter1.insert(&"a");
        filter2.insert(&"b");

        filter1.merge(&filter2).unwrap();

        assert!(filter1.contains(&"a"));
        assert!(filter1.contains(&"b"));
    }

    #[test]
    fn test_counting_bloom_filter() {
        let mut filter = CountingBloomFilter::new(1000, 5);

        filter.insert(&"hello");
        assert!(filter.contains(&"hello"));

        filter.remove(&"hello");
        assert!(!filter.contains(&"hello"));
    }

    #[test]
    fn test_fill_ratio() {
        let mut filter = BloomFilter::new(1000, 5);

        assert_eq!(filter.fill_ratio(), 0.0);

        for i in 0..100 {
            filter.insert(&i);
        }

        assert!(filter.fill_ratio() > 0.0);
        assert!(filter.fill_ratio() < 1.0);
    }
}
