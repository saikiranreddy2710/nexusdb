//! Predictive prefetching via Markov chain access pattern modeling.
//!
//! Tracks page/block access sequences and predicts which pages will be
//! accessed next based on historical transition probabilities.
//!
//! ## Model
//!
//! State: current page ID
//! Transition: P(next_page | current_page) estimated from observed accesses
//!
//! When page P is accessed, the model predicts the most likely next pages
//! and signals the prefetch layer to load them proactively.

use std::collections::HashMap;

/// Tracks access patterns and predicts future page accesses.
///
/// Uses a first-order Markov chain: given the current page, predicts
/// the next page based on historical transition counts.
pub struct MarkovPrefetcher {
    /// Transition counts: from_page -> { to_page -> count }.
    transitions: HashMap<u64, HashMap<u64, u32>>,
    /// Last accessed page (for tracking transitions).
    last_page: Option<u64>,
    /// Minimum transition count to consider a prediction valid.
    min_confidence: u32,
    /// Maximum number of predictions to return.
    max_predictions: usize,
    /// Total transitions recorded.
    total_transitions: u64,
}

impl MarkovPrefetcher {
    /// Create a new Markov-based prefetcher.
    ///
    /// - `min_confidence`: minimum transition count to predict (e.g., 2)
    /// - `max_predictions`: max pages to prefetch per access (e.g., 3)
    pub fn new(min_confidence: u32, max_predictions: usize) -> Self {
        Self {
            transitions: HashMap::new(),
            last_page: None,
            min_confidence: min_confidence.max(1),
            max_predictions: max_predictions.max(1),
            total_transitions: 0,
        }
    }

    /// Record a page access. Returns predicted next pages to prefetch.
    ///
    /// The caller should issue prefetch I/O for the returned page IDs.
    pub fn record_access(&mut self, page_id: u64) -> Vec<u64> {
        // Record transition from last page to current page
        if let Some(last) = self.last_page {
            let entry = self.transitions.entry(last).or_default();
            *entry.entry(page_id).or_insert(0) += 1;
            self.total_transitions += 1;
        }
        self.last_page = Some(page_id);

        // Predict next pages
        self.predict(page_id)
    }

    /// Predict the most likely next pages given the current page.
    pub fn predict(&self, current_page: u64) -> Vec<u64> {
        let Some(next_counts) = self.transitions.get(&current_page) else {
            return Vec::new();
        };

        // Sort by count (descending) and filter by confidence
        let mut candidates: Vec<(u64, u32)> = next_counts
            .iter()
            .filter(|(_, &count)| count >= self.min_confidence)
            .map(|(&page, &count)| (page, count))
            .collect();

        candidates.sort_by(|a, b| b.1.cmp(&a.1));
        candidates.truncate(self.max_predictions);
        candidates.into_iter().map(|(page, _)| page).collect()
    }

    /// Total number of unique pages seen.
    pub fn unique_pages(&self) -> usize {
        self.transitions.len()
    }

    /// Total transitions recorded.
    pub fn total_transitions(&self) -> u64 {
        self.total_transitions
    }

    /// Get the transition probability from one page to another.
    pub fn transition_probability(&self, from: u64, to: u64) -> f64 {
        let Some(next_counts) = self.transitions.get(&from) else {
            return 0.0;
        };
        let total: u32 = next_counts.values().sum();
        if total == 0 {
            return 0.0;
        }
        let count = next_counts.get(&to).copied().unwrap_or(0);
        count as f64 / total as f64
    }

    /// Reset all learned patterns.
    pub fn reset(&mut self) {
        self.transitions.clear();
        self.last_page = None;
        self.total_transitions = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_markov_basic() {
        let mut prefetcher = MarkovPrefetcher::new(2, 3);

        // Train: repeated pattern 1 → 2 → 3 → 1
        for _ in 0..5 {
            prefetcher.record_access(1);
            prefetcher.record_access(2);
            prefetcher.record_access(3);
        }

        // After page 1, should predict page 2
        let predictions = prefetcher.predict(1);
        assert!(!predictions.is_empty());
        assert_eq!(predictions[0], 2);

        // After page 2, should predict page 3
        let predictions = prefetcher.predict(2);
        assert_eq!(predictions[0], 3);
    }

    #[test]
    fn test_markov_confidence_filter() {
        let mut prefetcher = MarkovPrefetcher::new(3, 5);

        // Only 2 transitions 1→2 (below confidence threshold of 3)
        prefetcher.record_access(1);
        prefetcher.record_access(2);
        prefetcher.record_access(1);
        prefetcher.record_access(2);

        let predictions = prefetcher.predict(1);
        assert!(predictions.is_empty(), "should be filtered by confidence");
    }

    #[test]
    fn test_markov_transition_probability() {
        let mut prefetcher = MarkovPrefetcher::new(1, 5);

        // 3 times 1→2, 1 time 1→3
        prefetcher.record_access(1);
        prefetcher.record_access(2);
        prefetcher.record_access(1);
        prefetcher.record_access(2);
        prefetcher.record_access(1);
        prefetcher.record_access(2);
        prefetcher.record_access(1);
        prefetcher.record_access(3);

        assert!((prefetcher.transition_probability(1, 2) - 0.75).abs() < 0.01);
        assert!((prefetcher.transition_probability(1, 3) - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_markov_reset() {
        let mut prefetcher = MarkovPrefetcher::new(1, 3);

        prefetcher.record_access(1);
        prefetcher.record_access(2);
        assert!(prefetcher.total_transitions() > 0);

        prefetcher.reset();
        assert_eq!(prefetcher.total_transitions(), 0);
        assert_eq!(prefetcher.unique_pages(), 0);
    }

    #[test]
    fn test_markov_max_predictions() {
        let mut prefetcher = MarkovPrefetcher::new(1, 2);

        // Train many transitions from page 1
        for target in 1..=10 {
            for _ in 0..5 {
                prefetcher.record_access(0);
                prefetcher.record_access(target);
            }
        }

        let predictions = prefetcher.predict(0);
        assert!(predictions.len() <= 2, "should respect max_predictions");
    }
}
