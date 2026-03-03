//! Leveled compaction strategy.
//!
//! Based on the LevelDB/RocksDB leveled compaction approach:
//! - L0 has overlapping key ranges (sorted by recency)
//! - L1+ has non-overlapping key ranges (sorted by key)
//! - Each level's target size is `multiplier * previous_level_size`
//! - Compaction picks the level with the highest score and merges
//!   overlapping files into the next level

use super::{CompactionJob, CompactionStrategy, LevelInfo};
use crate::config::LsmConfig;
use crate::sstable::SSTableInfo;
use std::sync::atomic::{AtomicU64, Ordering};

/// Leveled compaction strategy implementation.
pub struct LeveledCompaction {
    /// Configuration parameters.
    config: LsmConfig,
    /// Next compaction job ID.
    next_job_id: AtomicU64,
}

impl LeveledCompaction {
    /// Create a new leveled compaction strategy.
    pub fn new(config: LsmConfig) -> Self {
        Self {
            config,
            next_job_id: AtomicU64::new(1),
        }
    }

    /// Calculate the score for L0 (based on file count).
    fn l0_score(&self, level: &LevelInfo) -> f64 {
        level.file_count() as f64 / self.config.l0_compaction_trigger as f64
    }

    /// Calculate the score for L1+ (based on total size).
    fn level_n_score(&self, level: &LevelInfo) -> f64 {
        if level.max_size == 0 {
            return 0.0;
        }
        level.total_size as f64 / level.max_size as f64
    }

    /// Find the SSTable in the input level with the greatest overlap
    /// with the output level. This minimizes write amplification.
    fn pick_compaction_file(
        &self,
        input_level: &LevelInfo,
        output_level: &LevelInfo,
    ) -> Option<usize> {
        if input_level.files.is_empty() {
            return None;
        }

        if input_level.level == 0 {
            // For L0, pick the oldest file (to reduce L0 overlap quickly)
            return Some(0);
        }

        // For L1+, pick the file with the most overlap with the next level
        let mut best_idx = 0;
        let mut best_overlap = 0u64;

        for (i, file) in input_level.files.iter().enumerate() {
            let overlap: u64 = output_level
                .overlapping_files(&file.smallest_key, &file.largest_key)
                .iter()
                .map(|f| f.file_size)
                .sum();

            // Prefer files with less overlap (less work)
            // But if all have similar overlap, round-robin via file index
            if i == 0 || overlap < best_overlap {
                best_idx = i;
                best_overlap = overlap;
            }
        }

        Some(best_idx)
    }

    /// For L0 compaction: find all L0 files that overlap with the picked file's range,
    /// then find all L1 files that overlap with the combined range.
    fn expand_l0_inputs(
        &self,
        levels: &[LevelInfo],
        picked_file: &SSTableInfo,
    ) -> (Vec<SSTableInfo>, Vec<SSTableInfo>) {
        let l0 = &levels[0];
        let l1 = &levels[1];

        // Start with the picked file's range
        let mut smallest = picked_file.smallest_key.clone();
        let mut largest = picked_file.largest_key.clone();

        // Expand to include all L0 files that overlap with this range
        // (L0 files can overlap each other)
        let mut input_files: Vec<SSTableInfo> = Vec::new();
        let mut changed = true;

        while changed {
            changed = false;
            for file in &l0.files {
                if file.overlaps(&smallest, &largest)
                    && !input_files.iter().any(|f: &SSTableInfo| f.id == file.id)
                {
                    // Expand the range
                    if file.smallest_key < smallest {
                        smallest = file.smallest_key.clone();
                        changed = true;
                    }
                    if file.largest_key > largest {
                        largest = file.largest_key.clone();
                        changed = true;
                    }
                    input_files.push(file.clone());
                }
            }
        }

        // Find all L1 files that overlap with the expanded range
        let output_files: Vec<SSTableInfo> = l1
            .overlapping_files(&smallest, &largest)
            .into_iter()
            .cloned()
            .collect();

        (input_files, output_files)
    }

    fn allocate_job_id(&self) -> u64 {
        self.next_job_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl CompactionStrategy for LeveledCompaction {
    fn pick_compaction(&self, levels: &[LevelInfo]) -> Option<CompactionJob> {
        if levels.is_empty() {
            return None;
        }

        // Find the level with the highest compaction score
        let mut best_level = None;
        let mut best_score = 1.0; // Only compact if score > 1.0

        for (i, level) in levels.iter().enumerate() {
            if i >= self.config.max_levels - 1 {
                break; // Can't compact the last level
            }
            let score = self.level_score(i, level);
            if score > best_score {
                best_level = Some(i);
                best_score = score;
            }
        }

        let input_level_idx = best_level?;

        if input_level_idx == 0 {
            // L0 → L1 compaction
            if levels.len() < 2 {
                return None;
            }

            let picked = self.pick_compaction_file(&levels[0], &levels[1])?;
            let picked_file = &levels[0].files[picked];

            let (input_files, output_files) = self.expand_l0_inputs(levels, picked_file);

            if input_files.is_empty() {
                return None;
            }

            Some(CompactionJob {
                id: self.allocate_job_id(),
                input_level: 0,
                output_level: 1,
                input_files,
                output_files,
                is_manual: false,
            })
        } else {
            // Li → Li+1 compaction
            let output_level_idx = input_level_idx + 1;
            if output_level_idx >= levels.len() {
                return None;
            }

            let picked =
                self.pick_compaction_file(&levels[input_level_idx], &levels[output_level_idx])?;
            let picked_file = levels[input_level_idx].files[picked].clone();

            // Find overlapping files in the output level
            let output_files: Vec<SSTableInfo> = levels[output_level_idx]
                .overlapping_files(&picked_file.smallest_key, &picked_file.largest_key)
                .into_iter()
                .cloned()
                .collect();

            Some(CompactionJob {
                id: self.allocate_job_id(),
                input_level: input_level_idx,
                output_level: output_level_idx,
                input_files: vec![picked_file],
                output_files,
                is_manual: false,
            })
        }
    }

    fn level_score(&self, level: usize, info: &LevelInfo) -> f64 {
        if level == 0 {
            self.l0_score(info)
        } else {
            self.level_n_score(info)
        }
    }

    fn max_concurrent_compactions(&self) -> usize {
        self.config.max_background_compactions
    }

    fn target_file_size(&self, _level: usize) -> u64 {
        self.config.target_file_size as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_file(id: u64, smallest: &str, largest: &str, size: u64) -> SSTableInfo {
        SSTableInfo {
            id,
            level: 0,
            smallest_key: smallest.as_bytes().to_vec(),
            largest_key: largest.as_bytes().to_vec(),
            file_size: size,
            entry_count: 100,
            min_sequence: 1,
            max_sequence: 100,
            data_block_count: 10,
        }
    }

    #[test]
    fn test_no_compaction_when_empty() {
        let config = LsmConfig::for_testing(PathBuf::from("/tmp/test"));
        let strategy = LeveledCompaction::new(config.clone());

        let levels = vec![
            LevelInfo::new(0, u64::MAX),
            LevelInfo::new(1, config.l1_max_bytes),
        ];

        assert!(strategy.pick_compaction(&levels).is_none());
    }

    #[test]
    fn test_l0_compaction_trigger() {
        let config = LsmConfig::for_testing(PathBuf::from("/tmp/test"));
        let strategy = LeveledCompaction::new(config.clone());

        let mut l0 = LevelInfo::new(0, u64::MAX);
        // Add enough L0 files to trigger compaction
        for i in 0..config.l0_compaction_trigger + 1 {
            l0.add_file(make_file(
                i as u64,
                &format!("{:04}", i * 10),
                &format!("{:04}", i * 10 + 9),
                1024,
            ));
        }

        let levels = vec![l0, LevelInfo::new(1, config.l1_max_bytes)];

        let job = strategy.pick_compaction(&levels);
        assert!(job.is_some());
        let job = job.unwrap();
        assert_eq!(job.input_level, 0);
        assert_eq!(job.output_level, 1);
    }

    #[test]
    fn test_level_score() {
        let config = LsmConfig::for_testing(PathBuf::from("/tmp/test"));
        let strategy = LeveledCompaction::new(config.clone());

        // L0 score based on file count
        let mut l0 = LevelInfo::new(0, u64::MAX);
        l0.add_file(make_file(1, "a", "z", 1024));
        assert!(strategy.level_score(0, &l0) < 1.0);

        // L1 score based on size
        let mut l1 = LevelInfo::new(1, 1000);
        l1.add_file(make_file(2, "a", "z", 2000));
        assert!(strategy.level_score(1, &l1) > 1.0);
    }
}
