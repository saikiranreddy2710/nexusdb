//! Compaction: background merging of SSTables to reduce read amplification.
//!
//! The compaction system is responsible for:
//! 1. Flushing immutable memtables to L0 SSTables
//! 2. Merging L0 SSTables into L1 (reducing overlap)
//! 3. Merging Li SSTables into Li+1 (leveled compaction)
//!
//! ## Leveled Compaction Strategy
//!
//! - **L0**: Contains freshly flushed SSTables with potentially overlapping
//!   key ranges. Bounded by file count (not size).
//! - **L1+**: Each level contains non-overlapping SSTables. Size of level i+1
//!   is `level_size_multiplier` times level i.
//! - **Compaction trigger**: When a level exceeds its target size, pick the
//!   SSTable with the most overlap with the next level and merge them.

pub mod leveled;

use crate::sstable::SSTableInfo;

/// Describes a compaction job to be executed.
#[derive(Debug, Clone)]
pub struct CompactionJob {
    /// Unique identifier for this compaction.
    pub id: u64,
    /// Source level (files to read from).
    pub input_level: usize,
    /// Target level (files to write to).
    pub output_level: usize,
    /// SSTable files from the input level.
    pub input_files: Vec<SSTableInfo>,
    /// SSTable files from the output level that overlap with input files.
    pub output_files: Vec<SSTableInfo>,
    /// Whether this is a manual compaction request.
    pub is_manual: bool,
}

impl CompactionJob {
    /// Total size of all input files.
    pub fn input_size(&self) -> u64 {
        self.input_files.iter().map(|f| f.file_size).sum::<u64>()
            + self.output_files.iter().map(|f| f.file_size).sum::<u64>()
    }

    /// Number of input files.
    pub fn input_file_count(&self) -> usize {
        self.input_files.len() + self.output_files.len()
    }
}

/// Trait for compaction strategy implementations.
pub trait CompactionStrategy: Send + Sync {
    /// Pick the next compaction job to execute, if any.
    ///
    /// Returns `None` if no compaction is needed.
    fn pick_compaction(&self, levels: &[LevelInfo]) -> Option<CompactionJob>;

    /// Calculate the score for a level (higher = more urgently needs compaction).
    fn level_score(&self, level: usize, info: &LevelInfo) -> f64;

    /// Returns the maximum number of concurrent compactions allowed.
    fn max_concurrent_compactions(&self) -> usize;

    /// Determine the target file size for a compaction output at the given level.
    fn target_file_size(&self, level: usize) -> u64;
}

/// Information about a single level in the LSM tree.
#[derive(Debug, Clone)]
pub struct LevelInfo {
    /// Level number (0 = memtable flush target).
    pub level: usize,
    /// SSTables at this level.
    pub files: Vec<SSTableInfo>,
    /// Total size of all files at this level.
    pub total_size: u64,
    /// Maximum allowed size for this level.
    pub max_size: u64,
}

impl LevelInfo {
    /// Create a new level info.
    pub fn new(level: usize, max_size: u64) -> Self {
        Self {
            level,
            files: Vec::new(),
            total_size: 0,
            max_size,
        }
    }

    /// Add an SSTable to this level.
    pub fn add_file(&mut self, file: SSTableInfo) {
        self.total_size += file.file_size;
        self.files.push(file);
    }

    /// Remove an SSTable from this level by ID.
    pub fn remove_file(&mut self, file_id: u64) {
        if let Some(pos) = self.files.iter().position(|f| f.id == file_id) {
            self.total_size -= self.files[pos].file_size;
            self.files.remove(pos);
        }
    }

    /// Sort files by smallest key (for non-L0 levels).
    pub fn sort_by_key(&mut self) {
        self.files
            .sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
    }

    /// Find files that overlap with the given key range.
    pub fn overlapping_files(&self, smallest: &[u8], largest: &[u8]) -> Vec<&SSTableInfo> {
        self.files
            .iter()
            .filter(|f| f.overlaps(smallest, largest))
            .collect()
    }

    /// Number of files at this level.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Whether this level is empty.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}
