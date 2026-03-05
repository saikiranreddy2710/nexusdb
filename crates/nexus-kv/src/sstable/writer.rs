//! SSTable writer: builds an SSTable file from sorted key-value entries.
//!
//! The writer accepts entries in sorted order and produces a complete SSTable
//! file with data blocks, bloom filter, index block, and footer.

use crate::config::CompressionType;
use crate::error::KvResult;
use crate::sstable::block::{
    encode_varint64, BlockBuilder, BlockCompressionType, BlockHandle, BLOCK_TRAILER_SIZE,
};
use crate::sstable::footer::{Footer, FOOTER_SIZE};
use std::io::Write;

/// Options for building an SSTable.
#[derive(Debug, Clone)]
pub struct SSTableWriterOptions {
    /// Target size for data blocks.
    pub block_size: usize,
    /// Number of keys between restart points.
    pub block_restart_interval: usize,
    /// Compression type for data blocks.
    pub compression: CompressionType,
    /// Bits per key for bloom filter (0 to disable).
    pub bloom_bits_per_key: usize,
}

impl Default for SSTableWriterOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            block_restart_interval: 16,
            compression: CompressionType::None,
            bloom_bits_per_key: 10,
        }
    }
}

/// Builds an SSTable file from sorted key-value entries.
///
/// Usage:
/// ```ignore
/// let mut writer = SSTableWriter::new(file, options)?;
/// writer.add(key1, value1)?;
/// writer.add(key2, value2)?;
/// let info = writer.finish()?;
/// ```
pub struct SSTableWriter<W: Write> {
    writer: W,
    options: SSTableWriterOptions,

    /// Current data block being built.
    data_block: BlockBuilder,
    /// Index block: maps last-key-of-block → block handle.
    index_block: BlockBuilder,

    /// All keys added (for bloom filter construction).
    /// We collect keys to build the filter at the end.
    filter_keys: Vec<Vec<u8>>,

    /// Current file offset.
    offset: u64,
    /// Number of entries written.
    entry_count: u64,
    /// Number of data blocks written.
    block_count: u64,

    /// Smallest key (first key added).
    smallest_key: Option<Vec<u8>>,
    /// Largest key (last key added).
    largest_key: Option<Vec<u8>>,
    /// Minimum sequence number seen.
    min_sequence: u64,
    /// Maximum sequence number seen.
    max_sequence: u64,

    /// Handle of the last written data block (for index).
    pending_handle: Option<BlockHandle>,
    /// Last key of the last written data block (for index).
    pending_last_key: Option<Vec<u8>>,

    /// Whether finish() has been called.
    finished: bool,
}

impl<W: Write> SSTableWriter<W> {
    /// Create a new SSTable writer.
    pub fn new(writer: W, options: SSTableWriterOptions) -> Self {
        let restart_interval = options.block_restart_interval;
        Self {
            writer,
            options,
            data_block: BlockBuilder::new(restart_interval),
            index_block: BlockBuilder::new(1), // No prefix compression for index
            filter_keys: Vec::new(),
            offset: 0,
            entry_count: 0,
            block_count: 0,
            smallest_key: None,
            largest_key: None,
            min_sequence: u64::MAX,
            max_sequence: 0,
            pending_handle: None,
            pending_last_key: None,
            finished: false,
        }
    }

    /// Add a key-value pair to the SSTable.
    ///
    /// Keys must be added in sorted order (by InternalKey ordering).
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> KvResult<()> {
        debug_assert!(!self.finished, "cannot add to finished writer");

        // Flush any pending index entry for the previous block
        if let (Some(handle), Some(last_key)) =
            (self.pending_handle.take(), self.pending_last_key.take())
        {
            // Find a short separator between last_key and current key
            let separator = short_separator(&last_key, key);
            let mut handle_buf = Vec::new();
            handle.encode(&mut handle_buf);
            self.index_block.add(&separator, &handle_buf);
        }

        // Track key range
        if self.smallest_key.is_none() {
            self.smallest_key = Some(key.to_vec());
        }
        self.largest_key = Some(key.to_vec());

        // Collect key for bloom filter
        if self.options.bloom_bits_per_key > 0 {
            self.filter_keys.push(key.to_vec());
        }

        // Add to current data block
        self.data_block.add(key, value);
        self.entry_count += 1;

        // Flush the data block if it exceeds the target size
        if self.data_block.estimated_size() >= self.options.block_size {
            self.flush_data_block()?;
        }

        Ok(())
    }

    /// Add a key-value pair with sequence number tracking.
    pub fn add_with_seq(&mut self, key: &[u8], value: &[u8], sequence: u64) -> KvResult<()> {
        self.min_sequence = self.min_sequence.min(sequence);
        self.max_sequence = self.max_sequence.max(sequence);
        self.add(key, value)
    }

    /// Finalize the SSTable and write the footer.
    ///
    /// Returns the total file size in bytes.
    pub fn finish(mut self) -> KvResult<SSTableWriteResult> {
        // Flush any remaining data block
        if !self.data_block.is_empty() {
            self.flush_data_block()?;
        }

        // Flush the last pending index entry
        if let (Some(handle), Some(last_key)) =
            (self.pending_handle.take(), self.pending_last_key.take())
        {
            let successor = short_successor(&last_key);
            let mut handle_buf = Vec::new();
            handle.encode(&mut handle_buf);
            self.index_block.add(&successor, &handle_buf);
        }

        // ── Write filter (bloom filter) block ──────────────────
        let filter_handle = self.write_filter_block()?;

        // ── Write meta-index block ─────────────────────────────
        let meta_index_handle = self.write_meta_index_block(filter_handle)?;

        // ── Write index block ──────────────────────────────────
        // Swap out index_block to avoid partial-move borrow conflict
        let mut temp_index = BlockBuilder::new(1);
        std::mem::swap(&mut self.index_block, &mut temp_index);
        let index_data = temp_index.finish();
        let index_handle = self.write_raw_block(index_data)?;

        // ── Write footer ───────────────────────────────────────
        let footer = Footer::new(meta_index_handle, index_handle);
        let footer_data = footer.encode();
        self.writer.write_all(&footer_data)?;
        self.offset += FOOTER_SIZE as u64;

        self.writer.flush()?;
        self.finished = true;

        Ok(SSTableWriteResult {
            file_size: self.offset,
            entry_count: self.entry_count,
            block_count: self.block_count,
            smallest_key: self.smallest_key.unwrap_or_default(),
            largest_key: self.largest_key.unwrap_or_default(),
            min_sequence: if self.min_sequence == u64::MAX {
                0
            } else {
                self.min_sequence
            },
            max_sequence: self.max_sequence,
        })
    }

    /// Flush the current data block to the file.
    fn flush_data_block(&mut self) -> KvResult<()> {
        let last_key = self.largest_key.as_ref().cloned().unwrap_or_default();

        // Build the block
        let mut new_builder = BlockBuilder::new(self.options.block_restart_interval);
        std::mem::swap(&mut self.data_block, &mut new_builder);
        let block_data = new_builder.finish();

        let handle = self.write_raw_block(block_data)?;
        self.block_count += 1;

        self.pending_handle = Some(handle);
        self.pending_last_key = Some(last_key);

        Ok(())
    }

    /// Write a raw block with trailer (compression type + checksum).
    fn write_raw_block(&mut self, data: Vec<u8>) -> KvResult<BlockHandle> {
        let compression_type = match self.options.compression {
            CompressionType::None => BlockCompressionType::None,
            CompressionType::Lz4 => BlockCompressionType::Lz4,
            CompressionType::Snappy => BlockCompressionType::Snappy,
            CompressionType::Zstd => BlockCompressionType::Zstd,
        };

        // For now, only uncompressed blocks are supported.
        // Compression hooks are in place for future implementation.
        let block_data = &data;

        // Compute CRC32 checksum over block data + compression type byte
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(block_data);
        hasher.update(&[compression_type as u8]);
        let checksum = hasher.finalize();

        let block_offset = self.offset;
        let block_size = block_data.len() as u64;

        // Write: [block_data] [compression_type (1 byte)] [checksum (4 bytes)]
        self.writer.write_all(block_data)?;
        self.writer.write_all(&[compression_type as u8])?;
        self.writer.write_all(&checksum.to_le_bytes())?;

        self.offset += block_size + BLOCK_TRAILER_SIZE as u64;

        Ok(BlockHandle::new(block_offset, block_size))
    }

    /// Write the bloom filter block.
    fn write_filter_block(&mut self) -> KvResult<Option<BlockHandle>> {
        if self.options.bloom_bits_per_key == 0 || self.filter_keys.is_empty() {
            return Ok(None);
        }

        // Build a simple bloom filter
        let num_bits = self.filter_keys.len() * self.options.bloom_bits_per_key;
        let num_bytes = (num_bits + 7) / 8;
        let num_bytes = num_bytes.max(8); // Minimum 8 bytes
        let num_hashes = ((num_bytes as f64 * 8.0 / self.filter_keys.len() as f64)
            * std::f64::consts::LN_2)
            .ceil() as usize;
        let num_hashes = num_hashes.clamp(1, 30);

        let mut filter = vec![0u8; num_bytes];

        // Insert all keys into the filter
        for key in &self.filter_keys {
            let mut h = bloom_hash(key);
            let delta = (h >> 17) | (h << 15);
            for _ in 0..num_hashes {
                let bit_pos = (h as usize) % (num_bytes * 8);
                filter[bit_pos / 8] |= 1 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }

        // Append the number of hashes as the last byte
        filter.push(num_hashes as u8);

        let handle = self.write_raw_block(filter)?;
        Ok(Some(handle))
    }

    /// Write the meta-index block (contains filter block handle, stats, etc.).
    fn write_meta_index_block(
        &mut self,
        filter_handle: Option<BlockHandle>,
    ) -> KvResult<BlockHandle> {
        let mut meta_index = BlockBuilder::new(1);

        if let Some(handle) = filter_handle {
            let mut buf = Vec::new();
            handle.encode(&mut buf);
            meta_index.add(b"filter.bloom", &buf);
        }

        // Add entry count as metadata
        let mut count_buf = Vec::new();
        encode_varint64(&mut count_buf, self.entry_count);
        meta_index.add(b"nexus.entry_count", &count_buf);

        self.write_raw_block(meta_index.finish())
    }
}

/// Result of writing an SSTable.
#[derive(Debug, Clone)]
pub struct SSTableWriteResult {
    /// Total file size in bytes.
    pub file_size: u64,
    /// Number of key-value entries.
    pub entry_count: u64,
    /// Number of data blocks.
    pub block_count: u64,
    /// Smallest key in the table.
    pub smallest_key: Vec<u8>,
    /// Largest key in the table.
    pub largest_key: Vec<u8>,
    /// Minimum sequence number.
    pub min_sequence: u64,
    /// Maximum sequence number.
    pub max_sequence: u64,
}

// ── Helpers ─────────────────────────────────────────────────────

/// Compute a hash for bloom filter insertion.
fn bloom_hash(key: &[u8]) -> u32 {
    // Simple hash function (similar to LevelDB's bloom hash)
    let mut h: u32 = 0;
    for &b in key {
        h = h.wrapping_mul(0x01000193).wrapping_add(b as u32);
    }
    h
}

/// Find a short string that is >= `start` and < `limit`.
/// This compresses the index block by using shorter separator keys.
fn short_separator(start: &[u8], limit: &[u8]) -> Vec<u8> {
    // Find the length of the common prefix
    let min_len = start.len().min(limit.len());
    let mut diff_index = 0;
    while diff_index < min_len && start[diff_index] == limit[diff_index] {
        diff_index += 1;
    }

    if diff_index < min_len {
        let diff_byte = start[diff_index];
        if diff_byte < 0xFF && diff_byte + 1 < limit[diff_index] {
            let mut result = start[..diff_index + 1].to_vec();
            result[diff_index] += 1;
            return result;
        }
    }

    // Can't shorten; return start as-is
    start.to_vec()
}

/// Find a short string that is >= `key`.
/// Used for the last key in the index block.
fn short_successor(key: &[u8]) -> Vec<u8> {
    for (i, &b) in key.iter().enumerate() {
        if b != 0xFF {
            let mut result = key[..i + 1].to_vec();
            result[i] += 1;
            return result;
        }
    }
    // All bytes are 0xFF; return key as-is
    key.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_separator() {
        // Can't shorten when diff_byte+1 == limit_byte (not strictly less)
        assert_eq!(short_separator(b"abc", b"abd"), b"abc");
        assert_eq!(short_separator(b"abc", b"b"), b"abc");
        assert_eq!(short_separator(b"abc", b"abc"), b"abc");
        // Can shorten when there's a gap of 2+ between diff bytes
        // "abc" -> increment 'b' to 'c' and truncate -> "ac"
        assert_eq!(short_separator(b"abc", b"adc"), b"ac");
        assert_eq!(short_separator(b"abc", b"aec"), b"ac");
    }

    #[test]
    fn test_short_successor() {
        assert_eq!(short_successor(b"abc"), b"b");
        assert_eq!(short_successor(b"\xff\xff"), b"\xff\xff");
        assert_eq!(short_successor(b"a\xff"), b"b");
    }

    #[test]
    fn test_write_sstable() {
        let mut buf = Vec::new();
        let options = SSTableWriterOptions {
            block_size: 128,
            block_restart_interval: 4,
            compression: CompressionType::None,
            bloom_bits_per_key: 10,
        };

        let mut writer = SSTableWriter::new(&mut buf, options);

        for i in 0..100u32 {
            let key = format!("{:08}", i);
            let value = format!("value_{}", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let result = writer.finish().unwrap();
        assert_eq!(result.entry_count, 100);
        assert!(result.file_size > 0);
        assert_eq!(result.smallest_key, b"00000000");
        assert_eq!(result.largest_key, b"00000099");
    }

    #[test]
    fn test_bloom_hash() {
        let h1 = bloom_hash(b"hello");
        let h2 = bloom_hash(b"world");
        assert_ne!(h1, h2);

        // Same key should produce same hash
        assert_eq!(bloom_hash(b"test"), bloom_hash(b"test"));
    }
}
