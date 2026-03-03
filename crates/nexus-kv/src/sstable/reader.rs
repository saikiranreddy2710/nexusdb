//! SSTable reader: reads data from an SSTable file.
//!
//! Supports point lookups via bloom filter + index binary search,
//! and range scans via sequential block iteration.

use crate::error::{KvError, KvResult};
use crate::sstable::block::{BlockCompressionType, BlockHandle, BlockReader, BLOCK_TRAILER_SIZE};
use crate::sstable::footer::{Footer, FOOTER_SIZE};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

/// Reads and queries an SSTable file.
pub struct SSTableReader<R: Read + Seek> {
    /// The underlying file reader.
    reader: R,
    /// File path (for error messages).
    path: PathBuf,
    /// Total file size.
    file_size: u64,
    /// The footer (entry point for the file).
    footer: Footer,
    /// Cached index block.
    index_block: BlockReader,
    /// Cached bloom filter data (if present).
    bloom_filter: Option<BloomFilterReader>,
}

/// In-memory bloom filter reader for fast negative lookups.
struct BloomFilterReader {
    data: Vec<u8>,
    num_hashes: usize,
}

impl BloomFilterReader {
    /// Create a bloom filter reader from raw filter data.
    fn new(data: Vec<u8>) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let num_hashes = *data.last()? as usize;
        if num_hashes == 0 {
            return None;
        }
        Some(Self { data, num_hashes })
    }

    /// Check if the filter might contain the given key.
    /// Returns `true` if the key might be present, `false` if definitely absent.
    fn may_contain(&self, key: &[u8]) -> bool {
        let num_bits = (self.data.len() - 1) * 8;
        if num_bits == 0 {
            return true; // Empty filter: assume present
        }

        let mut h = bloom_hash(key);
        let delta = (h >> 17) | (h << 15);

        for _ in 0..self.num_hashes {
            let bit_pos = (h as usize) % num_bits;
            if self.data[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }

        true
    }
}

impl<R: Read + Seek> SSTableReader<R> {
    /// Open an SSTable from a reader.
    pub fn open(mut reader: R, path: PathBuf) -> KvResult<Self> {
        // Get file size
        let file_size = reader.seek(SeekFrom::End(0))?;

        if file_size < FOOTER_SIZE as u64 {
            return Err(KvError::CorruptedTable {
                path: path.clone(),
                reason: format!(
                    "file too small: {} bytes (minimum {})",
                    file_size, FOOTER_SIZE
                ),
            });
        }

        // Read footer
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = vec![0u8; FOOTER_SIZE];
        reader.read_exact(&mut footer_buf)?;
        let footer = Footer::decode(&footer_buf)?;

        // Read and cache the index block
        let index_data = read_block(&mut reader, &footer.index_handle)?;
        let index_block = BlockReader::new(index_data)?;

        // Read bloom filter if present
        let bloom_filter = Self::read_bloom_filter(&mut reader, &footer)?;

        Ok(Self {
            reader,
            path,
            file_size,
            footer,
            index_block,
            bloom_filter,
        })
    }

    /// Read the bloom filter from the meta-index block.
    fn read_bloom_filter(reader: &mut R, footer: &Footer) -> KvResult<Option<BloomFilterReader>> {
        // Read meta-index block
        let meta_data = read_block(reader, &footer.meta_index_handle)?;
        let meta_block = BlockReader::new(meta_data)?;

        // Look for the "filter.bloom" entry
        let mut iter = meta_block.iter();
        iter.next();
        while iter.is_valid() {
            if iter.key() == b"filter.bloom" {
                let (handle, _) = BlockHandle::decode(iter.value())?;
                let filter_data = read_block(reader, &handle)?;
                return Ok(BloomFilterReader::new(filter_data));
            }
            iter.next();
        }

        Ok(None)
    }

    /// Look up a key in the SSTable.
    ///
    /// Returns the value if found, or `None` if not present.
    /// Uses bloom filter for fast rejection, then binary search on the index.
    pub fn get(&mut self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        // Check bloom filter first
        if let Some(ref filter) = self.bloom_filter {
            if !filter.may_contain(key) {
                return Ok(None);
            }
        }

        // Binary search the index block to find the right data block
        let block_handle = self.find_data_block(key)?;

        match block_handle {
            Some(handle) => {
                // Read the data block
                let block_data = read_block(&mut self.reader, &handle)?;
                let block = BlockReader::new(block_data)?;

                // Seek within the block
                let iter = block.seek(key);
                if iter.is_valid() && iter.key() == key {
                    Ok(Some(iter.value().to_vec()))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Find the data block that might contain the given key.
    fn find_data_block(&self, key: &[u8]) -> KvResult<Option<BlockHandle>> {
        let iter = self.index_block.seek(key);
        if !iter.is_valid() {
            return Ok(None);
        }

        // The index entry's value is the encoded block handle
        let (handle, _) = BlockHandle::decode(iter.value())?;
        Ok(Some(handle))
    }

    /// Create an iterator over all entries in the SSTable.
    pub fn iter(&mut self) -> KvResult<SSTableIterator> {
        let mut data_blocks = Vec::new();

        // Iterate over the index to get all data block handles
        let mut idx_iter = self.index_block.iter();
        idx_iter.next();
        while idx_iter.is_valid() {
            let (handle, _) = BlockHandle::decode(idx_iter.value())?;
            let block_data = read_block(&mut self.reader, &handle)?;
            data_blocks.push(BlockReader::new(block_data)?);
            idx_iter.next();
        }

        Ok(SSTableIterator::new(data_blocks))
    }

    /// Create an iterator starting from the first entry >= target.
    pub fn iter_from(&mut self, target: &[u8]) -> KvResult<SSTableIterator> {
        let mut data_blocks = Vec::new();
        let mut found_start = false;

        // Find the starting block and all subsequent blocks
        let mut idx_iter = self.index_block.iter();
        idx_iter.next();
        while idx_iter.is_valid() {
            let idx_key = idx_iter.key().to_vec();
            if !found_start && idx_key.as_slice() >= target {
                found_start = true;
            }

            if found_start {
                let (handle, _) = BlockHandle::decode(idx_iter.value())?;
                let block_data = read_block(&mut self.reader, &handle)?;
                data_blocks.push(BlockReader::new(block_data)?);
            }
            idx_iter.next();
        }

        let mut iter = SSTableIterator::new(data_blocks);
        // Skip entries before the target within the first block
        while iter.is_valid() && iter.key().unwrap() < target {
            iter.next();
        }

        Ok(iter)
    }

    /// Returns the total file size.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Returns the file path.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

/// Iterator over entries in an SSTable.
pub struct SSTableIterator {
    /// Data blocks in order.
    blocks: Vec<BlockReader>,
    /// Index of the current block.
    block_idx: usize,
    /// Current position within the current block.
    current_key: Vec<u8>,
    current_value: Vec<u8>,
    /// Offset within current block for tracking position.
    block_offset: usize,
    /// Whether we're at a valid position.
    valid: bool,
    /// Block iterators - we store the state for each block
    block_entries: Vec<Vec<(Vec<u8>, Vec<u8>)>>,
    entry_idx: usize,
}

impl SSTableIterator {
    fn new(blocks: Vec<BlockReader>) -> Self {
        // Pre-read all entries from all blocks
        let mut block_entries = Vec::with_capacity(blocks.len());
        for block in &blocks {
            let mut entries = Vec::new();
            let mut iter = block.iter();
            iter.next();
            while iter.is_valid() {
                entries.push((iter.key().to_vec(), iter.value().to_vec()));
                iter.next();
            }
            block_entries.push(entries);
        }

        let mut iter = Self {
            blocks,
            block_idx: 0,
            current_key: Vec::new(),
            current_value: Vec::new(),
            block_offset: 0,
            valid: false,
            block_entries,
            entry_idx: 0,
        };

        // Position at the first entry
        iter.advance_to_valid();
        iter
    }

    /// Whether the iterator is at a valid position.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Get the current key.
    pub fn key(&self) -> Option<&[u8]> {
        if self.valid {
            Some(&self.current_key)
        } else {
            None
        }
    }

    /// Get the current value.
    pub fn value(&self) -> Option<&[u8]> {
        if self.valid {
            Some(&self.current_value)
        } else {
            None
        }
    }

    /// Advance to the next entry.
    pub fn next(&mut self) {
        if !self.valid {
            return;
        }
        self.entry_idx += 1;
        self.advance_to_valid();
    }

    /// Find the next valid position across blocks.
    fn advance_to_valid(&mut self) {
        while self.block_idx < self.block_entries.len() {
            let entries = &self.block_entries[self.block_idx];
            if self.entry_idx < entries.len() {
                self.current_key = entries[self.entry_idx].0.clone();
                self.current_value = entries[self.entry_idx].1.clone();
                self.valid = true;
                return;
            }
            // Move to next block
            self.block_idx += 1;
            self.entry_idx = 0;
        }
        self.valid = false;
    }
}

// ── Helper Functions ────────────────────────────────────────────

/// Read a block from the file at the given handle.
fn read_block<R: Read + Seek>(reader: &mut R, handle: &BlockHandle) -> KvResult<Vec<u8>> {
    let total_size = handle.size as usize + BLOCK_TRAILER_SIZE;
    let mut buf = vec![0u8; total_size];

    reader.seek(SeekFrom::Start(handle.offset))?;
    reader.read_exact(&mut buf)?;

    // Verify checksum
    let block_data = &buf[..handle.size as usize];
    let compression_byte = buf[handle.size as usize];
    let stored_checksum = u32::from_le_bytes([
        buf[handle.size as usize + 1],
        buf[handle.size as usize + 2],
        buf[handle.size as usize + 3],
        buf[handle.size as usize + 4],
    ]);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(block_data);
    hasher.update(&[compression_byte]);
    let computed = hasher.finalize();

    if computed != stored_checksum {
        return Err(KvError::ChecksumMismatch {
            expected: stored_checksum,
            actual: computed,
        });
    }

    // Check compression type
    let _compression = BlockCompressionType::from_byte(compression_byte)?;
    // Future: decompress if needed

    Ok(block_data.to_vec())
}

/// Compute a bloom filter hash (must match writer's bloom_hash).
fn bloom_hash(key: &[u8]) -> u32 {
    let mut h: u32 = 0;
    for &b in key {
        h = h.wrapping_mul(0x01000193).wrapping_add(b as u32);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionType;
    use crate::sstable::writer::{SSTableWriter, SSTableWriterOptions};
    use std::io::Cursor;

    fn create_test_sstable(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut buf = Vec::new();
        let options = SSTableWriterOptions {
            block_size: 128,
            block_restart_interval: 4,
            compression: CompressionType::None,
            bloom_bits_per_key: 10,
        };

        let mut writer = SSTableWriter::new(&mut buf, options);
        for (k, v) in entries {
            writer.add(k.as_bytes(), v.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
        buf
    }

    #[test]
    fn test_read_sstable() {
        let entries = vec![
            ("apple", "red"),
            ("banana", "yellow"),
            ("cherry", "red"),
            ("date", "brown"),
        ];
        let data = create_test_sstable(&entries);

        let cursor = Cursor::new(data);
        let mut reader = SSTableReader::open(cursor, PathBuf::from("test.sst")).unwrap();

        // Point lookups
        assert_eq!(reader.get(b"apple").unwrap(), Some(b"red".to_vec()));
        assert_eq!(reader.get(b"banana").unwrap(), Some(b"yellow".to_vec()));
        assert_eq!(reader.get(b"grape").unwrap(), None);
    }

    #[test]
    fn test_sstable_iterator() {
        let entries = vec![("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")];
        let data = create_test_sstable(&entries);

        let cursor = Cursor::new(data);
        let mut reader = SSTableReader::open(cursor, PathBuf::from("test.sst")).unwrap();

        let mut iter = reader.iter().unwrap();
        let mut collected = Vec::new();
        while iter.is_valid() {
            collected.push((
                String::from_utf8_lossy(iter.key().unwrap()).to_string(),
                String::from_utf8_lossy(iter.value().unwrap()).to_string(),
            ));
            iter.next();
        }

        assert_eq!(collected.len(), 5);
        assert_eq!(collected[0], ("a".to_string(), "1".to_string()));
        assert_eq!(collected[4], ("e".to_string(), "5".to_string()));
    }

    #[test]
    fn test_sstable_bloom_filter() {
        let mut entries: Vec<(&str, &str)> = Vec::new();
        let keys: Vec<String> = (0..1000).map(|i| format!("{:08}", i)).collect();
        for k in &keys {
            entries.push((k.as_str(), "value"));
        }
        let data = create_test_sstable(&entries);

        let cursor = Cursor::new(data);
        let mut reader = SSTableReader::open(cursor, PathBuf::from("test.sst")).unwrap();

        // All inserted keys should be found
        for k in &keys {
            assert!(reader.get(k.as_bytes()).unwrap().is_some());
        }

        // Non-existent keys: bloom filter should reject most of them
        let mut false_positives = 0;
        for i in 1000..2000 {
            let k = format!("{:08}", i);
            if reader.get(k.as_bytes()).unwrap().is_some() {
                false_positives += 1;
            }
        }
        // With 10 bits/key, expect ~1% false positive rate
        assert!(
            false_positives < 50,
            "too many false positives: {}",
            false_positives
        );
    }
}
