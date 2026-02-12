//! Data block format for SSTables.
//!
//! Each block contains a sequence of key-value entries with optional prefix
//! compression, followed by restart points for binary search within the block.
//!
//! ## Block Layout
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │ Entry 0: [shared|unshared|vlen|key|value]   │
//! │ Entry 1: [shared|unshared|vlen|key|value]   │
//! │ ...                                          │
//! │ Entry N: [shared|unshared|vlen|key|value]   │
//! ├─────────────────────────────────────────────┤
//! │ Restart[0]: offset of entry 0  (4 bytes)    │
//! │ Restart[1]: offset of entry R  (4 bytes)    │
//! │ ...                                          │
//! │ Restart[M]: offset of entry M*R (4 bytes)   │
//! ├─────────────────────────────────────────────┤
//! │ num_restarts (4 bytes, little-endian)        │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! ## Entry Format
//!
//! Each entry uses prefix compression relative to the previous key:
//! - `shared_key_len` (varint): bytes shared with previous key
//! - `unshared_key_len` (varint): bytes unique to this key
//! - `value_len` (varint): length of the value
//! - `unshared_key_data`: the unique portion of the key
//! - `value_data`: the value bytes
//!
//! At restart points, `shared_key_len` is always 0 (full key stored).

use crate::error::{KvError, KvResult};

/// Maximum size of a single block (before compression).
pub const MAX_BLOCK_SIZE: usize = 256 * 1024; // 256 KB

// ── Varint Encoding ─────────────────────────────────────────────

/// Encode a u32 as a variable-length integer.
/// Returns the number of bytes written.
pub fn encode_varint32(buf: &mut Vec<u8>, mut value: u32) -> usize {
    let start = buf.len();
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
    buf.len() - start
}

/// Decode a variable-length u32 from the given slice.
/// Returns `(value, bytes_consumed)`.
pub fn decode_varint32(data: &[u8]) -> KvResult<(u32, usize)> {
    let mut value: u32 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in data.iter().enumerate() {
        if shift > 28 {
            return Err(KvError::Deserialization("varint32 overflow".into()));
        }
        // On the 5th byte (shift=28), only 4 bits are valid
        if shift == 28 && (byte & 0x70) != 0 {
            return Err(KvError::Deserialization("varint32 overflow".into()));
        }
        value |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
    }
    Err(KvError::Deserialization("truncated varint32".into()))
}

/// Encode a u64 as a variable-length integer.
pub fn encode_varint64(buf: &mut Vec<u8>, mut value: u64) -> usize {
    let start = buf.len();
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
    buf.len() - start
}

/// Decode a variable-length u64 from the given slice.
pub fn decode_varint64(data: &[u8]) -> KvResult<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift: u64 = 0;
    for (i, &byte) in data.iter().enumerate() {
        if shift > 63 {
            return Err(KvError::Deserialization("varint64 overflow".into()));
        }
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
    }
    Err(KvError::Deserialization("truncated varint64".into()))
}

// ── Block Builder ───────────────────────────────────────────────

/// Builds a data block by accepting key-value entries in sorted order.
///
/// Keys must be added in ascending order. Prefix compression is applied
/// relative to the previous key, with full keys stored at restart points.
pub struct BlockBuilder {
    /// Accumulated block data.
    buffer: Vec<u8>,
    /// Offsets of restart points within the buffer.
    restarts: Vec<u32>,
    /// Number of entries since the last restart point.
    counter: usize,
    /// Number of entries between restart points.
    restart_interval: usize,
    /// The last key that was added (for prefix compression).
    last_key: Vec<u8>,
    /// Number of entries in this block.
    entry_count: usize,
    /// Whether the block has been finalized.
    finished: bool,
}

impl BlockBuilder {
    /// Create a new block builder.
    ///
    /// `restart_interval` controls how many entries between full-key restart points.
    /// A value of 16 is typical (good balance between compression and seek speed).
    pub fn new(restart_interval: usize) -> Self {
        let restart_interval = restart_interval.max(1);
        Self {
            buffer: Vec::with_capacity(4096),
            restarts: vec![0], // First entry is always a restart
            counter: 0,
            restart_interval,
            last_key: Vec::new(),
            entry_count: 0,
            finished: false,
        }
    }

    /// Add a key-value pair to the block.
    ///
    /// Keys must be added in ascending order. This is not checked at runtime
    /// for performance; violation will produce a corrupted block.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        debug_assert!(!self.finished, "cannot add to a finished block");
        debug_assert!(
            self.entry_count == 0 || key >= self.last_key.as_slice(),
            "keys must be added in order"
        );

        let shared = if self.counter < self.restart_interval {
            // Compute shared prefix length with previous key
            shared_prefix_len(&self.last_key, key)
        } else {
            // Restart point: store full key
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
            0
        };

        let unshared = key.len() - shared;

        // Encode: shared_len | unshared_len | value_len | key_delta | value
        encode_varint32(&mut self.buffer, shared as u32);
        encode_varint32(&mut self.buffer, unshared as u32);
        encode_varint32(&mut self.buffer, value.len() as u32);
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);

        // Update state
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
        self.entry_count += 1;
    }

    /// Finalize the block and return the raw block data.
    ///
    /// Appends the restart array and restart count to the buffer.
    pub fn finish(mut self) -> Vec<u8> {
        // Append restart offsets
        for &restart in &self.restarts {
            self.buffer.extend_from_slice(&restart.to_le_bytes());
        }
        // Append number of restarts
        self.buffer
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.finished = true;
        self.buffer
    }

    /// Current estimated size of the block (including restart array).
    pub fn estimated_size(&self) -> usize {
        self.buffer.len() + (self.restarts.len() + 1) * 4
    }

    /// Number of entries added so far.
    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    /// Returns true if no entries have been added.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Reset the builder for reuse.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
        self.entry_count = 0;
        self.finished = false;
    }
}

// ── Block Reader ────────────────────────────────────────────────

/// A handle to a block in an SSTable file.
#[derive(Debug, Clone, Copy)]
pub struct BlockHandle {
    /// Byte offset of the block in the file.
    pub offset: u64,
    /// Size of the block in bytes (including trailer).
    pub size: u64,
}

impl BlockHandle {
    /// Create a new block handle.
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    /// Encode the block handle to bytes.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint64(buf, self.offset);
        encode_varint64(buf, self.size);
    }

    /// Decode a block handle from bytes.
    pub fn decode(data: &[u8]) -> KvResult<(Self, usize)> {
        let (offset, n1) = decode_varint64(data)?;
        let (size, n2) = decode_varint64(&data[n1..])?;
        Ok((Self { offset, size }, n1 + n2))
    }

    /// Maximum encoded size of a block handle (two 10-byte varints).
    pub const MAX_ENCODED_SIZE: usize = 20;
}

/// Reads entries from a finalized block.
///
/// Supports sequential iteration and binary search via restart points.
pub struct BlockReader {
    /// Raw block data (entries + restarts + num_restarts).
    data: Vec<u8>,
    /// Number of restart points.
    num_restarts: usize,
    /// Offset where the restart array begins.
    restarts_offset: usize,
}

impl BlockReader {
    /// Create a block reader from raw block data.
    pub fn new(data: Vec<u8>) -> KvResult<Self> {
        if data.len() < 4 {
            return Err(KvError::CorruptedBlock {
                offset: 0,
                reason: "block too small".into(),
            });
        }

        // Read num_restarts from the last 4 bytes
        let len = data.len();
        let num_restarts = u32::from_le_bytes([
            data[len - 4],
            data[len - 3],
            data[len - 2],
            data[len - 1],
        ]) as usize;

        if num_restarts == 0 {
            return Err(KvError::CorruptedBlock {
                offset: 0,
                reason: "zero restart points".into(),
            });
        }

        // Check for overflow before subtraction to prevent panic
        let restart_array_bytes = num_restarts.checked_mul(4).ok_or_else(|| {
            KvError::CorruptedBlock {
                offset: 0,
                reason: "restart array size overflow".into(),
            }
        })?;
        let required = restart_array_bytes.checked_add(4).ok_or_else(|| {
            KvError::CorruptedBlock {
                offset: 0,
                reason: "restart array size overflow".into(),
            }
        })?;
        if required > len {
            return Err(KvError::CorruptedBlock {
                offset: 0,
                reason: "invalid restart array size".into(),
            });
        }
        let restarts_offset = len - required;

        Ok(Self {
            data,
            num_restarts,
            restarts_offset,
        })
    }

    /// Create an iterator over all entries in the block.
    pub fn iter(&self) -> BlockIterator<'_> {
        BlockIterator {
            block: self,
            offset: 0,
            current_key: Vec::new(),
            current_value_offset: 0,
            current_value_len: 0,
            valid: false,
        }
    }

    /// Seek to the first entry with key >= target using binary search
    /// on restart points, then linear scan within the restart interval.
    pub fn seek(&self, target: &[u8]) -> BlockIterator<'_> {
        // Binary search over restart points
        let mut left = 0;
        let mut right = self.num_restarts;

        while left < right {
            let mid = left + (right - left) / 2;
            let restart_offset = self.get_restart(mid);

            // Decode the key at this restart point
            match self.decode_entry_at(restart_offset, &[]) {
                Some((key, _, _)) => {
                    if key.as_slice() < target {
                        left = mid + 1;
                    } else {
                        right = mid;
                    }
                }
                None => {
                    // Corrupted; fall back to linear scan
                    right = mid;
                }
            }
        }

        // Start from the restart point just before (or at) the target
        let restart_idx = if left > 0 { left - 1 } else { 0 };
        let start_offset = self.get_restart(restart_idx);

        // Linear scan from the restart point to find the target
        let mut iter = BlockIterator {
            block: self,
            offset: start_offset,
            current_key: Vec::new(),
            current_value_offset: 0,
            current_value_len: 0,
            valid: false,
        };

        // Scan forward until we find key >= target
        iter.advance();
        while iter.valid && iter.current_key.as_slice() < target {
            iter.advance();
        }

        iter
    }

    /// Get the offset of the i-th restart point.
    fn get_restart(&self, index: usize) -> usize {
        let pos = self.restarts_offset + index * 4;
        u32::from_le_bytes([
            self.data[pos],
            self.data[pos + 1],
            self.data[pos + 2],
            self.data[pos + 3],
        ]) as usize
    }

    /// Decode an entry at the given offset.
    /// Returns `(full_key, value_offset, value_len)`.
    fn decode_entry_at(
        &self,
        offset: usize,
        prev_key: &[u8],
    ) -> Option<(Vec<u8>, usize, usize)> {
        if offset >= self.restarts_offset {
            return None;
        }
        let data = &self.data[offset..self.restarts_offset];
        let (shared, n1) = decode_varint32(data).ok()?;
        let (unshared, n2) = decode_varint32(&data[n1..]).ok()?;
        let (value_len, n3) = decode_varint32(&data[n1 + n2..]).ok()?;

        let shared = shared as usize;
        let unshared = unshared as usize;
        let value_len = value_len as usize;

        let key_start = n1 + n2 + n3;
        if key_start + unshared + value_len > data.len() {
            return None;
        }

        // Reconstruct the full key
        let mut key = Vec::with_capacity(shared + unshared);
        if shared > 0 {
            if shared > prev_key.len() {
                return None;
            }
            key.extend_from_slice(&prev_key[..shared]);
        }
        key.extend_from_slice(&data[key_start..key_start + unshared]);

        let value_offset = offset + key_start + unshared;
        Some((key, value_offset, value_len))
    }
}

/// Iterator over entries in a block.
pub struct BlockIterator<'a> {
    block: &'a BlockReader,
    /// Current byte offset within the block data.
    offset: usize,
    /// The fully reconstructed current key.
    current_key: Vec<u8>,
    /// Offset of the current value within block data.
    current_value_offset: usize,
    /// Length of the current value.
    current_value_len: usize,
    /// Whether the iterator is positioned at a valid entry.
    valid: bool,
}

impl<'a> BlockIterator<'a> {
    /// Returns true if the iterator is at a valid entry.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Get the current key.
    pub fn key(&self) -> &[u8] {
        debug_assert!(self.valid);
        &self.current_key
    }

    /// Get the current value.
    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid);
        &self.block.data[self.current_value_offset..self.current_value_offset + self.current_value_len]
    }

    /// Advance to the next entry.
    pub fn next(&mut self) {
        if !self.valid {
            // First call: start from the beginning
            self.advance();
        } else {
            // Move past the current value
            self.offset = self.current_value_offset + self.current_value_len;
            self.advance();
        }
    }

    /// Internal: decode the entry at the current offset and advance.
    fn advance(&mut self) {
        if self.offset >= self.block.restarts_offset {
            self.valid = false;
            return;
        }

        let data = &self.block.data[self.offset..self.block.restarts_offset];
        if data.is_empty() {
            self.valid = false;
            return;
        }

        // Decode shared_len, unshared_len, value_len
        let (shared, n1) = match decode_varint32(data) {
            Ok(v) => v,
            Err(_) => {
                self.valid = false;
                return;
            }
        };
        let (unshared, n2) = match decode_varint32(&data[n1..]) {
            Ok(v) => v,
            Err(_) => {
                self.valid = false;
                return;
            }
        };
        let (value_len, n3) = match decode_varint32(&data[n1 + n2..]) {
            Ok(v) => v,
            Err(_) => {
                self.valid = false;
                return;
            }
        };

        let shared = shared as usize;
        let unshared = unshared as usize;
        let value_len = value_len as usize;

        let key_start = n1 + n2 + n3;
        if key_start + unshared + value_len > data.len() {
            self.valid = false;
            return;
        }

        // Reconstruct the full key using prefix compression
        self.current_key.truncate(shared);
        self.current_key
            .extend_from_slice(&data[key_start..key_start + unshared]);

        self.current_value_offset = self.offset + key_start + unshared;
        self.current_value_len = value_len;
        self.offset = self.current_value_offset + value_len;
        self.valid = true;
    }
}

// ── Helpers ─────────────────────────────────────────────────────

/// Compute the length of the shared prefix between two byte slices.
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ── Block Trailer ───────────────────────────────────────────────

/// Block trailer appended after each block in the SSTable file.
///
/// ```text
/// [compression_type (1 byte)] [checksum (4 bytes)]
/// ```
pub const BLOCK_TRAILER_SIZE: usize = 5;

/// Compression type for a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockCompressionType {
    None = 0,
    Lz4 = 1,
    Snappy = 2,
    Zstd = 3,
}

impl BlockCompressionType {
    pub fn from_byte(b: u8) -> KvResult<Self> {
        match b {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Zstd),
            _ => Err(KvError::Deserialization(format!(
                "unknown compression type: {}",
                b
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip() {
        for &val in &[0u32, 1, 127, 128, 255, 256, 16383, 16384, u32::MAX] {
            let mut buf = Vec::new();
            encode_varint32(&mut buf, val);
            let (decoded, _) = decode_varint32(&buf).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_block_builder_and_reader() {
        let mut builder = BlockBuilder::new(4);

        let entries = vec![
            (b"apple".to_vec(), b"red".to_vec()),
            (b"apricot".to_vec(), b"orange".to_vec()),
            (b"banana".to_vec(), b"yellow".to_vec()),
            (b"blueberry".to_vec(), b"blue".to_vec()),
            (b"cherry".to_vec(), b"red".to_vec()),
            (b"date".to_vec(), b"brown".to_vec()),
            (b"elderberry".to_vec(), b"purple".to_vec()),
            (b"fig".to_vec(), b"green".to_vec()),
        ];

        for (k, v) in &entries {
            builder.add(k, v);
        }

        let data = builder.finish();
        let reader = BlockReader::new(data).unwrap();

        // Sequential iteration
        let mut iter = reader.iter();
        for (expected_key, expected_value) in &entries {
            iter.next();
            assert!(iter.is_valid());
            assert_eq!(iter.key(), expected_key.as_slice());
            assert_eq!(iter.value(), expected_value.as_slice());
        }
        iter.next();
        assert!(!iter.is_valid());
    }

    #[test]
    fn test_block_seek() {
        let mut builder = BlockBuilder::new(4);
        let entries: Vec<(String, String)> = (0..100)
            .map(|i| (format!("{:06}", i), format!("val_{}", i)))
            .collect();

        for (k, v) in &entries {
            builder.add(k.as_bytes(), v.as_bytes());
        }

        let data = builder.finish();
        let reader = BlockReader::new(data).unwrap();

        // Seek to a specific key
        let iter = reader.seek(b"000050");
        assert!(iter.is_valid());
        assert_eq!(iter.key(), b"000050");
        assert_eq!(iter.value(), b"val_50");

        // Seek to a key between entries
        let iter = reader.seek(b"000050a");
        assert!(iter.is_valid());
        assert_eq!(iter.key(), b"000051");

        // Seek past all entries
        let iter = reader.seek(b"999999");
        assert!(!iter.is_valid());
    }

    #[test]
    fn test_block_handle_roundtrip() {
        let handle = BlockHandle::new(12345, 6789);
        let mut buf = Vec::new();
        handle.encode(&mut buf);
        let (decoded, _) = BlockHandle::decode(&buf).unwrap();
        assert_eq!(decoded.offset, 12345);
        assert_eq!(decoded.size, 6789);
    }

    #[test]
    fn test_prefix_compression() {
        let mut builder = BlockBuilder::new(16); // All entries share prefix
        builder.add(b"prefix_aaa", b"v1");
        builder.add(b"prefix_aab", b"v2");
        builder.add(b"prefix_aac", b"v3");

        let data_with_compression = builder.finish();

        let mut builder2 = BlockBuilder::new(1); // Restart every entry (no compression)
        builder2.add(b"prefix_aaa", b"v1");
        builder2.add(b"prefix_aab", b"v2");
        builder2.add(b"prefix_aac", b"v3");

        let data_without = builder2.finish();

        // Compressed block should be smaller
        assert!(data_with_compression.len() < data_without.len());

        // Both should produce the same entries
        let reader = BlockReader::new(data_with_compression).unwrap();
        let mut iter = reader.iter();
        iter.next();
        assert_eq!(iter.key(), b"prefix_aaa");
        iter.next();
        assert_eq!(iter.key(), b"prefix_aab");
        iter.next();
        assert_eq!(iter.key(), b"prefix_aac");
    }
}
