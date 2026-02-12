//! SSTable footer format.
//!
//! The footer is the last 48 bytes of every SSTable file, providing
//! the entry point for reading the table. It contains handles to
//! the meta-index block and the data index block, plus a magic number
//! for format validation.
//!
//! ## Footer Layout (48 bytes)
//!
//! ```text
//! ┌────────────────────────────────────────────┐
//! │ meta_index_handle (encoded BlockHandle)     │
//! │ index_handle (encoded BlockHandle)          │
//! │ padding (to 40 bytes)                       │
//! │ magic_number (8 bytes, little-endian)       │
//! └────────────────────────────────────────────┘
//! ```

use crate::error::{KvError, KvResult};
use crate::sstable::block::BlockHandle;

/// Fixed size of the SSTable footer in bytes.
pub const FOOTER_SIZE: usize = 48;

/// Magic number identifying NexusDB SSTable files.
/// Chosen to be unlikely to appear in random data.
pub const TABLE_MAGIC: u64 = 0x4E58_4B56_5353_5401; // "NXKVSS\x54\x01"

/// The footer of an SSTable file.
///
/// Contains references to the meta-index and data-index blocks,
/// which are needed to read the rest of the file.
#[derive(Debug, Clone)]
pub struct Footer {
    /// Handle to the meta-index block (contains bloom filter handle, etc.).
    pub meta_index_handle: BlockHandle,
    /// Handle to the data index block (contains handles to data blocks).
    pub index_handle: BlockHandle,
}

impl Footer {
    /// Create a new footer with the given block handles.
    pub fn new(meta_index_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Self {
            meta_index_handle,
            index_handle,
        }
    }

    /// Encode the footer to exactly `FOOTER_SIZE` bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);

        // Encode both handles
        self.meta_index_handle.encode(&mut buf);
        self.index_handle.encode(&mut buf);

        // Pad to 40 bytes (FOOTER_SIZE - 8 for magic)
        let padding_needed = FOOTER_SIZE - 8 - buf.len();
        buf.resize(buf.len() + padding_needed, 0);

        // Append magic number
        buf.extend_from_slice(&TABLE_MAGIC.to_le_bytes());

        debug_assert_eq!(buf.len(), FOOTER_SIZE);
        buf
    }

    /// Decode a footer from exactly `FOOTER_SIZE` bytes.
    pub fn decode(data: &[u8]) -> KvResult<Self> {
        if data.len() < FOOTER_SIZE {
            return Err(KvError::Deserialization(format!(
                "footer too small: {} bytes (need {})",
                data.len(),
                FOOTER_SIZE
            )));
        }

        let footer_data = &data[data.len() - FOOTER_SIZE..];

        // Verify magic number
        let magic_offset = FOOTER_SIZE - 8;
        let magic = u64::from_le_bytes([
            footer_data[magic_offset],
            footer_data[magic_offset + 1],
            footer_data[magic_offset + 2],
            footer_data[magic_offset + 3],
            footer_data[magic_offset + 4],
            footer_data[magic_offset + 5],
            footer_data[magic_offset + 6],
            footer_data[magic_offset + 7],
        ]);

        if magic != TABLE_MAGIC {
            return Err(KvError::InvalidMagic {
                expected: TABLE_MAGIC,
                actual: magic,
            });
        }

        // Decode handles
        let (meta_index_handle, n1) = BlockHandle::decode(footer_data)?;
        let (index_handle, _) = BlockHandle::decode(&footer_data[n1..])?;

        Ok(Self {
            meta_index_handle,
            index_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_roundtrip() {
        let footer = Footer::new(
            BlockHandle::new(100, 200),
            BlockHandle::new(300, 400),
        );

        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.meta_index_handle.offset, 100);
        assert_eq!(decoded.meta_index_handle.size, 200);
        assert_eq!(decoded.index_handle.offset, 300);
        assert_eq!(decoded.index_handle.size, 400);
    }

    #[test]
    fn test_footer_invalid_magic() {
        let mut data = vec![0u8; FOOTER_SIZE];
        // Write wrong magic
        data[FOOTER_SIZE - 8..].copy_from_slice(&0xDEADBEEFu64.to_le_bytes());

        let result = Footer::decode(&data);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), KvError::InvalidMagic { .. }));
    }
}
