//! WAL record header.
//!
//! The header is a fixed 40-byte structure that precedes every WAL record.

use bytes::{Buf, BufMut};
use nexus_common::constants::WAL_RECORD_HEADER_SIZE;
use nexus_common::types::{Lsn, TxnId};

use super::types::{RecordFlags, RecordType};
use crate::error::{WalError, WalResult};

/// WAL record header (40 bytes).
///
/// Layout:
/// - lsn: 8 bytes (Log Sequence Number)
/// - prev_lsn: 8 bytes (Previous LSN for this transaction)
/// - txn_id: 8 bytes (Transaction ID)
/// - record_type: 1 byte
/// - flags: 1 byte
/// - reserved: 2 bytes (for alignment)
/// - payload_length: 4 bytes
/// - checksum: 4 bytes (CRC32C of header + payload)
/// - header_checksum: 4 bytes (CRC32C of header only, for validation)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct RecordHeader {
    /// Log Sequence Number of this record.
    pub lsn: Lsn,
    /// Previous LSN for this transaction (for undo chain).
    pub prev_lsn: Lsn,
    /// Transaction ID that generated this record.
    pub txn_id: TxnId,
    /// Type of record.
    pub record_type: RecordType,
    /// Record flags.
    pub flags: RecordFlags,
    /// Length of the payload in bytes.
    pub payload_length: u32,
    /// CRC32C checksum of header + payload.
    pub checksum: u32,
}

impl RecordHeader {
    /// Size of the header in bytes.
    pub const SIZE: usize = WAL_RECORD_HEADER_SIZE;

    /// Creates a new record header.
    pub fn new(
        lsn: Lsn,
        prev_lsn: Lsn,
        txn_id: TxnId,
        record_type: RecordType,
        flags: RecordFlags,
        payload_length: u32,
    ) -> Self {
        Self {
            lsn,
            prev_lsn,
            txn_id,
            record_type,
            flags,
            payload_length,
            checksum: 0,
        }
    }

    /// Serializes the header to bytes.
    pub fn serialize(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.lsn.as_u64());
        buf.put_u64(self.prev_lsn.as_u64());
        buf.put_u64(self.txn_id.as_u64());
        buf.put_u8(self.record_type.as_u8());
        buf.put_u8(self.flags.bits());
        buf.put_u16(0); // reserved
        buf.put_u32(self.payload_length);
        buf.put_u32(self.checksum);
        buf.put_u32(0); // padding to 40 bytes
    }

    /// Serializes the header to a byte array.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        let mut cursor = &mut buf[..];
        self.serialize(&mut cursor);
        buf
    }

    /// Deserializes a header from bytes.
    pub fn deserialize(buf: &mut impl Buf) -> WalResult<Self> {
        if buf.remaining() < Self::SIZE {
            return Err(WalError::deserialization_error(format!(
                "Not enough bytes for header: {} < {}",
                buf.remaining(),
                Self::SIZE
            )));
        }

        let lsn = Lsn::new(buf.get_u64());
        let prev_lsn = Lsn::new(buf.get_u64());
        let txn_id = TxnId::new(buf.get_u64());
        let record_type = RecordType::from_u8(buf.get_u8())?;
        let flags = RecordFlags::from_bits_truncate(buf.get_u8());
        let _reserved = buf.get_u16();
        let payload_length = buf.get_u32();
        let checksum = buf.get_u32();
        let _padding = buf.get_u32(); // skip padding

        Ok(Self {
            lsn,
            prev_lsn,
            txn_id,
            record_type,
            flags,
            payload_length,
            checksum,
        })
    }

    /// Deserializes a header from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> WalResult<Self> {
        Self::deserialize(&mut &bytes[..])
    }

    /// Returns the total record size (header + payload).
    pub fn total_size(&self) -> usize {
        Self::SIZE + self.payload_length as usize
    }

    /// Computes the checksum for this header and the given payload.
    pub fn compute_checksum(&self, payload: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();

        // Hash header fields (excluding the checksum field itself)
        hasher.update(&self.lsn.as_u64().to_le_bytes());
        hasher.update(&self.prev_lsn.as_u64().to_le_bytes());
        hasher.update(&self.txn_id.as_u64().to_le_bytes());
        hasher.update(&[self.record_type.as_u8()]);
        hasher.update(&[self.flags.bits()]);
        hasher.update(&[0u8; 2]); // reserved
        hasher.update(&self.payload_length.to_le_bytes());

        // Hash payload
        hasher.update(payload);

        hasher.finalize()
    }

    /// Sets the checksum based on the payload.
    pub fn set_checksum(&mut self, payload: &[u8]) {
        self.checksum = self.compute_checksum(payload);
    }

    /// Verifies the checksum against the payload.
    pub fn verify_checksum(&self, payload: &[u8]) -> bool {
        self.checksum == self.compute_checksum(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(RecordHeader::SIZE, 40);
    }

    #[test]
    fn test_header_roundtrip() {
        let header = RecordHeader::new(
            Lsn::new(1000),
            Lsn::new(500),
            TxnId::new(42),
            RecordType::Insert,
            RecordFlags::REDO | RecordFlags::UNDO,
            256,
        );

        let bytes = header.to_bytes();
        let decoded = RecordHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.lsn, decoded.lsn);
        assert_eq!(header.prev_lsn, decoded.prev_lsn);
        assert_eq!(header.txn_id, decoded.txn_id);
        assert_eq!(header.record_type, decoded.record_type);
        assert_eq!(header.flags, decoded.flags);
        assert_eq!(header.payload_length, decoded.payload_length);
    }

    #[test]
    fn test_checksum() {
        let mut header = RecordHeader::new(
            Lsn::new(1000),
            Lsn::INVALID,
            TxnId::new(1),
            RecordType::Insert,
            RecordFlags::REDO,
            10,
        );

        let payload = b"test data!";
        header.set_checksum(payload);

        assert!(header.verify_checksum(payload));
        assert!(!header.verify_checksum(b"wrong data"));
    }

    #[test]
    fn test_total_size() {
        let header = RecordHeader::new(
            Lsn::new(1000),
            Lsn::INVALID,
            TxnId::new(1),
            RecordType::Commit,
            RecordFlags::empty(),
            100,
        );

        assert_eq!(header.total_size(), 140);
    }
}
