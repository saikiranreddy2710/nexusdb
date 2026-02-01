//! WAL record payloads.
//!
//! This module defines the payload structures for different WAL record types.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use nexus_common::constants::WAL_MAGIC;
use nexus_common::types::{Lsn, PageId, TxnId};

use crate::error::{WalError, WalResult};

/// Trait for serializable payloads.
pub trait Payload: Sized {
    /// Serializes the payload to bytes.
    fn serialize(&self) -> WalResult<Bytes>;
    /// Deserializes the payload from bytes.
    fn deserialize(bytes: &[u8]) -> WalResult<Self>;
}

/// Insert record payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertPayload {
    /// Page where the record was inserted.
    pub page_id: PageId,
    /// Slot ID within the page.
    pub slot_id: u16,
    /// Key that was inserted.
    pub key: Bytes,
    /// Value that was inserted.
    pub value: Bytes,
}

impl Payload for InsertPayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let size = 8 + 2 + 4 + self.key.len() + 4 + self.value.len();
        let mut buf = BytesMut::with_capacity(size);

        buf.put_u64(self.page_id.as_u64());
        buf.put_u16(self.slot_id);
        buf.put_u32(self.key.len() as u32);
        buf.extend_from_slice(&self.key);
        buf.put_u32(self.value.len() as u32);
        buf.extend_from_slice(&self.value);

        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        let mut buf = bytes;

        if buf.remaining() < 14 {
            return Err(WalError::deserialization_error("InsertPayload too short"));
        }

        let page_id = PageId::new(buf.get_u64());
        let slot_id = buf.get_u16();

        let key_len = buf.get_u32() as usize;
        if buf.remaining() < key_len + 4 {
            return Err(WalError::deserialization_error(
                "InsertPayload key truncated",
            ));
        }
        let key = Bytes::copy_from_slice(&buf[..key_len]);
        buf.advance(key_len);

        let value_len = buf.get_u32() as usize;
        if buf.remaining() < value_len {
            return Err(WalError::deserialization_error(
                "InsertPayload value truncated",
            ));
        }
        let value = Bytes::copy_from_slice(&buf[..value_len]);

        Ok(Self {
            page_id,
            slot_id,
            key,
            value,
        })
    }
}

/// Update record payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePayload {
    /// Page where the record exists.
    pub page_id: PageId,
    /// Slot ID within the page.
    pub slot_id: u16,
    /// Key of the record.
    pub key: Bytes,
    /// Old value (for undo).
    pub old_value: Bytes,
    /// New value (for redo).
    pub new_value: Bytes,
}

impl Payload for UpdatePayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let size = 8 + 2 + 4 + self.key.len() + 4 + self.old_value.len() + 4 + self.new_value.len();
        let mut buf = BytesMut::with_capacity(size);

        buf.put_u64(self.page_id.as_u64());
        buf.put_u16(self.slot_id);
        buf.put_u32(self.key.len() as u32);
        buf.extend_from_slice(&self.key);
        buf.put_u32(self.old_value.len() as u32);
        buf.extend_from_slice(&self.old_value);
        buf.put_u32(self.new_value.len() as u32);
        buf.extend_from_slice(&self.new_value);

        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        let mut buf = bytes;

        if buf.remaining() < 14 {
            return Err(WalError::deserialization_error("UpdatePayload too short"));
        }

        let page_id = PageId::new(buf.get_u64());
        let slot_id = buf.get_u16();

        let key_len = buf.get_u32() as usize;
        if buf.remaining() < key_len + 4 {
            return Err(WalError::deserialization_error(
                "UpdatePayload key truncated",
            ));
        }
        let key = Bytes::copy_from_slice(&buf[..key_len]);
        buf.advance(key_len);

        let old_value_len = buf.get_u32() as usize;
        if buf.remaining() < old_value_len + 4 {
            return Err(WalError::deserialization_error(
                "UpdatePayload old_value truncated",
            ));
        }
        let old_value = Bytes::copy_from_slice(&buf[..old_value_len]);
        buf.advance(old_value_len);

        let new_value_len = buf.get_u32() as usize;
        if buf.remaining() < new_value_len {
            return Err(WalError::deserialization_error(
                "UpdatePayload new_value truncated",
            ));
        }
        let new_value = Bytes::copy_from_slice(&buf[..new_value_len]);

        Ok(Self {
            page_id,
            slot_id,
            key,
            old_value,
            new_value,
        })
    }
}

/// Delete record payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePayload {
    /// Page where the record was deleted.
    pub page_id: PageId,
    /// Slot ID within the page.
    pub slot_id: u16,
    /// Key that was deleted.
    pub key: Bytes,
    /// Value that was deleted (for undo).
    pub value: Bytes,
}

impl Payload for DeletePayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let size = 8 + 2 + 4 + self.key.len() + 4 + self.value.len();
        let mut buf = BytesMut::with_capacity(size);

        buf.put_u64(self.page_id.as_u64());
        buf.put_u16(self.slot_id);
        buf.put_u32(self.key.len() as u32);
        buf.extend_from_slice(&self.key);
        buf.put_u32(self.value.len() as u32);
        buf.extend_from_slice(&self.value);

        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        let mut buf = bytes;

        if buf.remaining() < 14 {
            return Err(WalError::deserialization_error("DeletePayload too short"));
        }

        let page_id = PageId::new(buf.get_u64());
        let slot_id = buf.get_u16();

        let key_len = buf.get_u32() as usize;
        if buf.remaining() < key_len + 4 {
            return Err(WalError::deserialization_error(
                "DeletePayload key truncated",
            ));
        }
        let key = Bytes::copy_from_slice(&buf[..key_len]);
        buf.advance(key_len);

        let value_len = buf.get_u32() as usize;
        if buf.remaining() < value_len {
            return Err(WalError::deserialization_error(
                "DeletePayload value truncated",
            ));
        }
        let value = Bytes::copy_from_slice(&buf[..value_len]);

        Ok(Self {
            page_id,
            slot_id,
            key,
            value,
        })
    }
}

/// Commit record payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitPayload {
    /// Commit timestamp.
    pub commit_timestamp: u64,
}

impl Payload for CommitPayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(self.commit_timestamp);
        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        if bytes.len() < 8 {
            return Err(WalError::deserialization_error("CommitPayload too short"));
        }
        let mut buf = bytes;
        Ok(Self {
            commit_timestamp: buf.get_u64(),
        })
    }
}

/// Checkpoint begin payload.
#[derive(Debug, Clone)]
pub struct CheckpointBeginPayload {
    /// Checkpoint ID.
    pub checkpoint_id: u64,
    /// Number of active transactions.
    pub active_txn_count: u32,
    /// Active transaction IDs.
    pub active_txns: Vec<TxnId>,
}

impl Payload for CheckpointBeginPayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let size = 8 + 4 + self.active_txns.len() * 8;
        let mut buf = BytesMut::with_capacity(size);

        buf.put_u64(self.checkpoint_id);
        buf.put_u32(self.active_txns.len() as u32);
        for txn_id in &self.active_txns {
            buf.put_u64(txn_id.as_u64());
        }

        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        let mut buf = bytes;

        if buf.remaining() < 12 {
            return Err(WalError::deserialization_error(
                "CheckpointBeginPayload too short",
            ));
        }

        let checkpoint_id = buf.get_u64();
        let active_txn_count = buf.get_u32();

        if buf.remaining() < active_txn_count as usize * 8 {
            return Err(WalError::deserialization_error(
                "CheckpointBeginPayload txn list truncated",
            ));
        }

        let mut active_txns = Vec::with_capacity(active_txn_count as usize);
        for _ in 0..active_txn_count {
            active_txns.push(TxnId::new(buf.get_u64()));
        }

        Ok(Self {
            checkpoint_id,
            active_txn_count,
            active_txns,
        })
    }
}

/// Checkpoint end payload.
#[derive(Debug, Clone)]
pub struct CheckpointEndPayload {
    /// Checkpoint ID (matches begin).
    pub checkpoint_id: u64,
    /// LSN of the checkpoint begin record.
    pub begin_lsn: Lsn,
    /// Minimum recovery LSN (oldest dirty page LSN).
    pub min_recovery_lsn: Lsn,
}

impl Payload for CheckpointEndPayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let mut buf = BytesMut::with_capacity(24);
        buf.put_u64(self.checkpoint_id);
        buf.put_u64(self.begin_lsn.as_u64());
        buf.put_u64(self.min_recovery_lsn.as_u64());
        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        if bytes.len() < 24 {
            return Err(WalError::deserialization_error(
                "CheckpointEndPayload too short",
            ));
        }
        let mut buf = bytes;
        Ok(Self {
            checkpoint_id: buf.get_u64(),
            begin_lsn: Lsn::new(buf.get_u64()),
            min_recovery_lsn: Lsn::new(buf.get_u64()),
        })
    }
}

/// Page write payload (for physiological logging).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageWritePayload {
    /// Page ID.
    pub page_id: PageId,
    /// Offset within the page.
    pub offset: u16,
    /// Old data (for undo).
    pub old_data: Bytes,
    /// New data (for redo).
    pub new_data: Bytes,
}

impl Payload for PageWritePayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let size = 8 + 2 + 4 + self.old_data.len() + 4 + self.new_data.len();
        let mut buf = BytesMut::with_capacity(size);

        buf.put_u64(self.page_id.as_u64());
        buf.put_u16(self.offset);
        buf.put_u32(self.old_data.len() as u32);
        buf.extend_from_slice(&self.old_data);
        buf.put_u32(self.new_data.len() as u32);
        buf.extend_from_slice(&self.new_data);

        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        let mut buf = bytes;

        if buf.remaining() < 14 {
            return Err(WalError::deserialization_error(
                "PageWritePayload too short",
            ));
        }

        let page_id = PageId::new(buf.get_u64());
        let offset = buf.get_u16();

        let old_len = buf.get_u32() as usize;
        if buf.remaining() < old_len + 4 {
            return Err(WalError::deserialization_error(
                "PageWritePayload old_data truncated",
            ));
        }
        let old_data = Bytes::copy_from_slice(&buf[..old_len]);
        buf.advance(old_len);

        let new_len = buf.get_u32() as usize;
        if buf.remaining() < new_len {
            return Err(WalError::deserialization_error(
                "PageWritePayload new_data truncated",
            ));
        }
        let new_data = Bytes::copy_from_slice(&buf[..new_len]);

        Ok(Self {
            page_id,
            offset,
            old_data,
            new_data,
        })
    }
}

/// Compensation log record payload (for undo operations).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompensationPayload {
    /// LSN of the record being undone.
    pub undo_lsn: Lsn,
    /// Next LSN to undo in the chain.
    pub undo_next_lsn: Lsn,
    /// Redo-only data for the compensation.
    pub redo_data: Bytes,
}

impl Payload for CompensationPayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let size = 16 + 4 + self.redo_data.len();
        let mut buf = BytesMut::with_capacity(size);

        buf.put_u64(self.undo_lsn.as_u64());
        buf.put_u64(self.undo_next_lsn.as_u64());
        buf.put_u32(self.redo_data.len() as u32);
        buf.extend_from_slice(&self.redo_data);

        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        let mut buf = bytes;

        if buf.remaining() < 20 {
            return Err(WalError::deserialization_error(
                "CompensationPayload too short",
            ));
        }

        let undo_lsn = Lsn::new(buf.get_u64());
        let undo_next_lsn = Lsn::new(buf.get_u64());

        let redo_len = buf.get_u32() as usize;
        if buf.remaining() < redo_len {
            return Err(WalError::deserialization_error(
                "CompensationPayload redo_data truncated",
            ));
        }
        let redo_data = Bytes::copy_from_slice(&buf[..redo_len]);

        Ok(Self {
            undo_lsn,
            undo_next_lsn,
            redo_data,
        })
    }
}

/// Prepare record payload (for 2PC).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparePayload {
    /// Global transaction ID.
    pub global_txn_id: Bytes,
}

impl Payload for PreparePayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let mut buf = BytesMut::with_capacity(4 + self.global_txn_id.len());
        buf.put_u32(self.global_txn_id.len() as u32);
        buf.extend_from_slice(&self.global_txn_id);
        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        let mut buf = bytes;

        if buf.remaining() < 4 {
            return Err(WalError::deserialization_error("PreparePayload too short"));
        }

        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
            return Err(WalError::deserialization_error(
                "PreparePayload global_txn_id truncated",
            ));
        }
        let global_txn_id = Bytes::copy_from_slice(&buf[..len]);

        Ok(Self { global_txn_id })
    }
}

/// Segment header payload (first record in each segment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeaderPayload {
    /// Magic number for validation.
    pub magic: u32,
    /// WAL format version.
    pub version: u32,
    /// Segment ID.
    pub segment_id: u64,
    /// First LSN in this segment.
    pub first_lsn: Lsn,
    /// Previous segment's last LSN.
    pub prev_segment_last_lsn: Lsn,
    /// Creation timestamp.
    pub created_at: u64,
}

impl SegmentHeaderPayload {
    /// Current WAL version.
    pub const VERSION: u32 = 1;

    /// Creates a new segment header.
    pub fn new(segment_id: u64, first_lsn: Lsn, prev_segment_last_lsn: Lsn) -> Self {
        Self {
            magic: WAL_MAGIC,
            version: Self::VERSION,
            segment_id,
            first_lsn,
            prev_segment_last_lsn,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Validates the segment header.
    pub fn validate(&self) -> WalResult<()> {
        if self.magic != WAL_MAGIC {
            return Err(WalError::InvalidMagic {
                expected: WAL_MAGIC,
                found: self.magic,
            });
        }
        if self.version != Self::VERSION {
            return Err(WalError::UnsupportedVersion {
                expected: Self::VERSION,
                found: self.version,
            });
        }
        Ok(())
    }
}

impl Payload for SegmentHeaderPayload {
    fn serialize(&self) -> WalResult<Bytes> {
        let mut buf = BytesMut::with_capacity(40);
        buf.put_u32(self.magic);
        buf.put_u32(self.version);
        buf.put_u64(self.segment_id);
        buf.put_u64(self.first_lsn.as_u64());
        buf.put_u64(self.prev_segment_last_lsn.as_u64());
        buf.put_u64(self.created_at);
        Ok(buf.freeze())
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        if bytes.len() < 40 {
            return Err(WalError::deserialization_error(
                "SegmentHeaderPayload too short",
            ));
        }
        let mut buf = bytes;
        Ok(Self {
            magic: buf.get_u32(),
            version: buf.get_u32(),
            segment_id: buf.get_u64(),
            first_lsn: Lsn::new(buf.get_u64()),
            prev_segment_last_lsn: Lsn::new(buf.get_u64()),
            created_at: buf.get_u64(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_payload_roundtrip() {
        let payload = InsertPayload {
            page_id: PageId::new(42),
            slot_id: 10,
            key: Bytes::from("hello"),
            value: Bytes::from("world"),
        };

        let bytes = payload.serialize().unwrap();
        let decoded = InsertPayload::deserialize(&bytes).unwrap();

        assert_eq!(payload, decoded);
    }

    #[test]
    fn test_update_payload_roundtrip() {
        let payload = UpdatePayload {
            page_id: PageId::new(100),
            slot_id: 5,
            key: Bytes::from("key"),
            old_value: Bytes::from("old"),
            new_value: Bytes::from("new"),
        };

        let bytes = payload.serialize().unwrap();
        let decoded = UpdatePayload::deserialize(&bytes).unwrap();

        assert_eq!(payload, decoded);
    }

    #[test]
    fn test_delete_payload_roundtrip() {
        let payload = DeletePayload {
            page_id: PageId::new(200),
            slot_id: 15,
            key: Bytes::from("deleted_key"),
            value: Bytes::from("deleted_value"),
        };

        let bytes = payload.serialize().unwrap();
        let decoded = DeletePayload::deserialize(&bytes).unwrap();

        assert_eq!(payload, decoded);
    }

    #[test]
    fn test_commit_payload_roundtrip() {
        let payload = CommitPayload {
            commit_timestamp: 1234567890,
        };

        let bytes = payload.serialize().unwrap();
        let decoded = CommitPayload::deserialize(&bytes).unwrap();

        assert_eq!(payload, decoded);
    }

    #[test]
    fn test_checkpoint_payloads_roundtrip() {
        let begin = CheckpointBeginPayload {
            checkpoint_id: 1,
            active_txn_count: 3,
            active_txns: vec![TxnId::new(10), TxnId::new(20), TxnId::new(30)],
        };

        let bytes = begin.serialize().unwrap();
        let decoded = CheckpointBeginPayload::deserialize(&bytes).unwrap();

        assert_eq!(begin.checkpoint_id, decoded.checkpoint_id);
        assert_eq!(begin.active_txns.len(), decoded.active_txns.len());

        let end = CheckpointEndPayload {
            checkpoint_id: 1,
            begin_lsn: Lsn::new(1000),
            min_recovery_lsn: Lsn::new(500),
        };

        let bytes = end.serialize().unwrap();
        let decoded = CheckpointEndPayload::deserialize(&bytes).unwrap();

        assert_eq!(end.checkpoint_id, decoded.checkpoint_id);
        assert_eq!(end.begin_lsn, decoded.begin_lsn);
        assert_eq!(end.min_recovery_lsn, decoded.min_recovery_lsn);
    }

    #[test]
    fn test_page_write_payload_roundtrip() {
        let payload = PageWritePayload {
            page_id: PageId::new(50),
            offset: 100,
            old_data: Bytes::from(vec![1, 2, 3, 4]),
            new_data: Bytes::from(vec![5, 6, 7, 8]),
        };

        let bytes = payload.serialize().unwrap();
        let decoded = PageWritePayload::deserialize(&bytes).unwrap();

        assert_eq!(payload, decoded);
    }

    #[test]
    fn test_segment_header_validation() {
        let header = SegmentHeaderPayload::new(0, Lsn::FIRST, Lsn::INVALID);
        assert!(header.validate().is_ok());

        let invalid = SegmentHeaderPayload {
            magic: 0xDEADBEEF,
            ..header
        };
        assert!(matches!(
            invalid.validate(),
            Err(WalError::InvalidMagic { .. })
        ));
    }

    #[test]
    fn test_compensation_payload_roundtrip() {
        let payload = CompensationPayload {
            undo_lsn: Lsn::new(1000),
            undo_next_lsn: Lsn::new(500),
            redo_data: Bytes::from(vec![1, 2, 3]),
        };

        let bytes = payload.serialize().unwrap();
        let decoded = CompensationPayload::deserialize(&bytes).unwrap();

        assert_eq!(payload, decoded);
    }

    #[test]
    fn test_prepare_payload_roundtrip() {
        let payload = PreparePayload {
            global_txn_id: Bytes::from("global-txn-123"),
        };

        let bytes = payload.serialize().unwrap();
        let decoded = PreparePayload::deserialize(&bytes).unwrap();

        assert_eq!(payload, decoded);
    }
}
