//! WAL record types and flags.
//!
//! This module defines the record types and the main WalRecord enum.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use nexus_common::types::{Lsn, TxnId};

use super::header::RecordHeader;
use super::payload::*;
use crate::error::{WalError, WalResult};

/// Record type identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RecordType {
    /// Insert a new record.
    Insert = 1,
    /// Update an existing record.
    Update = 2,
    /// Delete a record.
    Delete = 3,
    /// Transaction commit.
    Commit = 4,
    /// Transaction abort/rollback.
    Abort = 5,
    /// Begin checkpoint.
    CheckpointBegin = 6,
    /// End checkpoint.
    CheckpointEnd = 7,
    /// Raw page write (for physiological logging).
    PageWrite = 8,
    /// Compensation log record (CLR) for undo.
    Compensation = 9,
    /// Transaction prepare (for 2PC).
    Prepare = 10,
    /// Segment header (first record in each segment).
    SegmentHeader = 11,
}

impl RecordType {
    /// Converts the record type to a u8.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Creates a record type from a u8.
    pub fn from_u8(value: u8) -> WalResult<Self> {
        match value {
            1 => Ok(Self::Insert),
            2 => Ok(Self::Update),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Commit),
            5 => Ok(Self::Abort),
            6 => Ok(Self::CheckpointBegin),
            7 => Ok(Self::CheckpointEnd),
            8 => Ok(Self::PageWrite),
            9 => Ok(Self::Compensation),
            10 => Ok(Self::Prepare),
            11 => Ok(Self::SegmentHeader),
            _ => Err(WalError::deserialization_error(format!(
                "Unknown record type: {}",
                value
            ))),
        }
    }

    /// Returns true if this record type represents a transaction operation.
    pub const fn is_transaction_record(self) -> bool {
        matches!(
            self,
            Self::Insert
                | Self::Update
                | Self::Delete
                | Self::Commit
                | Self::Abort
                | Self::Prepare
                | Self::Compensation
        )
    }

    /// Returns true if this record type is a checkpoint record.
    pub const fn is_checkpoint_record(self) -> bool {
        matches!(self, Self::CheckpointBegin | Self::CheckpointEnd)
    }
}

bitflags::bitflags! {
    /// Flags for WAL records.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RecordFlags: u8 {
        /// Record contains redo information.
        const REDO = 0b0000_0001;
        /// Record contains undo information.
        const UNDO = 0b0000_0010;
        /// Record is a compensation log record (CLR).
        const CLR = 0b0000_0100;
        /// Record spans multiple segments (continuation).
        const CONTINUATION = 0b0000_1000;
        /// Record is the last part of a spanning record.
        const LAST_FRAGMENT = 0b0001_0000;
    }
}

/// A complete WAL record with header and typed payload.
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// Record header.
    pub header: RecordHeader,
    /// Record payload.
    pub payload: WalPayload,
}

/// WAL record payload variants.
#[derive(Debug, Clone)]
pub enum WalPayload {
    /// Insert record payload.
    Insert(InsertPayload),
    /// Update record payload.
    Update(UpdatePayload),
    /// Delete record payload.
    Delete(DeletePayload),
    /// Commit record payload.
    Commit(CommitPayload),
    /// Abort record (no payload).
    Abort,
    /// Checkpoint begin payload.
    CheckpointBegin(CheckpointBeginPayload),
    /// Checkpoint end payload.
    CheckpointEnd(CheckpointEndPayload),
    /// Page write payload.
    PageWrite(PageWritePayload),
    /// Compensation log record payload.
    Compensation(CompensationPayload),
    /// Prepare record (for 2PC).
    Prepare(PreparePayload),
    /// Segment header.
    SegmentHeader(SegmentHeaderPayload),
    /// Raw bytes (for unknown or custom record types).
    Raw(Bytes),
}

impl WalRecord {
    /// Creates a new insert record.
    pub fn insert(
        lsn: Lsn,
        prev_lsn: Lsn,
        txn_id: TxnId,
        payload: InsertPayload,
    ) -> WalResult<Self> {
        let payload_bytes = payload.serialize()?;
        let header = RecordHeader::new(
            lsn,
            prev_lsn,
            txn_id,
            RecordType::Insert,
            RecordFlags::REDO | RecordFlags::UNDO,
            payload_bytes.len() as u32,
        );
        Ok(Self {
            header,
            payload: WalPayload::Insert(payload),
        })
    }

    /// Creates a new update record.
    pub fn update(
        lsn: Lsn,
        prev_lsn: Lsn,
        txn_id: TxnId,
        payload: UpdatePayload,
    ) -> WalResult<Self> {
        let payload_bytes = payload.serialize()?;
        let header = RecordHeader::new(
            lsn,
            prev_lsn,
            txn_id,
            RecordType::Update,
            RecordFlags::REDO | RecordFlags::UNDO,
            payload_bytes.len() as u32,
        );
        Ok(Self {
            header,
            payload: WalPayload::Update(payload),
        })
    }

    /// Creates a new delete record.
    pub fn delete(
        lsn: Lsn,
        prev_lsn: Lsn,
        txn_id: TxnId,
        payload: DeletePayload,
    ) -> WalResult<Self> {
        let payload_bytes = payload.serialize()?;
        let header = RecordHeader::new(
            lsn,
            prev_lsn,
            txn_id,
            RecordType::Delete,
            RecordFlags::REDO | RecordFlags::UNDO,
            payload_bytes.len() as u32,
        );
        Ok(Self {
            header,
            payload: WalPayload::Delete(payload),
        })
    }

    /// Creates a new commit record.
    pub fn commit(lsn: Lsn, prev_lsn: Lsn, txn_id: TxnId, commit_ts: u64) -> Self {
        let payload = CommitPayload {
            commit_timestamp: commit_ts,
        };
        let header = RecordHeader::new(
            lsn,
            prev_lsn,
            txn_id,
            RecordType::Commit,
            RecordFlags::REDO,
            8, // just the timestamp
        );
        Self {
            header,
            payload: WalPayload::Commit(payload),
        }
    }

    /// Creates a new abort record.
    pub fn abort(lsn: Lsn, prev_lsn: Lsn, txn_id: TxnId) -> Self {
        let header = RecordHeader::new(
            lsn,
            prev_lsn,
            txn_id,
            RecordType::Abort,
            RecordFlags::empty(),
            0,
        );
        Self {
            header,
            payload: WalPayload::Abort,
        }
    }

    /// Creates a checkpoint begin record.
    pub fn checkpoint_begin(lsn: Lsn, payload: CheckpointBeginPayload) -> WalResult<Self> {
        let payload_bytes = payload.serialize()?;
        let header = RecordHeader::new(
            lsn,
            Lsn::INVALID,
            TxnId::INVALID,
            RecordType::CheckpointBegin,
            RecordFlags::empty(),
            payload_bytes.len() as u32,
        );
        Ok(Self {
            header,
            payload: WalPayload::CheckpointBegin(payload),
        })
    }

    /// Creates a checkpoint end record.
    pub fn checkpoint_end(lsn: Lsn, payload: CheckpointEndPayload) -> WalResult<Self> {
        let payload_bytes = payload.serialize()?;
        let header = RecordHeader::new(
            lsn,
            Lsn::INVALID,
            TxnId::INVALID,
            RecordType::CheckpointEnd,
            RecordFlags::empty(),
            payload_bytes.len() as u32,
        );
        Ok(Self {
            header,
            payload: WalPayload::CheckpointEnd(payload),
        })
    }

    /// Returns the LSN of this record.
    pub fn lsn(&self) -> Lsn {
        self.header.lsn
    }

    /// Returns the transaction ID of this record.
    pub fn txn_id(&self) -> TxnId {
        self.header.txn_id
    }

    /// Returns the record type.
    pub fn record_type(&self) -> RecordType {
        self.header.record_type
    }

    /// Serializes the entire record (header + payload) to bytes.
    pub fn serialize(&self) -> WalResult<Bytes> {
        let payload_bytes = self.serialize_payload()?;
        let mut header = self.header;
        header.payload_length = payload_bytes.len() as u32;
        header.set_checksum(&payload_bytes);

        let total_size = RecordHeader::SIZE + payload_bytes.len();
        let mut buf = BytesMut::with_capacity(total_size);
        header.serialize(&mut buf);
        buf.extend_from_slice(&payload_bytes);

        Ok(buf.freeze())
    }

    /// Serializes just the payload.
    fn serialize_payload(&self) -> WalResult<Bytes> {
        match &self.payload {
            WalPayload::Insert(p) => p.serialize(),
            WalPayload::Update(p) => p.serialize(),
            WalPayload::Delete(p) => p.serialize(),
            WalPayload::Commit(p) => p.serialize(),
            WalPayload::Abort => Ok(Bytes::new()),
            WalPayload::CheckpointBegin(p) => p.serialize(),
            WalPayload::CheckpointEnd(p) => p.serialize(),
            WalPayload::PageWrite(p) => p.serialize(),
            WalPayload::Compensation(p) => p.serialize(),
            WalPayload::Prepare(p) => p.serialize(),
            WalPayload::SegmentHeader(p) => p.serialize(),
            WalPayload::Raw(bytes) => Ok(bytes.clone()),
        }
    }

    /// Deserializes a record from bytes.
    pub fn deserialize(mut buf: impl Buf) -> WalResult<Self> {
        let header = RecordHeader::deserialize(&mut buf)?;

        if buf.remaining() < header.payload_length as usize {
            return Err(WalError::deserialization_error(format!(
                "Not enough bytes for payload: {} < {}",
                buf.remaining(),
                header.payload_length
            )));
        }

        let payload_bytes = buf.copy_to_bytes(header.payload_length as usize);
        let payload = Self::deserialize_payload(header.record_type, &payload_bytes)?;

        Ok(Self { header, payload })
    }

    /// Deserializes the payload based on record type.
    fn deserialize_payload(record_type: RecordType, bytes: &[u8]) -> WalResult<WalPayload> {
        match record_type {
            RecordType::Insert => Ok(WalPayload::Insert(InsertPayload::deserialize(bytes)?)),
            RecordType::Update => Ok(WalPayload::Update(UpdatePayload::deserialize(bytes)?)),
            RecordType::Delete => Ok(WalPayload::Delete(DeletePayload::deserialize(bytes)?)),
            RecordType::Commit => Ok(WalPayload::Commit(CommitPayload::deserialize(bytes)?)),
            RecordType::Abort => Ok(WalPayload::Abort),
            RecordType::CheckpointBegin => Ok(WalPayload::CheckpointBegin(
                CheckpointBeginPayload::deserialize(bytes)?,
            )),
            RecordType::CheckpointEnd => Ok(WalPayload::CheckpointEnd(
                CheckpointEndPayload::deserialize(bytes)?,
            )),
            RecordType::PageWrite => {
                Ok(WalPayload::PageWrite(PageWritePayload::deserialize(bytes)?))
            }
            RecordType::Compensation => Ok(WalPayload::Compensation(
                CompensationPayload::deserialize(bytes)?,
            )),
            RecordType::Prepare => Ok(WalPayload::Prepare(PreparePayload::deserialize(bytes)?)),
            RecordType::SegmentHeader => Ok(WalPayload::SegmentHeader(
                SegmentHeaderPayload::deserialize(bytes)?,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_common::types::PageId;

    #[test]
    fn test_record_type_roundtrip() {
        for rt in [
            RecordType::Insert,
            RecordType::Update,
            RecordType::Delete,
            RecordType::Commit,
            RecordType::Abort,
            RecordType::CheckpointBegin,
            RecordType::CheckpointEnd,
            RecordType::PageWrite,
            RecordType::Compensation,
            RecordType::Prepare,
            RecordType::SegmentHeader,
        ] {
            let byte = rt.as_u8();
            let decoded = RecordType::from_u8(byte).unwrap();
            assert_eq!(rt, decoded);
        }
    }

    #[test]
    fn test_invalid_record_type() {
        assert!(RecordType::from_u8(0).is_err());
        assert!(RecordType::from_u8(255).is_err());
    }

    #[test]
    fn test_record_flags() {
        let flags = RecordFlags::REDO | RecordFlags::UNDO;
        assert!(flags.contains(RecordFlags::REDO));
        assert!(flags.contains(RecordFlags::UNDO));
        assert!(!flags.contains(RecordFlags::CLR));
    }

    #[test]
    fn test_commit_record_roundtrip() {
        let record = WalRecord::commit(Lsn::new(1000), Lsn::new(500), TxnId::new(42), 12345);

        let bytes = record.serialize().unwrap();
        let decoded = WalRecord::deserialize(&mut bytes.as_ref()).unwrap();

        assert_eq!(decoded.header.lsn, Lsn::new(1000));
        assert_eq!(decoded.header.prev_lsn, Lsn::new(500));
        assert_eq!(decoded.header.txn_id, TxnId::new(42));
        assert_eq!(decoded.header.record_type, RecordType::Commit);

        if let WalPayload::Commit(payload) = decoded.payload {
            assert_eq!(payload.commit_timestamp, 12345);
        } else {
            panic!("Expected Commit payload");
        }
    }

    #[test]
    fn test_insert_record_roundtrip() {
        let payload = InsertPayload {
            page_id: PageId::new(100),
            slot_id: 5,
            key: Bytes::from("test_key"),
            value: Bytes::from("test_value"),
        };

        let record =
            WalRecord::insert(Lsn::new(2000), Lsn::INVALID, TxnId::new(1), payload).unwrap();

        let bytes = record.serialize().unwrap();
        let decoded = WalRecord::deserialize(&mut bytes.as_ref()).unwrap();

        assert_eq!(decoded.header.record_type, RecordType::Insert);
        if let WalPayload::Insert(p) = decoded.payload {
            assert_eq!(p.page_id, PageId::new(100));
            assert_eq!(p.slot_id, 5);
            assert_eq!(p.key.as_ref(), b"test_key");
            assert_eq!(p.value.as_ref(), b"test_value");
        } else {
            panic!("Expected Insert payload");
        }
    }

    #[test]
    fn test_abort_record() {
        let record = WalRecord::abort(Lsn::new(3000), Lsn::new(2000), TxnId::new(99));

        let bytes = record.serialize().unwrap();
        let decoded = WalRecord::deserialize(&mut bytes.as_ref()).unwrap();

        assert_eq!(decoded.header.record_type, RecordType::Abort);
        assert!(matches!(decoded.payload, WalPayload::Abort));
    }
}
