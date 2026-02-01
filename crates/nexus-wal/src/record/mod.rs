//! WAL record types and serialization.
//!
//! This module defines the record types used in the Write-Ahead Log.
//! Records are serialized with a fixed-size header followed by variable-length payload.

pub mod header;
pub mod payload;
pub mod types;

pub use header::RecordHeader;
pub use payload::{
    CheckpointBeginPayload, CheckpointEndPayload, CommitPayload, CompensationPayload,
    DeletePayload, InsertPayload, PageWritePayload, Payload, PreparePayload, SegmentHeaderPayload,
    UpdatePayload,
};
pub use types::{RecordFlags, RecordType, WalPayload, WalRecord};
