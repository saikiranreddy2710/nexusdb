//! Network transport layer for Raft consensus.
//!
//! This module provides the networking infrastructure for Raft nodes to
//! communicate with each other. It includes:
//!
//! - `Transport` trait: Abstract interface for message sending/receiving
//! - `TcpTransport`: TCP-based transport for production use
//! - `MemoryTransport`: In-memory transport for testing
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │   Node 1    │────▶│  Transport  │────▶│   Node 2    │
//! │             │◀────│   Layer     │◀────│             │
//! └─────────────┘     └─────────────┘     └─────────────┘
//!                            │
//!                     ┌──────┴──────┐
//!                     │   Node 3    │
//!                     └─────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use nexus_raft::transport::{TcpTransport, Transport};
//!
//! // Create transport
//! let transport = TcpTransport::new(node_id, addr, peers).await?;
//!
//! // Send message
//! transport.send(target_node, message).await?;
//!
//! // Receive messages
//! while let Some((from, msg)) = transport.recv().await {
//!     // Handle message
//! }
//! ```

mod memory;
mod tcp;

pub use memory::MemoryTransport;
pub use tcp::TcpTransport;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use thiserror::Error;

use crate::rpc::{NodeId, RaftMessage};

/// Errors that can occur in transport operations.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Connection to a peer failed.
    #[error("connection failed to node {node_id}: {reason}")]
    ConnectionFailed {
        /// The target node ID.
        node_id: NodeId,
        /// The reason for failure.
        reason: String,
    },

    /// Message serialization failed.
    #[error("serialization failed: {0}")]
    SerializationFailed(String),

    /// Message deserialization failed.
    #[error("deserialization failed: {0}")]
    DeserializationFailed(String),

    /// The transport is closed.
    #[error("transport closed")]
    Closed,

    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The target node is unknown.
    #[error("unknown node: {0}")]
    UnknownNode(NodeId),

    /// Timeout waiting for response.
    #[error("timeout")]
    Timeout,
}

/// Result type for transport operations.
pub type TransportResult<T> = Result<T, TransportError>;

/// Network address for a Raft node.
#[derive(Debug, Clone)]
pub struct NodeAddr {
    /// The node ID.
    pub node_id: NodeId,
    /// The network address.
    pub addr: SocketAddr,
}

impl NodeAddr {
    /// Creates a new node address.
    pub fn new(node_id: NodeId, addr: SocketAddr) -> Self {
        Self { node_id, addr }
    }
}

/// An incoming message from a peer.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    /// The sender node ID.
    pub from: NodeId,
    /// The message.
    pub message: RaftMessage,
}

impl IncomingMessage {
    /// Creates a new incoming message.
    pub fn new(from: NodeId, message: RaftMessage) -> Self {
        Self { from, message }
    }
}

/// Trait for network transport implementations.
///
/// This trait abstracts the network layer, allowing different implementations
/// for production (TCP) and testing (in-memory).
pub trait Transport: Send + Sync {
    /// Sends a message to a peer.
    ///
    /// This is a fire-and-forget operation. The message is queued for sending
    /// and the method returns immediately. Use `send_wait` for synchronous sending.
    fn send(&self, to: NodeId, message: RaftMessage) -> TransportResult<()>;

    /// Sends a message and waits for it to be transmitted.
    fn send_wait(
        &self,
        to: NodeId,
        message: RaftMessage,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>>;

    /// Receives the next incoming message.
    ///
    /// Returns `None` if the transport is closed.
    fn recv(&self) -> Pin<Box<dyn Future<Output = Option<IncomingMessage>> + Send + '_>>;

    /// Tries to receive a message without blocking.
    ///
    /// Returns `None` if no message is available.
    fn try_recv(&self) -> Option<IncomingMessage>;

    /// Broadcasts a message to all known peers.
    fn broadcast(&self, message: RaftMessage) -> TransportResult<()>;

    /// Returns the local node ID.
    fn local_id(&self) -> NodeId;

    /// Returns the list of known peer IDs.
    fn peers(&self) -> Vec<NodeId>;

    /// Adds a new peer to the transport.
    fn add_peer(&self, addr: NodeAddr) -> TransportResult<()>;

    /// Removes a peer from the transport.
    fn remove_peer(&self, node_id: NodeId) -> TransportResult<()>;

    /// Closes the transport.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Returns true if the transport is closed.
    fn is_closed(&self) -> bool;
}

/// Message frame format for wire transmission.
///
/// ```text
/// +----------+----------+----------+------------------+
/// | Magic(4) | From(8)  | Len(4)   | Payload(Len)     |
/// +----------+----------+----------+------------------+
/// ```
pub mod frame {
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    use crate::rpc::{NodeId, RaftMessage};

    use super::{TransportError, TransportResult};

    /// Magic number for message framing.
    pub const FRAME_MAGIC: u32 = 0x52414654; // "RAFT"

    /// Maximum message size (16 MB).
    pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

    /// Header size in bytes.
    pub const HEADER_SIZE: usize = 4 + 8 + 4; // magic + from + len

    /// Encodes a message into a frame.
    pub fn encode(from: NodeId, message: &RaftMessage) -> TransportResult<Bytes> {
        let payload = bincode::serialize(message)
            .map_err(|e| TransportError::SerializationFailed(e.to_string()))?;

        if payload.len() > MAX_MESSAGE_SIZE {
            return Err(TransportError::SerializationFailed(format!(
                "message too large: {} bytes",
                payload.len()
            )));
        }

        let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
        buf.put_u32(FRAME_MAGIC);
        buf.put_u64(from);
        buf.put_u32(payload.len() as u32);
        buf.put_slice(&payload);

        Ok(buf.freeze())
    }

    /// Decodes a frame into a message.
    ///
    /// Returns `(from, message)` on success.
    pub fn decode(mut data: Bytes) -> TransportResult<(NodeId, RaftMessage)> {
        if data.len() < HEADER_SIZE {
            return Err(TransportError::DeserializationFailed(
                "frame too short".to_string(),
            ));
        }

        let magic = data.get_u32();
        if magic != FRAME_MAGIC {
            return Err(TransportError::DeserializationFailed(format!(
                "invalid magic: {:08x}",
                magic
            )));
        }

        let from = data.get_u64();
        let len = data.get_u32() as usize;

        if len > MAX_MESSAGE_SIZE {
            return Err(TransportError::DeserializationFailed(format!(
                "message too large: {} bytes",
                len
            )));
        }

        if data.len() < len {
            return Err(TransportError::DeserializationFailed(
                "incomplete frame".to_string(),
            ));
        }

        let payload = data.slice(..len);
        let message: RaftMessage = bincode::deserialize(&payload)
            .map_err(|e| TransportError::DeserializationFailed(e.to_string()))?;

        Ok((from, message))
    }

    /// Checks if a buffer contains a complete frame.
    ///
    /// Returns the frame size if complete, or `None` if more data is needed.
    pub fn frame_size(data: &[u8]) -> Option<usize> {
        if data.len() < HEADER_SIZE {
            return None;
        }

        let len = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
        let total = HEADER_SIZE + len;

        if data.len() >= total {
            Some(total)
        } else {
            None
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::rpc::{RequestVote, VoteResponse};

        #[test]
        fn test_encode_decode_roundtrip() {
            let from = 1;
            let message = RaftMessage::RequestVote(RequestVote::new(5, 1, 100, 4));

            let encoded = encode(from, &message).unwrap();
            let (decoded_from, decoded_message) = decode(encoded).unwrap();

            assert_eq!(decoded_from, from);
            assert_eq!(decoded_message, message);
        }

        #[test]
        fn test_encode_decode_vote_response() {
            let from = 2;
            let message = RaftMessage::VoteResponse(VoteResponse::grant(5));

            let encoded = encode(from, &message).unwrap();
            let (decoded_from, decoded_message) = decode(encoded).unwrap();

            assert_eq!(decoded_from, from);
            assert_eq!(decoded_message, message);
        }

        #[test]
        fn test_invalid_magic() {
            let mut data = BytesMut::new();
            data.put_u32(0xDEADBEEF); // Invalid magic
            data.put_u64(1);
            data.put_u32(0);

            let result = decode(data.freeze());
            assert!(matches!(result, Err(TransportError::DeserializationFailed(_))));
        }

        #[test]
        fn test_frame_size() {
            let from = 1;
            let message = RaftMessage::VoteResponse(VoteResponse::grant(5));
            let encoded = encode(from, &message).unwrap();

            // Complete frame
            assert_eq!(frame_size(&encoded), Some(encoded.len()));

            // Incomplete header
            assert_eq!(frame_size(&encoded[..10]), None);

            // Incomplete payload
            assert_eq!(frame_size(&encoded[..encoded.len() - 1]), None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_addr() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let node_addr = NodeAddr::new(1, addr);
        assert_eq!(node_addr.node_id, 1);
        assert_eq!(node_addr.addr, addr);
    }

    #[test]
    fn test_incoming_message() {
        let msg = IncomingMessage::new(1, RaftMessage::VoteResponse(crate::rpc::VoteResponse::grant(5)));
        assert_eq!(msg.from, 1);
    }

    #[test]
    fn test_transport_error_display() {
        let err = TransportError::ConnectionFailed {
            node_id: 2,
            reason: "connection refused".to_string(),
        };
        assert!(err.to_string().contains("node 2"));
        assert!(err.to_string().contains("connection refused"));

        let err = TransportError::UnknownNode(3);
        assert!(err.to_string().contains("3"));
    }
}
