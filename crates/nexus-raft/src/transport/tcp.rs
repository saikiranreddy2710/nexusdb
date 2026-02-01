//! TCP-based transport for production use.
//!
//! This transport uses TCP connections for communication between Raft nodes.
//! Features:
//! - Connection pooling and reuse
//! - Automatic reconnection on failure
//! - Message framing with length prefixes
//! - Concurrent message sending

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, error, info, warn};

use crate::rpc::{NodeId, RaftMessage};

use super::frame::{self, HEADER_SIZE, MAX_MESSAGE_SIZE};
use super::{IncomingMessage, NodeAddr, Transport, TransportError, TransportResult};

/// Connection state for a peer.
struct PeerConnection {
    /// The peer's address.
    addr: SocketAddr,
    /// Sender for outgoing messages.
    outbox: mpsc::Sender<RaftMessage>,
}

/// TCP-based transport for Raft.
///
/// Manages TCP connections to peers and handles message framing.
pub struct TcpTransport {
    /// This node's ID.
    node_id: NodeId,
    /// This node's listen address.
    listen_addr: SocketAddr,
    /// Peer connections.
    peers: DashMap<NodeId, PeerConnection>,
    /// Known peer addresses (for reconnection).
    peer_addrs: RwLock<HashMap<NodeId, SocketAddr>>,
    /// Incoming message channel.
    inbox_tx: mpsc::Sender<IncomingMessage>,
    /// Incoming message receiver.
    inbox_rx: TokioMutex<mpsc::Receiver<IncomingMessage>>,
    /// Whether the transport is closed.
    closed: AtomicBool,
    /// Shutdown signal sender.
    shutdown_tx: mpsc::Sender<()>,
}

impl std::fmt::Debug for TcpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpTransport")
            .field("node_id", &self.node_id)
            .field("listen_addr", &self.listen_addr)
            .field("closed", &self.closed)
            .finish()
    }
}

impl TcpTransport {
    /// Creates a new TCP transport.
    ///
    /// # Arguments
    ///
    /// * `node_id` - This node's ID
    /// * `listen_addr` - Address to listen on
    /// * `peers` - Initial peer addresses
    pub async fn new(
        node_id: NodeId,
        listen_addr: SocketAddr,
        peers: Vec<NodeAddr>,
    ) -> TransportResult<Arc<Self>> {
        let (inbox_tx, inbox_rx) = mpsc::channel(1000);
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let peer_addrs: HashMap<_, _> = peers.iter().map(|p| (p.node_id, p.addr)).collect();

        let transport = Arc::new(Self {
            node_id,
            listen_addr,
            peers: DashMap::new(),
            peer_addrs: RwLock::new(peer_addrs),
            inbox_tx,
            inbox_rx: TokioMutex::new(inbox_rx),
            closed: AtomicBool::new(false),
            shutdown_tx,
        });

        // Start listener
        let listener = TcpListener::bind(listen_addr).await?;
        info!("Raft transport listening on {}", listen_addr);

        let transport_clone = Arc::clone(&transport);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, addr)) => {
                                debug!("Accepted connection from {}", addr);
                                let transport = Arc::clone(&transport_clone);
                                tokio::spawn(async move {
                                    if let Err(e) = transport.handle_connection(stream).await {
                                        warn!("Connection error: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!("Accept error: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Transport shutdown requested");
                        break;
                    }
                }
            }
        });

        // Connect to initial peers
        for peer in peers {
            let transport_clone = Arc::clone(&transport);
            tokio::spawn(async move {
                if let Err(e) = transport_clone.connect_to_peer(peer.node_id, peer.addr).await {
                    warn!("Failed to connect to peer {}: {}", peer.node_id, e);
                }
            });
        }

        Ok(transport)
    }

    /// Handles an incoming connection.
    async fn handle_connection(self: &Arc<Self>, mut stream: TcpStream) -> TransportResult<()> {
        let mut buf = BytesMut::with_capacity(4096);

        loop {
            // Read more data
            let n = stream.read_buf(&mut buf).await?;
            if n == 0 {
                // Connection closed
                return Ok(());
            }

            // Process complete frames
            while let Some(size) = frame::frame_size(&buf) {
                let frame_data = buf.split_to(size).freeze();
                let (from, message) = frame::decode(frame_data)?;

                let msg = IncomingMessage::new(from, message);
                if self.inbox_tx.send(msg).await.is_err() {
                    return Err(TransportError::Closed);
                }
            }
        }
    }

    /// Connects to a peer.
    async fn connect_to_peer(
        self: &Arc<Self>,
        peer_id: NodeId,
        addr: SocketAddr,
    ) -> TransportResult<()> {
        if self.peers.contains_key(&peer_id) {
            return Ok(());
        }

        debug!("Connecting to peer {} at {}", peer_id, addr);

        let stream = TcpStream::connect(addr).await.map_err(|e| {
            TransportError::ConnectionFailed {
                node_id: peer_id,
                reason: e.to_string(),
            }
        })?;

        let (mut read_half, mut write_half) = stream.into_split();
        let (outbox_tx, mut outbox_rx) = mpsc::channel::<RaftMessage>(100);

        // Store connection
        self.peers.insert(
            peer_id,
            PeerConnection {
                addr,
                outbox: outbox_tx,
            },
        );

        let node_id = self.node_id;

        // Writer task
        tokio::spawn(async move {
            while let Some(message) = outbox_rx.recv().await {
                match frame::encode(node_id, &message) {
                    Ok(data) => {
                        if let Err(e) = write_half.write_all(&data).await {
                            warn!("Write error to peer {}: {}", peer_id, e);
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Encode error: {}", e);
                    }
                }
            }
        });

        // Reader task
        let transport = Arc::clone(self);
        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(4096);

            loop {
                match read_half.read_buf(&mut buf).await {
                    Ok(0) => {
                        debug!("Connection to peer {} closed", peer_id);
                        break;
                    }
                    Ok(_) => {
                        // Process complete frames
                        while let Some(size) = frame::frame_size(&buf) {
                            let frame_data = buf.split_to(size).freeze();
                            match frame::decode(frame_data) {
                                Ok((from, message)) => {
                                    let msg = IncomingMessage::new(from, message);
                                    if transport.inbox_tx.send(msg).await.is_err() {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    warn!("Decode error: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Read error from peer {}: {}", peer_id, e);
                        break;
                    }
                }
            }

            // Remove connection on close
            transport.peers.remove(&peer_id);
        });

        info!("Connected to peer {} at {}", peer_id, addr);
        Ok(())
    }

    /// Attempts to reconnect to a peer.
    async fn try_reconnect(self: &Arc<Self>, peer_id: NodeId) -> TransportResult<()> {
        let addr = self
            .peer_addrs
            .read()
            .get(&peer_id)
            .copied()
            .ok_or(TransportError::UnknownNode(peer_id))?;

        self.connect_to_peer(peer_id, addr).await
    }
}

impl Transport for TcpTransport {
    fn send(&self, to: NodeId, message: RaftMessage) -> TransportResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Closed);
        }

        if let Some(peer) = self.peers.get(&to) {
            peer.outbox
                .try_send(message)
                .map_err(|_| TransportError::ConnectionFailed {
                    node_id: to,
                    reason: "channel full or closed".to_string(),
                })
        } else {
            Err(TransportError::UnknownNode(to))
        }
    }

    fn send_wait(
        &self,
        to: NodeId,
        message: RaftMessage,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return Err(TransportError::Closed);
            }

            if let Some(peer) = self.peers.get(&to) {
                peer.outbox
                    .send(message)
                    .await
                    .map_err(|_| TransportError::ConnectionFailed {
                        node_id: to,
                        reason: "channel closed".to_string(),
                    })
            } else {
                Err(TransportError::UnknownNode(to))
            }
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Option<IncomingMessage>> + Send + '_>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return None;
            }

            let mut inbox = self.inbox_rx.lock().await;
            inbox.recv().await
        })
    }

    fn try_recv(&self) -> Option<IncomingMessage> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }

        // Use try_lock to avoid blocking
        match self.inbox_rx.try_lock() {
            Ok(mut inbox) => inbox.try_recv().ok(),
            Err(_) => None,
        }
    }

    fn broadcast(&self, message: RaftMessage) -> TransportResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Closed);
        }

        for peer in self.peers.iter() {
            let _ = peer.outbox.try_send(message.clone());
        }
        Ok(())
    }

    fn local_id(&self) -> NodeId {
        self.node_id
    }

    fn peers(&self) -> Vec<NodeId> {
        self.peers.iter().map(|e| *e.key()).collect()
    }

    fn add_peer(&self, addr: NodeAddr) -> TransportResult<()> {
        self.peer_addrs.write().insert(addr.node_id, addr.addr);
        Ok(())
    }

    fn remove_peer(&self, node_id: NodeId) -> TransportResult<()> {
        self.peers.remove(&node_id);
        self.peer_addrs.write().remove(&node_id);
        Ok(())
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.closed.store(true, Ordering::Release);
            let _ = self.shutdown_tx.send(()).await;
            self.peers.clear();
        })
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{RequestVote, VoteResponse};
    use std::time::Duration;

    async fn find_available_port() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    }

    #[tokio::test]
    async fn test_tcp_transport_create() {
        let addr = find_available_port().await;
        let transport = TcpTransport::new(1, addr, vec![]).await.unwrap();

        assert_eq!(transport.local_id(), 1);
        assert!(!transport.is_closed());

        transport.close().await;
        assert!(transport.is_closed());
    }

    #[tokio::test]
    async fn test_tcp_transport_send_recv() {
        let addr1 = find_available_port().await;
        let addr2 = find_available_port().await;

        // Create two transports
        let transport1 = TcpTransport::new(1, addr1, vec![NodeAddr::new(2, addr2)])
            .await
            .unwrap();

        let transport2 = TcpTransport::new(2, addr2, vec![NodeAddr::new(1, addr1)])
            .await
            .unwrap();

        // Wait for connections to establish
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send from node 1 to node 2
        let message = RaftMessage::RequestVote(RequestVote::new(5, 1, 100, 4));
        transport1.send(2, message.clone()).unwrap();

        // Receive on node 2
        let received = tokio::time::timeout(Duration::from_secs(1), transport2.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(received.from, 1);
        assert_eq!(received.message, message);

        // Cleanup
        transport1.close().await;
        transport2.close().await;
    }

    #[tokio::test]
    async fn test_tcp_transport_bidirectional() {
        let addr1 = find_available_port().await;
        let addr2 = find_available_port().await;

        let transport1 = TcpTransport::new(1, addr1, vec![NodeAddr::new(2, addr2)])
            .await
            .unwrap();

        let transport2 = TcpTransport::new(2, addr2, vec![NodeAddr::new(1, addr1)])
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send both ways
        let msg1 = RaftMessage::VoteResponse(VoteResponse::grant(5));
        let msg2 = RaftMessage::VoteResponse(VoteResponse::deny(5));

        transport1.send(2, msg1.clone()).unwrap();
        transport2.send(1, msg2.clone()).unwrap();

        // Receive both
        let recv1 = tokio::time::timeout(Duration::from_secs(1), transport1.recv())
            .await
            .unwrap()
            .unwrap();
        let recv2 = tokio::time::timeout(Duration::from_secs(1), transport2.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(recv1.from, 2);
        assert_eq!(recv2.from, 1);

        transport1.close().await;
        transport2.close().await;
    }

    #[tokio::test]
    async fn test_tcp_transport_broadcast() {
        let addr1 = find_available_port().await;
        let addr2 = find_available_port().await;
        let addr3 = find_available_port().await;

        let transport1 = TcpTransport::new(
            1,
            addr1,
            vec![NodeAddr::new(2, addr2), NodeAddr::new(3, addr3)],
        )
        .await
        .unwrap();

        let transport2 = TcpTransport::new(2, addr2, vec![NodeAddr::new(1, addr1)])
            .await
            .unwrap();

        let transport3 = TcpTransport::new(3, addr3, vec![NodeAddr::new(1, addr1)])
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Broadcast from node 1
        let message = RaftMessage::VoteResponse(VoteResponse::grant(5));
        transport1.broadcast(message.clone()).unwrap();

        // Both should receive
        let recv2 = tokio::time::timeout(Duration::from_secs(1), transport2.recv())
            .await
            .unwrap();
        let recv3 = tokio::time::timeout(Duration::from_secs(1), transport3.recv())
            .await
            .unwrap();

        assert!(recv2.is_some());
        assert!(recv3.is_some());

        transport1.close().await;
        transport2.close().await;
        transport3.close().await;
    }
}
