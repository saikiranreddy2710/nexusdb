//! In-memory transport for testing.
//!
//! This transport uses channels for communication, making it ideal for
//! unit tests and simulations. It can simulate network conditions like
//! delays and partitions.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::sync::Mutex as TokioMutex;

use crate::rpc::{NodeId, RaftMessage};

use super::{IncomingMessage, NodeAddr, Transport, TransportError, TransportResult};

/// Channel capacity for each connection.
const CHANNEL_CAPACITY: usize = 1000;

/// A shared network for memory transports.
///
/// All `MemoryTransport` instances connected to the same `MemoryNetwork`
/// can communicate with each other.
#[derive(Debug)]
pub struct MemoryNetwork {
    /// Senders to each node's inbox.
    nodes: DashMap<NodeId, mpsc::Sender<IncomingMessage>>,
    /// Network partitions: (from, to) pairs that are blocked.
    partitions: RwLock<Vec<(NodeId, NodeId)>>,
}

impl Default for MemoryNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryNetwork {
    /// Creates a new memory network.
    pub fn new() -> Self {
        Self {
            nodes: DashMap::new(),
            partitions: RwLock::new(Vec::new()),
        }
    }

    /// Creates a new memory network wrapped in Arc.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Creates a new transport connected to this network.
    pub fn create_transport(self: &Arc<Self>, node_id: NodeId) -> MemoryTransport {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        self.nodes.insert(node_id, tx);

        MemoryTransport {
            node_id,
            network: Arc::clone(self),
            inbox: TokioMutex::new(rx),
            closed: AtomicBool::new(false),
        }
    }

    /// Adds a network partition between two nodes.
    ///
    /// Messages from `from` to `to` will be dropped (one-way partition).
    pub fn add_partition(&self, from: NodeId, to: NodeId) {
        self.partitions.write().push((from, to));
    }

    /// Adds a bidirectional partition between two nodes.
    pub fn add_partition_bidirectional(&self, node1: NodeId, node2: NodeId) {
        let mut partitions = self.partitions.write();
        partitions.push((node1, node2));
        partitions.push((node2, node1));
    }

    /// Removes a network partition.
    pub fn remove_partition(&self, from: NodeId, to: NodeId) {
        self.partitions.write().retain(|&(f, t)| f != from || t != to);
    }

    /// Removes all partitions involving a node.
    pub fn heal_node(&self, node_id: NodeId) {
        self.partitions
            .write()
            .retain(|&(f, t)| f != node_id && t != node_id);
    }

    /// Removes all partitions.
    pub fn heal_all(&self) {
        self.partitions.write().clear();
    }

    /// Checks if there's a partition from `from` to `to`.
    pub fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        self.partitions.read().iter().any(|&(f, t)| f == from && t == to)
    }

    /// Sends a message through the network.
    fn send(&self, from: NodeId, to: NodeId, message: RaftMessage) -> TransportResult<()> {
        // Check for partition
        if self.is_partitioned(from, to) {
            // Silently drop the message (simulates network failure)
            return Ok(());
        }

        let sender = self
            .nodes
            .get(&to)
            .ok_or(TransportError::UnknownNode(to))?;

        let msg = IncomingMessage::new(from, message);

        // Use try_send to avoid blocking
        sender
            .try_send(msg)
            .map_err(|_| TransportError::ConnectionFailed {
                node_id: to,
                reason: "channel full or closed".to_string(),
            })
    }

    /// Returns all connected node IDs.
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|e| *e.key()).collect()
    }
}

/// In-memory transport for testing.
///
/// Uses channels for communication between nodes in the same process.
/// Supports network partition simulation for testing failure scenarios.
pub struct MemoryTransport {
    /// This node's ID.
    node_id: NodeId,
    /// The shared network.
    network: Arc<MemoryNetwork>,
    /// Incoming message channel.
    inbox: TokioMutex<mpsc::Receiver<IncomingMessage>>,
    /// Whether the transport is closed.
    closed: AtomicBool,
}

impl std::fmt::Debug for MemoryTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryTransport")
            .field("node_id", &self.node_id)
            .field("closed", &self.closed)
            .finish()
    }
}

impl Transport for MemoryTransport {
    fn send(&self, to: NodeId, message: RaftMessage) -> TransportResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Closed);
        }
        self.network.send(self.node_id, to, message)
    }

    fn send_wait(
        &self,
        to: NodeId,
        message: RaftMessage,
    ) -> Pin<Box<dyn Future<Output = TransportResult<()>> + Send + '_>> {
        Box::pin(async move { self.send(to, message) })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Option<IncomingMessage>> + Send + '_>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return None;
            }

            let mut inbox = self.inbox.lock().await;
            inbox.recv().await
        })
    }

    fn try_recv(&self) -> Option<IncomingMessage> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }

        // Use try_lock to avoid blocking
        match self.inbox.try_lock() {
            Ok(mut inbox) => inbox.try_recv().ok(),
            Err(_) => None,
        }
    }

    fn broadcast(&self, message: RaftMessage) -> TransportResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Closed);
        }

        for peer_id in self.peers() {
            // Ignore errors for individual sends
            let _ = self.send(peer_id, message.clone());
        }
        Ok(())
    }

    fn local_id(&self) -> NodeId {
        self.node_id
    }

    fn peers(&self) -> Vec<NodeId> {
        self.network
            .node_ids()
            .into_iter()
            .filter(|&id| id != self.node_id)
            .collect()
    }

    fn add_peer(&self, _addr: NodeAddr) -> TransportResult<()> {
        // In memory transport, peers are added through the network
        Ok(())
    }

    fn remove_peer(&self, _node_id: NodeId) -> TransportResult<()> {
        // In memory transport, peers are removed through the network
        Ok(())
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.closed.store(true, Ordering::Release);
            self.network.nodes.remove(&self.node_id);
        })
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// A cluster of memory transports for testing.
///
/// Provides convenient methods for creating and managing a test cluster.
pub struct MemoryCluster {
    /// The shared network.
    network: Arc<MemoryNetwork>,
    /// Node addresses (for API compatibility).
    addrs: HashMap<NodeId, NodeAddr>,
}

impl MemoryCluster {
    /// Creates a new cluster with the given node IDs.
    pub fn new(node_ids: &[NodeId]) -> (Self, Vec<MemoryTransport>) {
        let network = MemoryNetwork::shared();
        let mut transports = Vec::new();

        for &node_id in node_ids {
            transports.push(network.create_transport(node_id));
        }

        let addrs = node_ids
            .iter()
            .map(|&id| {
                let addr = format!("127.0.0.1:{}", 8000 + id).parse().unwrap();
                (id, NodeAddr::new(id, addr))
            })
            .collect();

        (Self { network, addrs }, transports)
    }

    /// Returns the shared network.
    pub fn network(&self) -> &Arc<MemoryNetwork> {
        &self.network
    }

    /// Partitions a node from all others.
    pub fn isolate_node(&self, node_id: NodeId) {
        for &other_id in self.addrs.keys() {
            if other_id != node_id {
                self.network.add_partition_bidirectional(node_id, other_id);
            }
        }
    }

    /// Heals all partitions for a node.
    pub fn reconnect_node(&self, node_id: NodeId) {
        self.network.heal_node(node_id);
    }

    /// Heals all partitions.
    pub fn heal_all(&self) {
        self.network.heal_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{RequestVote, VoteResponse};

    #[tokio::test]
    async fn test_memory_transport_send_recv() {
        let network = MemoryNetwork::shared();
        let transport1 = network.create_transport(1);
        let transport2 = network.create_transport(2);

        // Send from node 1 to node 2
        let message = RaftMessage::RequestVote(RequestVote::new(5, 1, 100, 4));
        transport1.send(2, message.clone()).unwrap();

        // Receive on node 2
        let received = transport2.recv().await.unwrap();
        assert_eq!(received.from, 1);
        assert_eq!(received.message, message);
    }

    #[tokio::test]
    async fn test_memory_transport_broadcast() {
        let network = MemoryNetwork::shared();
        let transport1 = network.create_transport(1);
        let transport2 = network.create_transport(2);
        let transport3 = network.create_transport(3);

        // Broadcast from node 1
        let message = RaftMessage::VoteResponse(VoteResponse::grant(5));
        transport1.broadcast(message.clone()).unwrap();

        // Both node 2 and node 3 should receive
        let msg2 = transport2.recv().await.unwrap();
        let msg3 = transport3.recv().await.unwrap();

        assert_eq!(msg2.from, 1);
        assert_eq!(msg3.from, 1);
    }

    #[tokio::test]
    async fn test_memory_transport_partition() {
        let network = MemoryNetwork::shared();
        let transport1 = network.create_transport(1);
        let transport2 = network.create_transport(2);

        // Add partition from 1 to 2
        network.add_partition(1, 2);

        // Send from 1 to 2 - should be silently dropped
        let message = RaftMessage::VoteResponse(VoteResponse::grant(5));
        transport1.send(2, message.clone()).unwrap();

        // Node 2 shouldn't receive anything
        let received = transport2.try_recv();
        assert!(received.is_none());

        // But 2 can still send to 1
        transport2.send(1, message.clone()).unwrap();
        let received = transport1.recv().await.unwrap();
        assert_eq!(received.from, 2);
    }

    #[tokio::test]
    async fn test_memory_transport_bidirectional_partition() {
        let network = MemoryNetwork::shared();
        let transport1 = network.create_transport(1);
        let transport2 = network.create_transport(2);

        // Add bidirectional partition
        network.add_partition_bidirectional(1, 2);

        let message = RaftMessage::VoteResponse(VoteResponse::grant(5));

        // Neither can send to the other
        transport1.send(2, message.clone()).unwrap();
        transport2.send(1, message.clone()).unwrap();

        assert!(transport1.try_recv().is_none());
        assert!(transport2.try_recv().is_none());

        // Heal the partition
        network.heal_all();

        // Now they can communicate
        transport1.send(2, message.clone()).unwrap();
        let received = transport2.recv().await.unwrap();
        assert_eq!(received.from, 1);
    }

    #[tokio::test]
    async fn test_memory_cluster() {
        let (cluster, transports) = MemoryCluster::new(&[1, 2, 3]);
        let [t1, t2, t3]: [MemoryTransport; 3] = transports.try_into().unwrap();

        // All nodes can communicate
        let message = RaftMessage::VoteResponse(VoteResponse::grant(5));
        t1.send(2, message.clone()).unwrap();
        t1.send(3, message.clone()).unwrap();

        assert!(t2.recv().await.is_some());
        assert!(t3.recv().await.is_some());

        // Isolate node 1
        cluster.isolate_node(1);

        // Node 1 can't send anymore
        t1.send(2, message.clone()).unwrap();
        assert!(t2.try_recv().is_none());

        // Reconnect
        cluster.reconnect_node(1);
        t1.send(2, message.clone()).unwrap();
        assert!(t2.recv().await.is_some());
    }

    #[tokio::test]
    async fn test_memory_transport_close() {
        let network = MemoryNetwork::shared();
        let transport1 = network.create_transport(1);
        let transport2 = network.create_transport(2);

        // Close transport 1
        transport1.close().await;

        assert!(transport1.is_closed());

        // Sending should fail
        let message = RaftMessage::VoteResponse(VoteResponse::grant(5));
        let result = transport1.send(2, message.clone());
        assert!(matches!(result, Err(TransportError::Closed)));

        // Receiving should return None
        assert!(transport1.try_recv().is_none());

        // Others can't send to closed transport
        let result = transport2.send(1, message);
        assert!(matches!(result, Err(TransportError::UnknownNode(1))));
    }

    #[test]
    fn test_peers() {
        let network = MemoryNetwork::shared();
        let transport1 = network.create_transport(1);
        let _transport2 = network.create_transport(2);
        let _transport3 = network.create_transport(3);

        let peers = transport1.peers();
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&2));
        assert!(peers.contains(&3));
        assert!(!peers.contains(&1));
    }
}
