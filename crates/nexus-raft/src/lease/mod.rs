//! Leader leases for linearizable local reads.
//!
//! This module implements leader leases, which allow a leader to serve reads
//! locally without contacting a quorum, while still maintaining linearizability.
//!
//! # How Leases Work
//!
//! 1. The leader maintains a lease that is renewed by heartbeat acknowledgments
//! 2. The lease has a duration shorter than the election timeout
//! 3. While the lease is valid, no other node can become leader
//! 4. Therefore, the current leader can serve reads locally
//!
//! # Safety
//!
//! Leases depend on bounded clock drift between nodes. The lease duration
//! must be significantly shorter than the minimum election timeout to account
//! for clock skew.
//!
//! ```text
//! lease_duration < min_election_timeout - max_clock_drift
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use nexus_raft::lease::{LeaseManager, LeaseConfig};
//!
//! let config = LeaseConfig::default();
//! let mut lease = LeaseManager::new(config);
//!
//! // On heartbeat response from peer
//! lease.on_heartbeat_response(peer_id, now);
//!
//! // Check if we can serve a read locally
//! if lease.can_read_locally(now) {
//!     // Serve read without quorum
//! }
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::rpc::NodeId;

/// Configuration for leader leases.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// Duration of the lease. Must be shorter than election timeout.
    pub lease_duration: Duration,
    /// Minimum number of responses needed to establish a lease.
    /// Usually a quorum.
    pub min_responses_for_lease: usize,
    /// Maximum clock drift to assume between nodes.
    pub max_clock_drift: Duration,
    /// Whether to enable lease-based reads.
    pub enabled: bool,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            // Default election timeout is ~150-300ms, so use 100ms lease
            lease_duration: Duration::from_millis(100),
            min_responses_for_lease: 1, // Will be overridden based on cluster size
            max_clock_drift: Duration::from_millis(10),
            enabled: true,
        }
    }
}

impl LeaseConfig {
    /// Creates a new lease config for a cluster of the given size.
    pub fn for_cluster_size(size: usize) -> Self {
        Self {
            min_responses_for_lease: size / 2, // Quorum minus self
            ..Default::default()
        }
    }

    /// Sets the lease duration.
    pub fn with_lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    /// Disables lease-based reads.
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

/// Tracks a lease for a leader to serve local reads.
#[derive(Debug)]
pub struct LeaseManager {
    /// Configuration.
    config: LeaseConfig,
    /// When the current lease expires.
    lease_expiry: Option<Instant>,
    /// Last heartbeat response time from each peer.
    peer_responses: HashMap<NodeId, Instant>,
    /// When we last renewed the lease.
    last_renewal: Option<Instant>,
    /// Number of peers in the cluster (excluding self).
    peer_count: usize,
}

impl LeaseManager {
    /// Creates a new lease manager.
    pub fn new(config: LeaseConfig) -> Self {
        Self {
            config,
            lease_expiry: None,
            peer_responses: HashMap::new(),
            last_renewal: None,
            peer_count: 0,
        }
    }

    /// Creates a new lease manager for a cluster of the given size.
    pub fn for_cluster(cluster_size: usize) -> Self {
        let peer_count = cluster_size.saturating_sub(1);
        Self {
            config: LeaseConfig::for_cluster_size(cluster_size),
            lease_expiry: None,
            peer_responses: HashMap::new(),
            last_renewal: None,
            peer_count,
        }
    }

    /// Updates the peer count (e.g., after membership change).
    pub fn set_peer_count(&mut self, count: usize) {
        self.peer_count = count;
        self.config.min_responses_for_lease = (count + 1) / 2; // Quorum minus self
    }

    /// Returns true if lease-based reads are enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Returns true if we currently hold a valid lease.
    pub fn has_lease(&self, now: Instant) -> bool {
        if !self.config.enabled {
            return false;
        }
        self.lease_expiry.map_or(false, |expiry| now < expiry)
    }

    /// Returns true if we can serve a read locally.
    ///
    /// This is true if:
    /// 1. Leases are enabled
    /// 2. We currently hold a valid lease
    pub fn can_read_locally(&self, now: Instant) -> bool {
        self.has_lease(now)
    }

    /// Returns the time remaining on the lease, if any.
    pub fn lease_remaining(&self, now: Instant) -> Option<Duration> {
        self.lease_expiry
            .filter(|&expiry| now < expiry)
            .map(|expiry| expiry.duration_since(now))
    }

    /// Called when we become leader.
    pub fn on_become_leader(&mut self, now: Instant) {
        self.peer_responses.clear();
        self.lease_expiry = None;
        self.last_renewal = Some(now);
    }

    /// Called when we step down from leader.
    pub fn on_step_down(&mut self) {
        self.peer_responses.clear();
        self.lease_expiry = None;
        self.last_renewal = None;
    }

    /// Called when we receive a successful heartbeat response from a peer.
    ///
    /// This extends the lease if we have enough responses.
    pub fn on_heartbeat_response(&mut self, peer_id: NodeId, now: Instant) {
        if !self.config.enabled {
            return;
        }

        // Record the response
        self.peer_responses.insert(peer_id, now);

        // Clean up old responses (older than 2x lease duration)
        let cutoff = now - (self.config.lease_duration * 2);
        self.peer_responses.retain(|_, &mut time| time > cutoff);

        // Check if we have enough recent responses for a lease
        self.maybe_renew_lease(now);
    }

    /// Called on each heartbeat interval to check lease status.
    pub fn on_tick(&mut self, now: Instant) {
        if !self.config.enabled {
            return;
        }

        // Clean up old responses
        let cutoff = now - self.config.lease_duration;
        self.peer_responses.retain(|_, &mut time| time > cutoff);

        // Maybe renew lease
        self.maybe_renew_lease(now);
    }

    /// Attempts to renew the lease if we have enough recent responses.
    fn maybe_renew_lease(&mut self, now: Instant) {
        let recent_cutoff = now - self.config.lease_duration;
        let recent_responses = self
            .peer_responses
            .values()
            .filter(|&&time| time > recent_cutoff)
            .count();

        // Need responses from a quorum (excluding ourselves)
        // For a 5-node cluster, we need 2 responses (quorum is 3, minus self)
        if recent_responses >= self.config.min_responses_for_lease {
            let new_expiry = now + self.config.lease_duration - self.config.max_clock_drift;
            self.lease_expiry = Some(new_expiry);
            self.last_renewal = Some(now);
        }
    }

    /// Returns the number of peers that have responded recently.
    pub fn recent_response_count(&self, now: Instant) -> usize {
        let cutoff = now - self.config.lease_duration;
        self.peer_responses
            .values()
            .filter(|&&time| time > cutoff)
            .count()
    }

    /// Returns the configuration.
    pub fn config(&self) -> &LeaseConfig {
        &self.config
    }

    /// Returns when the lease expires, if valid.
    pub fn lease_expiry(&self) -> Option<Instant> {
        self.lease_expiry
    }

    /// Returns the last lease renewal time.
    pub fn last_renewal(&self) -> Option<Instant> {
        self.last_renewal
    }
}

/// Read-only request tracking for linearizable reads.
///
/// This tracks pending read requests that need to wait for a read index
/// to be applied or for the lease to be confirmed.
#[derive(Debug)]
pub struct ReadIndex {
    /// The log index that must be applied before serving the read.
    pub index: u64,
    /// When the read request was made.
    pub requested_at: Instant,
    /// Optional request ID for correlation.
    pub request_id: Option<u64>,
}

impl ReadIndex {
    /// Creates a new read index request.
    pub fn new(index: u64, now: Instant) -> Self {
        Self {
            index,
            requested_at: now,
            request_id: None,
        }
    }

    /// Creates a new read index request with an ID.
    pub fn with_id(index: u64, now: Instant, id: u64) -> Self {
        Self {
            index,
            requested_at: now,
            request_id: Some(id),
        }
    }

    /// Returns true if the read can be served (index has been applied).
    pub fn is_ready(&self, last_applied: u64) -> bool {
        last_applied >= self.index
    }
}

/// Manages pending read requests.
#[derive(Debug)]
pub struct ReadIndexQueue {
    /// Pending read requests.
    pending: Vec<ReadIndex>,
    /// Maximum age for a pending request before it times out.
    max_age: Duration,
}

impl ReadIndexQueue {
    /// Creates a new read index queue.
    pub fn new(max_age: Duration) -> Self {
        Self {
            pending: Vec::new(),
            max_age,
        }
    }

    /// Adds a read request.
    pub fn add(&mut self, read_index: ReadIndex) {
        self.pending.push(read_index);
    }

    /// Removes and returns all reads that can be served.
    pub fn drain_ready(&mut self, last_applied: u64, now: Instant) -> Vec<ReadIndex> {
        let mut ready = Vec::new();
        let mut remaining = Vec::new();

        for read in self.pending.drain(..) {
            let age = now.duration_since(read.requested_at);

            if read.is_ready(last_applied) {
                ready.push(read);
            } else if age < self.max_age {
                remaining.push(read);
            }
            // Else: timed out, drop it
        }

        self.pending = remaining;
        ready
    }

    /// Returns the number of pending reads.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Returns true if there are no pending reads.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Clears all pending reads.
    pub fn clear(&mut self) {
        self.pending.clear();
    }

    /// Removes and returns all timed-out reads.
    pub fn drain_timed_out(&mut self, now: Instant) -> Vec<ReadIndex> {
        let mut timed_out = Vec::new();
        let mut remaining = Vec::new();

        for read in self.pending.drain(..) {
            let age = now.duration_since(read.requested_at);
            if age >= self.max_age {
                timed_out.push(read);
            } else {
                remaining.push(read);
            }
        }

        self.pending = remaining;
        timed_out
    }
}

/// Strategy for serving reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStrategy {
    /// Serve reads through Raft (adds latency but always linearizable).
    ThroughRaft,
    /// Use read index protocol (one round trip to confirm leadership).
    ReadIndex,
    /// Use leader lease (no round trip, requires clock synchronization).
    LeaderLease,
}

impl Default for ReadStrategy {
    fn default() -> Self {
        Self::ReadIndex
    }
}

impl ReadStrategy {
    /// Returns true if this strategy requires clock synchronization.
    pub fn requires_clock_sync(&self) -> bool {
        matches!(self, Self::LeaderLease)
    }

    /// Returns true if this strategy requires a quorum round-trip.
    pub fn requires_quorum_roundtrip(&self) -> bool {
        matches!(self, Self::ThroughRaft | Self::ReadIndex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_config_default() {
        let config = LeaseConfig::default();
        assert!(config.enabled);
        assert!(config.lease_duration < Duration::from_millis(200));
    }

    #[test]
    fn test_lease_config_for_cluster() {
        let config = LeaseConfig::for_cluster_size(5);
        // 5-node cluster: quorum is 3, minus self = 2
        assert_eq!(config.min_responses_for_lease, 2);
    }

    #[test]
    fn test_lease_manager_new() {
        let manager = LeaseManager::for_cluster(5);
        assert!(!manager.has_lease(Instant::now()));
        assert!(manager.can_read_locally(Instant::now()) == false);
    }

    #[test]
    fn test_lease_manager_disabled() {
        let config = LeaseConfig::default().disabled();
        let manager = LeaseManager::new(config);
        assert!(!manager.is_enabled());
        assert!(!manager.has_lease(Instant::now()));
    }

    #[test]
    fn test_lease_renewal() {
        let mut manager = LeaseManager::for_cluster(3); // 3-node cluster
        let now = Instant::now();

        manager.on_become_leader(now);

        // Need 1 response for quorum (3/2 = 1, minus self handled differently)
        // With min_responses_for_lease = 1, one response should work
        assert_eq!(manager.config.min_responses_for_lease, 1);

        // No lease yet
        assert!(!manager.has_lease(now));

        // Receive response from peer 2
        manager.on_heartbeat_response(2, now);

        // Should have lease now
        assert!(manager.has_lease(now));
        assert!(manager.can_read_locally(now));

        // Check remaining time
        let remaining = manager.lease_remaining(now).unwrap();
        assert!(remaining > Duration::from_millis(0));
    }

    #[test]
    fn test_lease_expiry() {
        let mut manager = LeaseManager::for_cluster(3);
        let now = Instant::now();

        manager.on_become_leader(now);
        manager.on_heartbeat_response(2, now);

        assert!(manager.has_lease(now));

        // After lease duration, should expire
        let future = now + manager.config.lease_duration + Duration::from_millis(50);
        assert!(!manager.has_lease(future));
    }

    #[test]
    fn test_lease_step_down() {
        let mut manager = LeaseManager::for_cluster(3);
        let now = Instant::now();

        manager.on_become_leader(now);
        manager.on_heartbeat_response(2, now);
        assert!(manager.has_lease(now));

        manager.on_step_down();
        assert!(!manager.has_lease(now));
    }

    #[test]
    fn test_read_index() {
        let now = Instant::now();
        let read = ReadIndex::new(100, now);

        assert!(!read.is_ready(50));
        assert!(!read.is_ready(99));
        assert!(read.is_ready(100));
        assert!(read.is_ready(150));
    }

    #[test]
    fn test_read_index_queue() {
        let now = Instant::now();
        let mut queue = ReadIndexQueue::new(Duration::from_secs(5));

        queue.add(ReadIndex::new(10, now));
        queue.add(ReadIndex::new(20, now));
        queue.add(ReadIndex::new(30, now));

        assert_eq!(queue.len(), 3);

        // Drain ready (last_applied = 15)
        let ready = queue.drain_ready(15, now);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].index, 10);
        assert_eq!(queue.len(), 2);

        // Drain more (last_applied = 25)
        let ready = queue.drain_ready(25, now);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].index, 20);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_read_index_queue_timeout() {
        let now = Instant::now();
        let mut queue = ReadIndexQueue::new(Duration::from_millis(100));

        queue.add(ReadIndex::new(100, now));
        queue.add(ReadIndex::new(200, now + Duration::from_millis(50)));

        // After 100ms, first request should time out
        let future = now + Duration::from_millis(120);
        let timed_out = queue.drain_timed_out(future);

        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].index, 100);
        assert_eq!(queue.len(), 1); // Second one still pending
    }

    #[test]
    fn test_read_strategy() {
        assert!(ReadStrategy::LeaderLease.requires_clock_sync());
        assert!(!ReadStrategy::ReadIndex.requires_clock_sync());

        assert!(!ReadStrategy::LeaderLease.requires_quorum_roundtrip());
        assert!(ReadStrategy::ReadIndex.requires_quorum_roundtrip());
        assert!(ReadStrategy::ThroughRaft.requires_quorum_roundtrip());
    }

    #[test]
    fn test_lease_recent_responses() {
        let mut manager = LeaseManager::for_cluster(5);
        let now = Instant::now();

        manager.on_become_leader(now);
        manager.on_heartbeat_response(2, now);
        manager.on_heartbeat_response(3, now);

        assert_eq!(manager.recent_response_count(now), 2);

        // After lease duration, responses are old
        let future = now + manager.config.lease_duration + Duration::from_millis(10);
        assert_eq!(manager.recent_response_count(future), 0);
    }

    #[test]
    fn test_lease_requires_quorum() {
        let mut manager = LeaseManager::for_cluster(5);
        let now = Instant::now();

        manager.on_become_leader(now);

        // 5-node cluster needs 2 responses
        assert_eq!(manager.config.min_responses_for_lease, 2);

        // One response - no lease
        manager.on_heartbeat_response(2, now);
        assert!(!manager.has_lease(now));

        // Second response - has lease
        manager.on_heartbeat_response(3, now);
        assert!(manager.has_lease(now));
    }
}
