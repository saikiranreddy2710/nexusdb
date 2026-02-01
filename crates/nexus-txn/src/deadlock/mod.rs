//! Deadlock detection using wait-for graph analysis.
//!
//! This module implements deadlock detection for the transaction manager:
//! - Wait-for graph (WFG) to track transaction dependencies
//! - Cycle detection using DFS
//! - Victim selection for deadlock resolution
//!
//! # Wait-For Graph
//!
//! The WFG tracks which transactions are waiting for which:
//! ```text
//! T1 waits for T2:  T1 -> T2
//! T2 waits for T3:  T2 -> T3
//! T3 waits for T1:  T3 -> T1 (cycle = deadlock!)
//! ```
//!
//! # Deadlock Resolution
//!
//! When a deadlock is detected, we must abort one transaction.
//! Selection criteria:
//! 1. Prefer younger transactions (less work to redo)
//! 2. Consider transaction priority if set
//! 3. Consider amount of work done (locks held, writes made)

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Instant;

use nexus_common::types::TxnId;
use parking_lot::RwLock;

/// A node in the wait-for graph representing a transaction.
#[derive(Debug, Clone)]
pub struct WfgNode {
    /// Transaction ID.
    pub txn_id: TxnId,
    /// When this transaction started.
    pub start_time: Instant,
    /// Priority (higher = less likely to be victim).
    pub priority: i32,
    /// Number of locks held.
    pub locks_held: usize,
    /// Number of writes made.
    pub writes_made: usize,
}

impl WfgNode {
    /// Creates a new WFG node.
    pub fn new(txn_id: TxnId) -> Self {
        Self {
            txn_id,
            start_time: Instant::now(),
            priority: 0,
            locks_held: 0,
            writes_made: 0,
        }
    }

    /// Creates a node with all fields specified.
    pub fn with_details(
        txn_id: TxnId,
        start_time: Instant,
        priority: i32,
        locks_held: usize,
        writes_made: usize,
    ) -> Self {
        Self {
            txn_id,
            start_time,
            priority,
            locks_held,
            writes_made,
        }
    }

    /// Computes a "cost" for aborting this transaction.
    /// Higher cost = less desirable to abort.
    pub fn abort_cost(&self) -> i64 {
        let age_ms = self.start_time.elapsed().as_millis() as i64;
        let work_cost = (self.locks_held + self.writes_made * 2) as i64;
        let priority_cost = self.priority as i64 * 1000;

        age_ms + work_cost * 100 + priority_cost
    }
}

/// Result of deadlock detection.
#[derive(Debug, Clone)]
pub struct DeadlockInfo {
    /// The cycle of transactions involved.
    pub cycle: Vec<TxnId>,
    /// The selected victim to abort.
    pub victim: TxnId,
    /// When the deadlock was detected.
    pub detected_at: Instant,
}

/// The wait-for graph for deadlock detection.
pub struct WaitForGraph {
    /// Nodes in the graph.
    nodes: RwLock<HashMap<TxnId, WfgNode>>,
    /// Edges: waiter -> set of holders it's waiting for.
    edges: RwLock<HashMap<TxnId, HashSet<TxnId>>>,
    /// Statistics.
    stats: DeadlockStats,
}

/// Statistics about deadlock detection.
#[derive(Debug, Default)]
pub struct DeadlockStats {
    /// Number of detection checks performed.
    pub checks: AtomicU64,
    /// Number of deadlocks found.
    pub deadlocks_found: AtomicU64,
    /// Number of victims selected.
    pub victims_selected: AtomicU64,
}

impl DeadlockStats {
    /// Creates new stats.
    pub fn new() -> Self {
        Self::default()
    }
}

impl WaitForGraph {
    /// Creates a new empty wait-for graph.
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            edges: RwLock::new(HashMap::new()),
            stats: DeadlockStats::new(),
        }
    }

    /// Adds a transaction to the graph.
    pub fn add_txn(&self, txn_id: TxnId) {
        let mut nodes = self.nodes.write();
        nodes.entry(txn_id).or_insert_with(|| WfgNode::new(txn_id));
    }

    /// Adds a transaction with details.
    pub fn add_txn_with_details(&self, node: WfgNode) {
        self.nodes.write().insert(node.txn_id, node);
    }

    /// Removes a transaction from the graph.
    pub fn remove_txn(&self, txn_id: TxnId) {
        self.nodes.write().remove(&txn_id);
        self.edges.write().remove(&txn_id);

        // Remove edges to this transaction
        let mut edges = self.edges.write();
        for holders in edges.values_mut() {
            holders.remove(&txn_id);
        }
    }

    /// Adds a wait-for edge: waiter is waiting for holder.
    pub fn add_wait(&self, waiter: TxnId, holder: TxnId) {
        // Ensure both transactions are in the graph
        self.add_txn(waiter);
        self.add_txn(holder);

        // Add edge
        let mut edges = self.edges.write();
        edges
            .entry(waiter)
            .or_insert_with(HashSet::new)
            .insert(holder);
    }

    /// Removes a wait-for edge.
    pub fn remove_wait(&self, waiter: TxnId, holder: TxnId) {
        let mut edges = self.edges.write();
        if let Some(holders) = edges.get_mut(&waiter) {
            holders.remove(&holder);
            if holders.is_empty() {
                edges.remove(&waiter);
            }
        }
    }

    /// Clears all waits for a transaction.
    pub fn clear_waits(&self, waiter: TxnId) {
        self.edges.write().remove(&waiter);
    }

    /// Updates transaction details.
    pub fn update_txn(&self, txn_id: TxnId, locks_held: usize, writes_made: usize) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(&txn_id) {
            node.locks_held = locks_held;
            node.writes_made = writes_made;
        }
    }

    /// Detects if there's a cycle involving the given transaction.
    pub fn detect_deadlock(&self, start_txn: TxnId) -> Option<DeadlockInfo> {
        self.stats.checks.fetch_add(1, AtomicOrdering::Relaxed);

        let edges = self.edges.read();
        let mut visited = HashSet::new();
        let mut path = Vec::new();

        if self.dfs_find_cycle(start_txn, &edges, &mut visited, &mut path) {
            self.stats
                .deadlocks_found
                .fetch_add(1, AtomicOrdering::Relaxed);

            // Find the cycle in the path
            let cycle = self.extract_cycle(&path, start_txn);
            let victim = self.select_victim(&cycle);

            self.stats
                .victims_selected
                .fetch_add(1, AtomicOrdering::Relaxed);

            Some(DeadlockInfo {
                cycle,
                victim,
                detected_at: Instant::now(),
            })
        } else {
            None
        }
    }

    /// DFS to find cycles.
    fn dfs_find_cycle(
        &self,
        current: TxnId,
        edges: &HashMap<TxnId, HashSet<TxnId>>,
        visited: &mut HashSet<TxnId>,
        path: &mut Vec<TxnId>,
    ) -> bool {
        if path.contains(&current) {
            path.push(current);
            return true; // Cycle found
        }

        if visited.contains(&current) {
            return false; // Already explored
        }

        visited.insert(current);
        path.push(current);

        if let Some(holders) = edges.get(&current) {
            for &holder in holders {
                if self.dfs_find_cycle(holder, edges, visited, path) {
                    return true;
                }
            }
        }

        path.pop();
        false
    }

    /// Extracts the cycle from the DFS path.
    fn extract_cycle(&self, path: &[TxnId], cycle_start: TxnId) -> Vec<TxnId> {
        let mut cycle = Vec::new();
        let mut found_start = false;

        for &txn in path {
            if txn == cycle_start {
                found_start = true;
            }
            if found_start {
                cycle.push(txn);
            }
        }

        cycle
    }

    /// Selects a victim from the cycle to abort.
    fn select_victim(&self, cycle: &[TxnId]) -> TxnId {
        let nodes = self.nodes.read();

        // Find transaction with lowest abort cost
        let mut min_cost = i64::MAX;
        let mut victim = cycle[0];

        for &txn_id in cycle {
            let cost = nodes.get(&txn_id).map(|n| n.abort_cost()).unwrap_or(0);

            if cost < min_cost {
                min_cost = cost;
                victim = txn_id;
            }
        }

        victim
    }

    /// Runs a full deadlock detection pass on all waiting transactions.
    pub fn detect_all_deadlocks(&self) -> Vec<DeadlockInfo> {
        let waiters: Vec<TxnId> = self.edges.read().keys().copied().collect();
        let mut deadlocks = Vec::new();
        let mut already_found = HashSet::new();

        for waiter in waiters {
            if already_found.contains(&waiter) {
                continue;
            }

            if let Some(info) = self.detect_deadlock(waiter) {
                for &txn in &info.cycle {
                    already_found.insert(txn);
                }
                deadlocks.push(info);
            }
        }

        deadlocks
    }

    /// Returns statistics.
    pub fn stats(&self) -> &DeadlockStats {
        &self.stats
    }

    /// Returns the number of transactions in the graph.
    pub fn txn_count(&self) -> usize {
        self.nodes.read().len()
    }

    /// Returns the number of wait edges.
    pub fn edge_count(&self) -> usize {
        self.edges.read().values().map(|s| s.len()).sum()
    }

    /// Returns what a transaction is waiting for.
    pub fn get_waits(&self, waiter: TxnId) -> Vec<TxnId> {
        self.edges
            .read()
            .get(&waiter)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Checks if a transaction is waiting.
    pub fn is_waiting(&self, txn_id: TxnId) -> bool {
        self.edges.read().contains_key(&txn_id)
    }
}

impl Default for WaitForGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for WaitForGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitForGraph")
            .field("txn_count", &self.txn_count())
            .field("edge_count", &self.edge_count())
            .finish()
    }
}

/// Deadlock detector that runs periodic checks.
pub struct DeadlockDetector {
    /// The wait-for graph.
    wfg: WaitForGraph,
}

impl DeadlockDetector {
    /// Creates a new deadlock detector.
    pub fn new() -> Self {
        Self {
            wfg: WaitForGraph::new(),
        }
    }

    /// Returns a reference to the wait-for graph.
    pub fn wfg(&self) -> &WaitForGraph {
        &self.wfg
    }

    /// Registers a transaction.
    pub fn register_txn(&self, txn_id: TxnId) {
        self.wfg.add_txn(txn_id);
    }

    /// Registers a transaction with details.
    pub fn register_txn_with_details(&self, node: WfgNode) {
        self.wfg.add_txn_with_details(node);
    }

    /// Unregisters a transaction.
    pub fn unregister_txn(&self, txn_id: TxnId) {
        self.wfg.remove_txn(txn_id);
    }

    /// Records that a transaction is waiting for another.
    pub fn add_wait(&self, waiter: TxnId, holder: TxnId) {
        self.wfg.add_wait(waiter, holder);
    }

    /// Records that a wait has ended.
    pub fn remove_wait(&self, waiter: TxnId, holder: TxnId) {
        self.wfg.remove_wait(waiter, holder);
    }

    /// Clears all waits for a transaction.
    pub fn clear_waits(&self, waiter: TxnId) {
        self.wfg.clear_waits(waiter);
    }

    /// Checks if there's a deadlock involving a transaction.
    pub fn check_deadlock(&self, txn_id: TxnId) -> Option<DeadlockInfo> {
        self.wfg.detect_deadlock(txn_id)
    }

    /// Runs a full deadlock detection pass.
    pub fn detect_all(&self) -> Vec<DeadlockInfo> {
        self.wfg.detect_all_deadlocks()
    }
}

impl Default for DeadlockDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for DeadlockDetector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeadlockDetector")
            .field("wfg", &self.wfg)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wfg_node_abort_cost() {
        let node1 = WfgNode::new(TxnId::new(1));
        let cost1 = node1.abort_cost();

        let node2 = WfgNode::with_details(
            TxnId::new(2),
            Instant::now(),
            10, // Higher priority
            5,  // More locks
            3,  // More writes
        );
        let cost2 = node2.abort_cost();

        // Node2 should have higher cost (less desirable to abort)
        assert!(cost2 > cost1);
    }

    #[test]
    fn test_wfg_add_remove() {
        let wfg = WaitForGraph::new();

        wfg.add_txn(TxnId::new(1));
        wfg.add_txn(TxnId::new(2));
        assert_eq!(wfg.txn_count(), 2);

        wfg.add_wait(TxnId::new(1), TxnId::new(2));
        assert_eq!(wfg.edge_count(), 1);

        wfg.remove_wait(TxnId::new(1), TxnId::new(2));
        assert_eq!(wfg.edge_count(), 0);

        wfg.remove_txn(TxnId::new(1));
        assert_eq!(wfg.txn_count(), 1);
    }

    #[test]
    fn test_no_deadlock() {
        let wfg = WaitForGraph::new();

        // T1 -> T2 -> T3 (no cycle)
        wfg.add_wait(TxnId::new(1), TxnId::new(2));
        wfg.add_wait(TxnId::new(2), TxnId::new(3));

        assert!(wfg.detect_deadlock(TxnId::new(1)).is_none());
        assert!(wfg.detect_deadlock(TxnId::new(2)).is_none());
    }

    #[test]
    fn test_simple_deadlock() {
        let wfg = WaitForGraph::new();

        // T1 -> T2 -> T1 (cycle)
        wfg.add_wait(TxnId::new(1), TxnId::new(2));
        wfg.add_wait(TxnId::new(2), TxnId::new(1));

        let deadlock = wfg.detect_deadlock(TxnId::new(1));
        assert!(deadlock.is_some());

        let info = deadlock.unwrap();
        assert_eq!(info.cycle.len(), 3); // [1, 2, 1]
        assert!(info.victim == TxnId::new(1) || info.victim == TxnId::new(2));
    }

    #[test]
    fn test_complex_deadlock() {
        let wfg = WaitForGraph::new();

        // T1 -> T2 -> T3 -> T1 (3-way cycle)
        wfg.add_wait(TxnId::new(1), TxnId::new(2));
        wfg.add_wait(TxnId::new(2), TxnId::new(3));
        wfg.add_wait(TxnId::new(3), TxnId::new(1));

        let deadlock = wfg.detect_deadlock(TxnId::new(1));
        assert!(deadlock.is_some());

        let info = deadlock.unwrap();
        assert!(info.cycle.len() >= 3);
    }

    #[test]
    fn test_detect_all_deadlocks() {
        let wfg = WaitForGraph::new();

        // Two separate cycles
        wfg.add_wait(TxnId::new(1), TxnId::new(2));
        wfg.add_wait(TxnId::new(2), TxnId::new(1));

        wfg.add_wait(TxnId::new(3), TxnId::new(4));
        wfg.add_wait(TxnId::new(4), TxnId::new(3));

        let deadlocks = wfg.detect_all_deadlocks();
        assert_eq!(deadlocks.len(), 2);
    }

    #[test]
    fn test_deadlock_detector() {
        let detector = DeadlockDetector::new();

        detector.register_txn(TxnId::new(1));
        detector.register_txn(TxnId::new(2));

        detector.add_wait(TxnId::new(1), TxnId::new(2));
        detector.add_wait(TxnId::new(2), TxnId::new(1));

        let deadlock = detector.check_deadlock(TxnId::new(1));
        assert!(deadlock.is_some());

        detector.clear_waits(TxnId::new(1));
        assert!(!detector.wfg().is_waiting(TxnId::new(1)));
    }

    #[test]
    fn test_victim_selection_by_priority() {
        let wfg = WaitForGraph::new();

        // T1 has higher priority than T2
        wfg.add_txn_with_details(WfgNode::with_details(
            TxnId::new(1),
            Instant::now(),
            10, // High priority
            0,
            0,
        ));
        wfg.add_txn_with_details(WfgNode::with_details(
            TxnId::new(2),
            Instant::now(),
            0, // Low priority
            0,
            0,
        ));

        wfg.add_wait(TxnId::new(1), TxnId::new(2));
        wfg.add_wait(TxnId::new(2), TxnId::new(1));

        let deadlock = wfg.detect_deadlock(TxnId::new(1)).unwrap();

        // T2 should be the victim (lower priority)
        assert_eq!(deadlock.victim, TxnId::new(2));
    }

    #[test]
    fn test_get_waits() {
        let wfg = WaitForGraph::new();

        wfg.add_wait(TxnId::new(1), TxnId::new(2));
        wfg.add_wait(TxnId::new(1), TxnId::new(3));

        let waits = wfg.get_waits(TxnId::new(1));
        assert_eq!(waits.len(), 2);
        assert!(waits.contains(&TxnId::new(2)));
        assert!(waits.contains(&TxnId::new(3)));
    }

    #[test]
    fn test_stats() {
        let wfg = WaitForGraph::new();

        wfg.add_wait(TxnId::new(1), TxnId::new(2));
        wfg.add_wait(TxnId::new(2), TxnId::new(1));

        wfg.detect_deadlock(TxnId::new(1));

        assert_eq!(wfg.stats().checks.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(wfg.stats().deadlocks_found.load(AtomicOrdering::Relaxed), 1);
    }
}
