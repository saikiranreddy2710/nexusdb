//! HNSW (Hierarchical Navigable Small World) index.
//!
//! Implements the algorithm from Malkov & Yashunin (2018):
//!   "Efficient and Robust Approximate Nearest Neighbor using
//!    Hierarchical Navigable Small World Graphs"
//!
//! Key ideas:
//! - Multi-layer skip-list-like graph where each node appears at a
//!   randomly chosen maximum layer.
//! - Greedy search starts at the entry point on the top layer and
//!   descends, refining candidates at each layer.
//! - At layer 0 the neighbourhood is larger (M_max0 = 2*M) to keep
//!   recall high.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use parking_lot::RwLock;
use rand::Rng;

use crate::distance::DistanceMetric;
use crate::error::{HnswError, HnswResult};

// ── Configuration ───────────────────────────────────────────────────

/// HNSW index configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HnswConfig {
    /// Number of dimensions per vector.
    pub dimensions: u32,
    /// Max bi-directional connections per node per layer (default 16).
    pub m: usize,
    /// Max connections at layer 0 (default 2 * m).
    pub m_max0: usize,
    /// Size of the dynamic candidate list during construction (default 200).
    pub ef_construction: usize,
    /// Size of the dynamic candidate list during search (default 50).
    pub ef_search: usize,
    /// Distance metric.
    pub metric: DistanceMetric,
    /// Random seed (0 = random).
    pub seed: u64,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            dimensions: 0,
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ef_search: 50,
            metric: DistanceMetric::L2,
            seed: 0,
        }
    }
}

impl HnswConfig {
    /// Creates a new config with given dimensions and metric.
    pub fn new(dimensions: u32, metric: DistanceMetric) -> Self {
        Self {
            dimensions,
            metric,
            ..Default::default()
        }
    }

    /// Sets M (max connections per layer).
    pub fn with_m(mut self, m: usize) -> Self {
        self.m = m;
        self.m_max0 = 2 * m;
        self
    }

    /// Sets ef_construction.
    pub fn with_ef_construction(mut self, ef: usize) -> Self {
        self.ef_construction = ef;
        self
    }

    /// Sets ef_search.
    pub fn with_ef_search(mut self, ef: usize) -> Self {
        self.ef_search = ef;
        self
    }

    /// Level multiplier: 1 / ln(M).
    fn ml(&self) -> f64 {
        1.0 / (self.m as f64).ln()
    }

    /// Maximum connections at the given layer.
    fn max_connections(&self, layer: usize) -> usize {
        if layer == 0 {
            self.m_max0
        } else {
            self.m
        }
    }
}

// ── Internal types ──────────────────────────────────────────────────

/// A unique identifier for a vector in the index.
pub type VectorId = u64;

/// A node in the HNSW graph.
#[derive(Debug)]
struct Node {
    /// External identifier.
    id: VectorId,
    /// The vector data.
    vector: Vec<f32>,
    /// Maximum layer this node appears on (0-indexed).
    max_layer: usize,
    /// Neighbors per layer. neighbors[l] = set of node indices at layer l.
    neighbors: Vec<Vec<usize>>,
}

/// A scored candidate used in priority-queue searches.
#[derive(Debug, Clone, Copy)]
struct Candidate {
    /// Index into `nodes` vec.
    index: usize,
    /// Distance to the query.
    distance: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.index == other.index
    }
}
impl Eq for Candidate {}

/// Min-heap ordering: smallest distance first.
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.index.cmp(&self.index))
    }
}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Max-heap wrapper (reverse ordering) for maintaining the worst candidate.
#[derive(Debug, Clone, Copy)]
struct ReverseCandidate(Candidate);

impl PartialEq for ReverseCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl Eq for ReverseCandidate {}

impl Ord for ReverseCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .distance
            .partial_cmp(&other.0.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.0.index.cmp(&other.0.index))
    }
}
impl PartialOrd for ReverseCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ── Search result ───────────────────────────────────────────────────

/// A single search result.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// The vector ID.
    pub id: VectorId,
    /// Distance from the query vector.
    pub distance: f32,
}

// ── HNSW Index ──────────────────────────────────────────────────────

/// The HNSW index.
///
/// Thread-safe via `RwLock`: concurrent reads, exclusive writes.
pub struct HnswIndex {
    config: HnswConfig,
    inner: RwLock<HnswInner>,
}

struct HnswInner {
    /// All nodes in insertion order.
    nodes: Vec<Node>,
    /// Maps external VectorId → index in `nodes`.
    id_to_index: HashMap<VectorId, usize>,
    /// Index of the entry-point node (top of the graph).
    entry_point: Option<usize>,
    /// Current max layer in the graph.
    max_level: usize,
    /// RNG for level generation.
    rng: rand::rngs::StdRng,
}

impl std::fmt::Debug for HnswIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        f.debug_struct("HnswIndex")
            .field("config", &self.config)
            .field("num_vectors", &inner.nodes.len())
            .field("max_level", &inner.max_level)
            .finish()
    }
}

impl HnswIndex {
    /// Creates a new empty HNSW index.
    pub fn new(config: HnswConfig) -> HnswResult<Self> {
        if config.dimensions == 0 {
            return Err(HnswError::InvalidDimensions(0));
        }
        if config.m == 0 {
            return Err(HnswError::InvalidConfig("M must be > 0".into()));
        }
        if config.ef_construction == 0 {
            return Err(HnswError::InvalidConfig(
                "ef_construction must be > 0".into(),
            ));
        }

        let rng = if config.seed != 0 {
            <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(config.seed)
        } else {
            <rand::rngs::StdRng as rand::SeedableRng>::from_entropy()
        };

        Ok(Self {
            config,
            inner: RwLock::new(HnswInner {
                nodes: Vec::new(),
                id_to_index: HashMap::new(),
                entry_point: None,
                max_level: 0,
                rng,
            }),
        })
    }

    /// Returns the current number of active vectors in the index.
    pub fn len(&self) -> usize {
        self.inner.read().id_to_index.len()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the index configuration.
    pub fn config(&self) -> &HnswConfig {
        &self.config
    }

    /// Returns true if the index contains the given vector ID.
    pub fn contains(&self, id: VectorId) -> bool {
        self.inner.read().id_to_index.contains_key(&id)
    }

    /// Retrieves the vector for a given ID, if it exists.
    pub fn get_vector(&self, id: VectorId) -> Option<Vec<f32>> {
        let inner = self.inner.read();
        inner
            .id_to_index
            .get(&id)
            .map(|&idx| inner.nodes[idx].vector.clone())
    }

    // ── Insert ──────────────────────────────────────────────────────

    /// Inserts a vector into the index.
    ///
    /// Returns an error if the vector dimensions don't match or the ID
    /// already exists.
    pub fn insert(&self, id: VectorId, vector: &[f32]) -> HnswResult<()> {
        if vector.iter().any(|&v| v.is_nan() || v.is_infinite()) {
            return Err(HnswError::InvalidConfig(
                "vector contains NaN or Infinity".into(),
            ));
        }

        if vector.len() != self.config.dimensions as usize {
            return Err(HnswError::DimensionMismatch {
                expected: self.config.dimensions as usize,
                got: vector.len(),
            });
        }

        let mut inner = self.inner.write();

        if inner.id_to_index.contains_key(&id) {
            return Err(HnswError::DuplicateId(id));
        }

        let node_index = inner.nodes.len();
        let level = self.random_level(&mut inner.rng);

        // Create node
        let mut node = Node {
            id,
            vector: vector.to_vec(),
            max_layer: level,
            neighbors: Vec::with_capacity(level + 1),
        };
        for _ in 0..=level {
            node.neighbors.push(Vec::new());
        }

        inner.nodes.push(node);
        inner.id_to_index.insert(id, node_index);

        // First node — just set as entry point
        if inner.entry_point.is_none() {
            inner.entry_point = Some(node_index);
            inner.max_level = level;
            return Ok(());
        }

        let ep = inner.entry_point.unwrap();
        let current_max_level = inner.max_level;

        // Phase 1: greedily descend from top layer to level+1
        let mut current_ep = ep;
        for lc in (level + 1..=current_max_level).rev() {
            current_ep = self.greedy_closest(&inner, &vector, current_ep, lc);
        }

        // Phase 2: at each layer from min(level, max_level) down to 0,
        //          find ef_construction nearest neighbors, then connect.
        let top = std::cmp::min(level, current_max_level);
        let mut ep_set = vec![current_ep];

        for lc in (0..=top).rev() {
            let candidates =
                self.search_layer(&inner, vector, &ep_set, self.config.ef_construction, lc);

            // Select M neighbors using the simple heuristic
            let max_conn = self.config.max_connections(lc);
            let neighbors = self.select_neighbors_simple(&candidates, max_conn);

            // Add bidirectional connections
            inner.nodes[node_index].neighbors[lc] = neighbors.iter().map(|c| c.index).collect();

            for &nb in &neighbors {
                inner.nodes[nb.index].neighbors[lc].push(node_index);

                // Shrink if over capacity
                let nb_len = inner.nodes[nb.index].neighbors[lc].len();
                if nb_len > max_conn {
                    // Clone what we need to avoid aliased borrows
                    let nb_vec = inner.nodes[nb.index].vector.clone();
                    let nb_neighbor_list = inner.nodes[nb.index].neighbors[lc].clone();

                    let mut scored: Vec<Candidate> = nb_neighbor_list
                        .iter()
                        .map(|&ni| Candidate {
                            index: ni,
                            distance: self
                                .config
                                .metric
                                .distance(&nb_vec, &inner.nodes[ni].vector),
                        })
                        .collect();
                    scored.sort_by(|a, b| {
                        a.distance
                            .partial_cmp(&b.distance)
                            .unwrap_or(Ordering::Equal)
                    });
                    scored.truncate(max_conn);
                    inner.nodes[nb.index].neighbors[lc] = scored.iter().map(|c| c.index).collect();
                }
            }

            // Update ep_set for next layer
            ep_set = candidates.iter().map(|c| c.index).collect();
        }

        // If new node has higher level, update entry point
        if level > current_max_level {
            inner.entry_point = Some(node_index);
            inner.max_level = level;
        }

        Ok(())
    }

    /// Bulk-insert multiple vectors.
    pub fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> HnswResult<usize> {
        let mut count = 0;
        for (id, vec) in vectors {
            self.insert(*id, vec)?;
            count += 1;
        }
        Ok(count)
    }

    // ── Search ──────────────────────────────────────────────────────

    /// Searches for the `k` nearest neighbors of `query`.
    ///
    /// Returns results sorted by ascending distance.
    pub fn search(&self, query: &[f32], k: usize) -> HnswResult<Vec<SearchResult>> {
        if query.len() != self.config.dimensions as usize {
            return Err(HnswError::DimensionMismatch {
                expected: self.config.dimensions as usize,
                got: query.len(),
            });
        }

        let inner = self.inner.read();

        if inner.entry_point.is_none() {
            return Ok(Vec::new());
        }

        let ep = inner.entry_point.unwrap();
        let max_level = inner.max_level;
        let ef = std::cmp::max(self.config.ef_search, k);

        // Phase 1: greedy descent from top to layer 1
        let mut current_ep = ep;
        for lc in (1..=max_level).rev() {
            current_ep = self.greedy_closest(&inner, query, current_ep, lc);
        }

        // Phase 2: search layer 0 with ef candidates
        let candidates = self.search_layer(&inner, query, &[current_ep], ef, 0);

        // Return top-k
        let mut results: Vec<SearchResult> = candidates
            .into_iter()
            .map(|c| SearchResult {
                id: inner.nodes[c.index].id,
                distance: c.distance,
            })
            .collect();
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });
        results.truncate(k);
        Ok(results)
    }

    /// Searches with a custom ef_search value (overrides config).
    pub fn search_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> HnswResult<Vec<SearchResult>> {
        if query.len() != self.config.dimensions as usize {
            return Err(HnswError::DimensionMismatch {
                expected: self.config.dimensions as usize,
                got: query.len(),
            });
        }

        let inner = self.inner.read();

        if inner.entry_point.is_none() {
            return Ok(Vec::new());
        }

        let ep = inner.entry_point.unwrap();
        let max_level = inner.max_level;
        let ef = std::cmp::max(ef_search, k);

        let mut current_ep = ep;
        for lc in (1..=max_level).rev() {
            current_ep = self.greedy_closest(&inner, query, current_ep, lc);
        }

        let candidates = self.search_layer(&inner, query, &[current_ep], ef, 0);

        let mut results: Vec<SearchResult> = candidates
            .into_iter()
            .map(|c| SearchResult {
                id: inner.nodes[c.index].id,
                distance: c.distance,
            })
            .collect();
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });
        results.truncate(k);
        Ok(results)
    }

    // ── Delete ──────────────────────────────────────────────────────

    /// Marks a vector as deleted (tombstone approach).
    ///
    /// The vector remains in the graph but is excluded from search results.
    /// A future `compact()` call can reclaim space.
    pub fn delete(&self, id: VectorId) -> HnswResult<()> {
        let mut inner = self.inner.write();

        let &node_idx = inner.id_to_index.get(&id).ok_or(HnswError::NotFound(id))?;

        // Save neighbor lists before clearing
        let max_layer = inner.nodes[node_idx].max_layer;
        let saved_neighbors: Vec<Vec<usize>> = inner.nodes[node_idx].neighbors.clone();

        // Remove from all neighbor lists
        for lc in 0..=max_layer {
            let neighbor_indices: Vec<usize> = inner.nodes[node_idx].neighbors[lc].clone();
            for &nb_idx in &neighbor_indices {
                if nb_idx < inner.nodes.len() {
                    inner.nodes[nb_idx].neighbors[lc].retain(|&x| x != node_idx);
                }
            }
            inner.nodes[node_idx].neighbors[lc].clear();
        }

        // Repair: connect former neighbors to each other at each layer
        for lc in 0..=max_layer {
            let former = &saved_neighbors[lc];
            let max_conn = self.config.max_connections(lc);
            for &nb_a in former {
                if !inner.id_to_index.contains_key(&inner.nodes[nb_a].id) {
                    continue;
                }
                for &nb_b in former {
                    if nb_a == nb_b {
                        continue;
                    }
                    if !inner.id_to_index.contains_key(&inner.nodes[nb_b].id) {
                        continue;
                    }
                    // Add connection if not already present and under capacity
                    if !inner.nodes[nb_a].neighbors[lc].contains(&nb_b)
                        && inner.nodes[nb_a].neighbors[lc].len() < max_conn
                    {
                        inner.nodes[nb_a].neighbors[lc].push(nb_b);
                    }
                }
            }
        }

        // Remove from lookup
        inner.id_to_index.remove(&id);

        // If this was the entry point, find the active node with highest max_layer
        if inner.entry_point == Some(node_idx) {
            let best_ep = inner
                .id_to_index
                .values()
                .copied()
                .max_by_key(|&idx| inner.nodes[idx].max_layer);
            inner.entry_point = best_ep;
            inner.max_level = best_ep.map(|idx| inner.nodes[idx].max_layer).unwrap_or(0);
        }

        Ok(())
    }

    // ── Stats ───────────────────────────────────────────────────────

    /// Returns statistics about the index.
    pub fn stats(&self) -> HnswStats {
        let inner = self.inner.read();
        let num_vectors = inner.id_to_index.len();
        let max_level = inner.max_level;

        let mut total_edges = 0usize;
        let mut level_counts = vec![0usize; max_level + 1];

        let active_indices: std::collections::HashSet<usize> =
            inner.id_to_index.values().copied().collect();
        for (i, node) in inner.nodes.iter().enumerate() {
            if !active_indices.contains(&i) {
                continue; // skip deleted
            }
            for (l, neighbors) in node.neighbors.iter().enumerate() {
                if l <= max_level {
                    level_counts[l] += 1;
                    total_edges += neighbors.len();
                }
            }
        }

        HnswStats {
            num_vectors,
            max_level,
            total_edges,
            dimensions: self.config.dimensions,
            metric: self.config.metric,
            memory_bytes: self.estimate_memory(),
        }
    }

    /// Estimates memory usage in bytes.
    pub fn estimate_memory(&self) -> usize {
        let inner = self.inner.read();
        let mut bytes = 0usize;
        for node in &inner.nodes {
            // vector data
            bytes += node.vector.len() * 4;
            // neighbor lists
            for nb in &node.neighbors {
                bytes += nb.len() * std::mem::size_of::<usize>();
            }
            // overhead
            bytes += std::mem::size_of::<Node>();
        }
        // HashMap overhead
        bytes += inner.id_to_index.len()
            * (std::mem::size_of::<VectorId>() + std::mem::size_of::<usize>() + 16);
        bytes
    }

    // ── Serialization ───────────────────────────────────────────────

    /// Serializes the index to JSON bytes.
    pub fn to_bytes(&self) -> HnswResult<Vec<u8>> {
        let inner = self.inner.read();
        let data = SerializableIndex {
            config: self.config.clone(),
            nodes: inner
                .nodes
                .iter()
                .map(|n| SerializableNode {
                    id: n.id,
                    vector: n.vector.clone(),
                    max_layer: n.max_layer,
                    neighbors: n.neighbors.clone(),
                })
                .collect(),
            id_to_index: inner.id_to_index.clone(),
            entry_point: inner.entry_point,
            max_level: inner.max_level,
        };
        serde_json::to_vec(&data).map_err(|e| HnswError::SerializationError(e.to_string()))
    }

    /// Deserializes an index from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> HnswResult<Self> {
        let data: SerializableIndex = serde_json::from_slice(bytes)
            .map_err(|e| HnswError::SerializationError(e.to_string()))?;

        let rng = if data.config.seed != 0 {
            <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(data.config.seed)
        } else {
            <rand::rngs::StdRng as rand::SeedableRng>::from_entropy()
        };

        let nodes: Vec<Node> = data
            .nodes
            .into_iter()
            .map(|sn| Node {
                id: sn.id,
                vector: sn.vector,
                max_layer: sn.max_layer,
                neighbors: sn.neighbors,
            })
            .collect();

        Ok(Self {
            config: data.config,
            inner: RwLock::new(HnswInner {
                nodes,
                id_to_index: data.id_to_index,
                entry_point: data.entry_point,
                max_level: data.max_level,
                rng,
            }),
        })
    }

    // ── Private helpers ─────────────────────────────────────────────

    /// Generates a random level for a new node using the exponential distribution.
    fn random_level(&self, rng: &mut rand::rngs::StdRng) -> usize {
        let ml = self.config.ml();
        let r: f64 = rng.gen();
        // level = floor(-ln(uniform) * mL)
        let level = (-r.ln() * ml).floor() as usize;
        // Cap at a reasonable max to prevent extreme outliers
        std::cmp::min(level, 32)
    }

    /// Greedy search: finds the single closest node to `query` at `layer`,
    /// starting from `ep`.
    fn greedy_closest(
        &self,
        inner: &HnswInner,
        query: &[f32],
        mut ep: usize,
        layer: usize,
    ) -> usize {
        let mut ep_dist = self.config.metric.distance(query, &inner.nodes[ep].vector);

        loop {
            let mut changed = false;
            let neighbors = &inner.nodes[ep].neighbors[layer];
            for &nb in neighbors {
                let d = self.config.metric.distance(query, &inner.nodes[nb].vector);
                if d < ep_dist {
                    ep = nb;
                    ep_dist = d;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        ep
    }

    /// Searches a single layer starting from `entry_points`, returning up to
    /// `ef` nearest candidates.
    fn search_layer(
        &self,
        inner: &HnswInner,
        query: &[f32],
        entry_points: &[usize],
        ef: usize,
        layer: usize,
    ) -> Vec<Candidate> {
        let mut visited = HashSet::new();
        // Min-heap: nearest candidates (for expansion)
        let mut candidates = BinaryHeap::<Candidate>::new();
        // Max-heap: worst of the current result set
        let mut results = BinaryHeap::<ReverseCandidate>::new();

        for &ep in entry_points {
            if visited.insert(ep) {
                let d = self.config.metric.distance(query, &inner.nodes[ep].vector);
                let c = Candidate {
                    index: ep,
                    distance: d,
                };
                candidates.push(c);
                results.push(ReverseCandidate(c));
            }
        }

        while let Some(nearest) = candidates.pop() {
            // If the nearest candidate is farther than the worst result, stop
            let worst_dist = results.peek().map(|r| r.0.distance).unwrap_or(f32::MAX);
            if nearest.distance > worst_dist {
                break;
            }

            // Expand neighbors
            let neighbors = &inner.nodes[nearest.index].neighbors[layer];
            for &nb in neighbors {
                if !visited.insert(nb) {
                    continue;
                }
                let d = self.config.metric.distance(query, &inner.nodes[nb].vector);

                let worst_dist = results.peek().map(|r| r.0.distance).unwrap_or(f32::MAX);
                if d < worst_dist || results.len() < ef {
                    let c = Candidate {
                        index: nb,
                        distance: d,
                    };
                    candidates.push(c);
                    results.push(ReverseCandidate(c));

                    if results.len() > ef {
                        results.pop(); // remove worst
                    }
                }
            }
        }

        results
            .into_sorted_vec()
            .into_iter()
            .map(|rc| rc.0)
            .collect()
    }

    /// Simple neighbor selection: pick the M closest candidates.
    fn select_neighbors_simple(
        &self,
        candidates: &[Candidate],
        max_count: usize,
    ) -> Vec<Candidate> {
        let mut sorted: Vec<Candidate> = candidates.to_vec();
        sorted.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });
        sorted.truncate(max_count);
        sorted
    }
}

// ── Stats ───────────────────────────────────────────────────────────

/// Statistics about an HNSW index.
#[derive(Debug, Clone)]
pub struct HnswStats {
    /// Number of active vectors.
    pub num_vectors: usize,
    /// Maximum layer level.
    pub max_level: usize,
    /// Total number of edges across all layers.
    pub total_edges: usize,
    /// Number of dimensions.
    pub dimensions: u32,
    /// Distance metric.
    pub metric: DistanceMetric,
    /// Estimated memory usage in bytes.
    pub memory_bytes: usize,
}

// ── Serialization structs ───────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
struct SerializableIndex {
    config: HnswConfig,
    nodes: Vec<SerializableNode>,
    id_to_index: HashMap<VectorId, usize>,
    entry_point: Option<usize>,
    max_level: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerializableNode {
    id: VectorId,
    vector: Vec<f32>,
    max_layer: usize,
    neighbors: Vec<Vec<usize>>,
}

// ── tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(dim: u32) -> HnswConfig {
        HnswConfig::new(dim, DistanceMetric::L2)
            .with_m(4)
            .with_ef_construction(32)
            .with_ef_search(16)
    }

    fn make_seeded_config(dim: u32) -> HnswConfig {
        let mut cfg = make_config(dim);
        cfg.seed = 42;
        cfg
    }

    #[test]
    fn test_empty_index() {
        let idx = HnswIndex::new(make_config(3)).unwrap();
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);
        let results = idx.search(&[0.0, 0.0, 0.0], 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_single_insert_and_search() {
        let idx = HnswIndex::new(make_config(3)).unwrap();
        idx.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        assert_eq!(idx.len(), 1);
        assert!(idx.contains(1));

        let results = idx.search(&[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 1);
        assert!(results[0].distance < 1e-5);
    }

    #[test]
    fn test_duplicate_insert() {
        let idx = HnswIndex::new(make_config(3)).unwrap();
        idx.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        let err = idx.insert(1, &[0.0, 1.0, 0.0]).unwrap_err();
        assert!(matches!(err, HnswError::DuplicateId(1)));
    }

    #[test]
    fn test_dimension_mismatch_insert() {
        let idx = HnswIndex::new(make_config(3)).unwrap();
        let err = idx.insert(1, &[1.0, 0.0]).unwrap_err();
        assert!(matches!(err, HnswError::DimensionMismatch { .. }));
    }

    #[test]
    fn test_dimension_mismatch_search() {
        let idx = HnswIndex::new(make_config(3)).unwrap();
        idx.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        let err = idx.search(&[1.0, 0.0], 1).unwrap_err();
        assert!(matches!(err, HnswError::DimensionMismatch { .. }));
    }

    #[test]
    fn test_knn_basic() {
        let idx = HnswIndex::new(make_seeded_config(2)).unwrap();

        // Insert points on a line: (0,0), (1,0), (2,0), ..., (9,0)
        for i in 0..10 {
            idx.insert(i, &[i as f32, 0.0]).unwrap();
        }

        // Query near (3.1, 0) — nearest should be 3, then 4, then 2
        let results = idx.search(&[3.1, 0.0], 3).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 3);
    }

    #[test]
    fn test_knn_returns_exact_k() {
        let idx = HnswIndex::new(make_seeded_config(2)).unwrap();
        for i in 0..100 {
            let angle = (i as f32) * std::f32::consts::TAU / 100.0;
            idx.insert(i, &[angle.cos(), angle.sin()]).unwrap();
        }

        let results = idx.search(&[1.0, 0.0], 10).unwrap();
        assert_eq!(results.len(), 10);

        // Distances should be monotonically non-decreasing
        for w in results.windows(2) {
            assert!(w[0].distance <= w[1].distance + 1e-6);
        }
    }

    #[test]
    fn test_delete() {
        let idx = HnswIndex::new(make_seeded_config(2)).unwrap();
        idx.insert(1, &[1.0, 0.0]).unwrap();
        idx.insert(2, &[2.0, 0.0]).unwrap();
        idx.insert(3, &[3.0, 0.0]).unwrap();

        idx.delete(2).unwrap();
        assert!(!idx.contains(2));
        assert_eq!(idx.len(), 2); // nodes vec still has 3, but id_to_index has 2
    }

    #[test]
    fn test_delete_not_found() {
        let idx = HnswIndex::new(make_config(2)).unwrap();
        let err = idx.delete(999).unwrap_err();
        assert!(matches!(err, HnswError::NotFound(999)));
    }

    #[test]
    fn test_get_vector() {
        let idx = HnswIndex::new(make_config(3)).unwrap();
        idx.insert(1, &[1.0, 2.0, 3.0]).unwrap();

        let v = idx.get_vector(1).unwrap();
        assert_eq!(v, vec![1.0, 2.0, 3.0]);
        assert!(idx.get_vector(999).is_none());
    }

    #[test]
    fn test_insert_batch() {
        let idx = HnswIndex::new(make_seeded_config(2)).unwrap();
        let vectors: Vec<(VectorId, Vec<f32>)> =
            (0..50).map(|i| (i as u64, vec![i as f32, 0.0])).collect();
        let count = idx.insert_batch(&vectors).unwrap();
        assert_eq!(count, 50);
        assert_eq!(idx.len(), 50);
    }

    #[test]
    fn test_search_with_ef() {
        let idx = HnswIndex::new(make_seeded_config(2)).unwrap();
        for i in 0..50 {
            idx.insert(i, &[i as f32, 0.0]).unwrap();
        }

        // Higher ef should give at least as good results
        let r1 = idx.search(&[25.3, 0.0], 5).unwrap();
        let r2 = idx.search_with_ef(&[25.3, 0.0], 5, 200).unwrap();
        assert_eq!(r1.len(), 5);
        assert_eq!(r2.len(), 5);
        // Both should find ID 25 as nearest
        assert_eq!(r1[0].id, 25);
        assert_eq!(r2[0].id, 25);
    }

    #[test]
    fn test_cosine_metric() {
        let cfg = HnswConfig::new(3, DistanceMetric::Cosine)
            .with_m(4)
            .with_ef_construction(32)
            .with_ef_search(16);
        let idx = HnswIndex::new(cfg).unwrap();

        idx.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        idx.insert(2, &[0.0, 1.0, 0.0]).unwrap();
        idx.insert(3, &[0.9, 0.1, 0.0]).unwrap(); // close to [1,0,0]

        let results = idx.search(&[1.0, 0.0, 0.0], 3).unwrap();
        // ID 1 should be the closest (identical direction)
        assert_eq!(results[0].id, 1);
        assert!(results[0].distance < 1e-5);
        // ID 3 should be second (nearly same direction)
        assert_eq!(results[1].id, 3);
    }

    #[test]
    fn test_inner_product_metric() {
        let cfg = HnswConfig::new(2, DistanceMetric::InnerProduct)
            .with_m(4)
            .with_ef_construction(32)
            .with_ef_search(16);
        let idx = HnswIndex::new(cfg).unwrap();

        idx.insert(1, &[1.0, 0.0]).unwrap();
        idx.insert(2, &[10.0, 0.0]).unwrap(); // highest dot product with [1,0]
        idx.insert(3, &[0.0, 10.0]).unwrap();

        let results = idx.search(&[1.0, 0.0], 3).unwrap();
        // ID 2 should have highest dot product (= lowest inner_product distance)
        assert_eq!(results[0].id, 2);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let idx = HnswIndex::new(make_seeded_config(3)).unwrap();
        idx.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        idx.insert(2, &[0.0, 1.0, 0.0]).unwrap();
        idx.insert(3, &[0.0, 0.0, 1.0]).unwrap();

        let bytes = idx.to_bytes().unwrap();
        let restored = HnswIndex::from_bytes(&bytes).unwrap();

        assert_eq!(restored.len(), 3);
        assert!(restored.contains(1));
        assert!(restored.contains(2));
        assert!(restored.contains(3));

        let v = restored.get_vector(2).unwrap();
        assert_eq!(v, vec![0.0, 1.0, 0.0]);

        // Search should work on restored index
        let results = restored.search(&[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(results[0].id, 1);
    }

    #[test]
    fn test_stats() {
        let idx = HnswIndex::new(make_seeded_config(4)).unwrap();
        for i in 0..20 {
            idx.insert(i, &[i as f32, 0.0, 0.0, 0.0]).unwrap();
        }

        let stats = idx.stats();
        assert_eq!(stats.num_vectors, 20);
        assert_eq!(stats.dimensions, 4);
        assert_eq!(stats.metric, DistanceMetric::L2);
        assert!(stats.total_edges > 0);
        assert!(stats.memory_bytes > 0);
    }

    #[test]
    fn test_invalid_config() {
        assert!(HnswIndex::new(HnswConfig::new(0, DistanceMetric::L2)).is_err());
        let mut cfg = HnswConfig::new(3, DistanceMetric::L2);
        cfg.m = 0;
        assert!(HnswIndex::new(cfg).is_err());

        let mut cfg = HnswConfig::new(3, DistanceMetric::L2);
        cfg.ef_construction = 0;
        assert!(HnswIndex::new(cfg).is_err());
    }

    #[test]
    fn test_recall_on_random_data() {
        // Verify reasonable recall on a small random dataset
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(123);

        let dim = 16;
        let n = 200;
        let cfg = HnswConfig::new(dim, DistanceMetric::L2)
            .with_m(8)
            .with_ef_construction(64)
            .with_ef_search(32);
        let idx = HnswIndex::new(cfg).unwrap();

        let mut vectors = Vec::with_capacity(n);
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
            idx.insert(i as u64, &v).unwrap();
            vectors.push(v);
        }

        // Brute-force k=10 for a random query
        let query: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>()).collect();
        let mut brute: Vec<(u64, f32)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, crate::distance::l2_distance(&query, v)))
            .collect();
        brute.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let brute_top10: HashSet<u64> = brute.iter().take(10).map(|&(id, _)| id).collect();

        let hnsw_results = idx.search(&query, 10).unwrap();
        let hnsw_top10: HashSet<u64> = hnsw_results.iter().map(|r| r.id).collect();

        let recall = brute_top10.intersection(&hnsw_top10).count() as f32 / 10.0;
        // Expect recall >= 0.7 for this small dataset with reasonable params
        assert!(recall >= 0.7, "recall {recall} too low (expected >= 0.7)");
    }
}
