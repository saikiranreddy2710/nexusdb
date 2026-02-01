//! Timestamp types for NexusDB.
//!
//! This module provides timestamp implementations including Hybrid Logical Clocks
//! for distributed systems.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::NodeId;

/// A simple monotonic timestamp (microseconds since epoch).
///
/// This is a lightweight timestamp for local ordering. For distributed
/// ordering, use `HlcTimestamp` instead.
///
/// # Example
///
/// ```rust
/// use nexus_common::types::Timestamp;
///
/// let ts = Timestamp::now();
/// assert!(ts.as_micros() > 0);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Zero timestamp (epoch).
    pub const ZERO: Self = Self(0);

    /// Maximum timestamp value.
    pub const MAX: Self = Self(u64::MAX);

    /// Creates a timestamp from microseconds since Unix epoch.
    #[inline]
    #[must_use]
    pub const fn from_micros(micros: u64) -> Self {
        Self(micros)
    }

    /// Creates a timestamp from the current system time.
    #[must_use]
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Self(duration.as_micros() as u64)
    }

    /// Returns the timestamp as microseconds since Unix epoch.
    #[inline]
    #[must_use]
    pub const fn as_micros(self) -> u64 {
        self.0
    }

    /// Returns the timestamp as milliseconds since Unix epoch.
    #[inline]
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0 / 1000
    }

    /// Returns the timestamp as seconds since Unix epoch.
    #[inline]
    #[must_use]
    pub const fn as_secs(self) -> u64 {
        self.0 / 1_000_000
    }

    /// Returns the duration since this timestamp.
    #[must_use]
    pub fn elapsed(self) -> Duration {
        let now = Self::now();
        Duration::from_micros(now.0.saturating_sub(self.0))
    }

    /// Adds a duration to this timestamp.
    #[inline]
    #[must_use]
    pub fn add(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(duration.as_micros() as u64))
    }

    /// Subtracts a duration from this timestamp.
    #[inline]
    #[must_use]
    pub fn sub(self, duration: Duration) -> Self {
        Self(self.0.saturating_sub(duration.as_micros() as u64))
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({}us)", self.0)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format as ISO 8601 if reasonable
        if self.0 > 0 && self.0 < i64::MAX as u64 {
            let secs = (self.0 / 1_000_000) as i64;
            let subsec_micros = (self.0 % 1_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, subsec_micros * 1000) {
                return write!(f, "{}", dt.format("%Y-%m-%dT%H:%M:%S%.6fZ"));
            }
        }
        write!(f, "{}us", self.0)
    }
}

impl From<u64> for Timestamp {
    #[inline]
    fn from(micros: u64) -> Self {
        Self::from_micros(micros)
    }
}

impl From<Timestamp> for u64 {
    #[inline]
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

/// Hybrid Logical Clock timestamp for distributed ordering.
///
/// HLC combines physical time with a logical counter to provide:
/// - Monotonicity: timestamps always increase
/// - Causality: if event A happened before B, HLC(A) < HLC(B)
/// - Bounded drift: HLC stays close to physical time
///
/// Layout (96 bits total):
/// - Physical time: 48 bits (milliseconds, good for ~8900 years)
/// - Logical counter: 16 bits (65536 events per millisecond)
/// - Node ID: 32 bits (identifies the originating node)
///
/// # Example
///
/// ```rust
/// use nexus_common::types::{HlcTimestamp, NodeId};
///
/// let node = NodeId::new(1);
/// let ts = HlcTimestamp::now(node);
/// let ts2 = ts.increment();
/// assert!(ts2 > ts);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Physical time in milliseconds since Unix epoch (48 bits used).
    physical: u64,
    /// Logical counter for ordering within the same physical time.
    logical: u16,
    /// Node ID that created this timestamp.
    node_id: NodeId,
}

impl HlcTimestamp {
    /// Zero HLC timestamp.
    pub const ZERO: Self = Self {
        physical: 0,
        logical: 0,
        node_id: NodeId::INVALID,
    };

    /// Maximum HLC timestamp.
    pub const MAX: Self = Self {
        physical: u64::MAX,
        logical: u16::MAX,
        node_id: NodeId::new(u32::MAX),
    };

    /// Creates a new HLC timestamp from the current time.
    #[must_use]
    pub fn now(node_id: NodeId) -> Self {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        Self {
            physical,
            logical: 0,
            node_id,
        }
    }

    /// Creates an HLC timestamp from components.
    #[inline]
    #[must_use]
    pub const fn new(physical: u64, logical: u16, node_id: NodeId) -> Self {
        Self {
            physical,
            logical,
            node_id,
        }
    }

    /// Returns the physical time component (milliseconds since epoch).
    #[inline]
    #[must_use]
    pub const fn physical(self) -> u64 {
        self.physical
    }

    /// Returns the logical counter component.
    #[inline]
    #[must_use]
    pub const fn logical(self) -> u16 {
        self.logical
    }

    /// Returns the node ID component.
    #[inline]
    #[must_use]
    pub const fn node_id(self) -> NodeId {
        self.node_id
    }

    /// Updates the HLC based on a received message timestamp.
    ///
    /// This implements the HLC receive algorithm:
    /// - Take the maximum of local time and received time
    /// - Increment the logical counter if times are equal
    #[must_use]
    pub fn receive(self, other: Self, local_node: NodeId) -> Self {
        let now_physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        let (physical, logical) = if now_physical > self.physical && now_physical > other.physical {
            // Physical time has advanced past both
            (now_physical, 0)
        } else if self.physical == other.physical {
            // Both have same physical time, take max logical + 1
            (
                self.physical,
                self.logical.max(other.logical).saturating_add(1),
            )
        } else if self.physical > other.physical {
            // Our time is ahead
            (self.physical, self.logical.saturating_add(1))
        } else {
            // Other time is ahead
            (other.physical, other.logical.saturating_add(1))
        };

        Self {
            physical,
            logical,
            node_id: local_node,
        }
    }

    /// Increments the HLC for a local event.
    #[must_use]
    pub fn increment(self) -> Self {
        let now_physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        let (physical, logical) = if now_physical > self.physical {
            (now_physical, 0)
        } else {
            (self.physical, self.logical.saturating_add(1))
        };

        Self {
            physical,
            logical,
            node_id: self.node_id,
        }
    }

    /// Converts to a 12-byte representation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[0..8].copy_from_slice(&self.physical.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.logical.to_be_bytes());
        bytes[10..12].copy_from_slice(&self.node_id.as_u32().to_be_bytes()[2..4]);
        bytes
    }

    /// Creates from a 12-byte representation.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 12]) -> Self {
        let physical = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let logical = u16::from_be_bytes([bytes[8], bytes[9]]);
        let node_id = NodeId::new(u16::from_be_bytes([bytes[10], bytes[11]]) as u32);

        Self {
            physical,
            logical,
            node_id,
        }
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.physical.cmp(&other.physical) {
            Ordering::Equal => match self.logical.cmp(&other.logical) {
                Ordering::Equal => self.node_id.cmp(&other.node_id),
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HlcTimestamp({}ms.{}.node{})",
            self.physical, self.logical, self.node_id
        )
    }
}

impl fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.physical, self.logical)
    }
}

/// Epoch number for garbage collection and versioning.
///
/// Epochs are used to track major version boundaries and coordinate
/// garbage collection across the cluster.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Epoch(u64);

impl Epoch {
    /// Initial epoch.
    pub const ZERO: Self = Self(0);

    /// Creates a new epoch from a raw value.
    #[inline]
    #[must_use]
    pub const fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    /// Returns the raw epoch value.
    #[inline]
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the next epoch.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Debug for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Epoch({})", self.0)
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Epoch {
    #[inline]
    fn from(epoch: u64) -> Self {
        Self::new(epoch)
    }
}

impl From<Epoch> for u64 {
    #[inline]
    fn from(epoch: Epoch) -> Self {
        epoch.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp() {
        let ts = Timestamp::now();
        assert!(ts.as_micros() > 0);

        let ts2 = Timestamp::from_micros(1_000_000);
        assert_eq!(ts2.as_secs(), 1);
        assert_eq!(ts2.as_millis(), 1000);
    }

    #[test]
    fn test_timestamp_arithmetic() {
        let ts = Timestamp::from_micros(1_000_000);
        let ts2 = ts.add(Duration::from_secs(1));
        assert_eq!(ts2.as_micros(), 2_000_000);

        let ts3 = ts2.sub(Duration::from_secs(1));
        assert_eq!(ts3, ts);
    }

    #[test]
    fn test_hlc_ordering() {
        let node = NodeId::new(1);
        let ts1 = HlcTimestamp::new(1000, 0, node);
        let ts2 = HlcTimestamp::new(1000, 1, node);
        let ts3 = HlcTimestamp::new(1001, 0, node);

        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
    }

    #[test]
    fn test_hlc_increment() {
        let node = NodeId::new(1);
        let ts1 = HlcTimestamp::new(1000, 0, node);
        let ts2 = ts1.increment();

        // Either physical advanced or logical incremented
        assert!(ts2 > ts1);
    }

    #[test]
    fn test_hlc_bytes() {
        let node = NodeId::new(42);
        let ts = HlcTimestamp::new(1234567890, 100, node);

        let bytes = ts.to_bytes();
        let ts2 = HlcTimestamp::from_bytes(bytes);

        assert_eq!(ts.physical, ts2.physical);
        assert_eq!(ts.logical, ts2.logical);
    }

    #[test]
    fn test_epoch() {
        let epoch = Epoch::ZERO;
        assert_eq!(epoch.as_u64(), 0);

        let next = epoch.next();
        assert_eq!(next.as_u64(), 1);
    }
}
