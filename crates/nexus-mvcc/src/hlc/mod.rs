//! Hybrid Logical Clock (HLC) implementation.
//!
//! HLC provides monotonic timestamps that combine physical time with logical
//! counters. This gives us:
//! - Causality tracking (like Lamport clocks)
//! - Bounded clock skew (tied to physical time)
//! - Total ordering of events across distributed nodes
//!
//! # Structure
//!
//! An HLC timestamp consists of:
//! - Physical time (wall clock, milliseconds since epoch)
//! - Logical counter (incremented when physical time hasn't advanced)
//! - Node ID (for tie-breaking in distributed systems)
//!
//! # Algorithm
//!
//! On local event:
//! 1. l' = l
//! 2. l = max(l', pt)
//! 3. if l == l' then c = c + 1 else c = 0
//!
//! On receive event with timestamp (l_m, c_m):
//! 1. l' = l
//! 2. l = max(l', l_m, pt)
//! 3. if l == l' == l_m then c = max(c, c_m) + 1
//!    else if l == l' then c = c + 1
//!    else if l == l_m then c = c_m + 1
//!    else c = 0

use parking_lot::Mutex;
use std::cmp::Ordering;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Maximum allowed clock skew in milliseconds.
/// If observed skew exceeds this, we'll wait or error.
pub const MAX_CLOCK_SKEW_MS: u64 = 500;

/// An HLC timestamp.
///
/// Packed format (128 bits total):
/// - bits 0-63: physical time (ms since epoch) << 16 | logical counter
/// - bits 64-95: node_id (u32)
/// - bits 96-127: reserved
#[derive(Clone, Copy, Default)]
pub struct HlcTimestamp {
    /// Physical time in milliseconds since UNIX epoch.
    pub physical: u64,
    /// Logical counter for events at the same physical time.
    pub logical: u32,
    /// Node ID for distributed tie-breaking.
    pub node_id: u32,
}

impl HlcTimestamp {
    /// The zero/minimum timestamp.
    pub const ZERO: Self = Self {
        physical: 0,
        logical: 0,
        node_id: 0,
    };

    /// The maximum timestamp.
    pub const MAX: Self = Self {
        physical: u64::MAX,
        logical: u32::MAX,
        node_id: u32::MAX,
    };

    /// Creates a new timestamp.
    pub const fn new(physical: u64, logical: u32, node_id: u32) -> Self {
        Self {
            physical,
            logical,
            node_id,
        }
    }

    /// Creates a timestamp from the current system time.
    pub fn now(node_id: u32) -> Self {
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

    /// Returns the timestamp as a single u128 for efficient comparison.
    #[inline]
    pub fn as_u128(&self) -> u128 {
        ((self.physical as u128) << 64) | ((self.logical as u128) << 32) | (self.node_id as u128)
    }

    /// Creates a timestamp from a u128.
    #[inline]
    pub fn from_u128(v: u128) -> Self {
        Self {
            physical: (v >> 64) as u64,
            logical: ((v >> 32) & 0xFFFFFFFF) as u32,
            node_id: (v & 0xFFFFFFFF) as u32,
        }
    }

    /// Returns the timestamp as bytes (16 bytes, big-endian).
    pub fn to_bytes(&self) -> [u8; 16] {
        self.as_u128().to_be_bytes()
    }

    /// Creates a timestamp from bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self::from_u128(u128::from_be_bytes(bytes))
    }

    /// Checks if this timestamp is zero.
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.physical == 0 && self.logical == 0
    }

    /// Returns the successor timestamp (same physical, logical + 1).
    pub fn successor(&self) -> Self {
        Self {
            physical: self.physical,
            logical: self.logical.saturating_add(1),
            node_id: self.node_id,
        }
    }
}

impl PartialEq for HlcTimestamp {
    fn eq(&self, other: &Self) -> bool {
        self.physical == other.physical && self.logical == other.logical
    }
}

impl Eq for HlcTimestamp {}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.physical.cmp(&other.physical) {
            Ordering::Equal => match self.logical.cmp(&other.logical) {
                Ordering::Equal => self.node_id.cmp(&other.node_id),
                other => other,
            },
            other => other,
        }
    }
}

impl fmt::Debug for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HLC({}.{:04}@{})",
            self.physical, self.logical, self.node_id
        )
    }
}

impl fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{:04}", self.physical, self.logical)
    }
}

/// Error types for HLC operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HlcError {
    /// Clock skew is too large.
    ClockSkewTooLarge {
        /// Local physical time.
        local_time: u64,
        /// Remote physical time.
        remote_time: u64,
        /// Observed skew in milliseconds.
        skew_ms: u64,
    },
    /// Logical counter overflow.
    LogicalOverflow,
}

impl fmt::Display for HlcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClockSkewTooLarge {
                local_time,
                remote_time,
                skew_ms,
            } => write!(
                f,
                "clock skew too large: local={}, remote={}, skew={}ms",
                local_time, remote_time, skew_ms
            ),
            Self::LogicalOverflow => write!(f, "logical counter overflow"),
        }
    }
}

impl std::error::Error for HlcError {}

/// Result type for HLC operations.
pub type HlcResult<T> = Result<T, HlcError>;

/// A Hybrid Logical Clock.
///
/// Thread-safe clock that maintains monotonic timestamps combining
/// physical time with logical counters.
pub struct HybridLogicalClock {
    /// Current physical time (cached).
    physical: AtomicU64,
    /// Current logical counter.
    logical: AtomicU64,
    /// Node ID for this clock.
    node_id: u32,
    /// Mutex for atomic read-modify-write operations.
    state: Mutex<()>,
    /// Maximum allowed clock skew.
    max_skew_ms: u64,
}

impl HybridLogicalClock {
    /// Creates a new HLC with the given node ID.
    pub fn new(node_id: u32) -> Self {
        let now = Self::physical_time();
        Self {
            physical: AtomicU64::new(now),
            logical: AtomicU64::new(0),
            node_id,
            state: Mutex::new(()),
            max_skew_ms: MAX_CLOCK_SKEW_MS,
        }
    }

    /// Creates an HLC with a custom maximum skew.
    pub fn with_max_skew(node_id: u32, max_skew_ms: u64) -> Self {
        let now = Self::physical_time();
        Self {
            physical: AtomicU64::new(now),
            logical: AtomicU64::new(0),
            node_id,
            state: Mutex::new(()),
            max_skew_ms,
        }
    }

    /// Returns the current physical time in milliseconds.
    #[inline]
    fn physical_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64
    }

    /// Returns the node ID.
    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    /// Generates a new timestamp for a local event.
    pub fn now(&self) -> HlcTimestamp {
        let _guard = self.state.lock();

        let pt = Self::physical_time();
        let prev_l = self.physical.load(AtomicOrdering::Acquire);

        let new_l = pt.max(prev_l);
        let new_c = if new_l == prev_l {
            self.logical.fetch_add(1, AtomicOrdering::AcqRel) + 1
        } else {
            self.physical.store(new_l, AtomicOrdering::Release);
            self.logical.store(0, AtomicOrdering::Release);
            0
        };

        HlcTimestamp {
            physical: new_l,
            logical: new_c as u32,
            node_id: self.node_id,
        }
    }

    /// Updates the clock based on a received timestamp.
    ///
    /// This implements the "receive" event of the HLC algorithm.
    pub fn update(&self, received: HlcTimestamp) -> HlcResult<HlcTimestamp> {
        let _guard = self.state.lock();

        let pt = Self::physical_time();
        let prev_l = self.physical.load(AtomicOrdering::Acquire);
        let prev_c = self.logical.load(AtomicOrdering::Acquire) as u32;

        // Check for excessive clock skew
        if received.physical > pt + self.max_skew_ms {
            return Err(HlcError::ClockSkewTooLarge {
                local_time: pt,
                remote_time: received.physical,
                skew_ms: received.physical - pt,
            });
        }

        let new_l = pt.max(prev_l).max(received.physical);

        let new_c = if new_l == prev_l && new_l == received.physical {
            prev_c
                .max(received.logical)
                .checked_add(1)
                .ok_or(HlcError::LogicalOverflow)?
        } else if new_l == prev_l {
            prev_c.checked_add(1).ok_or(HlcError::LogicalOverflow)?
        } else if new_l == received.physical {
            received
                .logical
                .checked_add(1)
                .ok_or(HlcError::LogicalOverflow)?
        } else {
            0
        };

        self.physical.store(new_l, AtomicOrdering::Release);
        self.logical.store(new_c as u64, AtomicOrdering::Release);

        Ok(HlcTimestamp {
            physical: new_l,
            logical: new_c,
            node_id: self.node_id,
        })
    }

    /// Returns the current timestamp without advancing the clock.
    pub fn read(&self) -> HlcTimestamp {
        HlcTimestamp {
            physical: self.physical.load(AtomicOrdering::Acquire),
            logical: self.logical.load(AtomicOrdering::Acquire) as u32,
            node_id: self.node_id,
        }
    }

    /// Checks if a timestamp is in the future relative to our clock.
    pub fn is_future(&self, ts: &HlcTimestamp) -> bool {
        let current = self.read();
        ts > &current
    }

    /// Resets the clock (for testing only).
    #[cfg(test)]
    pub fn reset(&self) {
        let _guard = self.state.lock();
        self.physical.store(0, AtomicOrdering::Release);
        self.logical.store(0, AtomicOrdering::Release);
    }
}

impl fmt::Debug for HybridLogicalClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HybridLogicalClock")
            .field("physical", &self.physical.load(AtomicOrdering::Relaxed))
            .field("logical", &self.logical.load(AtomicOrdering::Relaxed))
            .field("node_id", &self.node_id)
            .finish()
    }
}

/// A clock source trait for abstracting time access.
pub trait ClockSource: Send + Sync {
    /// Returns the current timestamp.
    fn now(&self) -> HlcTimestamp;

    /// Updates the clock with a received timestamp.
    fn update(&self, received: HlcTimestamp) -> HlcResult<HlcTimestamp>;

    /// Returns the current timestamp without advancing.
    fn read(&self) -> HlcTimestamp;
}

impl ClockSource for HybridLogicalClock {
    fn now(&self) -> HlcTimestamp {
        self.now()
    }

    fn update(&self, received: HlcTimestamp) -> HlcResult<HlcTimestamp> {
        self.update(received)
    }

    fn read(&self) -> HlcTimestamp {
        self.read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_timestamp_ordering() {
        let ts1 = HlcTimestamp::new(100, 0, 1);
        let ts2 = HlcTimestamp::new(100, 1, 1);
        let ts3 = HlcTimestamp::new(101, 0, 1);

        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
        assert!(ts1 < ts3);
    }

    #[test]
    fn test_hlc_timestamp_equality() {
        let ts1 = HlcTimestamp::new(100, 5, 1);
        let ts2 = HlcTimestamp::new(100, 5, 2);

        // Equality ignores node_id
        assert_eq!(ts1, ts2);

        // But ordering considers it
        assert!(ts1 < ts2);
    }

    #[test]
    fn test_hlc_timestamp_serialization() {
        let ts = HlcTimestamp::new(1234567890, 42, 7);

        let bytes = ts.to_bytes();
        let restored = HlcTimestamp::from_bytes(bytes);

        assert_eq!(restored.physical, ts.physical);
        assert_eq!(restored.logical, ts.logical);
        assert_eq!(restored.node_id, ts.node_id);
    }

    #[test]
    fn test_hlc_timestamp_u128() {
        let ts = HlcTimestamp::new(1000, 50, 3);

        let packed = ts.as_u128();
        let unpacked = HlcTimestamp::from_u128(packed);

        assert_eq!(unpacked.physical, ts.physical);
        assert_eq!(unpacked.logical, ts.logical);
        assert_eq!(unpacked.node_id, ts.node_id);
    }

    #[test]
    fn test_hlc_monotonic() {
        let clock = HybridLogicalClock::new(1);

        let mut prev = clock.now();
        for _ in 0..100 {
            let next = clock.now();
            assert!(next > prev, "HLC must be monotonic");
            prev = next;
        }
    }

    #[test]
    fn test_hlc_logical_increment() {
        let clock = HybridLogicalClock::new(1);

        let ts1 = clock.now();
        let ts2 = clock.now();

        // If physical time hasn't changed, logical should increment
        if ts1.physical == ts2.physical {
            assert!(ts2.logical > ts1.logical);
        }
    }

    #[test]
    fn test_hlc_update_from_past() {
        let clock = HybridLogicalClock::new(1);

        let current = clock.now();
        let past = HlcTimestamp::new(current.physical - 1000, 0, 2);

        let updated = clock.update(past).unwrap();

        // Should be at least as recent as current
        assert!(updated >= current);
    }

    #[test]
    fn test_hlc_update_from_future() {
        let clock = HybridLogicalClock::new(1);

        let current = clock.now();
        let future = HlcTimestamp::new(current.physical + 100, 50, 2);

        let updated = clock.update(future).unwrap();

        // Should adopt the future timestamp with incremented logical
        assert_eq!(updated.physical, future.physical);
        assert!(updated.logical > future.logical);
    }

    #[test]
    fn test_hlc_clock_skew_error() {
        let clock = HybridLogicalClock::with_max_skew(1, 100);

        let current = clock.now();
        let far_future = HlcTimestamp::new(current.physical + 1000, 0, 2);

        let result = clock.update(far_future);
        assert!(matches!(result, Err(HlcError::ClockSkewTooLarge { .. })));
    }

    #[test]
    fn test_hlc_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let clock = Arc::new(HybridLogicalClock::new(1));
        let mut handles = vec![];

        for _ in 0..10 {
            let clock_clone = Arc::clone(&clock);
            handles.push(thread::spawn(move || {
                let mut timestamps = vec![];
                for _ in 0..100 {
                    timestamps.push(clock_clone.now());
                }
                timestamps
            }));
        }

        let mut all_timestamps = vec![];
        for handle in handles {
            all_timestamps.extend(handle.join().unwrap());
        }

        // All timestamps should be unique
        let len_before = all_timestamps.len();
        all_timestamps.sort();
        all_timestamps.dedup_by(|a, b| a.as_u128() == b.as_u128());
        assert_eq!(
            all_timestamps.len(),
            len_before,
            "All timestamps should be unique"
        );
    }

    #[test]
    fn test_hlc_successor() {
        let ts = HlcTimestamp::new(100, 5, 1);
        let succ = ts.successor();

        assert_eq!(succ.physical, ts.physical);
        assert_eq!(succ.logical, ts.logical + 1);
    }

    #[test]
    fn test_hlc_display() {
        let ts = HlcTimestamp::new(1234567890, 42, 1);
        let display = format!("{}", ts);
        assert!(display.contains("1234567890"));
        assert!(display.contains("0042"));
    }

    #[test]
    fn test_hlc_zero() {
        let zero = HlcTimestamp::ZERO;
        assert!(zero.is_zero());
        assert_eq!(zero.physical, 0);
        assert_eq!(zero.logical, 0);
    }
}
