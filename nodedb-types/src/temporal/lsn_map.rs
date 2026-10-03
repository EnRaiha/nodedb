// SPDX-License-Identifier: Apache-2.0

//! LSN ↔ commit-time map built from WAL time anchors.
//!
//! The WAL writer appends one time anchor to every group-commit batch, inside
//! the batch's own write. An anchor names the batch's last LSN and the HLC wall
//! time (ns since the Unix epoch) the batch committed at. Every record at or
//! below an anchor's LSN committed at or before the anchor's time.
//!
//! Anchors are strictly increasing in both LSN and time. The map holds at most
//! `cap` anchors. When full, it first drops anchors that share a millisecond
//! with their successor, which no millisecond-granular lookup can return. If
//! that frees too little, it drops every other anchor in the older half, so
//! recent history stays dense and old history gets coarser.

use serde::{Deserialize, Serialize};

const NANOS_PER_MS: u64 = 1_000_000;

/// Default anchor capacity: 65,536 anchors, 1 MiB.
pub const DEFAULT_ANCHOR_CAP: usize = 1 << 16;

/// Smallest capacity a map accepts. Downsampling needs room to keep the first
/// and last anchors plus a thinned middle.
const MIN_ANCHOR_CAP: usize = 4;

/// One WAL time anchor: every record at or below `lsn` committed at or before
/// `hlc_wall_ns`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct LsnTimeAnchor {
    pub lsn: u64,
    /// HLC wall component, in nanoseconds since the Unix epoch.
    pub hlc_wall_ns: u64,
}

impl LsnTimeAnchor {
    pub const fn new(lsn: u64, hlc_wall_ns: u64) -> Self {
        Self { lsn, hlc_wall_ns }
    }
}

/// Error from the LSN ↔ time map.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LsnTimeError {
    /// An anchor with a higher LSN carried a time at or below the last anchor's.
    #[error(
        "time anchor is not monotonic: last=(lsn={last_lsn}, ns={last_ns}), \
         new=(lsn={new_lsn}, ns={new_ns})"
    )]
    NonMonotonic {
        last_lsn: u64,
        last_ns: u64,
        new_lsn: u64,
        new_ns: u64,
    },

    /// The map holds no anchor, so no time maps to an LSN.
    #[error("no WAL time anchor is known; no commit time maps to an LSN")]
    NoAnchors,

    /// The target time is before the oldest retained anchor.
    #[error(
        "time {target_ns}ns predates the oldest retained WAL time anchor \
         ({first_anchor_ns}ns); no committed state is known before it"
    )]
    BeforeFirstAnchor {
        target_ns: u64,
        first_anchor_ns: u64,
    },
}

/// Bounded, ordered LSN ↔ commit-time map.
///
/// Not synchronized. The owner wraps it in the primitive its plane needs.
#[derive(Debug, Clone)]
pub struct LsnTimeMap {
    anchors: Vec<LsnTimeAnchor>,
    cap: usize,
}

impl Default for LsnTimeMap {
    fn default() -> Self {
        Self::new()
    }
}

impl LsnTimeMap {
    pub const fn new() -> Self {
        Self::with_cap(DEFAULT_ANCHOR_CAP)
    }

    /// A map holding at most `cap` anchors (raised to 4 when smaller).
    pub const fn with_cap(cap: usize) -> Self {
        let cap = if cap < MIN_ANCHOR_CAP {
            MIN_ANCHOR_CAP
        } else {
            cap
        };
        Self {
            anchors: Vec::new(),
            cap,
        }
    }

    pub fn len(&self) -> usize {
        self.anchors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.anchors.is_empty()
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    /// The retained anchors, oldest first.
    pub fn anchors(&self) -> &[LsnTimeAnchor] {
        &self.anchors
    }

    pub fn last(&self) -> Option<LsnTimeAnchor> {
        self.anchors.last().copied()
    }

    /// Add an anchor. Returns `Ok(false)` when its LSN is at or below the last
    /// anchor's: replay re-reads anchors the map already holds.
    pub fn push(&mut self, anchor: LsnTimeAnchor) -> Result<bool, LsnTimeError> {
        if let Some(last) = self.anchors.last().copied() {
            if anchor.lsn <= last.lsn {
                return Ok(false);
            }
            if anchor.hlc_wall_ns <= last.hlc_wall_ns {
                return Err(LsnTimeError::NonMonotonic {
                    last_lsn: last.lsn,
                    last_ns: last.hlc_wall_ns,
                    new_lsn: anchor.lsn,
                    new_ns: anchor.hlc_wall_ns,
                });
            }
        }
        self.anchors.push(anchor);
        if self.anchors.len() > self.cap {
            self.downsample();
        }
        Ok(true)
    }

    /// The highest anchored LSN whose commit time is at or before `target_ns`.
    ///
    /// A target past the last anchor returns the last anchor's LSN. Records
    /// above it have no completed commit yet, so they are not part of any
    /// past state.
    ///
    /// A target before the oldest anchor returns LSN 0 when that anchor names
    /// LSN 0: nothing had committed by its time, so nothing had committed
    /// before it either. Before an oldest anchor above LSN 0, the records at
    /// or below it have no known commit time, and the lookup is an error.
    pub fn lsn_at_or_before(&self, target_ns: u64) -> Result<u64, LsnTimeError> {
        let first = self.anchors.first().ok_or(LsnTimeError::NoAnchors)?;
        let idx = self.anchors.partition_point(|a| a.hlc_wall_ns <= target_ns);
        if idx == 0 {
            if first.lsn == 0 {
                return Ok(0);
            }
            return Err(LsnTimeError::BeforeFirstAnchor {
                target_ns,
                first_anchor_ns: first.hlc_wall_ns,
            });
        }
        Ok(self.anchors[idx - 1].lsn)
    }

    /// [`Self::lsn_at_or_before`] for a millisecond target. The whole
    /// millisecond `target_ms` counts, so a commit at `target_ms` + 0.5 ms is
    /// included. A negative target is before every anchor.
    pub fn lsn_at_or_before_ms(&self, target_ms: i64) -> Result<u64, LsnTimeError> {
        let first = self.anchors.first().ok_or(LsnTimeError::NoAnchors)?;
        let Ok(ms) = u64::try_from(target_ms) else {
            if first.lsn == 0 {
                return Ok(0);
            }
            return Err(LsnTimeError::BeforeFirstAnchor {
                target_ns: 0,
                first_anchor_ns: first.hlc_wall_ns,
            });
        };
        let target_ns = ms
            .saturating_mul(NANOS_PER_MS)
            .saturating_add(NANOS_PER_MS - 1);
        self.lsn_at_or_before(target_ns)
    }

    /// Commit time of the batch holding `lsn`: the time of the first anchor at
    /// or above it. `None` when no anchor covers `lsn` yet.
    pub fn commit_ns_of(&self, lsn: u64) -> Option<u64> {
        let idx = self.anchors.partition_point(|a| a.lsn < lsn);
        self.anchors.get(idx).map(|a| a.hlc_wall_ns)
    }

    fn downsample(&mut self) {
        // Keep an anchor only when the next one falls in a later millisecond.
        // A millisecond target resolves to the last anchor of its millisecond,
        // so the dropped anchors were unreachable at that granularity.
        let n = self.anchors.len();
        let mut write = 0;
        for read in 0..n {
            let keep = read + 1 == n
                || self.anchors[read].hlc_wall_ns / NANOS_PER_MS
                    != self.anchors[read + 1].hlc_wall_ns / NANOS_PER_MS;
            if keep {
                self.anchors[write] = self.anchors[read];
                write += 1;
            }
        }
        self.anchors.truncate(write);

        // Leave a quarter of the cap free so thinning is not re-run per push.
        let target = self.cap - self.cap / 4;
        if self.anchors.len() <= target {
            return;
        }
        let half = self.anchors.len() / 2;
        let mut write = 0;
        for read in 0..self.anchors.len() {
            if read >= half || read % 2 == 0 {
                self.anchors[write] = self.anchors[read];
                write += 1;
            }
        }
        self.anchors.truncate(write);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = NANOS_PER_MS;

    fn map_of(anchors: &[(u64, u64)]) -> LsnTimeMap {
        let mut m = LsnTimeMap::new();
        for &(lsn, ns) in anchors {
            assert!(m.push(LsnTimeAnchor::new(lsn, ns)).unwrap());
        }
        m
    }

    #[test]
    fn empty_map_is_a_typed_error() {
        let m = LsnTimeMap::new();
        assert_eq!(m.lsn_at_or_before(5), Err(LsnTimeError::NoAnchors));
        assert_eq!(m.lsn_at_or_before_ms(5), Err(LsnTimeError::NoAnchors));
        assert_eq!(m.commit_ns_of(1), None);
    }

    #[test]
    fn floor_is_exact_at_batch_edges() {
        let m = map_of(&[(5, 100), (9, 200), (14, 300)]);
        assert_eq!(m.lsn_at_or_before(100).unwrap(), 5);
        assert_eq!(m.lsn_at_or_before(199).unwrap(), 5);
        assert_eq!(m.lsn_at_or_before(200).unwrap(), 9);
        assert_eq!(m.lsn_at_or_before(299).unwrap(), 9);
        assert_eq!(m.lsn_at_or_before(300).unwrap(), 14);
        assert_eq!(m.lsn_at_or_before(u64::MAX).unwrap(), 14);
    }

    #[test]
    fn target_before_first_anchor_is_an_error() {
        let m = map_of(&[(5, 100), (9, 200)]);
        assert_eq!(
            m.lsn_at_or_before(99),
            Err(LsnTimeError::BeforeFirstAnchor {
                target_ns: 99,
                first_anchor_ns: 100,
            })
        );
        assert!(matches!(
            m.lsn_at_or_before_ms(-1),
            Err(LsnTimeError::BeforeFirstAnchor { .. })
        ));
    }

    #[test]
    fn target_before_an_lsn_zero_anchor_is_the_empty_state() {
        let m = map_of(&[(0, 100), (4, 200)]);
        assert_eq!(m.lsn_at_or_before(99).unwrap(), 0);
        assert_eq!(m.lsn_at_or_before(0).unwrap(), 0);
        assert_eq!(m.lsn_at_or_before_ms(-1).unwrap(), 0);
        assert_eq!(m.lsn_at_or_before(200).unwrap(), 4);
    }

    #[test]
    fn millisecond_target_covers_the_whole_millisecond() {
        let m = map_of(&[(5, 7 * MS + 1), (9, 7 * MS + 999_999), (12, 8 * MS)]);
        assert!(matches!(
            m.lsn_at_or_before_ms(6),
            Err(LsnTimeError::BeforeFirstAnchor { .. })
        ));
        assert_eq!(m.lsn_at_or_before_ms(7).unwrap(), 9);
        assert_eq!(m.lsn_at_or_before_ms(8).unwrap(), 12);
    }

    #[test]
    fn commit_time_is_the_covering_anchor() {
        let m = map_of(&[(5, 100), (9, 200)]);
        assert_eq!(m.commit_ns_of(1), Some(100));
        assert_eq!(m.commit_ns_of(5), Some(100));
        assert_eq!(m.commit_ns_of(6), Some(200));
        assert_eq!(m.commit_ns_of(9), Some(200));
        assert_eq!(m.commit_ns_of(10), None);
    }

    #[test]
    fn replayed_anchor_is_ignored_and_regression_is_rejected() {
        let mut m = map_of(&[(10, 1_000)]);
        assert!(!m.push(LsnTimeAnchor::new(10, 1_000)).unwrap());
        assert!(!m.push(LsnTimeAnchor::new(5, 500)).unwrap());
        assert!(matches!(
            m.push(LsnTimeAnchor::new(20, 1_000)),
            Err(LsnTimeError::NonMonotonic { .. })
        ));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn same_millisecond_anchors_are_dropped_first() {
        let mut m = LsnTimeMap::with_cap(8);
        // Two anchors per millisecond: the first of each pair is unreachable
        // by a millisecond lookup.
        for i in 0..9u64 {
            m.push(LsnTimeAnchor::new(i + 1, (i / 2) * MS + (i % 2) * 10))
                .unwrap();
        }
        assert!(m.len() <= m.cap());
        for ms in 0..4i64 {
            assert_eq!(m.lsn_at_or_before_ms(ms).unwrap(), 2 * ms as u64 + 2);
        }
        assert_eq!(m.lsn_at_or_before_ms(4).unwrap(), 9);
    }

    #[test]
    fn map_stays_bounded_and_keeps_both_ends() {
        let mut m = LsnTimeMap::with_cap(64);
        for i in 1..=10_000u64 {
            m.push(LsnTimeAnchor::new(i, i * MS)).unwrap();
            assert!(m.len() <= m.cap());
        }
        let anchors = m.anchors();
        assert_eq!(anchors[0], LsnTimeAnchor::new(1, MS));
        assert_eq!(
            anchors[anchors.len() - 1],
            LsnTimeAnchor::new(10_000, 10_000 * MS)
        );
        assert!(anchors.windows(2).all(|w| w[0].lsn < w[1].lsn));
        // Recent history keeps full resolution.
        assert_eq!(m.lsn_at_or_before_ms(9_999).unwrap(), 9_999);
        // Old history is coarser, but the floor never overshoots.
        let lsn = m.lsn_at_or_before_ms(500).unwrap();
        assert!(lsn <= 500);
    }
}
