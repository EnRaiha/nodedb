// SPDX-License-Identifier: BUSL-1.1

//! The LSN ↔ commit-time map of an archive, built offline from the
//! `TimeAnchor` records of its segments.

use nodedb_types::temporal::{LsnTimeAnchor, LsnTimeError, LsnTimeMap};
use tracing::warn;

const NANOS_PER_MICRO: u64 = 1_000;

/// The last nanosecond of the microsecond `micros`: a target names a whole
/// microsecond, so a commit anywhere inside it is at or before the target.
pub fn target_ns(micros: u64) -> u64 {
    micros
        .saturating_mul(NANOS_PER_MICRO)
        .saturating_add(NANOS_PER_MICRO - 1)
}

/// Commit-time anchors read from archived segments.
#[derive(Debug, Clone)]
pub struct Timeline {
    map: LsnTimeMap,
}

impl Timeline {
    /// Anchors in LSN order. The map is unbounded, so no anchor is thinned
    /// away. An anchor that does not advance time is skipped: its records
    /// fall to the next anchor, which is later, so a lookup never returns an
    /// LSN committed after its target.
    pub fn from_anchors(anchors: impl IntoIterator<Item = LsnTimeAnchor>) -> Self {
        let mut map = LsnTimeMap::with_cap(usize::MAX);
        for anchor in anchors {
            if let Err(error) = map.push(anchor) {
                warn!(lsn = anchor.lsn, %error, "archived WAL time anchor skipped");
            }
        }
        Self { map }
    }

    /// The highest anchored LSN committed at or before `target_ns`.
    pub fn lsn_at_or_before(&self, target_ns: u64) -> Result<u64, LsnTimeError> {
        self.map.lsn_at_or_before(target_ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timeline(anchors: &[(u64, u64)]) -> Timeline {
        Timeline::from_anchors(anchors.iter().map(|&(l, t)| LsnTimeAnchor::new(l, t)))
    }

    #[test]
    fn a_target_resolves_to_the_last_anchor_at_or_before_it() {
        let t = timeline(&[(5, 100), (9, 200), (14, 300)]);
        assert_eq!(t.lsn_at_or_before(100), Ok(5));
        assert_eq!(t.lsn_at_or_before(250), Ok(9));
        assert_eq!(t.lsn_at_or_before(u64::MAX), Ok(14));
    }

    #[test]
    fn a_target_before_the_first_anchor_is_a_typed_error() {
        let t = timeline(&[(5, 100), (9, 200)]);
        assert_eq!(
            t.lsn_at_or_before(99),
            Err(LsnTimeError::BeforeFirstAnchor {
                target_ns: 99,
                first_anchor_ns: 100
            })
        );
        assert_eq!(
            timeline(&[]).lsn_at_or_before(1),
            Err(LsnTimeError::NoAnchors)
        );
    }

    #[test]
    fn an_anchor_that_does_not_advance_time_is_skipped() {
        let t = timeline(&[(5, 100), (9, 100), (14, 300)]);
        assert_eq!(t.lsn_at_or_before(200), Ok(5));
        assert_eq!(t.lsn_at_or_before(300), Ok(14));
    }

    #[test]
    fn a_target_covers_its_whole_microsecond() {
        assert_eq!(target_ns(7), 7_999);
        let t = timeline(&[(5, 7_500), (9, 8_000)]);
        assert_eq!(t.lsn_at_or_before(target_ns(7)), Ok(5));
    }
}
