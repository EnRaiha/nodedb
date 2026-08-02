// SPDX-License-Identifier: Apache-2.0

//! The retained-suffix invariant for replays that start mid-log.
//!
//! `replay_from(n)` promises the caller every record with `lsn >= n`. Checkpoint
//! truncation deletes whole segments below the checkpoint, so a caller whose
//! `from_lsn` falls inside an already-deleted range gets whatever happens to
//! survive above it — a *shorter* suffix that is byte-for-byte indistinguishable
//! from a complete one. [`SegmentContinuity`](super::SegmentContinuity) cannot
//! see it: a missing prefix leaves no boundary between two surviving segments to
//! be wrong, which is exactly why a missing prefix is legal there.
//!
//! The primary defence against that loss lives in the checkpoint manager, which
//! holds the truncation point at or below every consumer's persisted watermark
//! so no consumer's suffix is ever deleted. This check is the second line: it
//! makes a residual violation of that rule fail loudly at the point of use
//! instead of silently shortening the record stream. It is not the mechanism
//! that prevents the loss, and it must not be relied on as one — by the time it
//! fires, the records are already gone.

use crate::error::{Result, WalError};

use super::meta::SegmentMeta;

/// Verify that the WAL still retains every LSN at or above `from_lsn`.
///
/// `segments` must be the full discovered set for the directory, sorted by
/// `first_lsn` (as [`discover_segments`](super::discover_segments) returns it) —
/// not a pre-filtered live tail, whose first element is chosen using `from_lsn`
/// and therefore cannot contradict it.
///
/// Returns [`WalError::ReplayBelowRetainedFloor`] naming both the requested LSN
/// and the floor that survives.
pub fn check_retained_floor(segments: &[SegmentMeta], from_lsn: u64) -> Result<()> {
    // Zero is the "give me everything you still have" request used by full
    // recovery replay. It names no particular record, so no record can be
    // missing from it.
    if from_lsn == 0 {
        return Ok(());
    }

    let Some(earliest) = segments.first() else {
        // A directory with no segments is a WAL that was never written to, not
        // one that lost its prefix: there is no floor for the request to be
        // below, and the empty result it produces is the honest answer.
        return Ok(());
    };

    // Equality is the boundary case that must pass: a segment declares the LSN
    // of its own first record, so a request starting exactly at the floor asks
    // for a suffix that is fully retained.
    if from_lsn >= earliest.first_lsn {
        return Ok(());
    }

    let err = WalError::ReplayBelowRetainedFloor {
        from_lsn,
        retained_floor_lsn: earliest.first_lsn,
        earliest_segment: earliest.path.display().to_string(),
    };
    // Reported here and nowhere else: this is the only place that holds both
    // halves of the comparison. Callers above see the typed error and must
    // decide what to do about it, but they must not report it again — a second
    // report would carry its own fingerprint and look like a second bug.
    crate::diag::replay_below_retained_floor(&err, &earliest.path, from_lsn, earliest.first_lsn);
    Err(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn meta(first_lsn: u64) -> SegmentMeta {
        SegmentMeta {
            path: Path::new("/wal").join(super::super::segment_filename(first_lsn)),
            first_lsn,
            file_size: 0,
        }
    }

    #[test]
    fn suffix_above_the_floor_is_retained() {
        assert!(check_retained_floor(&[meta(5000), meta(9000)], 7000).is_ok());
    }

    #[test]
    fn suffix_exactly_at_the_floor_is_retained() {
        assert!(check_retained_floor(&[meta(5000), meta(9000)], 5000).is_ok());
    }

    #[test]
    fn suffix_below_the_floor_is_rejected_naming_both_lsns() {
        match check_retained_floor(&[meta(5000)], 4999) {
            Err(WalError::ReplayBelowRetainedFloor {
                from_lsn,
                retained_floor_lsn,
                ..
            }) => {
                assert_eq!(from_lsn, 4999);
                assert_eq!(retained_floor_lsn, 5000);
            }
            other => panic!("expected a retained-floor violation, got {other:?}"),
        }
    }

    #[test]
    fn full_replay_is_exempt() {
        assert!(check_retained_floor(&[meta(5000)], 0).is_ok());
    }

    #[test]
    fn empty_directory_has_no_floor() {
        assert!(check_retained_floor(&[], 42).is_ok());
    }
}
