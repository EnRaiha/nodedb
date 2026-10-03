// SPDX-License-Identifier: BUSL-1.1

//! The archived bound on WAL truncation. Truncation never deletes a segment
//! the archive does not hold.

use nodedb_wal::segment::SegmentMeta;

use crate::wal::WalManager;
use crate::wal::archiver::{ArchiveCursor, WalArchiver};

/// Bound that truncates nothing: no WAL segment precedes LSN 0.
const NO_TRUNCATION_BOUND: u64 = 0;

/// The LSN truncation must not pass.
///
/// `truncate_before(lsn)` deletes a segment when its successor starts at or
/// below `lsn`. A bound equal to the lowest unarchived segment's `first_lsn`
/// therefore keeps that segment and everything after it.
///
/// - `None` segments: the WAL directory was unlistable. Unknown is never
///   permissive, so nothing is truncated.
/// - `None` cursor: the archive listing has not succeeded yet. Every local
///   segment counts as unarchived.
fn archived_truncation_bound(
    segments: Option<&[SegmentMeta]>,
    cursor: Option<&ArchiveCursor>,
    checkpoint_lsn: u64,
) -> u64 {
    let Some(segments) = segments else {
        return NO_TRUNCATION_BOUND;
    };
    let lowest_unarchived = match cursor {
        Some(cursor) => cursor.lowest_unarchived(segments),
        None => segments.first().map(|seg| seg.first_lsn),
    };
    lowest_unarchived.map_or(checkpoint_lsn, |lsn| lsn.min(checkpoint_lsn))
}

/// The truncation bound after an archive pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ArchivedBound {
    /// The LSN truncation must not pass.
    pub lsn: u64,
    /// An unarchived sealed segment, or an unknown archive state, holds
    /// truncation below the checkpoint. A bound at the active segment is not
    /// held: the active segment is never deleted.
    pub held: bool,
}

/// Run an archive pass, then return the bound the upcoming truncation must
/// not pass.
///
/// A segment the archive did not accept holds truncation back at that
/// segment. The local WAL then grows until archival recovers. A full disk is
/// loud and recoverable. An archive hole is silent and permanent.
pub(crate) async fn archive_then_bound(
    archiver: &mut WalArchiver,
    wal: &WalManager,
    checkpoint_lsn: u64,
) -> ArchivedBound {
    let snapshot = archiver.tick(wal).await;
    let lsn = archived_truncation_bound(
        snapshot.as_ref().map(|s| s.segments.as_slice()),
        archiver.cursor(),
        checkpoint_lsn,
    );
    let active = snapshot.as_ref().map(|s| s.active_first_lsn);
    ArchivedBound {
        lsn,
        held: lsn < checkpoint_lsn && Some(lsn) != active,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use nodedb_wal::segment::{discover_segments, segment_path, truncate_segments};

    use super::*;

    fn seg(first_lsn: u64) -> SegmentMeta {
        SegmentMeta {
            path: segment_path(Path::new("/nonexistent"), first_lsn),
            first_lsn,
            file_size: 1,
        }
    }

    fn archived(lsns: &[u64]) -> ArchiveCursor {
        let mut cursor = ArchiveCursor::default();
        for lsn in lsns {
            cursor.mark_archived(*lsn);
        }
        cursor
    }

    /// With every sealed segment archived, the active segment is the bound.
    /// It is never deleted anyway, so the checkpoint alone decides.
    #[test]
    fn fully_archived_sealed_set_bounds_at_the_active_segment() {
        let segments = [seg(10), seg(20), seg(30)];
        let cursor = archived(&[10, 20]);
        assert_eq!(
            archived_truncation_bound(Some(&segments), Some(&cursor), 900),
            30
        );
    }

    /// An unarchived middle segment keeps itself and every later one.
    #[test]
    fn unarchived_middle_segment_bounds_truncation_at_that_segment() {
        let segments = [seg(10), seg(20), seg(30)];
        let cursor = archived(&[10, 30]);
        assert_eq!(
            archived_truncation_bound(Some(&segments), Some(&cursor), 900),
            20
        );
    }

    /// A checkpoint below the archived bound stays the binding floor.
    #[test]
    fn checkpoint_below_the_archived_bound_wins() {
        let segments = [seg(10), seg(20), seg(30)];
        let cursor = archived(&[10, 20]);
        assert_eq!(
            archived_truncation_bound(Some(&segments), Some(&cursor), 15),
            15
        );
    }

    /// Before the archive listing succeeds, the lowest local segment is the
    /// bound, so nothing is deleted.
    #[test]
    fn unrecovered_archiver_truncates_nothing() {
        let segments = [seg(10), seg(20), seg(30)];
        assert_eq!(archived_truncation_bound(Some(&segments), None, 900), 10);
    }

    /// An unlistable WAL directory hides which segments exist.
    #[test]
    fn list_segments_failure_truncates_nothing() {
        assert_eq!(archived_truncation_bound(None, None, 900), 0);
    }

    /// The bound applied to a real WAL directory: the checkpoint allows
    /// deleting every sealed segment, but the unarchived one survives, and so
    /// does everything after it.
    #[test]
    fn held_archive_bound_stops_truncation() {
        let dir = tempfile::tempdir().unwrap();
        for lsn in [10u64, 20, 30, 40] {
            std::fs::write(segment_path(dir.path(), lsn), b"segment").unwrap();
        }
        let segments = discover_segments(dir.path()).unwrap();
        let cursor = archived(&[10]);

        let bound = archived_truncation_bound(Some(&segments), Some(&cursor), 1_000);
        assert_eq!(bound, 20);
        let result = truncate_segments(dir.path(), bound, 40).unwrap();
        assert_eq!(result.segments_deleted, 1);

        let left: Vec<u64> = discover_segments(dir.path())
            .unwrap()
            .iter()
            .map(|s| s.first_lsn)
            .collect();
        assert_eq!(left, vec![20, 30, 40]);
    }
}
