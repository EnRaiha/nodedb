// SPDX-License-Identifier: BUSL-1.1

//! WAL archival to cold storage, run by the checkpoint cycle before it
//! truncates. Archival bounds truncation: a segment cold storage did not
//! accept stays on local disk instead of being deleted unarchived.

use tracing::{debug, warn};

use crate::wal::WalManager;

/// Bound that truncates nothing: no WAL segment precedes LSN 0.
const NO_TRUNCATION_BOUND: u64 = 0;

/// The LSN truncation must not pass, given the segments on disk and the
/// `first_lsn` of every segment whose archival failed.
///
/// The bound is `checkpoint_lsn` when every eligible segment reached cold
/// storage, and the lowest failed `first_lsn` otherwise, so that segment and
/// everything after it survive. `None` segments means `list_segments` itself
/// failed: the eligible set is unknown, and unknown is never permissive.
fn archived_truncation_bound(
    segment_first_lsns: Option<&[u64]>,
    failed_first_lsns: &[u64],
    checkpoint_lsn: u64,
) -> u64 {
    let Some(first_lsns) = segment_first_lsns else {
        return NO_TRUNCATION_BOUND;
    };
    let mut bound = checkpoint_lsn;
    for first_lsn in first_lsns {
        if failed_first_lsns.contains(first_lsn) {
            bound = bound.min(*first_lsn);
        }
    }
    bound
}

/// Archive WAL segments that the upcoming truncation deletes, and return the
/// LSN that truncation must not pass.
///
/// A segment is eligible for deletion (and therefore archival) when the segment
/// immediately following it has a `first_lsn <= checkpoint_lsn`. Each eligible
/// segment is uploaded before `truncate_before` deletes it, preserving a
/// continuous WAL archive in cold storage for point-in-time recovery.
///
/// A segment the archive did not accept holds truncation back at that segment.
/// The local WAL then grows until archival recovers. That is the intended
/// outcome: a full disk is loud and recoverable, an archive hole is silent and
/// permanent.
pub(crate) async fn archive_wal_segments_before_truncation(
    wal: &WalManager,
    checkpoint_lsn: u64,
    cold: &crate::storage::cold::ColdStorage,
) -> u64 {
    let segments = match wal.list_segments() {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                "WAL archival: segments unlistable — truncation holds until the next cycle"
            );
            crate::diag::wal_archival_failed_truncation_held(
                "list_segments",
                Some(&e),
                NO_TRUNCATION_BOUND,
            );
            return archived_truncation_bound(None, &[], checkpoint_lsn);
        }
    };

    // Determine which segments are eligible using the same logic as truncate_before:
    // a segment is deletable when its successor's first_lsn <= checkpoint_lsn.
    let mut failed_first_lsns: Vec<u64> = Vec::new();
    for seg in &segments {
        let next_first_lsn = segments
            .iter()
            .find(|s| s.first_lsn > seg.first_lsn)
            .map(|s| s.first_lsn)
            .unwrap_or(u64::MAX);

        if next_first_lsn > checkpoint_lsn {
            // Not eligible for deletion; skip.
            continue;
        }

        let segment_name = match seg.path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_owned(),
            None => {
                warn!(
                    path = %seg.path.display(),
                    "WAL archival: segment path unnameable — segment stays on local disk"
                );
                crate::diag::wal_archival_failed_truncation_held(
                    "segment_path",
                    None,
                    seg.first_lsn,
                );
                failed_first_lsns.push(seg.first_lsn);
                continue;
            }
        };

        match cold.upload_wal_segment(&seg.path, &segment_name).await {
            Ok(object_path) => {
                debug!(
                    segment = %segment_name,
                    object_path = %object_path,
                    first_lsn = seg.first_lsn,
                    "WAL segment archived before truncation"
                );
            }
            Err(e) => {
                warn!(
                    segment = %segment_name,
                    error = %e,
                    first_lsn = seg.first_lsn,
                    "WAL archival: upload failed — segment stays on local disk"
                );
                crate::diag::wal_archival_failed_truncation_held("upload", Some(&e), seg.first_lsn);
                failed_first_lsns.push(seg.first_lsn);
            }
        }
    }

    let first_lsns: Vec<u64> = segments.iter().map(|s| s.first_lsn).collect();
    archived_truncation_bound(Some(&first_lsns), &failed_first_lsns, checkpoint_lsn)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Archival that fully succeeded leaves truncation exactly where the
    /// checkpoint put it.
    #[test]
    fn every_upload_succeeded_does_not_lower_the_checkpoint_lsn() {
        assert_eq!(
            archived_truncation_bound(Some(&[10, 20, 30]), &[], 900),
            900
        );
    }

    /// A failed upload keeps its own segment and every later one on disk:
    /// deleting them would leave a hole the archive can never fill.
    #[test]
    fn middle_segment_upload_failure_bounds_truncation_at_that_segment() {
        assert_eq!(
            archived_truncation_bound(Some(&[10, 20, 30]), &[20], 900),
            20
        );
    }

    /// The lowest failed segment wins, so a later success cannot raise the
    /// bound past a gap.
    #[test]
    fn lowest_failed_segment_wins_over_a_later_one() {
        assert_eq!(
            archived_truncation_bound(Some(&[10, 20, 30]), &[30, 20], 900),
            20
        );
    }

    /// The first eligible segment failing truncates nothing: no segment
    /// precedes it.
    #[test]
    fn first_segment_upload_failure_truncates_nothing() {
        assert_eq!(
            archived_truncation_bound(Some(&[10, 20, 30]), &[10], 900),
            10
        );
    }

    /// An unlistable WAL directory hides which segments are eligible, so the
    /// cycle archives nothing and truncates nothing.
    #[test]
    fn list_segments_failure_truncates_nothing() {
        assert_eq!(archived_truncation_bound(None, &[], 900), 0);
    }

    /// A failed segment above the checkpoint LSN was never eligible for
    /// deletion, so it cannot lower the bound.
    #[test]
    fn failure_above_the_checkpoint_lsn_does_not_lower_the_bound() {
        assert_eq!(
            archived_truncation_bound(Some(&[10, 950]), &[950], 900),
            900
        );
    }
}
