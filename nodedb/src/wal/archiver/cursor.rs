// SPDX-License-Identifier: BUSL-1.1

//! Which local WAL segments the archive already holds.
//!
//! The archived objects are the durable record. After a restart the cursor is
//! rebuilt from an archive listing, so a segment uploaded before the restart
//! is never uploaded again and a segment that never reached the archive is
//! never skipped.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use nodedb_wal::segment::SegmentMeta;

/// Segments strictly below the writer's active segment.
///
/// The writer's `active_first_lsn` is the only proof of sealing. A roll
/// creates the next segment file before it seals the current one, so a roll
/// that fails at the seal leaves a newer file on disk while the older segment
/// is still being written. "Every file but the newest" takes that
/// still-open segment for sealed.
pub fn sealed_segments(segments: &[SegmentMeta], active_first_lsn: u64) -> &[SegmentMeta] {
    let end = segments.partition_point(|seg| seg.first_lsn < active_first_lsn);
    &segments[..end]
}

/// What an archive listing shows for one `first_lsn`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteSegment {
    /// Size of the segment object, when one exists.
    pub size: Option<u64>,
    /// Every checksum marker beside it. A re-upload deletes the markers that
    /// no longer match, so more than one exists only after an interrupted
    /// re-upload.
    pub crc32c: Vec<u32>,
}

/// The set of local segments, by `first_lsn`, that are in the archive.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveCursor {
    archived: BTreeSet<u64>,
    /// Markers found beside segments that recovery did not count as
    /// archived. The re-upload of each such segment deletes them.
    stale_markers: BTreeMap<u64, Vec<u32>>,
    /// Markers beside archived segments whose CRC differs from the local
    /// segment. A background sweep deletes them. Truncation never prunes
    /// them: the markers live in the archive, not on local disk.
    sweep_markers: BTreeMap<u64, Vec<u32>>,
}

impl ArchiveCursor {
    /// Rebuild the cursor from an archive listing.
    ///
    /// A sealed segment counts as archived only when the archive holds an
    /// object of the same size and a checksum marker equal to `local_crc32c`.
    /// Anything else is re-uploaded, which overwrites the object.
    pub fn recover(
        sealed: &[SegmentMeta],
        remote: &HashMap<u64, RemoteSegment>,
        local_crc32c: &HashMap<u64, u32>,
    ) -> Self {
        let mut cursor = Self::default();
        for seg in sealed {
            let Some(found) = remote.get(&seg.first_lsn) else {
                continue;
            };
            let matches = local_crc32c
                .get(&seg.first_lsn)
                .is_some_and(|local| found.crc32c.contains(local));
            if found.size == Some(seg.file_size) && matches {
                cursor.archived.insert(seg.first_lsn);
                let stale: Vec<u32> = local_crc32c
                    .get(&seg.first_lsn)
                    .map(|local| {
                        found
                            .crc32c
                            .iter()
                            .copied()
                            .filter(|crc| crc != local)
                            .collect()
                    })
                    .unwrap_or_default();
                if !stale.is_empty() {
                    cursor.sweep_markers.insert(seg.first_lsn, stale);
                }
            } else if !found.crc32c.is_empty() {
                cursor
                    .stale_markers
                    .insert(seg.first_lsn, found.crc32c.clone());
            }
        }
        cursor
    }

    /// Checksum markers the upload of `first_lsn` deletes once its own
    /// marker is written.
    pub fn stale_markers(&self, first_lsn: u64) -> &[u32] {
        self.stale_markers
            .get(&first_lsn)
            .map_or(&[], Vec::as_slice)
    }

    /// Markers of archived segments still waiting for the background sweep.
    pub fn sweep_markers(&self) -> Vec<(u64, Vec<u32>)> {
        self.sweep_markers
            .iter()
            .map(|(lsn, crcs)| (*lsn, crcs.clone()))
            .collect()
    }

    /// Record that the sweep deleted every marker listed for `first_lsn`.
    pub fn mark_swept(&mut self, first_lsn: u64) {
        self.sweep_markers.remove(&first_lsn);
    }

    /// Sealed segments whose object size matches, so their local checksum is
    /// worth computing before [`Self::recover`].
    pub fn size_matches<'a>(
        sealed: &'a [SegmentMeta],
        remote: &HashMap<u64, RemoteSegment>,
    ) -> Vec<&'a SegmentMeta> {
        sealed
            .iter()
            .filter(|seg| {
                remote
                    .get(&seg.first_lsn)
                    .is_some_and(|r| r.size == Some(seg.file_size))
            })
            .collect()
    }

    pub fn is_archived(&self, first_lsn: u64) -> bool {
        self.archived.contains(&first_lsn)
    }

    /// Record a completed upload. Its stale markers are gone with it.
    pub fn mark_archived(&mut self, first_lsn: u64) {
        self.archived.insert(first_lsn);
        self.stale_markers.remove(&first_lsn);
    }

    /// Forget segments no longer on local disk, so the set stays bounded by
    /// the local WAL.
    pub fn retain_local(&mut self, segments: &[SegmentMeta]) {
        let local = |lsn: &u64| segments.iter().any(|seg| seg.first_lsn == *lsn);
        self.archived.retain(local);
        self.stale_markers.retain(|lsn, _| local(lsn));
    }

    /// Sealed segments not yet in the archive, in LSN order.
    pub fn pending<'a>(
        &self,
        segments: &'a [SegmentMeta],
        active_first_lsn: u64,
    ) -> Vec<&'a SegmentMeta> {
        sealed_segments(segments, active_first_lsn)
            .iter()
            .filter(|seg| !self.is_archived(seg.first_lsn))
            .collect()
    }

    /// `first_lsn` of the lowest local segment the archive does not hold.
    /// The active segment is never archived, so this is `Some` whenever
    /// `segments` is non-empty.
    pub fn lowest_unarchived(&self, segments: &[SegmentMeta]) -> Option<u64> {
        segments
            .iter()
            .find(|seg| !self.is_archived(seg.first_lsn))
            .map(|seg| seg.first_lsn)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn seg(first_lsn: u64, file_size: u64) -> SegmentMeta {
        SegmentMeta {
            path: PathBuf::from(nodedb_wal::segment::segment_filename(first_lsn)),
            first_lsn,
            file_size,
        }
    }

    #[test]
    fn active_segment_is_never_pending() {
        let segments = [seg(1, 10), seg(20, 10), seg(40, 10)];
        let cursor = ArchiveCursor::default();
        let pending: Vec<u64> = cursor
            .pending(&segments, 40)
            .iter()
            .map(|s| s.first_lsn)
            .collect();
        assert_eq!(pending, vec![1, 20]);
    }

    /// A roll that failed at the seal leaves a newer file beside the segment
    /// still being written. Only the writer's active LSN decides sealing.
    #[test]
    fn an_orphan_file_above_the_active_segment_does_not_seal_it() {
        let segments = [seg(1, 10), seg(20, 10), seg(40, 0)];
        let sealed: Vec<u64> = sealed_segments(&segments, 20)
            .iter()
            .map(|s| s.first_lsn)
            .collect();
        assert_eq!(sealed, vec![1]);
    }

    fn remote(size: u64, crcs: &[u32]) -> RemoteSegment {
        RemoteSegment {
            size: Some(size),
            crc32c: crcs.to_vec(),
        }
    }

    #[test]
    fn recover_counts_only_same_size_objects() {
        let segments = [seg(1, 10), seg(20, 10), seg(40, 10)];
        let remote = HashMap::from([(1, remote(10, &[7])), (20, remote(9, &[8]))]);
        let local = HashMap::from([(1, 7), (20, 8)]);
        let cursor = ArchiveCursor::recover(sealed_segments(&segments, 40), &remote, &local);
        assert!(cursor.is_archived(1));
        assert!(!cursor.is_archived(20), "a size mismatch is a stale object");
        assert_eq!(cursor.lowest_unarchived(&segments), Some(20));
    }

    #[test]
    fn a_size_equal_object_with_different_content_is_not_archived() {
        let segments = [seg(1, 10), seg(20, 10)];
        let remote = HashMap::from([(1, remote(10, &[0xaaaa]))]);
        let local = HashMap::from([(1, 0xbbbb)]);
        let cursor = ArchiveCursor::recover(sealed_segments(&segments, 20), &remote, &local);
        assert!(!cursor.is_archived(1));
        assert_eq!(cursor.stale_markers(1), &[0xaaaa]);
    }

    #[test]
    fn an_archived_segment_records_its_non_matching_markers_for_the_sweep() {
        let segments = [seg(1, 10), seg(20, 10)];
        let remote = HashMap::from([(1, remote(10, &[0xaaaa, 0xbbbb]))]);
        let local = HashMap::from([(1, 0xbbbb)]);
        let mut cursor = ArchiveCursor::recover(sealed_segments(&segments, 20), &remote, &local);
        assert!(cursor.is_archived(1));
        assert_eq!(cursor.sweep_markers(), vec![(1, vec![0xaaaa])]);
        cursor.retain_local(&[seg(20, 10)]);
        assert_eq!(
            cursor.sweep_markers().len(),
            1,
            "truncation dropped a sweep"
        );
        cursor.mark_swept(1);
        assert!(cursor.sweep_markers().is_empty());
    }

    #[test]
    fn mark_archived_forgets_stale_markers() {
        let segments = [seg(1, 10), seg(20, 10)];
        let remote = HashMap::from([(1, remote(10, &[0xaaaa]))]);
        let local = HashMap::from([(1, 0xbbbb)]);
        let mut cursor = ArchiveCursor::recover(sealed_segments(&segments, 20), &remote, &local);
        cursor.mark_archived(1);
        assert!(cursor.stale_markers(1).is_empty());
    }

    #[test]
    fn an_object_without_a_checksum_marker_is_not_archived() {
        let segments = [seg(1, 10), seg(20, 10)];
        let remote = HashMap::from([(1, remote(10, &[]))]);
        let local = HashMap::from([(1, 5)]);
        let cursor = ArchiveCursor::recover(sealed_segments(&segments, 20), &remote, &local);
        assert!(!cursor.is_archived(1));
    }

    #[test]
    fn a_gap_below_an_archived_segment_is_still_pending() {
        let segments = [seg(1, 10), seg(20, 10), seg(40, 10), seg(60, 10)];
        let remote = HashMap::from([(20, remote(10, &[3]))]);
        let local = HashMap::from([(20, 3)]);
        let cursor = ArchiveCursor::recover(sealed_segments(&segments, 60), &remote, &local);
        let pending: Vec<u64> = cursor
            .pending(&segments, 60)
            .iter()
            .map(|s| s.first_lsn)
            .collect();
        assert_eq!(pending, vec![1, 40]);
        assert_eq!(cursor.lowest_unarchived(&segments), Some(1));
    }

    #[test]
    fn retain_local_drops_truncated_segments() {
        let mut cursor = ArchiveCursor::default();
        cursor.mark_archived(1);
        cursor.mark_archived(20);
        cursor.retain_local(&[seg(20, 10), seg(40, 10)]);
        assert!(!cursor.is_archived(1));
        assert!(cursor.is_archived(20));
    }
}
