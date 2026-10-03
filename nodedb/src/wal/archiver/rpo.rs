// SPDX-License-Identifier: BUSL-1.1

//! Recovery point objective gap: the local WAL the archive does not hold yet.

use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use nodedb_wal::segment::SegmentMeta;

use super::cursor::ArchiveCursor;
use crate::control::metrics::SystemMetrics;

/// What a lost local disk costs right now.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RpoGap {
    /// Bytes of every local segment the archive does not hold, active included.
    pub unarchived_bytes: u64,
    /// Time since the lowest unarchived segment was created.
    pub age: Duration,
}

/// The gap over `segments`. An unrecovered archiver passes an empty cursor, so
/// every segment counts as unarchived.
pub fn rpo_gap(segments: &[SegmentMeta], cursor: &ArchiveCursor, now: SystemTime) -> RpoGap {
    let unarchived_bytes = segments
        .iter()
        .filter(|seg| !cursor.is_archived(seg.first_lsn))
        .map(|seg| seg.file_size)
        .sum();
    let age = segments
        .iter()
        .find(|seg| !cursor.is_archived(seg.first_lsn))
        .and_then(segment_birth)
        .and_then(|birth| now.duration_since(birth).ok())
        .unwrap_or_default();
    RpoGap {
        unarchived_bytes,
        age,
    }
}

/// Creation time of the segment file. Filesystems without a birth time fall
/// back to the last modification time, which understates the age.
fn segment_birth(seg: &SegmentMeta) -> Option<SystemTime> {
    let meta = std::fs::metadata(&seg.path).ok()?;
    meta.created().or_else(|_| meta.modified()).ok()
}

/// Publish `gap` to the `nodedb_wal_archive_rpo_gap_*` gauges.
pub fn publish_rpo_gap(metrics: &SystemMetrics, gap: RpoGap) {
    metrics
        .wal_archive_rpo_gap_bytes
        .store(gap.unarchived_bytes, Ordering::Relaxed);
    metrics
        .wal_archive_rpo_gap_seconds
        .store(gap.age.as_secs(), Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg_file(dir: &std::path::Path, first_lsn: u64, bytes: usize) -> SegmentMeta {
        let path = nodedb_wal::segment::segment_path(dir, first_lsn);
        std::fs::write(&path, vec![0u8; bytes]).unwrap();
        SegmentMeta {
            path,
            first_lsn,
            file_size: bytes as u64,
        }
    }

    #[test]
    fn gap_counts_unarchived_segments_including_the_active_one() {
        let dir = tempfile::tempdir().unwrap();
        let segments = [
            seg_file(dir.path(), 1, 10),
            seg_file(dir.path(), 20, 7),
            seg_file(dir.path(), 40, 3),
        ];
        let mut cursor = ArchiveCursor::default();
        cursor.mark_archived(1);
        let now = SystemTime::now() + Duration::from_secs(60);
        let gap = rpo_gap(&segments, &cursor, now);
        assert_eq!(gap.unarchived_bytes, 10);
        assert!(gap.age >= Duration::from_secs(59), "{:?}", gap.age);
    }

    #[test]
    fn fully_archived_sealed_set_leaves_only_the_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let segments = [seg_file(dir.path(), 1, 10), seg_file(dir.path(), 20, 4)];
        let mut cursor = ArchiveCursor::default();
        cursor.mark_archived(1);
        let gap = rpo_gap(&segments, &cursor, SystemTime::now());
        assert_eq!(gap.unarchived_bytes, 4);
    }
}
