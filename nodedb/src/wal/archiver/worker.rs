// SPDX-License-Identifier: BUSL-1.1

//! The stateful WAL archiver owned by the checkpoint task.
//!
//! Each tick uploads every sealed segment the archive does not hold, in LSN
//! order, to `{prefix}wal/{node_id}/{incarnation}/`. It runs on the Control
//! Plane and reads sealed segment files only. It never touches the Data Plane.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use nodedb_wal::segment::SegmentMeta;
use tracing::{debug, warn};

use super::checksum::local_crc32c;
use super::cursor::{ArchiveCursor, sealed_segments};
use super::incarnation::{Incarnation, load_or_mint_incarnation};
use super::rpo::{RpoGap, publish_rpo_gap, rpo_gap};
use crate::control::metrics::SystemMetrics;
use crate::storage::cold::ColdStorage;
use crate::wal::WalManager;

/// What one archive pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArchivePass {
    /// Segments uploaded by this pass.
    pub uploaded: u64,
    /// A listing or an upload failed. The next tick retries.
    pub failed: bool,
    /// The gap left after this pass.
    pub rpo_gap: RpoGap,
}

/// The local WAL as one pass saw it.
#[derive(Debug, Clone)]
pub struct WalSnapshot {
    /// Every segment file on disk, in LSN order.
    pub segments: Vec<SegmentMeta>,
    /// The writer's active segment. Only segments below it are sealed.
    pub active_first_lsn: u64,
}

/// Archive state rebuilt from the data directory and the archive listing.
struct Recovered {
    incarnation: Incarnation,
    cursor: ArchiveCursor,
}

/// Streams sealed WAL segments to cold storage.
pub struct WalArchiver {
    node_id: u64,
    data_dir: PathBuf,
    cold: Arc<ColdStorage>,
    metrics: Option<Arc<SystemMetrics>>,
    /// `None` until recovery succeeds once. An unrecovered archiver uploads
    /// nothing and treats every local segment as unarchived.
    state: Option<Recovered>,
}

impl WalArchiver {
    /// `data_dir` holds the node incarnation file.
    pub fn new(
        node_id: u64,
        data_dir: PathBuf,
        cold: Arc<ColdStorage>,
        metrics: Option<Arc<SystemMetrics>>,
    ) -> Self {
        Self {
            node_id,
            data_dir,
            cold,
            metrics,
            state: None,
        }
    }

    /// The archived set, or `None` before the first successful recovery.
    pub fn cursor(&self) -> Option<&ArchiveCursor> {
        self.state.as_ref().map(|state| &state.cursor)
    }

    /// List the local WAL and archive every sealed segment not yet archived.
    ///
    /// Returns the snapshot the pass worked on, or `None` when the WAL
    /// directory is unlistable.
    pub async fn tick(&mut self, wal: &WalManager) -> Option<WalSnapshot> {
        let (segments, active_first_lsn) = match wal.segments_with_active() {
            Ok(listed) => listed,
            Err(e) => {
                warn!(error = %e, "WAL archival: segments unlistable, retrying next tick");
                crate::diag::wal_archival_failed_truncation_held("list_segments", Some(&e), 0);
                self.count_failure();
                return None;
            }
        };
        self.archive_segments(&segments, active_first_lsn).await;
        Some(WalSnapshot {
            segments,
            active_first_lsn,
        })
    }

    /// Archive every sealed segment in `segments` that the archive does not
    /// hold. Stops at the first failed upload, so the archive never gains a
    /// segment above a gap in one pass.
    pub async fn archive_segments(
        &mut self,
        segments: &[SegmentMeta],
        active_first_lsn: u64,
    ) -> ArchivePass {
        let mut pass = ArchivePass::default();
        // Taken for the pass and put back at its end. A pass dropped midway
        // leaves `None`, and the next pass recovers from the archive listing.
        let mut state = match self.state.take() {
            Some(state) => state,
            None => {
                let sealed = sealed_segments(segments, active_first_lsn);
                match recover(&self.cold, self.node_id, &self.data_dir, sealed).await {
                    Ok(recovered) => recovered,
                    Err(e) => {
                        let lowest = segments.first().map_or(0, |seg| seg.first_lsn);
                        warn!(
                            error = %e,
                            node_id = self.node_id,
                            "WAL archival: recovery failed, nothing uploads until it succeeds"
                        );
                        crate::diag::wal_archival_failed_truncation_held(
                            "recover",
                            Some(&e),
                            lowest,
                        );
                        pass.failed = true;
                        pass.rpo_gap =
                            rpo_gap(segments, &ArchiveCursor::default(), SystemTime::now());
                        publish(self.metrics.as_deref(), pass.rpo_gap, 0, 0, 1);
                        return pass;
                    }
                }
            }
        };
        state.cursor.retain_local(segments);

        let mut bytes = 0u64;
        for seg in state.cursor.pending(segments, active_first_lsn) {
            let stale = state.cursor.stale_markers(seg.first_lsn).to_vec();
            let uploaded = self
                .cold
                .upload_wal_segment(
                    &seg.path,
                    self.node_id,
                    state.incarnation.as_str(),
                    seg.first_lsn,
                    &stale,
                )
                .await;
            match uploaded {
                Ok(key) => {
                    state.cursor.mark_archived(seg.first_lsn);
                    pass.uploaded += 1;
                    bytes += seg.file_size;
                    debug!(key = %key, first_lsn = seg.first_lsn, "WAL segment archived");
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        first_lsn = seg.first_lsn,
                        "WAL archival: upload failed, segment stays on local disk"
                    );
                    crate::diag::wal_archival_failed_truncation_held(
                        "upload",
                        Some(&e),
                        seg.first_lsn,
                    );
                    pass.failed = true;
                    break;
                }
            }
        }

        pass.rpo_gap = rpo_gap(segments, &state.cursor, SystemTime::now());
        publish(
            self.metrics.as_deref(),
            pass.rpo_gap,
            pass.uploaded,
            bytes,
            u64::from(pass.failed),
        );
        self.state = Some(state);
        pass
    }

    /// Delete stale checksum markers beside archived segments. A failure is
    /// logged and retried on the next sweep. It never blocks archiving or
    /// truncation, since those segments already count as archived.
    ///
    /// Returns the number of segments whose markers are now all gone.
    pub async fn sweep_stale_markers(&mut self) -> u64 {
        let Some(state) = self.state.as_mut() else {
            return 0;
        };
        let mut swept = 0;
        for (first_lsn, crcs) in state.cursor.sweep_markers() {
            let deleted = self
                .cold
                .delete_wal_checksum_markers(
                    self.node_id,
                    state.incarnation.as_str(),
                    first_lsn,
                    &crcs,
                )
                .await;
            match deleted {
                Ok(()) => {
                    state.cursor.mark_swept(first_lsn);
                    swept += 1;
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        first_lsn,
                        "WAL archival: stale checksum marker not deleted, retrying next sweep"
                    );
                }
            }
        }
        swept
    }

    fn count_failure(&self) {
        if let Some(metrics) = self.metrics.as_deref() {
            metrics
                .wal_archive_failures_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Rebuild the archive state: load or mint the incarnation, list this node
/// life's archive from the lowest local sealed segment, and checksum every
/// local segment whose object size matches. The listing cost is bounded by
/// the local WAL, never by the archive's retained history.
async fn recover(
    cold: &ColdStorage,
    node_id: u64,
    data_dir: &std::path::Path,
    sealed: &[SegmentMeta],
) -> crate::Result<Recovered> {
    let dir = data_dir.to_path_buf();
    let incarnation = tokio::task::spawn_blocking(move || load_or_mint_incarnation(&dir))
        .await
        .map_err(|e| crate::Error::ColdStorage {
            detail: format!("spawn_blocking join: {e}"),
        })??;
    let Some(lowest) = sealed.first() else {
        return Ok(Recovered {
            incarnation,
            cursor: ArchiveCursor::default(),
        });
    };
    let remote = cold
        .archived_wal_segments(node_id, incarnation.as_str(), lowest.first_lsn)
        .await?;
    let candidates = ArchiveCursor::size_matches(sealed, &remote)
        .into_iter()
        .map(|seg| (seg.first_lsn, seg.path.clone()))
        .collect();
    let local = local_crc32c(candidates).await?;
    Ok(Recovered {
        incarnation,
        cursor: ArchiveCursor::recover(sealed, &remote, &local),
    })
}

fn publish(metrics: Option<&SystemMetrics>, gap: RpoGap, segments: u64, bytes: u64, failures: u64) {
    let Some(metrics) = metrics else {
        return;
    };
    publish_rpo_gap(metrics, gap);
    metrics
        .wal_archive_segments_total
        .fetch_add(segments, Ordering::Relaxed);
    metrics
        .wal_archive_bytes_total
        .fetch_add(bytes, Ordering::Relaxed);
    metrics
        .wal_archive_failures_total
        .fetch_add(failures, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::storage::cold::ColdStorageConfig;
    use crate::wal::archiver::key::{wal_archive_checksum_key, wal_archive_segment_key};

    fn cold_at(dir: &Path) -> Arc<ColdStorage> {
        let config = ColdStorageConfig {
            local_dir: Some(dir.to_path_buf()),
            ..Default::default()
        };
        Arc::new(ColdStorage::new(config).unwrap())
    }

    fn write_segment(wal_dir: &Path, first_lsn: u64, bytes: &[u8]) -> SegmentMeta {
        let path = nodedb_wal::segment::segment_path(wal_dir, first_lsn);
        std::fs::write(&path, bytes).unwrap();
        SegmentMeta {
            path,
            first_lsn,
            file_size: bytes.len() as u64,
        }
    }

    fn active(segments: &[SegmentMeta]) -> u64 {
        segments.last().map_or(0, |seg| seg.first_lsn)
    }

    fn archived_object(
        cold_dir: &Path,
        data_dir: &Path,
        node_id: u64,
        first_lsn: u64,
    ) -> std::path::PathBuf {
        let incarnation = load_or_mint_incarnation(data_dir).unwrap();
        cold_dir.join(wal_archive_segment_key(
            "data/",
            node_id,
            incarnation.as_str(),
            first_lsn,
        ))
    }

    fn archiver(node_id: u64, data_dir: &Path, cold: &Arc<ColdStorage>) -> WalArchiver {
        WalArchiver::new(node_id, data_dir.to_path_buf(), Arc::clone(cold), None)
    }

    async fn pass(archiver: &mut WalArchiver, segments: &[SegmentMeta]) -> ArchivePass {
        archiver.archive_segments(segments, active(segments)).await
    }

    #[tokio::test]
    async fn every_sealed_segment_uploads_and_the_active_one_does_not() {
        let wal_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let cold = cold_at(cold_dir.path());
        let segments = vec![
            write_segment(wal_dir.path(), 1, b"one"),
            write_segment(wal_dir.path(), 20, b"two"),
            write_segment(wal_dir.path(), 40, b"active"),
        ];
        let mut archiver = archiver(5, data_dir.path(), &cold);

        let result = pass(&mut archiver, &segments).await;

        assert_eq!(result.uploaded, 2);
        assert!(!result.failed);
        assert_eq!(result.rpo_gap.unarchived_bytes, 6);
        let object = |lsn| archived_object(cold_dir.path(), data_dir.path(), 5, lsn);
        assert!(object(1).exists());
        assert!(object(20).exists());
        assert!(!object(40).exists());
    }

    #[tokio::test]
    async fn segments_are_never_uploaded_twice_across_a_restart() {
        let wal_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let cold = cold_at(cold_dir.path());
        let mut segments = vec![
            write_segment(wal_dir.path(), 1, b"one"),
            write_segment(wal_dir.path(), 20, b"two"),
            write_segment(wal_dir.path(), 40, b"three"),
        ];

        let mut before = archiver(9, data_dir.path(), &cold);
        assert_eq!(pass(&mut before, &segments).await.uploaded, 2);
        drop(before);

        // The restarted archiver rebuilds its state from the archive alone.
        let mut after = archiver(9, data_dir.path(), &cold);
        let result = pass(&mut after, &segments).await;
        assert_eq!(
            result.uploaded, 0,
            "restart re-uploaded an archived segment"
        );

        // A roll seals segment 40. It uploads exactly once, and nothing else does.
        segments.push(write_segment(wal_dir.path(), 60, b"active"));
        assert_eq!(pass(&mut after, &segments).await.uploaded, 1);
        assert!(archived_object(cold_dir.path(), data_dir.path(), 9, 40).exists());
        assert_eq!(pass(&mut after, &segments).await.uploaded, 0);
    }

    #[tokio::test]
    async fn a_segment_missing_from_the_archive_uploads_after_a_restart() {
        let wal_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let cold = cold_at(cold_dir.path());
        let segments = vec![
            write_segment(wal_dir.path(), 1, b"one"),
            write_segment(wal_dir.path(), 20, b"two"),
            write_segment(wal_dir.path(), 40, b"active"),
        ];
        let mut before = archiver(3, data_dir.path(), &cold);
        pass(&mut before, &segments).await;
        std::fs::remove_file(archived_object(cold_dir.path(), data_dir.path(), 3, 1)).unwrap();

        let mut after = archiver(3, data_dir.path(), &cold);
        assert_eq!(pass(&mut after, &segments).await.uploaded, 1);
        assert!(archived_object(cold_dir.path(), data_dir.path(), 3, 1).exists());
    }

    /// A size-equal object whose content differs from the local segment is a
    /// stale upload: the restarted archiver uploads the segment again.
    #[tokio::test]
    async fn a_size_equal_object_with_different_content_is_re_uploaded() {
        let wal_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let cold = cold_at(cold_dir.path());
        let mut segments = vec![
            write_segment(wal_dir.path(), 1, b"one"),
            write_segment(wal_dir.path(), 20, b"active"),
        ];
        let mut before = archiver(4, data_dir.path(), &cold);
        assert_eq!(pass(&mut before, &segments).await.uploaded, 1);

        segments[0] = write_segment(wal_dir.path(), 1, b"uno");
        let mut after = archiver(4, data_dir.path(), &cold);
        assert_eq!(pass(&mut after, &segments).await.uploaded, 1);
        let object = archived_object(cold_dir.path(), data_dir.path(), 4, 1);
        assert_eq!(std::fs::read(object).unwrap(), b"uno");

        // The re-upload leaves exactly one marker: the one for "uno".
        let marker = |crc| {
            let incarnation = load_or_mint_incarnation(data_dir.path()).unwrap();
            cold_dir.path().join(wal_archive_checksum_key(
                "data/",
                4,
                incarnation.as_str(),
                1,
                crc,
            ))
        };
        assert!(marker(crc32c::crc32c(b"uno")).exists());
        assert!(
            !marker(crc32c::crc32c(b"one")).exists(),
            "the stale marker survived the re-upload"
        );
    }

    /// An archived segment with an extra stale marker keeps its archived
    /// status, and the sweep leaves only the matching marker.
    #[tokio::test]
    async fn the_sweep_removes_a_stale_marker_beside_an_archived_segment() {
        let wal_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let cold = cold_at(cold_dir.path());
        let segments = vec![
            write_segment(wal_dir.path(), 1, b"one"),
            write_segment(wal_dir.path(), 20, b"active"),
        ];
        let mut before = archiver(2, data_dir.path(), &cold);
        assert_eq!(pass(&mut before, &segments).await.uploaded, 1);

        let incarnation = load_or_mint_incarnation(data_dir.path()).unwrap();
        let marker = |crc| {
            cold_dir.path().join(wal_archive_checksum_key(
                "data/",
                2,
                incarnation.as_str(),
                1,
                crc,
            ))
        };
        let stale = marker(crc32c::crc32c(b"xyz"));
        std::fs::write(&stale, b"").unwrap();

        let mut after = archiver(2, data_dir.path(), &cold);
        assert_eq!(pass(&mut after, &segments).await.uploaded, 0);
        assert!(stale.exists(), "the upload pass itself never sweeps");
        assert_eq!(after.sweep_stale_markers().await, 1);
        assert!(!stale.exists());
        assert!(marker(crc32c::crc32c(b"one")).exists());
        assert_eq!(after.sweep_stale_markers().await, 0);
    }

    /// A wiped data directory mints a new incarnation, so the node reusing its
    /// id uploads into a fresh directory instead of matching its old objects.
    #[tokio::test]
    async fn a_new_incarnation_re_uploads() {
        let wal_dir = tempfile::tempdir().unwrap();
        let old_data = tempfile::tempdir().unwrap();
        let new_data = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let cold = cold_at(cold_dir.path());
        let segments = vec![
            write_segment(wal_dir.path(), 1, b"one"),
            write_segment(wal_dir.path(), 20, b"active"),
        ];
        let mut old_life = archiver(6, old_data.path(), &cold);
        assert_eq!(pass(&mut old_life, &segments).await.uploaded, 1);

        let mut new_life = archiver(6, new_data.path(), &cold);
        assert_eq!(pass(&mut new_life, &segments).await.uploaded, 1);
        assert!(archived_object(cold_dir.path(), old_data.path(), 6, 1).exists());
        assert!(archived_object(cold_dir.path(), new_data.path(), 6, 1).exists());
    }

    #[tokio::test]
    async fn nodes_sharing_a_bucket_keep_separate_archives() {
        let wal_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let cold = cold_at(cold_dir.path());
        let segments = vec![
            write_segment(wal_dir.path(), 1, b"one"),
            write_segment(wal_dir.path(), 20, b"active"),
        ];
        // Same incarnation on purpose: the node id alone must separate them.
        let mut node_a = archiver(1, data_dir.path(), &cold);
        let mut node_b = archiver(2, data_dir.path(), &cold);
        assert_eq!(pass(&mut node_a, &segments).await.uploaded, 1);
        assert_eq!(
            pass(&mut node_b, &segments).await.uploaded,
            1,
            "node 2 took node 1's object as its own"
        );
        assert!(archived_object(cold_dir.path(), data_dir.path(), 1, 1).exists());
        assert!(archived_object(cold_dir.path(), data_dir.path(), 2, 1).exists());
    }

    /// A roll that failed at the seal leaves a newer file on disk. The segment
    /// still being written is not uploaded.
    #[tokio::test]
    async fn an_orphan_newer_file_does_not_seal_the_active_segment() {
        let wal_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let cold = cold_at(cold_dir.path());
        let segments = vec![
            write_segment(wal_dir.path(), 1, b"one"),
            write_segment(wal_dir.path(), 20, b"still-open"),
            write_segment(wal_dir.path(), 40, b""),
        ];
        let mut archiver = archiver(8, data_dir.path(), &cold);
        assert_eq!(archiver.archive_segments(&segments, 20).await.uploaded, 1);
        assert!(!archived_object(cold_dir.path(), data_dir.path(), 8, 20).exists());
    }
}
