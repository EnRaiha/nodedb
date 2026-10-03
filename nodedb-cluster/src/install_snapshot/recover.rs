// SPDX-License-Identifier: BUSL-1.1

//! Boot recovery of staged snapshot installs.
//!
//! A staged file under `<data_dir>/recv_snapshots/` is an install whose host
//! apply may have started but that did not finish. Recovery completes each
//! one, in one of two ways:
//!
//! - Raft boundary at or past the staged index: the host apply acknowledged
//!   and was durable before the boundary moved, so only the file remains.
//!   It is removed.
//! - Raft boundary below the staged index: the host state of the group is
//!   undetermined, so the snapshot is applied again through the
//!   [`SnapshotApplier`], the boundary adopted, and the file removed.
//!
//! The re-apply runs after the Data Plane's WAL replay: a core takes a
//! request only once it has replayed. That order is correct here. The install
//! appended its WAL barrier before any core changed, so replay applied no
//! pre-install record to the group's collections. And the boundary never
//! moved, so no entry after the snapshot applied either: the WAL holds no
//! record for these collections that the re-apply could erase.
//!
//! Recovery also removes every `<group>.snap` file. Nothing reads one: the
//! installed state is durable on its own, and the leader builds every
//! snapshot it sends from live engine state.

use std::path::Path;

use tracing::{info, warn};

use crate::error::ClusterError;
use crate::multi_raft::MultiRaft;
use crate::raft_loop::SnapshotApplier;

use super::staged::{discard, finish, list_staged, remove_snap_files};

/// Complete every staged install under `<data_dir>/recv_snapshots/`.
///
/// Returns the number of staged installs completed. A staged install of a
/// group this node no longer mounts is removed: the node holds no Raft state
/// for it. `applier` is `None` only in cluster-only tests with no host state
/// machine.
pub async fn recover_staged_installs(
    data_dir: &Path,
    multi_raft: &mut MultiRaft,
    applier: Option<&dyn SnapshotApplier>,
) -> Result<usize, ClusterError> {
    let recv_dir = data_dir.join("recv_snapshots");
    let removed = remove_snap_files(&recv_dir)?;
    if removed > 0 {
        info!(removed, "removed unused snapshot files");
    }

    let mut completed = 0usize;
    for staged in list_staged(&recv_dir)? {
        let group_id = staged.group_id;
        let index = staged.last_included_index;
        if !multi_raft.contains_group(group_id) {
            warn!(
                group_id,
                snapshot_index = index,
                "staged snapshot install of an unmounted group, removing"
            );
            discard(&staged.path)?;
            continue;
        }
        let (_, boundary, _) = multi_raft.snapshot_metadata(group_id)?;
        if boundary < index {
            let bytes = std::fs::read(&staged.path).map_err(|e| ClusterError::Storage {
                detail: format!("read staged install {}: {e}", staged.path.display()),
            })?;
            if !bytes.is_empty()
                && let Some(applier) = applier
            {
                applier
                    .apply_snapshot(group_id, &bytes)
                    .await
                    .map_err(|e| ClusterError::SnapshotApplyFailed {
                        group_id,
                        detail: format!(
                            "boot re-apply of the staged install at index {index}: {e}"
                        ),
                    })?;
                if group_id == crate::metadata_group::METADATA_GROUP_ID {
                    multi_raft.sync_group_membership_from_routing(group_id)?;
                }
            }
            multi_raft.adopt_snapshot_boundary(group_id, index, staged.last_included_term)?;
            if !bytes.is_empty() && applier.is_some() {
                multi_raft.refresh_snapshot_requirement(group_id);
            }
        }
        finish(&staged, &recv_dir)?;
        info!(
            group_id,
            snapshot_index = index,
            "completed staged snapshot install"
        );
        completed += 1;
    }
    Ok(completed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::install_snapshot::staged::staged_path;
    use crate::routing::RoutingTable;

    #[derive(Default)]
    struct RecordingApplier {
        applied: std::sync::Mutex<Vec<(u64, Vec<u8>)>>,
    }

    #[async_trait::async_trait]
    impl SnapshotApplier for RecordingApplier {
        async fn apply_snapshot(
            &self,
            group_id: u64,
            snapshot_bytes: &[u8],
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.applied
                .lock()
                .unwrap()
                .push((group_id, snapshot_bytes.to_vec()));
            Ok(())
        }
    }

    fn multi_raft(dir: &Path) -> MultiRaft {
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.to_path_buf());
        mr.add_group(7, vec![]).unwrap();
        mr
    }

    fn recv_dir(dir: &Path) -> std::path::PathBuf {
        let recv = dir.join("recv_snapshots");
        std::fs::create_dir_all(&recv).unwrap();
        recv
    }

    /// An install interrupted before the boundary moved is applied again,
    /// then the boundary moves and the staged file goes.
    #[tokio::test]
    async fn interrupted_install_is_reapplied_then_adopted() {
        let dir = tempfile::tempdir().unwrap();
        let recv = recv_dir(dir.path());
        std::fs::write(staged_path(&recv, 7, 42, 2), b"payload").unwrap();
        let mut mr = multi_raft(dir.path());
        let applier = RecordingApplier::default();

        let completed =
            recover_staged_installs(dir.path(), &mut mr, Some(&applier as &dyn SnapshotApplier))
                .await
                .unwrap();
        assert_eq!(completed, 1);

        assert_eq!(
            *applier.applied.lock().unwrap(),
            vec![(7, b"payload".to_vec())]
        );
        let node = mr.groups_mut().get(&7).unwrap();
        assert_eq!(node.log_snapshot_index(), 42);
        assert_eq!(node.durable_applied_index(), 42);
        assert!(list_staged(&recv).unwrap().is_empty());
    }

    /// An install whose boundary moved before the crash was durable already:
    /// it is not applied again, only its file removed.
    #[tokio::test]
    async fn install_past_the_boundary_is_not_reapplied() {
        let dir = tempfile::tempdir().unwrap();
        let recv = recv_dir(dir.path());
        let mut mr = multi_raft(dir.path());
        mr.adopt_snapshot_boundary(7, 50, 2).unwrap();
        std::fs::write(staged_path(&recv, 7, 42, 2), b"payload").unwrap();
        let applier = RecordingApplier::default();

        let completed =
            recover_staged_installs(dir.path(), &mut mr, Some(&applier as &dyn SnapshotApplier))
                .await
                .unwrap();
        assert_eq!(completed, 1);

        assert!(applier.applied.lock().unwrap().is_empty());
        let node = mr.groups_mut().get(&7).unwrap();
        assert_eq!(node.log_snapshot_index(), 50);
        assert!(list_staged(&recv).unwrap().is_empty());
    }

    #[tokio::test]
    async fn staged_install_of_unmounted_group_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let recv = recv_dir(dir.path());
        std::fs::write(staged_path(&recv, 9, 42, 2), b"payload").unwrap();
        let mut mr = multi_raft(dir.path());
        let applier = RecordingApplier::default();

        let completed =
            recover_staged_installs(dir.path(), &mut mr, Some(&applier as &dyn SnapshotApplier))
                .await
                .unwrap();
        assert_eq!(completed, 0);
        assert!(applier.applied.lock().unwrap().is_empty());
        assert!(!staged_path(&recv, 9, 42, 2).exists());
    }

    /// A finished install's `.snap` file is never applied at boot: it is
    /// removed.
    #[tokio::test]
    async fn snap_files_are_removed_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let recv = recv_dir(dir.path());
        std::fs::write(recv.join("7.snap"), b"old-install").unwrap();
        let mut mr = multi_raft(dir.path());
        let applier = RecordingApplier::default();

        recover_staged_installs(dir.path(), &mut mr, Some(&applier as &dyn SnapshotApplier))
            .await
            .unwrap();
        assert!(applier.applied.lock().unwrap().is_empty());
        assert!(!recv.join("7.snap").exists());
    }
}
