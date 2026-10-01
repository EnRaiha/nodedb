// SPDX-License-Identifier: BUSL-1.1

//! Final snapshot commit: CRC validation → stage → host apply → Raft log
//! boundary advance → finish.
//!
//! Called only when the last chunk (`done == true`) has been written to the
//! `.partial` file. Runs these steps in order:
//!
//! 1. **CRC validation** — re-reads the assembled file and recomputes the
//!    CRC32C. On a mismatch the partial file is left in place for inspection
//!    and `SnapshotCrcMismatch` is returned.
//! 2. **Need check** — a snapshot from a stale term, or one at or below the
//!    group's applied index, is not applied: installing it would put the state
//!    machine behind the log it keeps applying from. The partial is removed
//!    and only the term bookkeeping runs.
//! 3. **Stage** — the partial is renamed to its staged name
//!    ([`super::staged`]). From here until step 6 the staged file marks the
//!    install as in progress, and boot recovery completes it after a crash.
//! 4. **Host apply** — the [`SnapshotApplier`] installs the snapshot on every
//!    Data-Plane core and returns only once the install is durable without
//!    the WAL. An error returns [`ClusterError::SnapshotApplyFailed`] with the
//!    Raft boundary and durable floor unmoved and the staged file kept. The
//!    leader's next send re-installs over it.
//! 5. **Raft advance** — the group adopts the snapshot as its log boundary
//!    and durable applied floor, and persists any term bump.
//! 6. **Finish** — the staged file is removed. No copy is kept: the host
//!    state is durable, and the leader builds every snapshot it sends from
//!    live engine state.

use std::sync::{Arc, Mutex};

use nodedb_raft::{InstallSnapshotRequest, InstallSnapshotResponse};

use crate::error::ClusterError;
use crate::install_snapshot::staged::{discard, finish, stage};
use crate::install_snapshot::state::PartialSnapshotState;
use crate::multi_raft::MultiRaft;
use crate::raft_loop::SnapshotApplier;

/// The outcome of [`commit`].
#[derive(Debug)]
pub struct CommitResult {
    /// The reply from `MultiRaft::handle_install_snapshot`, carrying the Raft
    /// term back to the leader.
    pub response: InstallSnapshotResponse,
    /// Whether this node's state machine holds the state through the
    /// snapshot index: the host applied the snapshot, or the node had already
    /// applied past it. False when the boundary moved on an empty stub with no
    /// host state to restore.
    pub state_installed: bool,
}

/// Validate, stage, apply, advance Raft, and finish after the last chunk.
pub async fn commit(
    state: PartialSnapshotState,
    multi_raft: &Arc<Mutex<MultiRaft>>,
    snapshot_applier: Option<&Arc<dyn SnapshotApplier>>,
) -> Result<CommitResult, ClusterError> {
    let group_id = state.group_id;
    let partial_path = state.partial_path.clone();
    let expected_crc = state.running_crc;
    let index = state.last_included_index;
    let snapshot_term = state.last_included_term;

    // Flush and close the partial file before reading it back.
    // `state.partial_file` may be `None` if the snapshot had zero bytes
    // (bootstrap stub). In that case skip the I/O validation.
    if let Some(file) = state.partial_file {
        blocking(group_id, move || {
            file.sync_all().map_err(|e| ClusterError::Storage {
                detail: format!("sync partial file for group {group_id}: {e}"),
            })
        })
        .await?;
    }

    let file_bytes = blocking(group_id, {
        let path = partial_path.clone();
        move || {
            std::fs::read(&path).map_err(|e| ClusterError::Storage {
                detail: format!("read partial file for group {group_id}: {e}"),
            })
        }
    })
    .await?;

    if !file_bytes.is_empty() {
        let computed = crc32c::crc32c(&file_bytes);
        if computed != expected_crc {
            return Err(ClusterError::SnapshotCrcMismatch {
                group_id,
                stored: expected_crc,
                computed,
            });
        }
    }

    // Term and leader bookkeeping only. The boundary moves through
    // `adopt_snapshot_boundary`, after the host apply.
    let bookkeeping = InstallSnapshotRequest {
        term: state.term,
        leader_id: state.leader_id,
        last_included_index: index,
        last_included_term: snapshot_term,
        offset: 0,
        data: Vec::new(),
        done: false,
        group_id,
        total_size: 0,
        voters: Vec::new(),
        learners: Vec::new(),
    };

    // Held from the need check through the boundary advance. No committed
    // entry of the group reaches the state machine in between, so none the
    // snapshot covers can land on top of the restore.
    let apply_gates = multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .apply_gates();
    let mut install_permit = apply_gates.install(group_id).await;

    let needed = multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .snapshot_install_needed(group_id, state.term, index)?;
    if !needed {
        blocking(group_id, {
            let path = partial_path.clone();
            move || discard(&path)
        })
        .await?;
        let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let response = mr.handle_install_snapshot(&bookkeeping)?;
        mr.persist_group_hard_state(group_id)?;
        return Ok(CommitResult {
            response,
            state_installed: true,
        });
    }

    // An empty payload carries no metadata state. Adopting it would move
    // group 0's boundary past entries this node never applied.
    if group_id == crate::metadata_group::METADATA_GROUP_ID
        && file_bytes.is_empty()
        && snapshot_applier.is_some()
    {
        blocking(group_id, {
            let path = partial_path.clone();
            move || discard(&path)
        })
        .await?;
        return Err(ClusterError::SnapshotApplyFailed {
            group_id,
            detail: format!(
                "empty metadata snapshot at index {index} carries no state; raft boundary unmoved"
            ),
        });
    }

    let recv_dir = partial_path
        .parent()
        .map(std::path::Path::to_path_buf)
        .ok_or_else(|| ClusterError::PartialSnapshotCorrupt {
            group_id,
            detail: format!("partial path {} has no directory", partial_path.display()),
        })?;
    let staged = blocking(group_id, {
        let partial = partial_path.clone();
        let recv_dir = recv_dir.clone();
        move || stage(&partial, &recv_dir, group_id, index, snapshot_term)
    })
    .await?;

    // The empty bootstrap stub carries no engine data, so there is nothing
    // to apply.
    let state_installed = !file_bytes.is_empty() && snapshot_applier.is_some();
    if !file_bytes.is_empty()
        && let Some(applier) = snapshot_applier
    {
        applier
            .apply_snapshot(group_id, &file_bytes)
            .await
            .map_err(|e| ClusterError::SnapshotApplyFailed {
                group_id,
                detail: format!(
                    "install of snapshot index {index} did not settle on every core, \
                     raft boundary unmoved, staged install kept for re-install: {e}"
                ),
            })?;
    }

    let response = {
        let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let resp = mr.handle_install_snapshot(&bookkeeping)?;
        // A metadata install wrote the routing table its skipped conf
        // changes produced. The group's own membership follows it.
        if group_id == crate::metadata_group::METADATA_GROUP_ID && state_installed {
            mr.sync_group_membership_from_routing(group_id)?;
        }
        mr.adopt_snapshot_boundary(group_id, index, snapshot_term)?;
        install_permit.adopted(index);
        // The host recorded the Calvin state the snapshot brought, so a
        // replica that refused entries for want of it takes them again. A
        // sequencer snapshot decides every data group again: the inputs it
        // skipped are gone from this node's Calvin state.
        if state_installed {
            mr.refresh_snapshot_requirement(group_id);
        }
        // Persist any term bump (become_follower) durably before replying.
        mr.persist_group_hard_state(group_id)?;
        resp
    };
    if let Some(applier) = snapshot_applier {
        applier.snapshot_adopted(group_id, index);
    }
    drop(install_permit);

    blocking(group_id, move || finish(&staged, &recv_dir)).await?;
    Ok(CommitResult {
        response,
        state_installed,
    })
}

/// Run blocking filesystem work off the async runtime.
async fn blocking<T, F>(group_id: u64, work: F) -> Result<T, ClusterError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, ClusterError> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| ClusterError::PartialSnapshotCorrupt {
            group_id,
            detail: format!("spawn_blocking join error: {e}"),
        })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::install_snapshot::staged::list_staged;
    use crate::routing::RoutingTable;

    /// Every file name left in `dir`.
    fn files_in(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    }

    /// Recording applier: proves the state machine saw the snapshot bytes
    /// (i.e. the DATA is applied), and can be told to fail.
    #[derive(Default)]
    struct RecordingApplier {
        applied: std::sync::Mutex<Vec<(u64, Vec<u8>)>>,
        fail: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl SnapshotApplier for RecordingApplier {
        async fn apply_snapshot(
            &self,
            group_id: u64,
            snapshot_bytes: &[u8],
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("injected applier failure".into());
            }
            self.applied
                .lock()
                .unwrap()
                .push((group_id, snapshot_bytes.to_vec()));
            Ok(())
        }
    }

    fn partial_state(dir: &std::path::Path, data: &[u8], index: u64) -> PartialSnapshotState {
        // Write the assembled snapshot bytes into a .partial file and compute
        // the CRC exactly as the chunk receiver would.
        let partial_path = dir.join("7.partial");
        std::fs::write(&partial_path, data).unwrap();
        PartialSnapshotState {
            group_id: 7,
            leader_id: 2,
            term: 1,
            last_included_index: index,
            last_included_term: 1,
            next_expected_offset: data.len() as u64,
            running_crc: crc32c::crc32c(data),
            running_crc_initialized: true,
            partial_file: None,
            partial_path,
        }
    }

    fn multi_raft() -> (Arc<Mutex<MultiRaft>>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(7, vec![]).unwrap();
        (Arc::new(Mutex::new(mr)), dir)
    }

    /// The data must be applied to the state machine BEFORE the Raft log
    /// boundary advances. A snapshot whose data is applied must advance the
    /// group's snapshot boundary and leave no file behind.
    #[tokio::test]
    async fn snapshot_data_applied_before_raft_advances() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(RecordingApplier::default());
        let applier: Arc<dyn SnapshotApplier> = inner.clone();
        let (mr, _keep) = multi_raft();

        let state = partial_state(dir.path(), b"snapshot-payload", 42);
        let resp = commit(state, &mr, Some(&applier)).await.unwrap().response;

        // Data reached the state machine exactly once, with the payload.
        let applied = inner.applied.lock().unwrap();
        assert_eq!(applied.len(), 1, "snapshot data must be applied");
        assert_eq!(applied[0].0, 7);
        assert_eq!(applied[0].1, b"snapshot-payload");

        // Raft boundary advanced to the snapshot index.
        let mut mr = mr.lock().unwrap();
        let node = mr.groups_mut().get(&7).unwrap();
        assert_eq!(node.log_snapshot_index(), 42);
        assert_eq!(node.commit_index(), 42);
        assert_eq!(node.durable_applied_index(), 42);
        assert_eq!(resp.term, 1);

        assert!(
            files_in(dir.path()).is_empty(),
            "a finished install keeps no file"
        );
    }

    /// A failed apply leaves Raft untouched and keeps the staged install.
    /// The leader's next send re-installs over it and completes.
    #[tokio::test]
    async fn snapshot_apply_failure_keeps_staged_install_for_retry() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(RecordingApplier::default());
        inner.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let applier: Arc<dyn SnapshotApplier> = inner.clone();
        let (mr, _keep) = multi_raft();

        let state = partial_state(dir.path(), b"first-attempt", 42);
        let err = commit(state, &mr, Some(&applier))
            .await
            .expect_err("apply failure must surface as an error");
        assert!(
            matches!(err, ClusterError::SnapshotApplyFailed { group_id: 7, .. }),
            "unexpected error: {err}"
        );

        {
            let mut guard = mr.lock().unwrap();
            let node = guard.groups_mut().get(&7).unwrap();
            assert_eq!(node.log_snapshot_index(), 0, "boundary must not move");
            assert_eq!(node.commit_index(), 0, "commit index must not move");
            assert_eq!(node.durable_applied_index(), 0, "floor must not move");
        }
        let staged = list_staged(dir.path()).unwrap();
        assert_eq!(
            staged.len(),
            1,
            "the staged install marks the failed install"
        );
        assert_eq!(staged[0].last_included_index, 42);

        inner.fail.store(false, std::sync::atomic::Ordering::SeqCst);
        let state = partial_state(dir.path(), b"second-attempt", 42);
        commit(state, &mr, Some(&applier)).await.unwrap();

        let mut guard = mr.lock().unwrap();
        let node = guard.groups_mut().get(&7).unwrap();
        assert_eq!(node.log_snapshot_index(), 42);
        assert_eq!(
            inner.applied.lock().unwrap().as_slice(),
            &[(7, b"second-attempt".to_vec())]
        );
        assert!(files_in(dir.path()).is_empty());
    }

    /// A snapshot at or below the applied index is never applied: it would
    /// move the state machine behind the log.
    #[tokio::test]
    async fn snapshot_behind_applied_index_is_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(RecordingApplier::default());
        let applier: Arc<dyn SnapshotApplier> = inner.clone();
        let (mr, _keep) = multi_raft();
        mr.lock()
            .unwrap()
            .adopt_snapshot_boundary(7, 50, 1)
            .unwrap();

        let state = partial_state(dir.path(), b"old-snapshot", 42);
        commit(state, &mr, Some(&applier)).await.unwrap();

        assert!(inner.applied.lock().unwrap().is_empty());
        assert!(!dir.path().join("7.partial").exists());
        assert!(list_staged(dir.path()).unwrap().is_empty());
        let mut guard = mr.lock().unwrap();
        assert_eq!(guard.groups_mut().get(&7).unwrap().log_snapshot_index(), 50);
    }
}
