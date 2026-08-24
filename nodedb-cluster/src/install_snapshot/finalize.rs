// SPDX-License-Identifier: BUSL-1.1

//! Final snapshot commit: CRC validation → atomic rename → Raft log boundary advance.
//!
//! Called only when the last chunk (`done == true`) has been written to the
//! `.partial` file. Performs three operations in sequence:
//!
//! 1. **CRC validation** — re-reads the assembled file and recomputes the
//!    CRC32C. If it disagrees with the running CRC accumulated during chunk
//!    writes, the partial file is left in place and `SnapshotCrcMismatch` is
//!    returned. The partial file is intentionally *not* deleted on CRC failure
//!    so the operator can inspect it.
//!
//! 2. **Atomic rename** — the `.partial` file is renamed to `<group_id>.snap`.
//!    The rename is atomic on POSIX filesystems (same directory, same inode
//!    table). If the process crashes between steps 1 and 2, the partial file
//!    survives; the GC sweeper will remove it after `orphan_partial_max_age_secs`.
//!
//! 3. **Raft log boundary advance** — calls
//!    `MultiRaft::handle_install_snapshot` to advance the Raft log pointer to
//!    `last_included_index` / `last_included_term`. This is the same call the
//!    existing stub in `handle_rpc.rs` made; we now call it only here, after
//!    CRC validation, to prevent advancing Raft state on corrupt data.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use nodedb_raft::{InstallSnapshotRequest, InstallSnapshotResponse};

use crate::error::ClusterError;
use crate::install_snapshot::state::PartialSnapshotState;
use crate::multi_raft::MultiRaft;
use crate::raft_loop::SnapshotApplier;

/// Validate, rename, and advance Raft state after the last chunk.
///
/// Returns the `InstallSnapshotResponse` produced by
/// `MultiRaft::handle_install_snapshot` so callers can propagate the
/// Raft term back to the leader.
pub async fn commit(
    state: PartialSnapshotState,
    multi_raft: &Arc<Mutex<MultiRaft>>,
    snapshot_applier: Option<&Arc<dyn SnapshotApplier>>,
) -> Result<InstallSnapshotResponse, ClusterError> {
    let group_id = state.group_id;
    let partial_path = state.partial_path.clone();
    let expected_crc = state.running_crc;

    // Flush and close the partial file before reading it back.
    // `state.partial_file` may be `None` if the snapshot had zero bytes
    // (bootstrap stub). In that case skip the I/O validation.
    if let Some(file) = state.partial_file {
        tokio::task::spawn_blocking(move || -> std::io::Result<()> { file.sync_all() })
            .await
            .map_err(|e| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: format!("spawn_blocking join error on sync: {e}"),
            })?
            .map_err(|e| ClusterError::Storage {
                detail: format!("sync partial file for group {group_id}: {e}"),
            })?;
    }

    // CRC validation: re-read the file and compare against running CRC.
    // If the file is empty (bootstrap stub), skip.
    let file_bytes = tokio::task::spawn_blocking({
        let path = partial_path.clone();
        move || std::fs::read(&path)
    })
    .await
    .map_err(|e| ClusterError::PartialSnapshotCorrupt {
        group_id,
        detail: format!("spawn_blocking join error on read: {e}"),
    })?
    .map_err(|e| ClusterError::Storage {
        detail: format!("read partial file for group {group_id}: {e}"),
    })?;

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

    // Atomic rename: .partial → .snap
    let snap_path = snap_path_for(&partial_path);
    tokio::task::spawn_blocking({
        let from = partial_path.clone();
        let to = snap_path.clone();
        move || std::fs::rename(&from, &to)
    })
    .await
    .map_err(|e| ClusterError::PartialSnapshotCorrupt {
        group_id,
        detail: format!("spawn_blocking join error on rename: {e}"),
    })?
    .map_err(|e| ClusterError::Storage {
        detail: format!("rename partial to snap for group {group_id}: {e}"),
    })?;

    // Apply the snapshot to the local Data-Plane state machine BEFORE advancing
    // Raft, so the data is visible on this node before the Raft log boundary
    // moves. An apply failure is fatal — we return WITHOUT advancing Raft so the
    // follower retries the install (no silent partial success). The empty
    // bootstrap stub (no engine data) is skipped: there is nothing to apply, and
    // group 0 (metadata) is a no-op the applier handles internally.
    if !file_bytes.is_empty()
        && let Some(applier) = snapshot_applier
    {
        applier
            .apply_snapshot(group_id, &file_bytes)
            .await
            .map_err(|e| ClusterError::SnapshotApplyFailed {
                group_id,
                detail: e.to_string(),
            })?;
    }

    // Advance Raft log boundary. Build a minimal InstallSnapshotRequest
    // that satisfies `handle_install_snapshot` — `data` is the assembled
    // bytes (may be empty for the bootstrap stub), `done` is always `true`.
    let req = InstallSnapshotRequest {
        term: state.term,
        leader_id: state.leader_id,
        last_included_index: state.last_included_index,
        last_included_term: state.last_included_term,
        offset: 0,
        data: file_bytes,
        done: true,
        group_id,
        total_size: 0,
    };

    let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
    let resp = mr.handle_install_snapshot(&req)?;
    // Persist any term bump (become_follower) durably before replying.
    mr.persist_group_hard_state(group_id)?;
    Ok(resp)
}

/// Derive the `.snap` path from the `.partial` path (same directory, stem only).
fn snap_path_for(partial: &std::path::Path) -> PathBuf {
    let parent = partial
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let stem = partial
        .file_stem()
        .unwrap_or_else(|| std::ffi::OsStr::new("unknown"));
    parent.join(format!("{}.snap", stem.to_string_lossy()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RoutingTable;

    /// Recording applier: proves the state machine saw the snapshot bytes
    /// (i.e. the DATA is applied), and can be told to fail.
    #[derive(Default)]
    struct RecordingApplier {
        applied: std::sync::Mutex<Vec<(u64, Vec<u8>)>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl SnapshotApplier for RecordingApplier {
        async fn apply_snapshot(
            &self,
            group_id: u64,
            snapshot_bytes: &[u8],
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if self.fail {
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
    /// boundary advances. A snapshot whose data is applied must
    /// advance the group's snapshot boundary; one whose apply FAILS must
    /// leave Raft untouched (follower retries; no silent divergence).
    #[tokio::test]
    async fn snapshot_data_applied_before_raft_advances() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(RecordingApplier::default());
        let applier: Arc<dyn SnapshotApplier> = inner.clone();
        let (mr, _keep) = multi_raft();

        let state = partial_state(dir.path(), b"snapshot-payload", 42);
        let resp = commit(state, &mr, Some(&applier)).await.unwrap();

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
        assert_eq!(resp.term, 1);
    }

    #[tokio::test]
    async fn snapshot_apply_failure_does_not_advance_raft() {
        let dir = tempfile::tempdir().unwrap();
        let applier: Arc<dyn SnapshotApplier> = Arc::new(RecordingApplier {
            fail: true,
            ..Default::default()
        });
        let (mr, _keep) = multi_raft();

        let state = partial_state(dir.path(), b"corrupt-in-applier", 42);
        let res = commit(state, &mr, Some(&applier)).await;
        assert!(
            res.is_err(),
            "apply failure must surface as an error, not silent partial success"
        );

        // Raft must not advance: the follower retries the install. Advancing
        // here without the data is the divergence this order prevents.
        let mut mr = mr.lock().unwrap();
        let node = mr.groups_mut().get(&7).unwrap();
        assert_eq!(
            node.log_snapshot_index(),
            0,
            "raft must not advance on apply failure"
        );
        assert_eq!(
            node.commit_index(),
            0,
            "commit index must not advance on apply failure"
        );
    }
}
