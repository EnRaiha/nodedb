// SPDX-License-Identifier: BUSL-1.1

//! Follower-side chunk accumulator for `InstallSnapshot` RPCs.
//!
//! Each incoming `InstallSnapshotRequest` chunk is:
//! 1. Validated for offset monotonicity (`req.offset == next_expected_offset`).
//! 2. Written to `<data_dir>/recv_snapshots/<group_id>.partial` via
//!    `tokio::task::spawn_blocking` (standard `std::fs::File` — NOT O_DIRECT,
//!    NOT io_uring).
//! 3. The running CRC32C across all written bytes is updated.
//! 4. When `req.done == true`, [`super::finalize::commit`] is called.
//!
//! # Restart resume
//!
//! On `offset == 0`, we always truncate and rewrite the partial file. If a
//! `.partial` already exists from a prior interrupted transfer, the leader is
//! expected to restart from offset 0 (it detects the follower reset via the
//! response and retransmits from the beginning). This keeps the receiver
//! stateless across restarts: on startup the caller need not load partial state
//! into the map — an incoming `offset == 0` chunk rebuilds it naturally.
//!
//! Choice rationale: trusting the partial file and resuming mid-stream requires
//! re-hashing the file contents on startup to rebuild the running CRC, and
//! requires the leader to query the follower's current offset before sending.
//! Neither primitive exists yet. The simpler approach (always retransmit from
//! zero on leader-restart or follower-restart) is correct and the cost is one
//! extra round of RPC traffic.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use std::collections::HashMap;

use nodedb_raft::InstallSnapshotRequest;

use crate::error::ClusterError;
use crate::install_snapshot::finalize;
use crate::install_snapshot::state::PartialSnapshotState;
use crate::multi_raft::MultiRaft;
use crate::raft_loop::SnapshotApplier;

/// Thread-safe map of in-progress partial snapshot receives, keyed by `group_id`.
pub type PartialSnapshotMap = Mutex<HashMap<u64, PartialSnapshotState>>;

/// Outcome of processing a single incoming chunk.
#[derive(Debug)]
pub enum ChunkOutcome {
    /// More chunks are expected.
    Pending,
    /// The final chunk was received, CRC validated, and the snapshot committed.
    /// Contains the `InstallSnapshotResponse` from `MultiRaft::handle_install_snapshot`.
    Committed(nodedb_raft::InstallSnapshotResponse),
}

/// Process a single incoming `InstallSnapshotRequest` chunk.
///
/// Locks `partial_map` for the duration of state access but releases it before
/// any blocking I/O via `spawn_blocking`.
pub async fn handle_chunk(
    req: &InstallSnapshotRequest,
    partial_map: &PartialSnapshotMap,
    data_dir: &Path,
    multi_raft: &std::sync::Arc<std::sync::Mutex<MultiRaft>>,
    snapshot_applier: Option<&std::sync::Arc<dyn SnapshotApplier>>,
) -> Result<ChunkOutcome, ClusterError> {
    let group_id = req.group_id;
    let recv_dir = data_dir.join("recv_snapshots");

    // Ensure the receive directory exists.
    tokio::task::spawn_blocking({
        let recv_dir = recv_dir.clone();
        move || std::fs::create_dir_all(&recv_dir)
    })
    .await
    .map_err(|e| ClusterError::PartialSnapshotCorrupt {
        group_id,
        detail: format!("spawn_blocking join error: {e}"),
    })?
    .map_err(|e| ClusterError::Storage {
        detail: format!("create recv_snapshots dir: {e}"),
    })?;

    if req.offset == 0 {
        // Start (or restart) — open partial file with truncation.
        let partial_path = partial_path_for(&recv_dir, group_id);
        let partial_file = tokio::task::spawn_blocking({
            let path = partial_path.clone();
            move || {
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)
            }
        })
        .await
        .map_err(|e| ClusterError::PartialSnapshotCorrupt {
            group_id,
            detail: format!("spawn_blocking join error: {e}"),
        })?
        .map_err(|e| ClusterError::Storage {
            detail: format!("open partial file for group {group_id}: {e}"),
        })?;

        let state = PartialSnapshotState {
            group_id,
            leader_id: req.leader_id,
            term: req.term,
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            next_expected_offset: 0,
            running_crc: 0,
            running_crc_initialized: false,
            partial_file: Some(partial_file),
            partial_path,
        };

        let mut map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        map.insert(group_id, state);
    } else {
        // Continuation — validate the state entry exists and offset matches.
        let map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        match map.get(&group_id) {
            None => {
                // No partial state for this group. This happens after a
                // follower restart when the leader is mid-stream. Return
                // an offset regression error; the leader will restart
                // from offset 0.
                return Err(ClusterError::SnapshotOffsetRegression {
                    group_id,
                    expected: 0,
                    actual: req.offset,
                });
            }
            Some(state) if state.next_expected_offset != req.offset => {
                let expected = state.next_expected_offset;
                let actual = req.offset;
                // Drop the lock before returning the error. The caller
                // is responsible for resetting the partial state on regression.
                drop(map);
                return Err(ClusterError::SnapshotOffsetRegression {
                    group_id,
                    expected,
                    actual,
                });
            }
            Some(_) => {}
        }
        // Lock dropped here.
    }

    // Write chunk bytes to the partial file via spawn_blocking.
    let written_len = req.data.len() as u64;

    // Take the file out of the state, write via spawn_blocking, then restore it.
    let taken_file = {
        let mut map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        let state = map
            .get_mut(&group_id)
            .ok_or_else(|| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: "partial state disappeared during write".into(),
            })?;
        state
            .partial_file
            .take()
            .ok_or_else(|| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: "partial file already taken".into(),
            })?
    };
    let file = {
        tokio::task::spawn_blocking({
            let bytes = req.data.clone();
            move || -> std::io::Result<std::fs::File> {
                let mut f = taken_file;
                f.write_all(&bytes)?;
                f.flush()?;
                Ok(f)
            }
        })
        .await
        .map_err(|e| ClusterError::PartialSnapshotCorrupt {
            group_id,
            detail: format!("spawn_blocking join error during write: {e}"),
        })?
        .map_err(|e| ClusterError::Storage {
            detail: format!("write to partial file for group {group_id}: {e}"),
        })?
    };

    // Update running CRC and put the file back.
    {
        let mut map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        let state = map
            .get_mut(&group_id)
            .ok_or_else(|| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: "partial state disappeared after write".into(),
            })?;

        // Update running CRC over the raw chunk payload bytes.
        if written_len > 0 {
            if !state.running_crc_initialized {
                state.running_crc = crc32c::crc32c(&req.data);
                state.running_crc_initialized = true;
            } else {
                state.running_crc = crc32c::crc32c_append(state.running_crc, &req.data);
            }
        }

        state.next_expected_offset += written_len;
        state.partial_file = Some(file);
    }

    if !req.done {
        return Ok(ChunkOutcome::Pending);
    }

    // Final chunk: validate and commit.
    let state = {
        let mut map = partial_map.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(&group_id)
            .ok_or_else(|| ClusterError::PartialSnapshotCorrupt {
                group_id,
                detail: "partial state disappeared before finalization".into(),
            })?
    };

    let resp = finalize::commit(state, multi_raft, snapshot_applier).await?;
    Ok(ChunkOutcome::Committed(resp))
}

pub fn partial_path_for(recv_dir: &Path, group_id: u64) -> PathBuf {
    recv_dir.join(format!("{group_id}.partial"))
}
