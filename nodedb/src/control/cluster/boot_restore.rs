// SPDX-License-Identifier: BUSL-1.1

//! Follower boot-restore of persisted Raft snapshots.
//!
//! On node startup the committed `.snap` files left under
//! `<data_dir>/recv_snapshots/` by the install-snapshot RECEIVE path survive
//! across restarts (the startup orphan sweep only removes `.partial` files).
//! Those `.snap` files hold the engine state for the log prefix that Raft
//! compacted away after applying the snapshot — once the leader compacts its
//! log past the snapshot index, that prefix can NEVER be re-replayed. The
//! persisted snapshot is therefore the only source for that state, and it must
//! be re-installed into the Data Plane BEFORE the apply loop replays the
//! post-snapshot log tail. Otherwise a restarted follower comes up missing all
//! pre-snapshot rows even though full scans of the tail succeed.
//!
//! This mirrors the in-band [`crate::control::cluster::snapshot_applier`]
//! RECEIVE path: it reuses the same [`DataPlaneSnapshotApplier`] so an apply
//! failure is FATAL (a node must not come up with partially-restored state).

use std::path::Path;

use tracing::{info, warn};

use crate::control::cluster::snapshot_applier::DataPlaneSnapshotApplier;
use nodedb_cluster::SnapshotApplier;

/// Re-install every persisted `.snap` snapshot under
/// `<data_dir>/recv_snapshots/` into the local Data Plane via `applier`.
///
/// Returns the number of group snapshots applied. Snapshots are applied in
/// ascending `group_id` order for deterministic boot behaviour. A `.snap` file
/// whose stem is not a `u64` group id, or whose body is empty, is logged and
/// skipped (it cannot name a real group / has nothing to restore). An apply
/// failure is FATAL and returned to the caller: the node must not finish boot
/// with missing engine state.
pub async fn restore_persisted_snapshots(
    data_dir: &Path,
    applier: &DataPlaneSnapshotApplier,
) -> crate::Result<usize> {
    let recv_dir = data_dir.join("recv_snapshots");
    // No receive directory means this node never received a snapshot — nothing
    // to restore.
    if !recv_dir.exists() {
        return Ok(0);
    }

    // Collect `.snap` paths paired with their parsed group id, then sort by
    // group id so the apply order is stable across boots.
    let mut snaps: Vec<(u64, std::path::PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(&recv_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("snap") {
            continue;
        }
        let group_id = match path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(id) => id,
            None => {
                warn!(
                    path = %path.display(),
                    "boot-restore: skipping snapshot with non-numeric group id stem"
                );
                continue;
            }
        };
        snaps.push((group_id, path));
    }
    snaps.sort_by_key(|(group_id, _)| *group_id);

    let mut applied = 0usize;
    for (group_id, path) in snaps {
        let bytes = std::fs::read(&path)?;
        if bytes.is_empty() {
            warn!(
                group_id,
                path = %path.display(),
                "boot-restore: skipping empty persisted snapshot"
            );
            continue;
        }

        // An apply failure here is FATAL: the same rationale as the in-band
        // RECEIVE path — a node must not come up with partially-restored
        // engine state. The applier's boxed error is mapped onto the crate's
        // typed error.
        applier
            .apply_snapshot(group_id, &bytes)
            .await
            .map_err(|e| crate::Error::Internal {
                detail: format!("boot-restore: apply group {group_id} snapshot: {e}"),
            })?;

        info!(
            group_id,
            bytes = bytes.len(),
            "boot-restore: re-installed persisted group snapshot"
        );
        applied += 1;
    }

    Ok(applied)
}
