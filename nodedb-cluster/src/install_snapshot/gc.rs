// SPDX-License-Identifier: BUSL-1.1

//! Orphan partial-snapshot cleanup.
//!
//! Scans `<data_dir>/recv_snapshots/` for `.partial` files whose last
//! modification time is older than `max_age_secs`. Removes those files.
//! Fresh `.partial` files (recently-modified) are left untouched.
//!
//! # When to call
//!
//! `sweep_orphans` is called at two points in the node lifecycle:
//! - Once at node startup (via [`crate::raft_loop::loop_core::RaftLoop::run`]),
//!   to remove leftover files from previous runs that did not complete.
//! - Periodically (~every 60 s) from the Raft tick loop, to reclaim disk space
//!   from partial files that accumulate during the node's lifetime without
//!   requiring a restart.

use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::error::ClusterError;

/// Remove orphaned `.partial` snapshot files older than `max_age_secs`.
///
/// Errors on individual files are returned as `PartialSnapshotCleanupFailed`
/// variants inside the result `Vec`. All files are attempted; a failure on one
/// does not abort the sweep. The caller may log or surface these errors.
///
/// Returns `Ok(removed_count)` even if some individual removals failed (those
/// failures are in the returned error vec). Returns `Err` only on directory
/// enumeration failure.
pub fn sweep_orphans(
    data_dir: &Path,
    max_age_secs: u64,
) -> Result<(usize, Vec<ClusterError>), ClusterError> {
    let recv_dir = data_dir.join("recv_snapshots");

    // If the directory doesn't exist yet there is nothing to sweep.
    if !recv_dir.exists() {
        return Ok((0, vec![]));
    }

    let entries = std::fs::read_dir(&recv_dir).map_err(|e| ClusterError::Storage {
        detail: format!("read_dir recv_snapshots: {e}"),
    })?;

    let max_age = Duration::from_secs(max_age_secs);
    let now = SystemTime::now();

    let mut removed = 0usize;
    let mut errors = Vec::new();

    for entry_result in entries {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                errors.push(ClusterError::Storage {
                    detail: format!("iterate recv_snapshots: {e}"),
                });
                continue;
            }
        };

        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("partial") {
            continue;
        }

        // Extract group_id from the file stem for error messages.
        let group_id: u64 = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let age = match entry.metadata().and_then(|m| m.modified()) {
            Ok(mtime) => match now.duration_since(mtime) {
                Ok(d) => d,
                Err(_) => Duration::ZERO, // mtime in the future; treat as fresh
            },
            Err(e) => {
                errors.push(ClusterError::PartialSnapshotCleanupFailed {
                    group_id,
                    detail: format!("stat {}: {e}", path.display()),
                });
                continue;
            }
        };

        if age < max_age {
            continue;
        }

        if let Err(e) = std::fs::remove_file(&path) {
            errors.push(ClusterError::PartialSnapshotCleanupFailed {
                group_id,
                detail: format!("remove {}: {e}", path.display()),
            });
        } else {
            removed += 1;
        }
    }

    Ok((removed, errors))
}
