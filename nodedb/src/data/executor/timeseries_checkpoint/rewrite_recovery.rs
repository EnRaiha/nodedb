// SPDX-License-Identifier: BUSL-1.1

//! Settle a `REWRITE PARTITIONS` that a crash interrupted.
//!
//! A rewrite stages a partition as `rewrite-{name}`, swaps it in, and moves the
//! replaced copy to `old-{name}` before removing it. Both copies hold the same
//! rows, so recovery keeps whichever is complete and live:
//!
//! - `old-{name}` with `{name}` present: the swap finished. The backup goes.
//! - `old-{name}` with `{name}` missing: the swap stopped between its two
//!   renames. The backup is renamed back to `{name}`.
//! - `rewrite-{name}`: the swap never consumed it. It goes.
//!
//! Recovery runs before the registry of the collection directory loads.

use std::path::Path;

use crate::data::executor::handlers::timeseries::partition_rewrite::{
    REWRITE_BACKUP_PREFIX, REWRITE_STAGED_PREFIX,
};
use crate::data::executor::handlers::timeseries::truncate::remove_dir_tree;

/// Settle every interrupted rewrite in the collection directory `ts_dir`.
pub(in crate::data::executor) fn recover_interrupted_rewrites(ts_dir: &Path) -> crate::Result<()> {
    let names = directory_names(ts_dir)?;
    let mut restored = false;
    for name in &names {
        let Some(live) = name.strip_prefix(REWRITE_BACKUP_PREFIX) else {
            continue;
        };
        if !nodedb_types::is_plain_path_component(live) {
            continue;
        }
        let backup = ts_dir.join(name);
        let live_dir = ts_dir.join(live);
        if live_dir.exists() {
            remove_dir_tree(&backup)?;
        } else {
            std::fs::rename(&backup, &live_dir).map_err(|e| crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!(
                    "restore partition {} from an interrupted rewrite: {e}",
                    live_dir.display()
                ),
            })?;
            restored = true;
        }
    }
    if restored {
        nodedb_wal::segment::fsync_directory(ts_dir).map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("fsync {} after a rewrite recovery: {e}", ts_dir.display()),
        })?;
    }
    for name in &names {
        if name.starts_with(REWRITE_STAGED_PREFIX) {
            remove_dir_tree(&ts_dir.join(name))?;
        }
    }
    Ok(())
}

/// The names of the directories directly under `dir`. A missing `dir` has
/// none.
fn directory_names(dir: &Path) -> crate::Result<Vec<String>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!("read partition dir {}: {e}", dir.display()),
            });
        }
    };
    Ok(entries
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make(dir: &Path, name: &str) {
        std::fs::create_dir_all(dir.join(name)).expect("create");
    }

    #[test]
    fn a_backup_beside_its_live_partition_is_removed() {
        let dir = tempfile::tempdir().expect("tempdir");
        make(dir.path(), "ts-a");
        make(dir.path(), "old-ts-a");
        recover_interrupted_rewrites(dir.path()).expect("recovers");
        assert!(dir.path().join("ts-a").is_dir());
        assert!(!dir.path().join("old-ts-a").exists());
    }

    #[test]
    fn a_backup_without_its_live_partition_is_restored() {
        let dir = tempfile::tempdir().expect("tempdir");
        make(dir.path(), "old-ts-a");
        make(dir.path(), "rewrite-ts-a");
        recover_interrupted_rewrites(dir.path()).expect("recovers");
        assert!(dir.path().join("ts-a").is_dir());
        assert!(!dir.path().join("old-ts-a").exists());
        assert!(!dir.path().join("rewrite-ts-a").exists());
    }

    #[test]
    fn a_staged_copy_is_removed() {
        let dir = tempfile::tempdir().expect("tempdir");
        make(dir.path(), "ts-a");
        make(dir.path(), "rewrite-ts-a");
        recover_interrupted_rewrites(dir.path()).expect("recovers");
        assert!(dir.path().join("ts-a").is_dir());
        assert!(!dir.path().join("rewrite-ts-a").exists());
    }
}
