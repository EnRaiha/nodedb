// SPDX-License-Identifier: BUSL-1.1

//! Staged snapshot installs: the on-disk marker of an install in progress.
//!
//! A received snapshot whose CRC passed is renamed from `<group>.partial` to
//! `<group>.<index>.<term>.staged` before the state machine sees it. The
//! staged file exists from the moment the host apply can start until the Raft
//! boundary has moved, and is then removed: the host install is durable on
//! its own, and the leader builds every snapshot it sends from live engine
//! state, so no copy of a finished install is kept. A staged file found at
//! boot is an install that did not finish, and [`super::recover`] completes
//! it.
//!
//! These functions do blocking filesystem I/O. Async callers run them on
//! `spawn_blocking`.

use std::path::{Path, PathBuf};

use crate::error::ClusterError;

const STAGED_EXTENSION: &str = "staged";

/// Extension of per-group snapshot files. Nothing reads one. Boot recovery
/// removes any it finds.
const SNAP_EXTENSION: &str = "snap";

/// One staged install found on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedInstall {
    pub group_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub path: PathBuf,
}

/// Path of the staged file for `(group_id, index, term)` under `recv_dir`.
pub fn staged_path(recv_dir: &Path, group_id: u64, index: u64, term: u64) -> PathBuf {
    recv_dir.join(format!("{group_id}.{index}.{term}.{STAGED_EXTENSION}"))
}

/// Parse `<group>.<index>.<term>.staged` into `(group, index, term)`.
fn parse_staged_name(name: &str) -> Option<(u64, u64, u64)> {
    let stem = name.strip_suffix(STAGED_EXTENSION)?.strip_suffix('.')?;
    let mut parts = stem.split('.');
    let group_id = parts.next()?.parse().ok()?;
    let index = parts.next()?.parse().ok()?;
    let term = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((group_id, index, term))
}

fn storage_error(action: &str, path: &Path, e: std::io::Error) -> ClusterError {
    ClusterError::Storage {
        detail: format!("{action} {}: {e}", path.display()),
    }
}

/// Fsync `dir` so a rename or unlink inside it survives a crash.
fn sync_dir(dir: &Path) -> Result<(), ClusterError> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| storage_error("fsync directory", dir, e))
}

/// Every staged install under `recv_dir`, one per group: the highest index
/// wins, and older staged files of the same group are removed.
pub fn list_staged(recv_dir: &Path) -> Result<Vec<StagedInstall>, ClusterError> {
    if !recv_dir.exists() {
        return Ok(Vec::new());
    }
    let entries =
        std::fs::read_dir(recv_dir).map_err(|e| storage_error("read_dir", recv_dir, e))?;
    let mut found: Vec<StagedInstall> = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|e| storage_error("iterate", recv_dir, e))?
            .path();
        let Some((group_id, last_included_index, last_included_term)) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(parse_staged_name)
        else {
            continue;
        };
        found.push(StagedInstall {
            group_id,
            last_included_index,
            last_included_term,
            path,
        });
    }
    found.sort_by_key(|s| (s.group_id, std::cmp::Reverse(s.last_included_index)));

    let mut newest: Vec<StagedInstall> = Vec::with_capacity(found.len());
    for staged in found {
        if newest.last().is_some_and(|n| n.group_id == staged.group_id) {
            discard(&staged.path)?;
        } else {
            newest.push(staged);
        }
    }
    Ok(newest)
}

/// Rename a CRC-checked `partial` file to its staged name and make the
/// rename durable. Removes any older staged file of the same group first, so
/// one group never has two staged installs.
pub fn stage(
    partial: &Path,
    recv_dir: &Path,
    group_id: u64,
    index: u64,
    term: u64,
) -> Result<StagedInstall, ClusterError> {
    for older in list_staged(recv_dir)?
        .into_iter()
        .filter(|s| s.group_id == group_id)
    {
        discard(&older.path)?;
    }
    let path = staged_path(recv_dir, group_id, index, term);
    std::fs::rename(partial, &path).map_err(|e| storage_error("stage", partial, e))?;
    sync_dir(recv_dir)?;
    Ok(StagedInstall {
        group_id,
        last_included_index: index,
        last_included_term: term,
        path,
    })
}

/// Remove a finished install's staged file and make the removal durable.
pub fn finish(staged: &StagedInstall, recv_dir: &Path) -> Result<(), ClusterError> {
    discard(&staged.path)?;
    sync_dir(recv_dir)
}

/// Remove every `<group>.snap` file under `recv_dir`. Returns how many.
pub fn remove_snap_files(recv_dir: &Path) -> Result<usize, ClusterError> {
    if !recv_dir.exists() {
        return Ok(0);
    }
    let entries =
        std::fs::read_dir(recv_dir).map_err(|e| storage_error("read_dir", recv_dir, e))?;
    let mut removed = 0usize;
    for entry in entries {
        let path = entry
            .map_err(|e| storage_error("iterate", recv_dir, e))?
            .path();
        if path.extension().and_then(|e| e.to_str()) == Some(SNAP_EXTENSION) {
            discard(&path)?;
            removed += 1;
        }
    }
    if removed > 0 {
        sync_dir(recv_dir)?;
    }
    Ok(removed)
}

/// Remove a staged or partial file. A missing file is not an error.
pub fn discard(path: &Path) -> Result<(), ClusterError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(storage_error("remove", path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_name_round_trips() {
        let dir = Path::new("/r");
        let path = staged_path(dir, 7, 42, 3);
        let name = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(parse_staged_name(name), Some((7, 42, 3)));
        assert_eq!(parse_staged_name("7.snap"), None);
        assert_eq!(parse_staged_name("7.partial"), None);
        assert_eq!(parse_staged_name("7.42.staged"), None);
        assert_eq!(parse_staged_name("7.42.3.1.staged"), None);
    }

    #[test]
    fn stage_replaces_an_older_staged_install_of_the_group() {
        let dir = tempfile::tempdir().unwrap();
        let recv = dir.path();
        let first = recv.join("7.partial");
        std::fs::write(&first, b"a").unwrap();
        stage(&first, recv, 7, 10, 1).unwrap();
        let second = recv.join("7.partial");
        std::fs::write(&second, b"b").unwrap();
        let staged = stage(&second, recv, 7, 20, 1).unwrap();

        let listed = list_staged(recv).unwrap();
        assert_eq!(listed, vec![staged.clone()]);
        assert!(!staged_path(recv, 7, 10, 1).exists());

        finish(&staged, recv).unwrap();
        assert!(list_staged(recv).unwrap().is_empty());
    }

    #[test]
    fn list_keeps_the_newest_staged_install_per_group() {
        let dir = tempfile::tempdir().unwrap();
        let recv = dir.path();
        for (group, index) in [(7, 10), (7, 30), (8, 5)] {
            std::fs::write(staged_path(recv, group, index, 1), b"x").unwrap();
        }
        let listed = list_staged(recv).unwrap();
        let keys: Vec<(u64, u64)> = listed
            .iter()
            .map(|s| (s.group_id, s.last_included_index))
            .collect();
        assert_eq!(keys, vec![(7, 30), (8, 5)]);
        assert!(!staged_path(recv, 7, 10, 1).exists());
    }

    #[test]
    fn snap_files_are_removed_and_staged_files_kept() {
        let dir = tempfile::tempdir().unwrap();
        let recv = dir.path();
        std::fs::write(recv.join("7.snap"), b"x").unwrap();
        std::fs::write(recv.join("8.snap"), b"y").unwrap();
        std::fs::write(staged_path(recv, 9, 5, 1), b"z").unwrap();

        assert_eq!(remove_snap_files(recv).unwrap(), 2);
        assert!(!recv.join("7.snap").exists());
        assert_eq!(list_staged(recv).unwrap().len(), 1);
    }
}
