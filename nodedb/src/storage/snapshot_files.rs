// SPDX-License-Identifier: BUSL-1.1

//! File I/O for physical snapshots: read a consistent image of a store, and
//! write it back durably under an empty data directory.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::data::snapshot::{SnapshotComponent, SnapshotFile};

/// Read a redb file while holding a write transaction on its database.
///
/// Every commit here is `Durability::Immediate`, so the file on disk holds
/// the last commit in full. The held write transaction blocks every other
/// writer, so no commit lands while the file is read. The image opens as a
/// crash-consistent copy: redb repairs its allocator state on first open.
///
/// Blocks until other write transactions finish. Never call it from an
/// async task directly.
pub fn read_redb_image(db: &redb::Database, path: &Path) -> crate::Result<Vec<u8>> {
    let txn = db
        .begin_write()
        .map_err(|e| redb_error(path, "begin write", e))?;
    // no-objectstore: the image is read from the engine's local redb file.
    let read = std::fs::read(path).map_err(|e| io_error(path, "read redb image", e));
    txn.abort()
        .map_err(|e| redb_error(path, "abort write", e))?;
    read
}

/// Append every regular file under `data_dir/root` to `out`, in path order.
/// A missing root appends nothing.
pub fn collect_tree(
    data_dir: &Path,
    root: &Path,
    component: SnapshotComponent,
    out: &mut Vec<SnapshotFile>,
) -> crate::Result<()> {
    let abs = data_dir.join(root);
    if !abs.exists() {
        return Ok(());
    }
    let mut pending = vec![root.to_path_buf()];
    while let Some(rel_dir) = pending.pop() {
        let dir = data_dir.join(&rel_dir);
        // no-objectstore: engine files are captured from the local data directory.
        let entries = std::fs::read_dir(&dir).map_err(|e| io_error(&dir, "list", e))?;
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| io_error(&dir, "list entry", e))?;
            names.push(entry.file_name());
        }
        names.sort();
        let mut subdirs = Vec::new();
        for name in names {
            let rel = rel_dir.join(&name);
            let path = data_dir.join(&rel);
            // no-objectstore: engine files are captured from the local data directory.
            let meta = std::fs::symlink_metadata(&path).map_err(|e| io_error(&path, "stat", e))?;
            if meta.is_dir() {
                subdirs.push(rel);
            } else if meta.is_file() {
                // no-objectstore: engine files are captured from the local data directory.
                let bytes = std::fs::read(&path).map_err(|e| io_error(&path, "read", e))?;
                out.push(SnapshotFile {
                    component,
                    path: rel_path_string(&rel)?,
                    bytes,
                });
            } else {
                return Err(crate::Error::Storage {
                    engine: "snapshot".into(),
                    detail: format!(
                        "{} is neither a file nor a directory; engine trees hold only both",
                        path.display()
                    ),
                });
            }
        }
        // Reversed so the stack pops them in name order.
        pending.extend(subdirs.into_iter().rev());
    }
    Ok(())
}

/// Append the regular file at `data_dir/rel` to `out`. A missing file is an
/// error: the caller names only files a manifest says exist.
pub fn collect_file(
    data_dir: &Path,
    rel: &Path,
    component: SnapshotComponent,
    out: &mut Vec<SnapshotFile>,
) -> crate::Result<()> {
    let path = data_dir.join(rel);
    // no-objectstore: engine files are captured from the local data directory.
    let bytes = std::fs::read(&path).map_err(|e| io_error(&path, "read", e))?;
    out.push(SnapshotFile {
        component,
        path: rel_path_string(rel)?,
        bytes,
    });
    Ok(())
}

/// Remove every entry under `dir`, leaving `dir` itself empty.
pub fn clear_dir_contents(dir: &Path) -> crate::Result<()> {
    // no-objectstore: the restore target is a local data directory.
    let entries = std::fs::read_dir(dir).map_err(|e| io_error(dir, "list", e))?;
    for entry in entries {
        let entry = entry.map_err(|e| io_error(dir, "list entry", e))?;
        let path = entry.path();
        // no-objectstore: the restore target is a local data directory.
        let meta = std::fs::symlink_metadata(&path).map_err(|e| io_error(&path, "stat", e))?;
        // no-objectstore: the restore target is a local data directory.
        let removed = if meta.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        removed.map_err(|e| io_error(&path, "remove", e))?;
    }
    fsync_dir(dir)
}

/// A relative path as the `/`-separated string a [`SnapshotFile`] carries.
pub fn rel_path_string(rel: &Path) -> crate::Result<String> {
    let mut parts = Vec::new();
    for component in rel.components() {
        let part = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("path {} is not UTF-8", rel.display()),
            })?;
        parts.push(part);
    }
    Ok(parts.join("/"))
}

/// Create `dir` if it is absent. Refuse it when it holds any entry.
pub fn require_empty_dir(dir: &Path) -> crate::Result<()> {
    if !dir.exists() {
        // no-objectstore: the restore target is a local data directory.
        std::fs::create_dir_all(dir).map_err(|e| io_error(dir, "create", e))?;
        if let Some(parent) = dir.parent() {
            fsync_dir(parent)?;
        }
        return Ok(());
    }
    // no-objectstore: the restore target is a local data directory.
    let mut entries = std::fs::read_dir(dir).map_err(|e| io_error(dir, "list", e))?;
    if entries.next().is_some() {
        return Err(crate::Error::RestoreTargetNotEmpty {
            path: dir.to_path_buf(),
        });
    }
    Ok(())
}

/// Writes files under one data directory, then makes every directory it
/// created durable.
pub struct RestoreWriter<'a> {
    data_dir: &'a Path,
    dirs: BTreeSet<PathBuf>,
    files: u64,
    bytes: u64,
}

impl<'a> RestoreWriter<'a> {
    pub fn new(data_dir: &'a Path) -> Self {
        Self {
            data_dir,
            dirs: BTreeSet::new(),
            files: 0,
            bytes: 0,
        }
    }

    /// Write `bytes` to `data_dir/rel` and fsync the file and its directory.
    pub fn write(&mut self, rel: &Path, bytes: &[u8]) -> crate::Result<()> {
        let path = self.data_dir.join(rel);
        let (Some(parent), Some(name)) = (path.parent(), rel.file_name().and_then(|n| n.to_str()))
        else {
            return Err(crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("restore path {} names no file", rel.display()),
            });
        };
        // no-objectstore: restored engine files land in the local data directory.
        std::fs::create_dir_all(parent).map_err(|e| io_error(parent, "create", e))?;
        nodedb_wal::segment::atomic_write_fsync(parent, name, bytes).map_err(|e| {
            crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("write {}: {e}", path.display()),
            }
        })?;
        self.note_dirs(rel.parent());
        self.files += 1;
        self.bytes += bytes.len() as u64;
        Ok(())
    }

    /// Create the directory `data_dir/rel`, which can stay empty.
    /// [`Self::finish`] fsyncs it and every ancestor it created.
    pub fn create_dir(&mut self, rel: &Path) -> crate::Result<()> {
        let path = self.data_dir.join(rel);
        // no-objectstore: restored engine directories land in the local data directory.
        std::fs::create_dir_all(&path).map_err(|e| io_error(&path, "create", e))?;
        self.note_dirs(Some(rel));
        Ok(())
    }

    /// Record `dir` and each of its ancestors for the final fsync.
    fn note_dirs(&mut self, dir: Option<&Path>) {
        let mut ancestor = dir;
        while let Some(dir) = ancestor {
            if !self.dirs.insert(dir.to_path_buf()) {
                break;
            }
            ancestor = dir.parent();
        }
    }

    /// Fsync every directory a write created, so each new entry survives a
    /// crash. Returns the file and byte counts written.
    pub fn finish(self) -> crate::Result<(u64, u64)> {
        for dir in &self.dirs {
            fsync_dir(&self.data_dir.join(dir))?;
        }
        Ok((self.files, self.bytes))
    }
}

fn fsync_dir(dir: &Path) -> crate::Result<()> {
    nodedb_wal::segment::fsync_directory(dir).map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("fsync {}: {e}", dir.display()),
    })
}

fn io_error(path: &Path, op: &str, e: std::io::Error) -> crate::Error {
    crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("{op} {}: {e}", path.display()),
    }
}

fn redb_error(path: &Path, op: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("{op} on {}: {e}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tree_is_collected_in_path_order_with_relative_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = Path::new("kv-ckpt/core-0");
        std::fs::create_dir_all(dir.path().join(root).join("gen-1")).unwrap();
        std::fs::write(dir.path().join(root).join("MANIFEST"), b"m").unwrap();
        std::fs::write(dir.path().join(root).join("gen-1/a.ckpt"), b"a").unwrap();

        let mut out = Vec::new();
        collect_tree(dir.path(), root, SnapshotComponent::Kv, &mut out).unwrap();
        let paths: Vec<_> = out.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(
            paths,
            ["kv-ckpt/core-0/MANIFEST", "kv-ckpt/core-0/gen-1/a.ckpt"]
        );

        let mut none = Vec::new();
        collect_tree(
            dir.path(),
            Path::new("absent"),
            SnapshotComponent::Kv,
            &mut none,
        )
        .unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn clearing_leaves_the_directory_empty() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("a/b")).unwrap();
        std::fs::write(dir.path().join("a/b/f"), b"x").unwrap();
        std::fs::write(dir.path().join("g"), b"y").unwrap();
        clear_dir_contents(dir.path()).unwrap();
        require_empty_dir(dir.path()).unwrap();
    }

    #[test]
    fn a_non_empty_target_is_refused_by_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("stale"), b"x").unwrap();
        match require_empty_dir(dir.path()) {
            Err(crate::Error::RestoreTargetNotEmpty { path }) => assert_eq!(path, dir.path()),
            other => panic!("expected RestoreTargetNotEmpty, got {other:?}"),
        }
    }

    #[test]
    fn an_absent_target_is_created_and_an_empty_one_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("fresh");
        require_empty_dir(&target).unwrap();
        assert!(target.is_dir());
        require_empty_dir(&target).unwrap();
    }

    #[test]
    fn a_redb_image_opens_with_its_committed_rows() {
        const T: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("t");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.redb");
        let db = redb::Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(T).unwrap().insert("k", 7).unwrap();
        txn.commit().unwrap();

        let image = read_redb_image(&db, &path).unwrap();
        let copy = dir.path().join("b.redb");
        std::fs::write(&copy, image).unwrap();
        drop(db);

        use redb::{ReadableDatabase, ReadableTable};
        let reopened = redb::Database::create(&copy).unwrap();
        let read = reopened.begin_read().unwrap();
        let table = read.open_table(T).unwrap();
        assert_eq!(table.get("k").unwrap().map(|v| v.value()), Some(7));
    }
}
