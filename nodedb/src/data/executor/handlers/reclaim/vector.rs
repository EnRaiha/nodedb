// SPDX-License-Identifier: BUSL-1.1

//! Vector engine reclaim — unlink per-collection HNSW checkpoint files.
//!
//! Checkpoint layout (see `vector_checkpoint.rs::checkpoint_vector_indexes`):
//! `{data_dir}/vector-ckpt/core-{core_id}/{db}:{tid}:{coll}.ckpt` for the bare
//! collection, plus one `core-{core_id}/{db}:{tid}:{coll}:{field}.ckpt` per
//! named-field index, written into WHICHEVER core's subdirectory happened to
//! hold that collection's vectors. Reclaim of a whole collection therefore
//! walks EVERY core's subdirectory — the caller does not know which core(s)
//! ever held the collection — and unlinks any file whose stem is
//! `{db}:{tid}:{coll}` or begins with `{db}:{tid}:{coll}:`. The prefix
//! mirrors `vector_checkpoint_filename` so the two never drift. Reusing
//! `vector_ckpt_dir` from the write path is what keeps the per-core layout
//! from drifting between writer and reclaimer the way it did before.

use std::path::Path;

use tracing::debug;

use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::vector_checkpoint::vector_ckpt_dir;

/// Unlink every vector checkpoint file for `(database_id, tenant_id, collection)`
/// across every core's checkpoint subdirectory. Returns stats; idempotent
/// (missing files count as 0).
pub fn reclaim_vector_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> Result<ReclaimStats> {
    let root = data_dir.join("vector-ckpt");
    let cores = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReclaimStats::default());
        }
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read vector checkpoint root",
                path: root,
                source,
            });
        }
    };

    let prefix_exact = format!("{database_id}:{tenant_id}:{collection}");
    let prefix_field = format!("{database_id}:{tenant_id}:{collection}:");

    let mut stats = ReclaimStats::default();
    for core_entry in cores {
        let core_entry = core_entry.map_err(|source| ReclaimError::Io {
            operation: "read vector checkpoint core entry",
            path: root.clone(),
            source,
        })?;
        let core_dir = core_entry.path();
        if !core_dir.is_dir() {
            continue;
        }
        reclaim_core_dir(&core_dir, &prefix_exact, &prefix_field, &mut stats)?;
    }
    Ok(stats)
}

/// Unlink every matching file directly inside one core's checkpoint
/// subdirectory. A missing subdirectory (e.g. a `core-N` entry that turned
/// out not to be a real checkpoint dir) is a no-op, not an error.
fn reclaim_core_dir(
    core_dir: &Path,
    prefix_exact: &str,
    prefix_field: &str,
    stats: &mut ReclaimStats,
) -> Result<()> {
    let entries = match std::fs::read_dir(core_dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read vector checkpoint core directory",
                path: core_dir.to_path_buf(),
                source,
            });
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| ReclaimError::Io {
            operation: "read vector checkpoint entry",
            path: core_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        // Match bare `"{db}:{tid}:{coll}"` or `"{db}:{tid}:{coll}:{field}"`.
        // The trailing `:` on `prefix_field` is what stops a collection whose
        // name is a prefix of another's (e.g. "docs" vs "docs_archive") from
        // matching: "docs_archive" never equals "0:1:docs" and never starts
        // with "0:1:docs:".
        if stem != prefix_exact && !stem.starts_with(prefix_field) {
            continue;
        }
        // Only unlink `.ckpt` and `.ckpt.tmp` files (skip unrelated
        // artifacts that happen to share the stem).
        let ext_ok = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e == "ckpt" || e == "tmp")
            .unwrap_or(false);
        if !ext_ok {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                stats.files_unlinked = stats.files_unlinked.saturating_add(1);
                stats.bytes_freed = stats.bytes_freed.saturating_add(size);
                debug!(path = %path.display(), size, "vector reclaim: unlinked ckpt");
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ReclaimError::Io {
                    operation: "unlink vector checkpoint",
                    path,
                    source,
                });
            }
        }
    }
    Ok(())
}

/// Unlink the checkpoint of exactly one vector index on one core — the file
/// `{db}:{tid}:{coll}.ckpt` for the default field, or
/// `{db}:{tid}:{coll}:{field}.ckpt` for a named one — leaving every other
/// index of the same collection in place. Idempotent.
///
/// Unlike [`reclaim_vector_checkpoints`], this does not fan out across every
/// core's subdirectory: `VectorOp::DropIndex` runs independently on each core
/// that owns a copy of the collection's in-memory state, and each such core
/// unlinks only the file it itself could have written — `vector_ckpt_dir`
/// keyed by that core's own `core_id`. That keeps concurrent cores dropping
/// the same index from touching each other's subdirectories.
pub fn reclaim_vector_index_checkpoint(
    data_dir: &Path,
    core_id: usize,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    field_name: &str,
) -> Result<ReclaimStats> {
    let stem = if field_name.is_empty() {
        format!("{database_id}:{tenant_id}:{collection}")
    } else {
        format!("{database_id}:{tenant_id}:{collection}:{field_name}")
    };
    let ckpt_dir = vector_ckpt_dir(data_dir, core_id);
    let mut stats = ReclaimStats::default();
    for extension in ["ckpt", "ckpt.tmp"] {
        let path = ckpt_dir.join(format!("{stem}.{extension}"));
        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                stats.files_unlinked = stats.files_unlinked.saturating_add(1);
                stats.bytes_freed = stats.bytes_freed.saturating_add(size);
                debug!(path = %path.display(), size, "vector reclaim: unlinked index ckpt");
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ReclaimError::Io {
                    operation: "unlink vector index checkpoint",
                    path,
                    source,
                });
            }
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn unlinks_bare_and_named_field_ckpts() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        let ckpt = vector_ckpt_dir(base, 0);
        write(&ckpt.join("0:1:users.ckpt"), b"x");
        write(&ckpt.join("0:1:users:email.ckpt"), b"xy");
        write(&ckpt.join("0:1:users:name.ckpt.tmp"), b"xyz");
        // Other collection: must not touch.
        write(&ckpt.join("0:1:orders.ckpt"), b"keep");
        // Different tenant: must not touch.
        write(&ckpt.join("0:2:users.ckpt"), b"keep2");
        // Different database: must not touch.
        write(&ckpt.join("1:1:users.ckpt"), b"keepdb");

        let stats = reclaim_vector_checkpoints(base, 0, 1, "users").unwrap();
        assert_eq!(stats.files_unlinked, 3);
        assert_eq!(stats.bytes_freed, 1 + 2 + 3);
        assert!(ckpt.join("0:1:orders.ckpt").exists());
        assert!(ckpt.join("0:2:users.ckpt").exists());
        assert!(ckpt.join("1:1:users.ckpt").exists());
        assert!(!ckpt.join("0:1:users.ckpt").exists());
    }

    /// The dropped collection's vectors could have been checkpointed by ANY
    /// core sharing `data_dir` — reclaim must reach every `core-N`
    /// subdirectory, not just one, or a file survives the DROP indefinitely.
    #[test]
    fn unlinks_across_every_core_subdirectory() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        write(&vector_ckpt_dir(base, 0).join("0:1:docs.ckpt"), b"a");
        write(&vector_ckpt_dir(base, 1).join("0:1:docs:emb.ckpt"), b"bb");
        // Different core, different collection: must survive.
        write(&vector_ckpt_dir(base, 1).join("0:1:posts.ckpt"), b"keep");

        let stats = reclaim_vector_checkpoints(base, 0, 1, "docs").unwrap();
        assert_eq!(stats.files_unlinked, 2, "both cores' files must be reached");
        assert_eq!(stats.bytes_freed, 1 + 2);
        assert!(!vector_ckpt_dir(base, 0).join("0:1:docs.ckpt").exists());
        assert!(!vector_ckpt_dir(base, 1).join("0:1:docs:emb.ckpt").exists());
        assert!(vector_ckpt_dir(base, 1).join("0:1:posts.ckpt").exists());
    }

    #[test]
    fn unlink_failure_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().unwrap();
        let invalid_target = vector_ckpt_dir(tmp.path(), 0).join("0:1:users.ckpt");
        std::fs::create_dir_all(&invalid_target).unwrap();

        let error = reclaim_vector_checkpoints(tmp.path(), 0, 1, "users").unwrap_err();
        assert!(error.to_string().contains("unlink vector checkpoint"));
        assert!(invalid_target.exists());
    }

    #[test]
    fn empty_dir_is_noop() {
        let tmp = TempDir::new().unwrap();
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "x").unwrap();
        assert_eq!(stats.files_unlinked, 0);
    }

    #[test]
    fn index_scoped_reclaim_spares_sibling_indexes() {
        let tmp = TempDir::new().unwrap();
        let ckpt = vector_ckpt_dir(tmp.path(), 0);
        write(&ckpt.join("0:1:docs.ckpt"), b"default");
        write(&ckpt.join("0:1:docs:text_emb.ckpt"), b"text");
        write(&ckpt.join("0:1:docs:image_emb.ckpt"), b"image");

        let stats =
            reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "text_emb").unwrap();
        assert_eq!(stats.files_unlinked, 1);
        assert!(!ckpt.join("0:1:docs:text_emb.ckpt").exists());
        assert!(ckpt.join("0:1:docs:image_emb.ckpt").exists());
        assert!(ckpt.join("0:1:docs.ckpt").exists());

        // The default (unnamed) field targets the bare stem only.
        reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "").unwrap();
        assert!(!ckpt.join("0:1:docs.ckpt").exists());
        assert!(ckpt.join("0:1:docs:image_emb.ckpt").exists());
    }

    #[test]
    fn index_scoped_reclaim_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let stats = reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "emb").unwrap();
        assert_eq!(stats.files_unlinked, 0);
    }

    /// `reclaim_vector_index_checkpoint` only touches the core it is told
    /// about — a file checkpointed by a different core must survive, proving
    /// the single-index drop path does not race a sibling core's own state.
    #[test]
    fn index_scoped_reclaim_does_not_touch_other_cores() {
        let tmp = TempDir::new().unwrap();
        write(&vector_ckpt_dir(tmp.path(), 1).join("0:1:docs.ckpt"), b"x");

        let stats = reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "").unwrap();
        assert_eq!(stats.files_unlinked, 0);
        assert!(
            vector_ckpt_dir(tmp.path(), 1)
                .join("0:1:docs.ckpt")
                .exists()
        );
    }

    #[test]
    fn no_false_prefix_match() {
        let tmp = TempDir::new().unwrap();
        let ckpt = vector_ckpt_dir(tmp.path(), 0);
        // Prefix overlap but distinct collection name.
        write(&ckpt.join("0:1:users_archive.ckpt"), b"keep");
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "users").unwrap();
        assert_eq!(stats.files_unlinked, 0);
        assert!(ckpt.join("0:1:users_archive.ckpt").exists());
    }
}
