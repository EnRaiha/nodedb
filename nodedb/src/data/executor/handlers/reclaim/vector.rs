// SPDX-License-Identifier: BUSL-1.1

//! Vector engine reclaim — unlink per-collection HNSW checkpoint files.
//!
//! Checkpoint layout (see `vector_checkpoint.rs::checkpoint_vector_indexes`):
//! `{data_dir}/vector-ckpt/{db}:{tid}:{coll}.ckpt` for the bare collection,
//! plus one `{db}:{tid}:{coll}:{field}.ckpt` per named-field index. We
//! unlink any file whose stem is `{db}:{tid}:{coll}` or begins with
//! `{db}:{tid}:{coll}:`. The prefix mirrors
//! `vector_checkpoint_filename` so the two never drift.

use std::path::Path;

use tracing::debug;

use super::{ReclaimError, ReclaimStats, Result};

/// Unlink every vector checkpoint file for `(database_id, tenant_id, collection)`.
/// Returns stats; idempotent (missing files count as 0).
pub fn reclaim_vector_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> Result<ReclaimStats> {
    let ckpt_dir = data_dir.join("vector-ckpt");
    let entries = match std::fs::read_dir(&ckpt_dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReclaimStats::default());
        }
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read vector checkpoint directory",
                path: ckpt_dir,
                source,
            });
        }
    };
    let prefix_exact = format!("{database_id}:{tenant_id}:{collection}");
    let prefix_field = format!("{database_id}:{tenant_id}:{collection}:");

    let mut stats = ReclaimStats::default();
    for entry in entries {
        let entry = entry.map_err(|source| ReclaimError::Io {
            operation: "read vector checkpoint entry",
            path: ckpt_dir.clone(),
            source,
        })?;
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        // Match bare `"{tid}:{coll}"` or `"{tid}:{coll}:{field}"`.
        if stem != prefix_exact && !stem.starts_with(&prefix_field) {
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
    Ok(stats)
}

/// Unlink the checkpoint of exactly one vector index — the file
/// `{db}:{tid}:{coll}.ckpt` for the default field, or
/// `{db}:{tid}:{coll}:{field}.ckpt` for a named one — leaving every other
/// index of the same collection in place. Idempotent.
pub fn reclaim_vector_index_checkpoint(
    data_dir: &Path,
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
    let ckpt_dir = data_dir.join("vector-ckpt");
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
        let ckpt = base.join("vector-ckpt");
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

    #[test]
    fn unlink_failure_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().unwrap();
        let invalid_target = tmp.path().join("vector-ckpt/0:1:users.ckpt");
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
        let ckpt = tmp.path().join("vector-ckpt");
        write(&ckpt.join("0:1:docs.ckpt"), b"default");
        write(&ckpt.join("0:1:docs:text_emb.ckpt"), b"text");
        write(&ckpt.join("0:1:docs:image_emb.ckpt"), b"image");

        let stats = reclaim_vector_index_checkpoint(tmp.path(), 0, 1, "docs", "text_emb").unwrap();
        assert_eq!(stats.files_unlinked, 1);
        assert!(!ckpt.join("0:1:docs:text_emb.ckpt").exists());
        assert!(ckpt.join("0:1:docs:image_emb.ckpt").exists());
        assert!(ckpt.join("0:1:docs.ckpt").exists());

        // The default (unnamed) field targets the bare stem only.
        reclaim_vector_index_checkpoint(tmp.path(), 0, 1, "docs", "").unwrap();
        assert!(!ckpt.join("0:1:docs.ckpt").exists());
        assert!(ckpt.join("0:1:docs:image_emb.ckpt").exists());
    }

    #[test]
    fn index_scoped_reclaim_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let stats = reclaim_vector_index_checkpoint(tmp.path(), 0, 1, "docs", "emb").unwrap();
        assert_eq!(stats.files_unlinked, 0);
    }

    #[test]
    fn no_false_prefix_match() {
        let tmp = TempDir::new().unwrap();
        let ckpt = tmp.path().join("vector-ckpt");
        // Prefix overlap but distinct collection name.
        write(&ckpt.join("0:1:users_archive.ckpt"), b"keep");
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "users").unwrap();
        assert_eq!(stats.files_unlinked, 0);
        assert!(ckpt.join("0:1:users_archive.ckpt").exists());
    }
}
