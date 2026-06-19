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

use tracing::{debug, warn};

use super::ReclaimStats;

/// Unlink every vector checkpoint file for `(database_id, tenant_id, collection)`.
/// Returns stats; idempotent (missing files count as 0).
pub fn reclaim_vector_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> ReclaimStats {
    let ckpt_dir = data_dir.join("vector-ckpt");
    if !ckpt_dir.exists() {
        return ReclaimStats::default();
    }
    let prefix_exact = format!("{database_id}:{tenant_id}:{collection}");
    let prefix_field = format!("{database_id}:{tenant_id}:{collection}:");

    let mut stats = ReclaimStats::default();
    let entries = match std::fs::read_dir(&ckpt_dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(
                dir = %ckpt_dir.display(),
                error = %e,
                "vector reclaim: failed to read ckpt dir"
            );
            return stats;
        }
    };
    for entry in entries.flatten() {
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
            Err(e) => warn!(
                path = %path.display(),
                error = %e,
                "vector reclaim: unlink failed"
            ),
        }
    }
    stats
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

        let stats = reclaim_vector_checkpoints(base, 0, 1, "users");
        assert_eq!(stats.files_unlinked, 3);
        assert_eq!(stats.bytes_freed, 1 + 2 + 3);
        assert!(ckpt.join("0:1:orders.ckpt").exists());
        assert!(ckpt.join("0:2:users.ckpt").exists());
        assert!(ckpt.join("1:1:users.ckpt").exists());
        assert!(!ckpt.join("0:1:users.ckpt").exists());
    }

    #[test]
    fn empty_dir_is_noop() {
        let tmp = TempDir::new().unwrap();
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "x");
        assert_eq!(stats.files_unlinked, 0);
    }

    #[test]
    fn no_false_prefix_match() {
        let tmp = TempDir::new().unwrap();
        let ckpt = tmp.path().join("vector-ckpt");
        // Prefix overlap but distinct collection name.
        write(&ckpt.join("0:1:users_archive.ckpt"), b"keep");
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "users");
        assert_eq!(stats.files_unlinked, 0);
        assert!(ckpt.join("0:1:users_archive.ckpt").exists());
    }
}
