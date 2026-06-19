// SPDX-License-Identifier: BUSL-1.1

//! Sparse-vector engine reclaim — unlink per-collection checkpoint files.
//!
//! Checkpoint layout (see `sparse_vector_checkpoint.rs`):
//! `{data_dir}/sparse-vector-ckpt/{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt`.
//! The filename prefix is built by the SAME encoder the write path uses
//! ([`sparse_vector_checkpoint_prefix`]) so the `starts_with` match can never
//! drift from the on-disk names.

use std::path::Path;

use tracing::{debug, warn};

use super::ReclaimStats;
use crate::data::executor::sparse_vector_checkpoint::sparse_vector_checkpoint_prefix;

/// Unlink every sparse-vector checkpoint file for
/// `(database_id, tenant_id, collection)`. Returns stats; idempotent.
pub fn reclaim_sparse_vector_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> ReclaimStats {
    let ckpt_dir = data_dir.join("sparse-vector-ckpt");
    if !ckpt_dir.exists() {
        return ReclaimStats::default();
    }

    // Build the prefix via the shared encoder so it always matches the
    // filenames produced by `checkpoint_sparse_vector_indexes`.
    let prefix = sparse_vector_checkpoint_prefix(database_id, tenant_id, collection);

    let mut stats = ReclaimStats::default();
    let entries = match std::fs::read_dir(&ckpt_dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(
                dir = %ckpt_dir.display(),
                error = %e,
                "sparse-vector reclaim: failed to read ckpt dir"
            );
            return stats;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.starts_with(&prefix) {
            continue;
        }
        let is_ours = name.ends_with(".ckpt") || name.ends_with(".ckpt.tmp");
        if !is_ours {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                stats.files_unlinked = stats.files_unlinked.saturating_add(1);
                stats.bytes_freed = stats.bytes_freed.saturating_add(size);
                debug!(path = %path.display(), size, "sparse-vector reclaim: unlinked");
            }
            Err(e) => warn!(
                path = %path.display(),
                error = %e,
                "sparse-vector reclaim: unlink failed"
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
    fn matches_tenant_collection_prefix() {
        let tmp = TempDir::new().unwrap();
        let ckpt = tmp.path().join("sparse-vector-ckpt");
        // New-format names: {db}_{tid}_{enc(coll)}_{enc(field)}.
        write(&ckpt.join("0_1_docs_title.ckpt"), b"x");
        write(&ckpt.join("0_1_docs_body.ckpt"), b"yy");
        // Keep: different collection.
        write(&ckpt.join("0_1_posts_title.ckpt"), b"keep");
        // Keep: different tenant.
        write(&ckpt.join("0_2_docs_title.ckpt"), b"keep");
        // Keep: different database.
        write(&ckpt.join("1_1_docs_title.ckpt"), b"keep3");

        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 0, 1, "docs");
        assert_eq!(stats.files_unlinked, 2);
        assert!(ckpt.join("0_1_posts_title.ckpt").exists());
        assert!(ckpt.join("0_2_docs_title.ckpt").exists());
        assert!(ckpt.join("1_1_docs_title.ckpt").exists());
    }
}
