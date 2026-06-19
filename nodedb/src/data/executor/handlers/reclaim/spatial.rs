// SPDX-License-Identifier: BUSL-1.1

//! Spatial engine reclaim — unlink per-collection R*-tree checkpoint
//! + docmap files.
//!
//! Checkpoint layout (see `spatial_checkpoint.rs`):
//! `{data_dir}/spatial-ckpt/{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt` + `.docmap`.
//! The filename prefix is built by the SAME encoder the write path uses
//! ([`spatial_checkpoint_prefix`]) so the `starts_with` match can never drift
//! from the on-disk names.

use std::path::Path;

use tracing::{debug, warn};

use super::ReclaimStats;
use crate::data::executor::spatial_checkpoint::spatial_checkpoint_prefix;

/// Unlink every spatial checkpoint + docmap file for
/// `(database_id, tenant_id, collection)`. Returns stats; idempotent.
pub fn reclaim_spatial_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> ReclaimStats {
    let ckpt_dir = data_dir.join("spatial-ckpt");
    if !ckpt_dir.exists() {
        return ReclaimStats::default();
    }

    // Build the prefix via the shared encoder so it always matches the
    // filenames produced by `checkpoint_spatial_indexes`.
    let prefix = spatial_checkpoint_prefix(database_id, tenant_id, collection);

    let mut stats = ReclaimStats::default();
    let entries = match std::fs::read_dir(&ckpt_dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(
                dir = %ckpt_dir.display(),
                error = %e,
                "spatial reclaim: failed to read ckpt dir"
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
        // Only sweep `.ckpt`, `.ckpt.tmp`, `.docmap`, `.docmap.tmp`.
        let is_ours = name.ends_with(".ckpt")
            || name.ends_with(".ckpt.tmp")
            || name.ends_with(".docmap")
            || name.ends_with(".docmap.tmp");
        if !is_ours {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                stats.files_unlinked = stats.files_unlinked.saturating_add(1);
                stats.bytes_freed = stats.bytes_freed.saturating_add(size);
                debug!(path = %path.display(), size, "spatial reclaim: unlinked");
            }
            Err(e) => warn!(
                path = %path.display(),
                error = %e,
                "spatial reclaim: unlink failed"
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
    fn unlinks_ckpt_and_docmap_for_matching_field_indexes() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        let ckpt = base.join("spatial-ckpt");
        // New-format names: {db}_{tid}_{coll}_{field}.
        write(&ckpt.join("0_1_places_geom.ckpt"), b"x");
        write(&ckpt.join("0_1_places_geom.docmap"), b"yy");
        write(&ckpt.join("0_1_places_home.ckpt"), b"zzz");
        // Keep: different collection.
        write(&ckpt.join("0_1_stores_geom.ckpt"), b"keep");
        // Keep: different tenant.
        write(&ckpt.join("0_2_places_geom.ckpt"), b"keep2");
        // Keep: different database.
        write(&ckpt.join("1_1_places_geom.ckpt"), b"keep3");

        let stats = reclaim_spatial_checkpoints(base, 0, 1, "places");
        assert_eq!(stats.files_unlinked, 3);
        assert_eq!(stats.bytes_freed, 1 + 2 + 3);
        assert!(ckpt.join("0_1_stores_geom.ckpt").exists());
        assert!(ckpt.join("0_2_places_geom.ckpt").exists());
        assert!(ckpt.join("1_1_places_geom.ckpt").exists());
    }

    #[test]
    fn empty_dir_is_noop() {
        let tmp = TempDir::new().unwrap();
        let s = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "x");
        assert_eq!(s.files_unlinked, 0);
    }
}
