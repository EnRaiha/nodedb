// SPDX-License-Identifier: BUSL-1.1

//! Spatial engine reclaim — unlink per-collection R*-tree checkpoint
//! + docmap files.
//!
//! Checkpoint layout (see `spatial_checkpoint.rs`) is
//! `{data_dir}/spatial-ckpt/core-{core_id}/{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt`
//! plus a paired `.docmap`, written into WHICHEVER core's subdirectory happened
//! to hold that collection's R-tree. Reclaim of a whole collection therefore
//! enumerates every `core-*` subdirectory that exists rather than assuming a
//! core count — the caller does not know which core(s) ever held the collection.
//! The filename prefix is built by the SAME encoder the write path uses
//! ([`spatial_checkpoint_prefix`]), so the match can never drift from the
//! on-disk names.

use std::path::Path;

use tracing::debug;

use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::spatial_checkpoint::spatial_checkpoint_prefix;

/// Unlink every spatial checkpoint + docmap file for
/// `(database_id, tenant_id, collection)`, across every core's checkpoint
/// subdirectory. Returns stats; idempotent.
pub fn reclaim_spatial_checkpoints(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> Result<ReclaimStats> {
    let root = data_dir.join("spatial-ckpt");
    let cores = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReclaimStats::default());
        }
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read spatial checkpoint root",
                path: root,
                source,
            });
        }
    };

    // Build the prefix via the shared encoder so it always matches the
    // filenames produced by `checkpoint_spatial_indexes`.
    let prefix = spatial_checkpoint_prefix(database_id, tenant_id, collection);

    let mut stats = ReclaimStats::default();
    for core_entry in cores {
        let core_entry = core_entry.map_err(|source| ReclaimError::Io {
            operation: "read spatial checkpoint core entry",
            path: root.clone(),
            source,
        })?;
        let core_dir = core_entry.path();
        if !core_dir.is_dir() {
            continue;
        }
        reclaim_core_dir(&core_dir, &prefix, &mut stats)?;
    }
    Ok(stats)
}

/// Unlink every matching file directly inside one core's checkpoint
/// subdirectory. A missing subdirectory is a no-op, not an error.
fn reclaim_core_dir(core_dir: &Path, prefix: &str, stats: &mut ReclaimStats) -> Result<()> {
    let entries = match std::fs::read_dir(core_dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read spatial checkpoint core directory",
                path: core_dir.to_path_buf(),
                source,
            });
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| ReclaimError::Io {
            operation: "read spatial checkpoint entry",
            path: core_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.starts_with(prefix) {
            continue;
        }
        // Only sweep `.ckpt`, `.ckpt.tmp`, `.docmap`, `.docmap.tmp` — both the
        // R-tree checkpoint AND its paired docmap must go, or the docmap is
        // left orphaned on disk with no checkpoint to resolve entries for.
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
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ReclaimError::Io {
                    operation: "unlink spatial checkpoint",
                    path,
                    source,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::spatial_checkpoint::spatial_ckpt_dir;
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
        let ckpt = spatial_ckpt_dir(base, 0);
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

        let stats = reclaim_spatial_checkpoints(base, 0, 1, "places").unwrap();
        assert_eq!(stats.files_unlinked, 3);
        assert_eq!(stats.bytes_freed, 1 + 2 + 3);
        assert!(ckpt.join("0_1_stores_geom.ckpt").exists());
        assert!(ckpt.join("0_2_places_geom.ckpt").exists());
        assert!(ckpt.join("1_1_places_geom.ckpt").exists());
    }

    /// The dropped collection's R-tree could have been checkpointed by ANY
    /// core sharing `data_dir` — reclaim must reach every `core-N`
    /// subdirectory, and it must take the `.docmap` companion along with the
    /// `.ckpt`, or the docmap is left orphaned.
    #[test]
    fn unlinks_across_every_core_subdirectory() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        write(
            &spatial_ckpt_dir(base, 0).join("0_1_places_geom.ckpt"),
            b"a",
        );
        write(
            &spatial_ckpt_dir(base, 0).join("0_1_places_geom.docmap"),
            b"bb",
        );
        write(
            &spatial_ckpt_dir(base, 1).join("0_1_places_home.ckpt"),
            b"ccc",
        );
        // Different core, different collection: must survive.
        write(
            &spatial_ckpt_dir(base, 1).join("0_1_stores_geom.ckpt"),
            b"keep",
        );

        let stats = reclaim_spatial_checkpoints(base, 0, 1, "places").unwrap();
        assert_eq!(stats.files_unlinked, 3, "both cores' files must be reached");
        assert_eq!(stats.bytes_freed, 1 + 2 + 3);
        assert!(
            !spatial_ckpt_dir(base, 0)
                .join("0_1_places_geom.ckpt")
                .exists()
        );
        assert!(
            !spatial_ckpt_dir(base, 0)
                .join("0_1_places_geom.docmap")
                .exists()
        );
        assert!(
            !spatial_ckpt_dir(base, 1)
                .join("0_1_places_home.ckpt")
                .exists()
        );
        assert!(
            spatial_ckpt_dir(base, 1)
                .join("0_1_stores_geom.ckpt")
                .exists()
        );
    }

    #[test]
    fn empty_dir_is_noop() {
        let tmp = TempDir::new().unwrap();
        let s = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "x").unwrap();
        assert_eq!(s.files_unlinked, 0);
    }
}
