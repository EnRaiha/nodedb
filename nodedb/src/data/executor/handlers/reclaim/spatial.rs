// SPDX-License-Identifier: BUSL-1.1

//! Spatial engine reclaim — unlink a dropped collection's R*-tree checkpoint
//! and docmap files.
//!
//! Checkpoint layout:
//! `{data_dir}/spatial-ckpt/core-{n}/gen-{m}/{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt`
//! plus its paired `.docmap`, with each core's `MANIFEST` naming its live
//! generation. Reclaim walks EVERY core's directory, because `data_dir` is
//! shared and the dropped collection may have been routed to any of them, and
//! resolves each core's live generation through the same manifest reader the
//! load path uses so the two can never disagree about which files are
//! reachable. The filename prefix is built by the SAME encoder the write path
//! uses ([`spatial_checkpoint_prefix`]), so the match can never drift from the
//! on-disk names.
//!
//! Unlinking a file out of a published generation does not break the manifest's
//! promise. That promise is about the collections that are LIVE, and this runs
//! only once a collection is dropped: leaving the files would resurrect a
//! dropped collection's geometry on the next boot.

use std::path::Path;

use tracing::debug;

use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::spatial_checkpoint::{
    read_spatial_manifest_at, spatial_checkpoint_prefix, spatial_ckpt_gen_dir,
};

/// Unlink every spatial checkpoint + docmap file for
/// `(database_id, tenant_id, collection)`, across every core's live
/// generation. Returns stats; idempotent.
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
        // No manifest means no reachable generation for this core, so there is
        // nothing a boot could restore and nothing to reclaim. A corrupt
        // manifest is fail-closed: skipping it could release a same-name CREATE
        // while predecessor files remain reachable.
        let gen_dir = match read_spatial_manifest_at(&core_dir) {
            Ok(Some(manifest)) => spatial_ckpt_gen_dir(&core_dir, manifest.generation),
            Ok(None) => continue,
            Err(error) => {
                return Err(ReclaimError::Manifest {
                    engine: "spatial",
                    path: core_dir.join("MANIFEST"),
                    detail: error.to_string(),
                });
            }
        };
        reclaim_generation(&gen_dir, &prefix, &mut stats)?;
    }
    Ok(stats)
}

/// Unlink every matching file in one core's live generation. A missing
/// generation directory is a no-op, not an error.
fn reclaim_generation(gen_dir: &Path, prefix: &str, stats: &mut ReclaimStats) -> Result<()> {
    let entries = match std::fs::read_dir(gen_dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read spatial checkpoint live generation",
                path: gen_dir.to_path_buf(),
                source,
            });
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| ReclaimError::Io {
            operation: "read spatial checkpoint entry",
            path: gen_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.starts_with(prefix) {
            continue;
        }
        // Both the R-tree checkpoint AND its paired docmap must go, or the
        // docmap is left orphaned with no checkpoint to resolve entries for.
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
    use crate::data::executor::spatial_checkpoint::{spatial_ckpt_dir, test_manifest_bytes};
    use tempfile::TempDir;

    /// Publish a live generation under `core_id` holding `files`, through the
    /// same manifest the load path reads.
    fn publish(data_dir: &Path, core_id: usize, generation: u64, files: &[(&str, &[u8])]) {
        let ckpt_dir = spatial_ckpt_dir(data_dir, core_id);
        let gen_dir = spatial_ckpt_gen_dir(&ckpt_dir, generation);
        std::fs::create_dir_all(&gen_dir).expect("mkdir");
        for (name, bytes) in files {
            std::fs::write(gen_dir.join(name), bytes).expect("write");
        }
        let manifest = test_manifest_bytes(generation);
        let path = ckpt_dir.join("MANIFEST");
        let tmp = ckpt_dir.join("MANIFEST.tmp");
        nodedb_wal::segment::write_checkpoint_framed(&tmp, &path, &manifest).expect("manifest");
    }

    fn gen_dir_of(data_dir: &Path, core_id: usize, generation: u64) -> std::path::PathBuf {
        spatial_ckpt_gen_dir(&spatial_ckpt_dir(data_dir, core_id), generation)
    }

    /// Both halves of a pair go, across every core's live generation — a
    /// surviving docmap is an orphan, and a surviving R-tree resurrects
    /// geometry for a collection that no longer exists.
    #[test]
    fn unlinks_both_halves_across_cores() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        publish(
            base,
            0,
            3,
            &[
                ("0_1_places_geom.ckpt", b"x"),
                ("0_1_places_geom.docmap", b"yy"),
            ],
        );
        publish(
            base,
            1,
            0,
            &[
                ("0_1_places_home.ckpt", b"zzz"),
                // Different collection: must survive.
                ("0_1_stores_geom.ckpt", b"keep"),
            ],
        );

        let stats = reclaim_spatial_checkpoints(base, 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 3);
        assert!(!gen_dir_of(base, 0, 3).join("0_1_places_geom.ckpt").exists());
        assert!(
            !gen_dir_of(base, 0, 3)
                .join("0_1_places_geom.docmap")
                .exists()
        );
        assert!(!gen_dir_of(base, 1, 0).join("0_1_places_home.ckpt").exists());
        assert!(gen_dir_of(base, 1, 0).join("0_1_stores_geom.ckpt").exists());
    }

    /// A collection whose encoded name merely starts with the target's bytes
    /// must survive — the encoder escapes literal underscores precisely so the
    /// structural separator cannot collide.
    #[test]
    fn does_not_unlink_a_longer_collection_name() {
        let tmp = TempDir::new().expect("tempdir");
        publish(
            tmp.path(),
            0,
            0,
            &[("0_1_places%5Farchive_geom.ckpt", b"x")],
        );

        let stats = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
    }

    /// Files under a SUPERSEDED generation are already unreachable.
    #[test]
    fn ignores_superseded_generations() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 1, &[("0_1_places_geom.ckpt", b"live")]);
        let stale = gen_dir_of(tmp.path(), 0, 0);
        std::fs::create_dir_all(&stale).expect("mkdir");
        std::fs::write(stale.join("0_1_places_geom.ckpt"), b"old").expect("write");

        let stats = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 1, "only the live generation's file");
        assert!(stale.join("0_1_places_geom.ckpt").exists());
    }

    #[test]
    fn corrupt_manifest_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        let core_dir = spatial_ckpt_dir(tmp.path(), 0);
        std::fs::create_dir_all(&core_dir).expect("mkdir");
        std::fs::write(core_dir.join("MANIFEST"), b"not-a-manifest").expect("manifest");

        let error = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "places").expect_err("must fail");
        assert!(error.to_string().contains("manifest"));
    }

    #[test]
    fn absent_root_is_a_noop() {
        let tmp = TempDir::new().expect("tempdir");
        let stats = reclaim_spatial_checkpoints(tmp.path(), 0, 1, "places").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
    }
}
