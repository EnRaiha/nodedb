// SPDX-License-Identifier: BUSL-1.1

//! Vector engine reclaim — unlink a dropped collection's HNSW checkpoint files.
//!
//! Checkpoint layout:
//! `{data_dir}/vector-ckpt/core-{n}/gen-{m}/{db}:{tid}:{coll}.ckpt` for the bare
//! collection, plus one `{db}:{tid}:{coll}:{field}.ckpt` per named-field index,
//! with each core's `MANIFEST` naming its live generation. Reclaim of a whole
//! collection walks EVERY core's directory — the caller does not know which
//! core(s) ever held the collection — and resolves each core's live generation
//! through the same manifest reader the load path uses, so the two can never
//! disagree about which files are reachable.
//!
//! Unlinking a file out of a published generation does not break the manifest's
//! promise. That promise is about the collections that are LIVE, and this runs
//! only once a collection is dropped: the load path restoring nothing for it is
//! the correct outcome, and leaving the file would resurrect a dropped
//! collection's index on the next boot.

use std::path::Path;

use tracing::debug;

use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::vector_checkpoint::{
    read_vector_manifest_at, vector_ckpt_collection_stem, vector_ckpt_dir, vector_ckpt_gen_dir,
};

/// Unlink every vector checkpoint file for `(database_id, tenant_id, collection)`
/// across every core's live generation. Returns stats; idempotent (missing
/// files count as 0).
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

    // Built through the write path's own encoder so the match can never drift
    // from the on-disk names.
    let prefix_exact = vector_ckpt_collection_stem(database_id, tenant_id, collection);
    let prefix_field = format!("{prefix_exact}:");

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
        let Some(gen_dir) = live_generation_dir(&core_dir)? else {
            continue;
        };
        reclaim_generation(&gen_dir, &prefix_exact, &prefix_field, &mut stats)?;
    }
    Ok(stats)
}

/// Resolve one core's live generation directory, or `Ok(None)` when the core
/// has published nothing.
///
/// A corrupt manifest is fail-closed: skipping it could release a same-name
/// CREATE while the predecessor's index files remain reachable.
fn live_generation_dir(core_dir: &Path) -> Result<Option<std::path::PathBuf>> {
    match read_vector_manifest_at(core_dir) {
        Ok(Some(manifest)) => Ok(Some(vector_ckpt_gen_dir(core_dir, manifest.generation))),
        Ok(None) => Ok(None),
        Err(error) => Err(ReclaimError::Manifest {
            engine: "vector",
            path: core_dir.join("MANIFEST"),
            detail: error.to_string(),
        }),
    }
}

/// Unlink every matching file in one core's live generation. A missing
/// generation directory is a no-op, not an error: the manifest alone decides
/// what is reachable, so there is nothing a boot could restore.
fn reclaim_generation(
    gen_dir: &Path,
    prefix_exact: &str,
    prefix_field: &str,
    stats: &mut ReclaimStats,
) -> Result<()> {
    let entries = match std::fs::read_dir(gen_dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read vector checkpoint live generation",
                path: gen_dir.to_path_buf(),
                source,
            });
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| ReclaimError::Io {
            operation: "read vector checkpoint entry",
            path: gen_dir.to_path_buf(),
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
/// unlinks only the file it itself could have written — its own core id's live
/// generation. That keeps concurrent cores dropping the same index from
/// touching each other's subdirectories.
pub fn reclaim_vector_index_checkpoint(
    data_dir: &Path,
    core_id: usize,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    field_name: &str,
) -> Result<ReclaimStats> {
    let bare = vector_ckpt_collection_stem(database_id, tenant_id, collection);
    let stem = if field_name.is_empty() {
        bare
    } else {
        format!("{bare}:{field_name}")
    };
    let core_dir = vector_ckpt_dir(data_dir, core_id);
    let Some(gen_dir) = live_generation_dir(&core_dir)? else {
        return Ok(ReclaimStats::default());
    };
    let mut stats = ReclaimStats::default();
    for extension in ["ckpt", "ckpt.tmp"] {
        let path = gen_dir.join(format!("{stem}.{extension}"));
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
    use crate::data::executor::vector_checkpoint::test_manifest_bytes;
    use tempfile::TempDir;

    /// Publish a live generation under `core_id` holding `files`, through the
    /// same manifest the load path reads.
    fn publish(data_dir: &Path, core_id: usize, generation: u64, files: &[(&str, &[u8])]) {
        let ckpt_dir = vector_ckpt_dir(data_dir, core_id);
        let gen_dir = vector_ckpt_gen_dir(&ckpt_dir, generation);
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
        vector_ckpt_gen_dir(&vector_ckpt_dir(data_dir, core_id), generation)
    }

    #[test]
    fn unlinks_bare_and_named_field_ckpts() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        publish(
            base,
            0,
            0,
            &[
                ("0:1:users.ckpt", b"x"),
                ("0:1:users:email.ckpt", b"xy"),
                ("0:1:users:name.ckpt.tmp", b"xyz"),
                // Other collection: must not touch.
                ("0:1:orders.ckpt", b"keep"),
                // Different tenant: must not touch.
                ("0:2:users.ckpt", b"keep2"),
                // Different database: must not touch.
                ("1:1:users.ckpt", b"keepdb"),
            ],
        );

        let stats = reclaim_vector_checkpoints(base, 0, 1, "users").expect("reclaim");
        assert_eq!(stats.files_unlinked, 3);
        assert_eq!(stats.bytes_freed, 1 + 2 + 3);
        let live = gen_dir_of(base, 0, 0);
        assert!(live.join("0:1:orders.ckpt").exists());
        assert!(live.join("0:2:users.ckpt").exists());
        assert!(live.join("1:1:users.ckpt").exists());
        assert!(!live.join("0:1:users.ckpt").exists());
    }

    /// The dropped collection's vectors could have been checkpointed by ANY
    /// core sharing `data_dir` — reclaim must reach every `core-N`
    /// subdirectory, or a file survives the DROP indefinitely.
    #[test]
    fn unlinks_across_every_core_subdirectory() {
        let tmp = TempDir::new().expect("tempdir");
        let base = tmp.path();
        publish(base, 0, 2, &[("0:1:docs.ckpt", b"a")]);
        publish(
            base,
            1,
            0,
            &[
                ("0:1:docs:emb.ckpt", b"bb"),
                // Different core, different collection: must survive.
                ("0:1:posts.ckpt", b"keep"),
            ],
        );

        let stats = reclaim_vector_checkpoints(base, 0, 1, "docs").expect("reclaim");
        assert_eq!(stats.files_unlinked, 2, "both cores' files must be reached");
        assert_eq!(stats.bytes_freed, 1 + 2);
        assert!(!gen_dir_of(base, 0, 2).join("0:1:docs.ckpt").exists());
        assert!(!gen_dir_of(base, 1, 0).join("0:1:docs:emb.ckpt").exists());
        assert!(gen_dir_of(base, 1, 0).join("0:1:posts.ckpt").exists());
    }

    /// Files under a SUPERSEDED generation are already unreachable: the manifest
    /// alone decides what a boot restores, so reclaim leaves them to the write
    /// path's own cleanup rather than walking directories nothing can read.
    #[test]
    fn ignores_superseded_generations() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 1, &[("0:1:docs.ckpt", b"live")]);
        let stale = gen_dir_of(tmp.path(), 0, 0);
        std::fs::create_dir_all(&stale).expect("mkdir");
        std::fs::write(stale.join("0:1:docs.ckpt"), b"old").expect("write");

        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "docs").expect("reclaim");
        assert_eq!(stats.files_unlinked, 1, "only the live generation's file");
        assert!(stale.join("0:1:docs.ckpt").exists());
    }

    #[test]
    fn corrupt_manifest_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        let core_dir = vector_ckpt_dir(tmp.path(), 0);
        std::fs::create_dir_all(&core_dir).expect("mkdir");
        std::fs::write(core_dir.join("MANIFEST"), b"not-a-manifest").expect("manifest");

        let error = reclaim_vector_checkpoints(tmp.path(), 0, 1, "docs").expect_err("must fail");
        assert!(error.to_string().contains("manifest"));
    }

    #[test]
    fn unlink_failure_is_returned_to_lifecycle_barrier() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 0, &[]);
        // A directory where a checkpoint file is expected: `remove_file` fails.
        let invalid_target = gen_dir_of(tmp.path(), 0, 0).join("0:1:users.ckpt");
        std::fs::create_dir_all(&invalid_target).expect("mkdir");

        let error = reclaim_vector_checkpoints(tmp.path(), 0, 1, "users").expect_err("must fail");
        assert!(error.to_string().contains("unlink vector checkpoint"));
        assert!(invalid_target.exists());
    }

    #[test]
    fn empty_dir_is_noop() {
        let tmp = TempDir::new().expect("tempdir");
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "x").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
    }

    #[test]
    fn index_scoped_reclaim_spares_sibling_indexes() {
        let tmp = TempDir::new().expect("tempdir");
        publish(
            tmp.path(),
            0,
            0,
            &[
                ("0:1:docs.ckpt", b"default"),
                ("0:1:docs:text_emb.ckpt", b"text"),
                ("0:1:docs:image_emb.ckpt", b"image"),
            ],
        );
        let live = gen_dir_of(tmp.path(), 0, 0);

        let stats = reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "text_emb")
            .expect("reclaim");
        assert_eq!(stats.files_unlinked, 1);
        assert!(!live.join("0:1:docs:text_emb.ckpt").exists());
        assert!(live.join("0:1:docs:image_emb.ckpt").exists());
        assert!(live.join("0:1:docs.ckpt").exists());

        // The default (unnamed) field targets the bare stem only.
        reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "").expect("reclaim");
        assert!(!live.join("0:1:docs.ckpt").exists());
        assert!(live.join("0:1:docs:image_emb.ckpt").exists());
    }

    #[test]
    fn index_scoped_reclaim_is_idempotent() {
        let tmp = TempDir::new().expect("tempdir");
        let stats =
            reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "emb").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
    }

    /// `reclaim_vector_index_checkpoint` only touches the core it is told
    /// about — a file checkpointed by a different core must survive, proving
    /// the single-index drop path does not race a sibling core's own state.
    #[test]
    fn index_scoped_reclaim_does_not_touch_other_cores() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 1, 0, &[("0:1:docs.ckpt", b"x")]);

        let stats =
            reclaim_vector_index_checkpoint(tmp.path(), 0, 0, 1, "docs", "").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
        assert!(gen_dir_of(tmp.path(), 1, 0).join("0:1:docs.ckpt").exists());
    }

    #[test]
    fn no_false_prefix_match() {
        let tmp = TempDir::new().expect("tempdir");
        publish(tmp.path(), 0, 0, &[("0:1:users_archive.ckpt", b"keep")]);
        let stats = reclaim_vector_checkpoints(tmp.path(), 0, 1, "users").expect("reclaim");
        assert_eq!(stats.files_unlinked, 0);
        assert!(
            gen_dir_of(tmp.path(), 0, 0)
                .join("0:1:users_archive.ckpt")
                .exists()
        );
    }
}
