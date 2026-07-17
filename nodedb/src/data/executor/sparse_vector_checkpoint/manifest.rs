// SPDX-License-Identifier: BUSL-1.1

//! Reading the manifest that names the live sparse-vector checkpoint
//! generation, plus the shared typed-error constructor for this module's
//! filesystem failures.

use tracing::warn;

use super::format::{SPARSE_VECTOR_CKPT_FORMAT_VERSION, SparseVectorCheckpointManifest};
use super::paths::SPARSE_VECTOR_CKPT_MANIFEST;

/// Read the live manifest under `ckpt_dir`, or `None` when it is absent,
/// unreadable, undecodable, or stamped with an unknown version.
///
/// `None` is always safe in both directions: the write path starts a fresh
/// generation, and the load path restores nothing, so replay falls back to the
/// full WAL. `core_id` is only used to attribute the warning.
///
/// A free function rather than a `CoreLoop` method because reclaim — which runs
/// against a data dir and not a live core — resolves the live generation through
/// this same reader, and the two must never diverge on what "live" means.
pub(crate) fn read_sparse_vector_manifest_at(
    ckpt_dir: &std::path::Path,
    core_id: usize,
) -> Option<SparseVectorCheckpointManifest> {
    let path = ckpt_dir.join(SPARSE_VECTOR_CKPT_MANIFEST);
    if !path.exists() {
        return None;
    }
    let bytes = match nodedb_wal::segment::read_checkpoint_framed(&path) {
        Ok(b) => b,
        Err(e) => {
            warn!(
                core = core_id,
                error = %e,
                "sparse-vector checkpoint manifest unreadable; treating as absent"
            );
            return None;
        }
    };
    let manifest = match zerompk::from_msgpack::<SparseVectorCheckpointManifest>(&bytes) {
        Ok(m) => m,
        Err(e) => {
            warn!(
                core = core_id,
                error = %e,
                "sparse-vector checkpoint manifest undecodable; treating as absent"
            );
            return None;
        }
    };
    if manifest.format_version != SPARSE_VECTOR_CKPT_FORMAT_VERSION {
        warn!(
            core = core_id,
            found = manifest.format_version,
            expected = SPARSE_VECTOR_CKPT_FORMAT_VERSION,
            "unknown sparse-vector checkpoint manifest version; treating as absent"
        );
        return None;
    }
    Some(manifest)
}

/// Wrap a filesystem failure as the sparse-vector engine's typed storage error.
pub(super) fn storage_err(
    path: &std::path::Path,
    action: &str,
    e: &dyn std::fmt::Display,
) -> crate::Error {
    crate::Error::Storage {
        engine: "sparse_vector".to_string(),
        detail: format!(
            "sparse-vector checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}
