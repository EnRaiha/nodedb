// SPDX-License-Identifier: BUSL-1.1

//! Reading the manifest that names the live spatial checkpoint generation, plus
//! the shared typed-error constructor for this module's filesystem failures.

use super::format::{SPATIAL_CKPT_FORMAT_VERSION, SpatialCheckpointManifest};
use super::paths::SPATIAL_CKPT_MANIFEST;
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;

/// Read the live manifest under `ckpt_dir`, or `Ok(None)` when the file is
/// genuinely absent.
///
/// A present-but-unreadable, undecodable, or version-mismatched manifest is
/// `Err`, not `None`: treating corruption as "no generation" would restore no
/// geometry while the rows it indexes are still there, so spatial predicates
/// would silently stop matching rows a full scan still returns.
///
/// A free function rather than a `CoreLoop` method because reclaim — which runs
/// against a data dir and not a live core — resolves the live generation
/// through this same reader, and the two must never diverge on what "live"
/// means.
pub(crate) fn read_spatial_manifest_at(
    ckpt_dir: &std::path::Path,
) -> Result<Option<SpatialCheckpointManifest>, CheckpointDecodeError> {
    let path = ckpt_dir.join(SPATIAL_CKPT_MANIFEST);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
        CheckpointDecodeError::ReadFile {
            path: path.clone(),
            source,
        }
    })?;
    let manifest =
        zerompk::from_msgpack::<SpatialCheckpointManifest>(&bytes).map_err(|source| {
            CheckpointDecodeError::MsgpackDecode {
                path: path.clone(),
                source,
            }
        })?;
    if manifest.format_version != SPATIAL_CKPT_FORMAT_VERSION {
        return Err(CheckpointDecodeError::FormatVersion {
            path,
            found: manifest.format_version,
            expected: SPATIAL_CKPT_FORMAT_VERSION,
        });
    }
    Ok(Some(manifest))
}

/// Wrap a filesystem or encode failure as the spatial engine's typed storage
/// error.
pub(super) fn storage_err(
    path: &std::path::Path,
    action: &str,
    e: &dyn std::fmt::Display,
) -> crate::Error {
    crate::Error::Storage {
        engine: "spatial".to_string(),
        detail: format!(
            "spatial checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}
