// SPDX-License-Identifier: BUSL-1.1

//! Reading the manifest that names the live CRDT checkpoint generation, plus
//! the shared typed-error constructor for this module's filesystem failures.

use super::format::{CRDT_CKPT_FORMAT_VERSION, CrdtCheckpointManifest};
use super::paths::CRDT_CKPT_MANIFEST;
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;

/// Read the live manifest under `ckpt_dir`, or `Ok(None)` when the file is
/// genuinely absent.
///
/// A present-but-unreadable, undecodable, or version-mismatched manifest is
/// `Err`, not `None`: a `TenantCrdtEngine` has no store behind it, so treating
/// corruption as "no generation" would silently bring the documents back at
/// whatever the WAL still holds — with every edit below the truncation point
/// gone and no error at read time to show for it.
///
/// A free function rather than a `CoreLoop` method because reclaim — which runs
/// against a data dir and not a live core — resolves the live generation
/// through this same reader, and the two must never diverge on what "live"
/// means.
pub(crate) fn read_crdt_manifest_at(
    ckpt_dir: &std::path::Path,
) -> Result<Option<CrdtCheckpointManifest>, CheckpointDecodeError> {
    let path = ckpt_dir.join(CRDT_CKPT_MANIFEST);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
        CheckpointDecodeError::ReadFile {
            path: path.clone(),
            source,
        }
    })?;
    let manifest = zerompk::from_msgpack::<CrdtCheckpointManifest>(&bytes).map_err(|source| {
        CheckpointDecodeError::MsgpackDecode {
            path: path.clone(),
            source,
        }
    })?;
    if manifest.format_version != CRDT_CKPT_FORMAT_VERSION {
        return Err(CheckpointDecodeError::FormatVersion {
            path,
            found: manifest.format_version,
            expected: CRDT_CKPT_FORMAT_VERSION,
        });
    }
    Ok(Some(manifest))
}

/// Wrap a filesystem failure as the CRDT engine's typed storage error.
pub(crate) fn storage_err(
    path: &std::path::Path,
    action: &str,
    e: &dyn std::fmt::Display,
) -> crate::Error {
    crate::Error::Storage {
        engine: "crdt".to_string(),
        detail: format!(
            "CRDT checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}
