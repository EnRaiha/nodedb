// SPDX-License-Identifier: BUSL-1.1

//! Reading the manifest that names the live columnar checkpoint generation, plus
//! the shared typed-error constructor for this module's filesystem failures.

use tracing::warn;

use super::format::{COLUMNAR_CKPT_FORMAT_VERSION, ColumnarCheckpointManifest};
use super::paths::COLUMNAR_CKPT_MANIFEST;
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Read the live manifest, or `None` when absent / unreadable / unknown
    /// version.
    ///
    /// `None` is always safe in both directions: the write path starts a fresh
    /// generation, and the load path restores nothing and installs no floor, so
    /// replay falls back to the full WAL.
    pub(super) fn read_columnar_manifest(
        &self,
        ckpt_dir: &std::path::Path,
    ) -> Option<ColumnarCheckpointManifest> {
        let path = ckpt_dir.join(COLUMNAR_CKPT_MANIFEST);
        if !path.exists() {
            return None;
        }
        let bytes = match nodedb_wal::segment::read_checkpoint_framed(&path) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    "columnar checkpoint manifest unreadable; treating as absent"
                );
                return None;
            }
        };
        let manifest = match zerompk::from_msgpack::<ColumnarCheckpointManifest>(&bytes) {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    "columnar checkpoint manifest undecodable; treating as absent"
                );
                return None;
            }
        };
        if manifest.format_version != COLUMNAR_CKPT_FORMAT_VERSION {
            warn!(
                core = self.core_id,
                found = manifest.format_version,
                expected = COLUMNAR_CKPT_FORMAT_VERSION,
                "unknown columnar checkpoint manifest version; treating as absent"
            );
            return None;
        }
        Some(manifest)
    }
}

/// Wrap a filesystem failure as the columnar engine's typed storage error.
pub(super) fn storage_err(
    path: &std::path::Path,
    action: &str,
    e: &dyn std::fmt::Display,
) -> crate::Error {
    crate::Error::Storage {
        engine: "columnar".to_string(),
        detail: format!(
            "columnar checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}
