// SPDX-License-Identifier: BUSL-1.1

//! On-disk types for the spatial checkpoint.
//!
//! Only the manifest is defined here. The per-index files carry the bytes
//! `RTree::checkpoint_to_bytes` produces (optionally AES-256-GCM enveloped) and
//! the MessagePack doc_map beside them; the generation's version belongs on the
//! manifest that publishes both rather than repeated in every file.

use serde::{Deserialize, Serialize};

/// On-disk format version for the manifest and the generation it names.
pub(crate) const SPATIAL_CKPT_FORMAT_VERSION: u16 = 1;

/// Names the live generation. Writing this file is what publishes a checkpoint.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct SpatialCheckpointManifest {
    /// Always [`SPATIAL_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// Which `gen-{n}/` directory holds the live R-tree and doc_map files.
    pub generation: u64,
    /// The LSN every index in that generation is durable THROUGH (inclusive).
    ///
    /// Restores `spatial_durable_lsn` after a restart, which is the point a
    /// failed flush clamps WAL truncation to.
    pub durable_through_lsn: u64,
}

/// Encode a manifest publishing `generation`, for tests that need a live
/// generation on disk without driving a whole core through a flush.
///
/// Shares this module's writer so a test's idea of "published" can never drift
/// from the real one.
#[cfg(test)]
pub(crate) fn test_manifest_bytes(generation: u64) -> Vec<u8> {
    zerompk::to_msgpack_vec(&SpatialCheckpointManifest {
        format_version: SPATIAL_CKPT_FORMAT_VERSION,
        generation,
        durable_through_lsn: 0,
    })
    .expect("manifest encode is infallible for this fixed struct")
}
