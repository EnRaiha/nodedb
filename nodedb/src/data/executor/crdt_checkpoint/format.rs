// SPDX-License-Identifier: BUSL-1.1

//! On-disk types for the CRDT checkpoint.
//!
//! Only the manifest is defined here. The per-collection files carry the raw
//! Loro snapshot bytes, unwrapped: Loro's snapshot format self-checksums and
//! carries its own version, and the generation's version belongs on the
//! manifest that publishes it rather than repeated in every file.

use serde::{Deserialize, Serialize};

/// On-disk format version for the manifest and the generation it names.
pub(crate) const CRDT_CKPT_FORMAT_VERSION: u16 = 1;

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
pub(crate) struct CrdtCheckpointManifest {
    /// Always [`CRDT_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// Which `gen-{n}/` directory holds the live per-collection files.
    pub generation: u64,
    /// The LSN every collection in that generation is durable THROUGH
    /// (inclusive).
    ///
    /// Restores `crdt_durable_lsn` after a restart, which is the point a failed
    /// flush clamps WAL truncation to.
    pub durable_through_lsn: u64,
}

/// Encode a manifest publishing `generation`, for tests that need a live
/// generation on disk without driving a whole core through a flush.
///
/// Shares this module's writer so a test's idea of "published" can never drift
/// from the real one.
#[cfg(test)]
pub(crate) fn test_manifest_bytes(generation: u64) -> Vec<u8> {
    zerompk::to_msgpack_vec(&CrdtCheckpointManifest {
        format_version: CRDT_CKPT_FORMAT_VERSION,
        generation,
        durable_through_lsn: 0,
    })
    .expect("manifest encode is infallible for this fixed struct")
}
