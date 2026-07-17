// SPDX-License-Identifier: BUSL-1.1

//! On-disk types for the columnar checkpoint: the manifest that publishes a
//! generation, and the per-collection files it names.

use serde::{Deserialize, Serialize};

/// On-disk format version for the manifest and the collection files.
///
/// A file stamped with any other version is refused rather than misparsed.
/// Refusing costs a WAL replay; misparsing would install wrong rows AND a floor
/// that suppresses the records which would have corrected them.
pub(crate) const COLUMNAR_CKPT_FORMAT_VERSION: u16 = 1;

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
pub(crate) struct ColumnarCheckpointManifest {
    /// Always [`COLUMNAR_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// Which `gen-{n}/` directory holds the live collection files.
    pub generation: u64,
    /// The LSN every collection in that generation is durable THROUGH
    /// (inclusive).
    ///
    /// This is what makes a generation self-describing: WAL replay skips
    /// columnar records at or below it and replays everything above. Without it
    /// a restore could not know which records it had already folded in.
    /// Columnar has no safe fallback for that ignorance: `ColumnarOp::Update` is
    /// delete-old-PK + insert-new-row, so re-applying one duplicates the row,
    /// and on a `bitemporal=true` collection re-applying an `Insert` appends a
    /// second version that `AS OF` queries can see.
    pub durable_through_lsn: u64,
}

/// One collection's full engine state within a generation.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub(crate) struct ColumnarCheckpointFile {
    /// Always [`COLUMNAR_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// The complete `MutationEngine` state: memtable columns and their per-row
    /// surrogates, PK index, delete bitmaps, segment-id counters, schema, and
    /// the flushed segment blobs together with their per-row surrogate sidecar.
    ///
    /// One field, not two, deliberately: the segment blobs and their surrogate
    /// table are held in lockstep by position (outer index == segment index),
    /// and `nodedb_columnar` exports and imports both halves in a single call.
    /// Anything that could restore one without the other would corrupt every
    /// cross-engine prefilter silently, so this format cannot express it.
    ///
    /// The LSN lives in the manifest, not here — see the module docs for why a
    /// per-file stamp is unsound.
    pub engine: nodedb_columnar::ColumnarEngineSnapshot,
}
