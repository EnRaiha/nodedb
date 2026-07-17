// SPDX-License-Identifier: BUSL-1.1

//! On-disk types for the KV checkpoint: the manifest that publishes a
//! generation, and the per-collection files it names.

use serde::{Deserialize, Serialize};

use super::index_format::KvCheckpointIndexes;

/// On-disk format version for the manifest and the collection files.
///
/// A file stamped with any other version is refused rather than misparsed.
/// Refusing costs a WAL replay; misparsing would install wrong rows AND a floor
/// that suppresses the records which would have corrected them.
pub(crate) const KV_CKPT_FORMAT_VERSION: u16 = 2;

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
pub(crate) struct KvCheckpointManifest {
    /// Always [`KV_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// Which `gen-{n}/` directory holds the live collection files.
    pub generation: u64,
    /// The LSN every collection in that generation is durable THROUGH
    /// (inclusive).
    ///
    /// This is what makes a generation self-describing: WAL replay skips KV
    /// records at or below it and replays everything above. Without it a restore
    /// could not know which records it had already folded in, and would have to
    /// either re-apply deltas (double-counting) or skip everything (losing every
    /// write made after the flush).
    pub durable_through_lsn: u64,
}

/// One checkpointed KV row.
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
pub(crate) struct KvCheckpointEntry {
    /// Primary key bytes.
    pub key: Vec<u8>,
    /// Value bytes (msgpack, as the engine stores them).
    pub value: Vec<u8>,
    /// Absolute expiry instant in ms since epoch, `0` for no TTL. Stored
    /// absolute, never as a remaining duration, so a restore installs the exact
    /// instant the original write computed instead of pushing expiry forward by
    /// the checkpoint-to-restart delay.
    pub expire_at_ms: u64,
    /// Stable cross-engine row identity (`0` when unbound). Without it the
    /// restored row would be invisible to every bitmap intersection that joins
    /// KV against the vector / FTS / spatial engines.
    pub surrogate: u32,
}

/// One collection's rows within a generation.
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
pub(crate) struct KvCheckpointFile {
    /// Always [`KV_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// Every live row in the collection at flush time. An empty vector is
    /// meaningful, not a skip: it records that the collection is durably EMPTY.
    ///
    /// The LSN lives in the manifest, not here — see the module docs for why a
    /// per-file stamp is unsound.
    pub entries: Vec<KvCheckpointEntry>,
    /// Every index registration on the collection, with its content.
    ///
    /// In the same file as the rows, deliberately: a registration's only other
    /// durable record is a WAL record that this checkpoint's floor gates out and
    /// truncation then deletes, so rows and registrations must become durable
    /// together or not at all. Sharing the file makes the alternative
    /// unrepresentable.
    pub indexes: KvCheckpointIndexes,
}
