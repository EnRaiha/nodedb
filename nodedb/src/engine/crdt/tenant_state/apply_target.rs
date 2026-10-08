// SPDX-License-Identifier: BUSL-1.1

//! What a validated delta apply writes into, and how it imports the bytes.

use nodedb_crdt::state::{CrdtState, ImportAdmission, WriteSetImport};
use nodedb_types::Surrogate;

/// What a validated delta apply writes into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyTarget<'a> {
    /// One document's delta, under the row's bound surrogate. The caller
    /// refuses `Surrogate::ZERO` before it builds this target. A delta that
    /// writes any other row is refused as malformed.
    ///
    /// A document delta is a sync delta from a peer. It imports under the
    /// peer byte and operation ceilings.
    Document {
        document_id: &'a str,
        surrogate: Surrogate,
    },
    /// A per-collection snapshot import. It can write any row of the
    /// collection and binds no row identity.
    ///
    /// The snapshot is one this node or its backup exported: a checkpoint,
    /// a restored collection, or the WAL record of either. It imports
    /// without the peer ceilings and keeps every structural check, so a
    /// collection that grew past the ceilings still loads.
    Collection,
}

impl ApplyTarget<'_> {
    /// Whether a delta applied to this target can write `row`.
    pub(super) fn admits_row(&self, row: &str) -> bool {
        match self {
            Self::Document { document_id, .. } => row == *document_id,
            Self::Collection => true,
        }
    }

    /// The identity `row` validates under: the document's surrogate for a
    /// document target. A snapshot import names no row identity, and the
    /// validator's change record carries `Surrogate::ZERO` for it. The
    /// validator never reads that field.
    pub(super) fn validation_surrogate(&self, row: &str) -> Surrogate {
        match self {
            Self::Document {
                document_id,
                surrogate,
            } if row == *document_id => *surrogate,
            Self::Document { .. } | Self::Collection => Surrogate::ZERO,
        }
    }

    /// Import `delta` into `state` and report the rows it wrote.
    pub(super) fn import_with_write_set(&self, state: &CrdtState, delta: &[u8]) -> WriteSetImport {
        match self {
            Self::Document { .. } => state.import_with_write_set(delta),
            Self::Collection => state.import_local_with_write_set(delta),
        }
    }

    /// Import `delta` into `state`.
    pub(super) fn import(
        &self,
        state: &CrdtState,
        delta: &[u8],
    ) -> nodedb_crdt::Result<ImportAdmission> {
        match self {
            Self::Document { .. } => state.import(delta),
            Self::Collection => state.import_local(delta),
        }
    }
}
