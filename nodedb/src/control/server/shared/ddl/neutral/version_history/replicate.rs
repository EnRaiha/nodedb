// SPDX-License-Identifier: BUSL-1.1

//! Replicated writes for the version-history checkpoint handlers.
//!
//! Every mutation of `_system.checkpoints` proposes a `CatalogEntry`, so each
//! node writes the row. A checkpoint created on one node resolves on all.
//! Checkpoints have no in-memory mirror, so there is no post-apply install.

use crate::control::catalog_entry::apply::checkpoint as apply;
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::types::CheckpointRecord;
use crate::control::state::SharedState;

use super::super::super::result::DdlError;
use super::super::replicate::propose_and_apply;

/// Propose the checkpoint row. The leader reports the duplicate before
/// proposing, so apply is a plain write that never rejects.
pub(super) fn propose_put(state: &SharedState, record: &CheckpointRecord) -> Result<(), DdlError> {
    let entry = CatalogEntry::PutCheckpoint(Box::new(record.clone()));
    propose_and_apply(state, &entry, || {
        apply::put(record, state.credentials.catalog())
            .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))
    })
}

/// Propose removal of one checkpoint row on every node.
///
/// The leader reports the missing checkpoint before proposing, so apply stays
/// idempotent under replay.
pub(super) fn propose_delete(
    state: &SharedState,
    tenant_id: u64,
    collection: &str,
    doc_id: &str,
    checkpoint_name: &str,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteCheckpoint {
        tenant_id,
        collection: collection.to_string(),
        doc_id: doc_id.to_string(),
        checkpoint_name: checkpoint_name.to_string(),
    };
    propose_and_apply(state, &entry, || {
        apply::delete(
            tenant_id,
            collection,
            doc_id,
            checkpoint_name,
            state.credentials.catalog(),
        )
        .map_err(|e| DdlError::new("XX000", format!("catalog delete: {e}")))
    })
}

/// Propose the COMPACT HISTORY range delete as one entry carrying the boundary.
///
/// Shipping the boundary rather than N per-row deletes keeps followers in step
/// with the leader even when their scan order differs.
pub(super) fn propose_delete_before(
    state: &SharedState,
    tenant_id: u64,
    collection: &str,
    doc_id: &str,
    before_timestamp: u64,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteCheckpointsBefore {
        tenant_id,
        collection: collection.to_string(),
        doc_id: doc_id.to_string(),
        before_timestamp,
    };
    propose_and_apply(state, &entry, || {
        apply::delete_before(
            tenant_id,
            collection,
            doc_id,
            before_timestamp,
            state.credentials.catalog(),
        )
        .map_err(|e| DdlError::new("XX000", format!("catalog range delete: {e}")))
    })
}
