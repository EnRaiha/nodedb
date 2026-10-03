// SPDX-License-Identifier: BUSL-1.1

//! Replicated writes for the version-history checkpoint handlers.
//!
//! Every mutation of `_system.checkpoints` proposes a `CatalogEntry`, so each
//! node writes the row. A checkpoint created on one node resolves on all.
//! Checkpoints have no in-memory mirror, so there is no post-apply install.
//!
//! The oplog compaction behind `COMPACT HISTORY` is node-local physical state,
//! so it runs from the post-apply lane on every node that applies the entry.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::types::{CheckpointDoc, CheckpointRecord};
use crate::control::state::SharedState;

use super::super::super::result::DdlError;
use super::super::replicate::propose_and_apply_async;

/// Propose the checkpoint row. The leader reports the duplicate before
/// proposing, so apply is a plain write that never rejects.
pub(super) async fn propose_put(
    state: &SharedState,
    record: &CheckpointRecord,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::PutCheckpoint(Box::new(record.clone()));
    propose_and_apply_async(state, &entry).await
}

/// Propose removal of one checkpoint row on every node.
///
/// The leader reports the missing checkpoint before proposing, so apply stays
/// idempotent under replay.
pub(super) async fn propose_delete(
    state: &SharedState,
    doc: CheckpointDoc<'_>,
    checkpoint_name: &str,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteCheckpoint {
        database_id: doc.database_id,
        tenant_id: doc.tenant_id,
        collection: doc.collection.to_string(),
        doc_id: doc.doc_id.to_string(),
        checkpoint_name: checkpoint_name.to_string(),
    };
    propose_and_apply_async(state, &entry).await
}

/// Propose one COMPACT HISTORY statement as a single entry carrying the
/// checkpoint boundary and the compaction target.
///
/// Shipping the boundary rather than N per-row deletes keeps followers in step
/// with the leader even when their scan order differs. `target_version_json`
/// rides along because post-apply compacts each node's oplog to it, and apply
/// has already deleted the checkpoint row that holds it.
///
/// The post-apply lane runs this node's compaction dispatch, which this call
/// awaits.
pub(super) async fn propose_compact_history(
    state: &SharedState,
    doc: CheckpointDoc<'_>,
    before_timestamp: u64,
    target_version_json: &str,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::CompactHistory {
        tenant_id: doc.tenant_id,
        database_id: doc.database_id,
        collection: doc.collection.to_string(),
        doc_id: doc.doc_id.to_string(),
        before_timestamp,
        target_version_json: target_version_json.to_string(),
    };
    propose_and_apply_async(state, &entry).await
}
