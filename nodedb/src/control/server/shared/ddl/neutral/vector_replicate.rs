// SPDX-License-Identifier: BUSL-1.1

//! Replicated writes for vector model metadata and vector-index parameters.
//!
//! Every mutation of `_system.vector_model_metadata` and
//! `_system.vector_index_params` proposes a `CatalogEntry`, so each node
//! writes the row. Neither table has an in-memory mirror, so there is no
//! post-apply install.
//!
//! The Data Plane index and its WAL redo record are node-local physical
//! state, so both are dispatched from the post-apply lane, which runs on
//! every node that applies the entry.

use nodedb_types::{StoredVectorIndexParams, VectorModelEntry};

use crate::control::catalog_entry::apply::vector as apply;
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::state::SharedState;

use super::super::result::DdlError;
use super::replicate::{propose_and_apply, propose_and_apply_outcome};

/// Propose the embedding-model row for one collection column.
pub(crate) fn propose_put_model(
    state: &SharedState,
    entry: &VectorModelEntry,
) -> Result<(), DdlError> {
    let catalog_entry = CatalogEntry::PutVectorModel(Box::new(entry.clone()));
    propose_and_apply(state, &catalog_entry, || {
        apply::put_model(entry, state.credentials.catalog())
            .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))
    })
}

/// Propose removal of one column's embedding-model row on every node.
///
/// The caller skips the proposal when the row is already absent, but apply
/// still treats a missing row as a no-op, so replay on every node stays
/// idempotent.
pub(crate) fn propose_delete_model(
    state: &SharedState,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    column: &str,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteVectorModel {
        database_id,
        tenant_id,
        collection: collection.to_string(),
        column: column.to_string(),
    };
    propose_and_apply(state, &entry, || {
        apply::delete_model(
            database_id,
            tenant_id,
            collection,
            column,
            state.credentials.catalog(),
        )
        .map_err(|e| DdlError::new("XX000", format!("catalog delete: {e}")))
    })
}

/// Propose the build-parameter row for one vector index.
///
/// The handler reports the duplicate index before proposing, so apply is a
/// plain write that never rejects. The returned outcome tells the handler
/// whether a post-apply lane will run this node's WAL append and dispatch.
pub(crate) fn propose_put_params(
    state: &SharedState,
    entry: &StoredVectorIndexParams,
) -> Result<ProposeOutcome, DdlError> {
    let catalog_entry = CatalogEntry::PutVectorIndexParams(Box::new(entry.clone()));
    propose_and_apply_outcome(state, &catalog_entry, || {
        apply::put_params(entry, state.credentials.catalog())
            .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))
    })
}

/// Propose removal of one vector index's build parameters on every node.
///
/// Apply stays idempotent under replay: a missing row is not an error. The
/// returned outcome tells the handler whether a post-apply lane will run this
/// node's WAL append and dispatch.
pub(crate) fn propose_delete_params(
    state: &SharedState,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    field_name: &str,
) -> Result<ProposeOutcome, DdlError> {
    let entry = CatalogEntry::DeleteVectorIndexParams {
        database_id,
        tenant_id,
        collection: collection.to_string(),
        field_name: field_name.to_string(),
    };
    propose_and_apply_outcome(state, &entry, || {
        apply::delete_params(
            database_id,
            tenant_id,
            collection,
            field_name,
            state.credentials.catalog(),
        )
        .map_err(|e| DdlError::new("XX000", format!("catalog delete: {e}")))
    })
}
