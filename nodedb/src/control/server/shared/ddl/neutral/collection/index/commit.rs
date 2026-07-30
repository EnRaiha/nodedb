// SPDX-License-Identifier: BUSL-1.1

//! Commit a mutated collection record from an index DDL path.

use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::super::result::DdlError;

pub(super) fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Commit a mutated [`StoredCollection`] through the replicated metadata
/// Raft group (cluster) or straight to the local `SystemCatalog`
/// (single-node fallback), then re-dispatch a `Register` to this node's
/// Data Plane so the new index vector lands in `doc_configs` immediately.
///
/// [`StoredCollection`]: crate::control::security::catalog::StoredCollection
pub(super) async fn commit_collection_mutation(
    state: &SharedState,
    coll: &crate::control::security::catalog::StoredCollection,
    database_id: DatabaseId,
) -> Result<(), DdlError> {
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", e.to_string()))?;
    if log_index == 0 {
        {
            let catalog = state.credentials.catalog();
            catalog
                .put_collection(database_id, coll)
                .map_err(|e| err("XX000", e.to_string()))?;
        }
        // Single-node path bypasses the applier post-apply hook, so the
        // Register refresh has to be fired here. In cluster mode the
        // applier's `put_async` does it on every node.
        super::super::dispatch_register_from_stored(state, coll)
            .await
            .map_err(|e| err("XX000", e.to_string()))?;
    }
    Ok(())
}
