// SPDX-License-Identifier: BUSL-1.1

//! Commit a mutated collection record from an index DDL path.

use crate::control::state::SharedState;

use super::super::super::super::result::DdlError;

pub(super) fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Commit a mutated [`StoredCollection`] through the metadata proposer. Its
/// apply re-dispatches a `Register` to this node's Data Plane, so the new
/// index vector lands in `doc_configs` before the propose returns.
///
/// [`StoredCollection`]: crate::control::security::catalog::StoredCollection
pub(super) async fn commit_collection_mutation(
    state: &SharedState,
    coll: &crate::control::security::catalog::StoredCollection,
) -> Result<(), DdlError> {
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    Ok(())
}
