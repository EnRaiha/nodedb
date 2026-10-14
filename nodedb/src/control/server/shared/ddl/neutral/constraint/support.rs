// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the protocol-neutral constraint DDL handlers.

use crate::control::catalog_entry::persist_collection_replicated;
use crate::control::security::catalog::StoredCollection;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and message.
pub(super) fn err(code: &str, msg: &str) -> DdlError {
    DdlError::new(code, msg)
}

/// Replicate the changed constraints, register them on this node's Data Plane,
/// and bump the schema version.
///
/// State and transition constraints are enforced on the Data Plane. A
/// proposal buffered in an open transaction runs no post-apply, so this call
/// registers the new shape for the transaction's later statements.
pub(super) async fn persist_constraints(
    state: &SharedState,
    coll: &StoredCollection,
) -> Result<(), DdlError> {
    let outcome = persist_collection_replicated(state, coll)
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    super::super::collection::register_proposed_collection(state, outcome, coll)
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    state.schema_version.bump();
    Ok(())
}
