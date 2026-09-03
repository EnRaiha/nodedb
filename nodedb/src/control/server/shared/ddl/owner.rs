// SPDX-License-Identifier: BUSL-1.1

//! Helpers for proposing object ownership through the metadata
//! raft group.
//!
//! Used by every handler that creates or drops an object whose
//! parent doesn't already replicate via a `Stored*` variant
//! (indexes, spatial indexes, `ALTER OBJECT OWNER`, DSL paths).
//! Handlers whose object DOES have a parent variant (collection,
//! function, procedure, trigger, materialized_view, sequence,
//! schedule, change_stream) replicate ownership automatically via
//! the parent's `post_apply` and must NOT call this helper.
//!
//! These are protocol-neutral: they build [`DdlError`] on failure and
//! carry no pgwire types, so both the neutral DDL handlers and the
//! (still-pgwire) `collection::index` / `spatial` handlers can call them
//! (pgwire callers map [`DdlError`] back into a `PgWireError` via
//! `sqlstate_error`).

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::permission::prepare_owner;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::result::DdlError;

fn owner_err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Propose `PutOwner` through raft, falling back to a direct redb
/// write + in-memory install on single-node mode.
///
/// `database_id` must name the database the object lives in. The owner row is
/// keyed by it, and every authorization check looks it up database-scoped.
pub fn propose_owner(
    state: &SharedState,
    object_type: &str,
    database_id: u64,
    tenant_id: TenantId,
    object_name: &str,
    owner_username: &str,
) -> Result<(), DdlError> {
    let stored = prepare_owner(
        object_type,
        database_id,
        tenant_id,
        object_name,
        owner_username,
    );
    let entry = CatalogEntry::PutOwner(Box::new(stored.clone()));
    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| owner_err("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        {
            let catalog = state.credentials.catalog();
            catalog
                .put_owner(&stored)
                .map_err(|e| owner_err("XX000", format!("catalog write: {e}")))?;
        }
        state.permissions.install_replicated_owner(&stored);
    }
    Ok(())
}

/// Propose `DeleteOwner` through raft with the same single-node
/// fallback shape.
///
/// `database_id` must match the value the matching [`propose_owner`] wrote.
pub fn propose_delete_owner(
    state: &SharedState,
    object_type: &str,
    database_id: u64,
    tenant_id: TenantId,
    object_name: &str,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteOwner {
        object_type: object_type.to_string(),
        database_id,
        tenant_id: tenant_id.as_u64(),
        object_name: object_name.to_string(),
    };
    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| owner_err("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        {
            let catalog = state.credentials.catalog();
            catalog
                .delete_owner(object_type, database_id, tenant_id.as_u64(), object_name)
                .map_err(|e| owner_err("XX000", format!("catalog write: {e}")))?;
        }
        state.permissions.install_replicated_remove_owner(
            object_type,
            database_id,
            tenant_id.as_u64(),
            object_name,
        );
    }
    Ok(())
}
