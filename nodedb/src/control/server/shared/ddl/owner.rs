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
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Propose `PutOwner` through raft, falling back to a direct redb
/// write + in-memory install on single-node mode.
///
/// Files the row under database 0. Objects that live in a named database must
/// use [`propose_owner_in_database`] instead, or their ownership row is
/// unreachable from the database-scoped lookups every authorization check
/// performs.
pub fn propose_owner(
    state: &SharedState,
    object_type: &str,
    tenant_id: TenantId,
    object_name: &str,
    owner_username: &str,
) -> Result<(), DdlError> {
    propose_owner_in_database(
        state,
        object_type,
        0,
        tenant_id,
        object_name,
        owner_username,
    )
}

/// Propose `PutOwner` for an object that belongs to a specific database.
pub fn propose_owner_in_database(
    state: &SharedState,
    object_type: &str,
    database_id: u64,
    tenant_id: TenantId,
    object_name: &str,
    owner_username: &str,
) -> Result<(), DdlError> {
    let mut stored = prepare_owner(object_type, tenant_id, object_name, owner_username);
    stored.database_id = database_id;
    let entry = CatalogEntry::PutOwner(Box::new(stored.clone()));
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| owner_err("XX000", format!("metadata propose: {e}")))?;
    if log_index == 0 {
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
pub fn propose_delete_owner(
    state: &SharedState,
    object_type: &str,
    tenant_id: TenantId,
    object_name: &str,
) -> Result<(), DdlError> {
    propose_delete_owner_in_database(state, object_type, 0, tenant_id, object_name)
}

/// Propose `DeleteOwner` for an object that belongs to a specific database.
pub fn propose_delete_owner_in_database(
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
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| owner_err("XX000", format!("metadata propose: {e}")))?;
    if log_index == 0 {
        {
            let catalog = state.credentials.catalog();
            catalog
                .delete_owner(object_type, database_id, tenant_id.as_u64(), object_name)
                .map_err(|e| owner_err("XX000", format!("catalog write: {e}")))?;
        }
        state
            .permissions
            .install_replicated_remove_owner_in_database(
                object_type,
                database_id,
                tenant_id.as_u64(),
                object_name,
            );
    }
    Ok(())
}
