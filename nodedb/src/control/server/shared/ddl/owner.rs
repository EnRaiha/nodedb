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
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::permission::prepare_owner;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::result::DdlError;

/// Propose `PutOwner`.
///
/// `database_id` must name the database the object lives in. The owner row is
/// keyed by it, and every authorization check looks it up database-scoped.
pub async fn propose_owner(
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
    let entry = CatalogEntry::PutOwner(Box::new(stored));
    propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    Ok(())
}

/// Propose `DeleteOwner`.
///
/// `database_id` must match the value the matching [`propose_owner`] wrote.
pub async fn propose_delete_owner(
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
    propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    Ok(())
}
