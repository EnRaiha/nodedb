// SPDX-License-Identifier: BUSL-1.1

//! Owner reassignment + grant sweep for `DROP USER`.
//!
//! Dropping a user that owns catalog objects, or that has grants made
//! *to* it, will leave dangling references behind:
//!
//! - every owned object's `StoredOwner` row (and its in-band `.owner`
//!   field) still names the deleted user — the boot integrity verifier
//!   flags each as `DanglingReference { from_kind: "owner" }`;
//! - every `StoredPermission` granted to the user still names it as
//!   `grantee` — flagged as `DanglingReference { from_kind:
//!   "permission" }`.
//!
//! Either class of dangling reference makes the boot catalog sanity
//! check reject startup with no repair path — a permanently unbootable
//! data directory. This module rewrites every owned object to the
//! tenant admin and revokes every grant made to the user *before* the
//! user row is removed. It is fail-closed: if any reassignment or
//! revoke fails the error propagates and the caller must NOT delete the
//! user (a partially-reassigned + deleted user is the very dangling-ref
//! bug this prevents).
//!
//! The per-kind reassignment match is exhaustive over every
//! owner-bearing object kind and carries no catch-all arm, so adding a
//! new owner-bearing kind is a compile error here until it is wired —
//! and an owner row whose `object_type` maps to no known kind is a hard
//! `DROP USER` error rather than a silently-skipped dangling reference.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::{StoredOwner, SystemCatalog};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::DdlError;
use super::owner_kind::OwnerKind;

/// Reassign every object owned by `username` (within `user_tenant`) to the
/// tenant's validated ownership fallback, then revoke every grant made to the
/// user. A fallback is required only when owned objects exist, allowing tenant
/// teardown to remove an object-free final admin. Returns the selected target.
pub(super) async fn reassign_owned_and_sweep_grants(
    state: &SharedState,
    username: &str,
    user_tenant: TenantId,
) -> Result<Option<String>, DdlError> {
    let catalog = state.credentials.catalog();

    let owned = catalog
        .owners_for_user(username, user_tenant.as_u64())
        .map_err(|e| DdlError::from_error_in_context("load owner rows", &e))?;
    if owned.is_empty() {
        sweep_grants(state, catalog, username).await?;
        return Ok(None);
    }
    let admin_name = catalog
        .resolve_ownership_fallback(user_tenant.as_u64(), username)
        .map_err(|e| DdlError::from_error_in_context("resolve ownership fallback", &e))?
        .ok_or_else(|| {
            DdlError::new(
                "55000",
                format!(
                    "cannot drop user '{username}': tenant {} has no active administrative \
                     principal available for ownership reassignment",
                    user_tenant.as_u64()
                ),
            )
        })?;
    for owner in &owned {
        let kind = OwnerKind::from_object_type(&owner.object_type).ok_or_else(|| {
            DdlError::internal(format!(
                "cannot reassign object of unknown owner type '{}' ('{}') owned by \
                 '{username}' — refusing to drop user to avoid a dangling owner reference",
                owner.object_type, owner.object_name
            ))
        })?;
        reassign_one(
            state,
            catalog,
            kind,
            owner.database_id,
            user_tenant,
            &owner.object_name,
            &admin_name,
        )
        .await?;
    }

    sweep_grants(state, catalog, username).await?;
    Ok(Some(admin_name))
}

/// Reassign a single owned object to `admin_name`. Re-proposes the
/// object's `Put<Kind>` catalog entry with the rewritten in-band owner.
/// Its apply rewrites the primary row, the `StoredOwner` row, and the
/// in-memory owner map on every node.
async fn reassign_one(
    state: &SharedState,
    catalog: &SystemCatalog,
    kind: OwnerKind,
    database_id: u64,
    tenant: TenantId,
    name: &str,
    admin_name: &str,
) -> Result<(), DdlError> {
    let tenant_id = tenant.as_u64();
    let object_type = kind.as_object_type();
    match kind {
        OwnerKind::Collection => {
            let database_id = nodedb_types::DatabaseId::new(database_id);
            let mut stored = catalog
                .get_collection(database_id, tenant_id, name)
                .map_err(object_error("get", object_type, name))?
                .ok_or_else(|| missing(object_type, name))?;
            stored.owner = admin_name.to_string();
            let entry = CatalogEntry::PutCollection(Box::new(stored));
            propose(state, &entry).await?;
        }
        OwnerKind::Function => {
            let mut s = catalog
                .get_function_in_database(
                    nodedb_types::DatabaseId::new(database_id),
                    tenant_id,
                    name,
                )
                .map_err(object_error("get", object_type, name))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutFunction(Box::new(s));
            propose(state, &entry).await?;
        }
        OwnerKind::Procedure => {
            let mut s = catalog
                .get_procedure_in_database(
                    nodedb_types::DatabaseId::new(database_id),
                    tenant_id,
                    name,
                )
                .map_err(object_error("get", object_type, name))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutProcedure(Box::new(s));
            propose(state, &entry).await?;
        }
        OwnerKind::Trigger => {
            let mut s = catalog
                .get_trigger_in_database(
                    nodedb_types::DatabaseId::new(database_id),
                    tenant_id,
                    name,
                )
                .map_err(object_error("get", object_type, name))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutTrigger(Box::new(s));
            propose(state, &entry).await?;
        }
        OwnerKind::MaterializedView => {
            let mut s = catalog
                .get_materialized_view(database_id, tenant_id, name)
                .map_err(object_error("get", object_type, name))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutMaterializedView(Box::new(s));
            propose(state, &entry).await?;
        }
        OwnerKind::StreamingMaterializedView => {
            let mut s = catalog
                .load_all_streaming_mvs()
                .map_err(|e| {
                    DdlError::from_error_in_context("load streaming materialized views", &e)
                })?
                .into_iter()
                .find(|mv| {
                    mv.database_id.as_u64() == database_id
                        && mv.tenant_id == tenant_id
                        && mv.name == name
                })
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutStreamingMaterializedView(Box::new(s));
            propose(state, &entry).await?;
        }
        OwnerKind::Sequence => {
            let mut s = catalog
                .get_sequence(database_id, tenant_id, name)
                .map_err(object_error("get", object_type, name))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutSequence(Box::new(s));
            propose(state, &entry).await?;
        }
        OwnerKind::Schedule => {
            // Schedules have no single-key getter; find within the tenant.
            let mut s = catalog
                .load_all_schedules()
                .map_err(|e| DdlError::from_error_in_context("load schedules", &e))?
                .into_iter()
                .find(|d| {
                    d.database_id == database_id && d.tenant_id == tenant_id && d.name == name
                })
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutSchedule(Box::new(s));
            propose(state, &entry).await?;
        }
        OwnerKind::ChangeStream => {
            let mut s = catalog
                .get_change_stream(crate::types::DatabaseId::new(database_id), tenant_id, name)
                .map_err(object_error("get", object_type, name))?
                .ok_or_else(|| missing(object_type, name))?;
            s.owner = admin_name.to_string();
            let entry = CatalogEntry::PutChangeStream(Box::new(s));
            propose(state, &entry).await?;
        }
        OwnerKind::ContinuousAggregate => {
            let mut stored = catalog
                .get_continuous_aggregate(database_id, tenant_id, name)
                .map_err(object_error("get", object_type, name))?
                .ok_or_else(|| missing(object_type, name))?;
            stored.owner = admin_name.to_string();
            let entry = CatalogEntry::PutContinuousAggregate(Box::new(stored));
            propose(state, &entry).await?;
        }
        OwnerKind::Index => {
            // Standalone owner row — the `StoredOwner` row is the whole
            // object, so there is no parent primary to re-propose.
            let stored = StoredOwner {
                database_id,
                object_type: object_type.to_string(),
                object_name: name.to_string(),
                tenant_id,
                owner_username: admin_name.to_string(),
            };
            let entry = CatalogEntry::PutOwner(Box::new(stored));
            propose(state, &entry).await?;
        }
    }
    Ok(())
}

/// Revoke every grant whose grantee is the dropped user, so no
/// `permission.grantee → user` reference outlives the user row.
pub(super) async fn sweep_grants(
    state: &SharedState,
    catalog: &SystemCatalog,
    username: &str,
) -> Result<(), DdlError> {
    let grantee = format!("user:{username}");
    let grants = catalog
        .load_all_permissions()
        .map_err(|e| DdlError::from_error_in_context("load permissions", &e))?;
    for grant in grants.into_iter().filter(|grant| grant.grantee == grantee) {
        let entry = CatalogEntry::DeletePermission {
            target: grant.target.clone(),
            grantee: grantee.clone(),
            permission: grant.permission.clone(),
        };
        propose(state, &entry).await?;
    }
    Ok(())
}

/// Propose `entry` and await its apply on this node, post-apply included.
pub(super) async fn propose(state: &SharedState, entry: &CatalogEntry) -> Result<(), DdlError> {
    propose_catalog_entry_async(state, entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    Ok(())
}

/// The error for a failed catalog read or write of one owned object. It keeps
/// the SQLSTATE of the typed error and names the object before its message.
fn object_error<'a>(
    op: &'a str,
    object_type: &'a str,
    name: &'a str,
) -> impl FnOnce(crate::Error) -> DdlError + 'a {
    move |error| DdlError::from_error_in_context(&format!("{op} {object_type} '{name}'"), &error)
}

fn missing(object_type: &str, name: &str) -> DdlError {
    DdlError::internal(format!(
        "owned {object_type} '{name}' has an owner row but no primary record — \
         cannot reassign; refusing to drop user"
    ))
}
