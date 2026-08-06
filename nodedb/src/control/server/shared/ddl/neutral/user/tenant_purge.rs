// SPDX-License-Identifier: BUSL-1.1

//! Terminal tenant-administrator object purge used only by `DROP TENANT`.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredOwner, SystemCatalog};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::DdlError;
use super::reassign_owned::{OwnerKind, ddl_err, propose, sweep_grants};

/// Purge every object owned by the tenant administrator during `DROP TENANT`,
/// returning the number of owned objects deleted so the caller can record an
/// accurate audit trail for the destructive teardown.
pub(super) fn purge_owned_for_tenant_teardown(
    state: &SharedState,
    username: &str,
    tenant: TenantId,
) -> Result<usize, DdlError> {
    let catalog = state.credentials.catalog();
    let mut owned = catalog
        .owners_for_user(username, tenant.as_u64())
        .map_err(|e| ddl_err(format!("load owner rows: {e}")))?;
    owned.sort_by_key(|owner| owner.object_type == object_type::COLLECTION);
    let purged = owned.len();

    for owner in owned {
        let kind = OwnerKind::from_object_type(&owner.object_type).ok_or_else(|| {
            ddl_err(format!(
                "cannot delete object of unknown owner type '{}' ('{}') during tenant teardown",
                owner.object_type, owner.object_name
            ))
        })?;
        if kind == OwnerKind::Collection {
            purge_collection_rls_policies(state, catalog, tenant, &owner.object_name)?;
            purge_collection_redaction_policies(state, catalog, tenant, &owner.object_name)?;
        }
        let entry = teardown_delete_entry(kind, tenant, &owner);
        let log_index = propose(state, &entry)?;
        crate::control::catalog_entry::apply::local::apply_locally_if_needed(
            state, &entry, log_index,
        );
        // A `PurgeCollection` apply only deactivates the catalog row — the
        // durable owner/collection deletion and storage reclaim are the
        // post-apply half. On the clustered path the metadata applier schedules
        // that reclaim on every node; on the single-node path (`log_index == 0`)
        // there is no applier, so drive the same reclaim `drop.rs` runs inline.
        // Without this the teardown leaves the owner row and never reclaims the
        // collection's storage. Other owner kinds delete fully in their apply.
        if kind == OwnerKind::Collection && log_index == 0 {
            let purge_lsn = state.wal.next_lsn().as_u64();
            let reclaim = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(
                    crate::control::server::shared::ddl::neutral::collection::purge::hard_purge_collection(
                        state,
                        owner.database_id,
                        tenant.as_u64(),
                        &owner.object_name,
                        purge_lsn,
                        false,
                    ),
                )
            });
            reclaim.map_err(|failure| {
                ddl_err(format!(
                    "tenant teardown collection reclaim failed for '{}': {}",
                    owner.object_name, failure.error
                ))
            })?;
        }
        if log_index == 0 {
            if kind == OwnerKind::StreamingMaterializedView {
                state.mv_registry.unregister(
                    crate::types::DatabaseId::new(owner.database_id),
                    tenant.as_u64(),
                    &owner.object_name,
                );
            }
            state
                .permissions
                .install_replicated_remove_owner_in_database(
                    &owner.object_type,
                    owner.database_id,
                    tenant.as_u64(),
                    &owner.object_name,
                );
        }
    }
    sweep_grants(state, catalog, username)?;
    Ok(purged)
}

fn purge_collection_rls_policies(
    state: &SharedState,
    catalog: &SystemCatalog,
    tenant: TenantId,
    collection: &str,
) -> Result<(), DdlError> {
    let tenant_id = tenant.as_u64();
    let policies = catalog
        .load_all_rls_policies()
        .map_err(|e| ddl_err(format!("load RLS policies: {e}")))?;
    for policy in policies
        .into_iter()
        .filter(|policy| policy.tenant_id == tenant_id && policy.collection == collection)
    {
        let entry = CatalogEntry::DeleteRlsPolicy {
            tenant_id,
            collection: collection.to_string(),
            name: policy.name.clone(),
        };
        let log_index = propose(state, &entry)?;
        crate::control::catalog_entry::apply::local::apply_locally_if_needed(
            state, &entry, log_index,
        );
        if log_index == 0 {
            state
                .rls
                .install_replicated_drop_policy(tenant_id, collection, &policy.name);
        }
    }
    Ok(())
}

/// Delete every column-redaction policy bound to `collection`.
///
/// The twin of [`purge_collection_rls_policies`]: a policy left behind would
/// resurrect against a collection later re-created under the same name, since
/// its key carries no collection generation.
fn purge_collection_redaction_policies(
    state: &SharedState,
    catalog: &SystemCatalog,
    tenant: TenantId,
    collection: &str,
) -> Result<(), DdlError> {
    let tenant_id = tenant.as_u64();
    let roles = crate::control::cascade::redaction::find_redaction_policies_on(
        catalog, tenant_id, collection,
    )
    .map_err(|e| ddl_err(format!("load redaction policies: {e}")))?;
    for for_role in roles {
        let entry = CatalogEntry::DeleteRedactionPolicy {
            tenant_id,
            collection: collection.to_string(),
            for_role: for_role.clone(),
        };
        let log_index = propose(state, &entry)?;
        crate::control::catalog_entry::apply::local::apply_locally_if_needed(
            state, &entry, log_index,
        );
        if log_index == 0 {
            state
                .redaction
                .install_replicated_drop_policy(tenant_id, collection, &for_role);
        }
    }
    Ok(())
}

fn teardown_delete_entry(kind: OwnerKind, tenant: TenantId, owner: &StoredOwner) -> CatalogEntry {
    let tenant_id = tenant.as_u64();
    let name = owner.object_name.clone();
    match kind {
        OwnerKind::Collection => CatalogEntry::PurgeCollection {
            database_id: owner.database_id,
            tenant_id,
            name,
        },
        OwnerKind::Function => CatalogEntry::DeleteFunction {
            database_id: crate::types::DatabaseId::new(owner.database_id),
            tenant_id,
            name,
        },
        OwnerKind::Procedure => CatalogEntry::DeleteProcedure {
            database_id: crate::types::DatabaseId::new(owner.database_id),
            tenant_id,
            name,
        },
        OwnerKind::Trigger => CatalogEntry::DeleteTrigger {
            database_id: crate::types::DatabaseId::new(owner.database_id),
            tenant_id,
            name,
        },
        OwnerKind::MaterializedView => CatalogEntry::DeleteMaterializedView { tenant_id, name },
        OwnerKind::StreamingMaterializedView => CatalogEntry::DeleteStreamingMaterializedView {
            database_id: owner.database_id,
            tenant_id,
            name,
        },
        OwnerKind::Sequence => CatalogEntry::DeleteSequence { tenant_id, name },
        OwnerKind::Schedule => CatalogEntry::DeleteSchedule {
            database_id: crate::types::DatabaseId::new(owner.database_id),
            tenant_id,
            name,
        },
        OwnerKind::ChangeStream => CatalogEntry::DeleteChangeStream {
            database_id: owner.database_id,
            tenant_id,
            name,
        },
        OwnerKind::ContinuousAggregate => CatalogEntry::DeleteContinuousAggregate {
            database_id: owner.database_id,
            tenant_id,
            name,
        },
        OwnerKind::Index => CatalogEntry::DeleteOwner {
            object_type: owner.object_type.clone(),
            database_id: owner.database_id,
            tenant_id,
            object_name: name,
        },
    }
}
