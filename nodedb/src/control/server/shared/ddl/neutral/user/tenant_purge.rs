// SPDX-License-Identifier: BUSL-1.1

//! Terminal tenant-administrator object purge used only by `DROP TENANT`.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredOwner, SystemCatalog};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::DdlError;
use super::owner_kind::OwnerKind;
use super::reassign_owned::{propose, sweep_grants};

/// Purge every object owned by the tenant administrator during `DROP TENANT`,
/// returning the number of owned objects deleted so the caller can record an
/// accurate audit trail for the destructive teardown.
pub(super) async fn purge_owned_for_tenant_teardown(
    state: &SharedState,
    username: &str,
    tenant: TenantId,
) -> Result<usize, DdlError> {
    let catalog = state.credentials.catalog();
    let mut owned = catalog
        .owners_for_user(username, tenant.as_u64())
        .map_err(|e| DdlError::from_error_in_context("load owner rows", &e))?;
    owned.sort_by_key(|owner| owner.object_type == object_type::COLLECTION);
    let purged = owned.len();

    for owner in owned {
        let kind = OwnerKind::from_object_type(&owner.object_type).ok_or_else(|| {
            DdlError::internal(format!(
                "cannot delete object of unknown owner type '{}' ('{}') during tenant teardown",
                owner.object_type, owner.object_name
            ))
        })?;
        if kind == OwnerKind::Collection {
            purge_collection_rls_policies(
                state,
                catalog,
                tenant,
                crate::types::DatabaseId::new(owner.database_id),
                &owner.object_name,
            )
            .await?;
            purge_collection_redaction_policies(
                state,
                catalog,
                tenant,
                crate::types::DatabaseId::new(owner.database_id),
                &owner.object_name,
            )
            .await?;
        }
        let entry = teardown_delete_entry(kind, tenant, &owner);
        propose(state, &entry).await?;
    }
    sweep_grants(state, catalog, username).await?;
    Ok(purged)
}

async fn purge_collection_rls_policies(
    state: &SharedState,
    catalog: &SystemCatalog,
    tenant: TenantId,
    database_id: crate::types::DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
    let tenant_id = tenant.as_u64();
    // RLS policies are keyed by `db_qualified(database_id, collection)`, not
    // the bare collection name the owner catalog carries — match on that.
    let qualified_collection =
        crate::control::planner::sql_plan_convert::convert::db_qualified(database_id, collection);
    let policies = catalog
        .load_all_rls_policies()
        .map_err(|e| DdlError::from_error_in_context("load RLS policies", &e))?;
    for policy in policies
        .into_iter()
        .filter(|policy| policy.tenant_id == tenant_id && policy.collection == qualified_collection)
    {
        let entry = CatalogEntry::DeleteRlsPolicy {
            tenant_id,
            collection: qualified_collection.clone(),
            name: policy.name.clone(),
        };
        propose(state, &entry).await?;
    }
    Ok(())
}

/// Delete every column-redaction policy bound to `collection`.
///
/// The twin of [`purge_collection_rls_policies`]: a policy left behind will
/// resurrect against a collection later re-created under the same name, since
/// its key carries no collection generation.
async fn purge_collection_redaction_policies(
    state: &SharedState,
    catalog: &SystemCatalog,
    tenant: TenantId,
    database_id: crate::types::DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
    let tenant_id = tenant.as_u64();
    // Redaction policies are keyed by `db_qualified(database_id, collection)`,
    // not the bare collection name the owner catalog carries — match on that.
    let qualified_collection =
        crate::control::planner::sql_plan_convert::convert::db_qualified(database_id, collection);
    let roles = crate::control::cascade::redaction::find_redaction_policies_on(
        catalog,
        database_id,
        tenant_id,
        collection,
    )
    .map_err(|e| DdlError::from_error_in_context("load redaction policies", &e))?;
    for for_role in roles {
        let entry = CatalogEntry::DeleteRedactionPolicy {
            tenant_id,
            collection: qualified_collection.clone(),
            for_role: for_role.clone(),
        };
        propose(state, &entry).await?;
    }
    Ok(())
}

/// Fenced deletes carry an unstamped target. The proposer freezes it.
fn teardown_delete_entry(kind: OwnerKind, tenant: TenantId, owner: &StoredOwner) -> CatalogEntry {
    let tenant_id = tenant.as_u64();
    let name = owner.object_name.clone();
    match kind {
        OwnerKind::Collection => CatalogEntry::PurgeCollection {
            database_id: owner.database_id,
            tenant_id,
            name,
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        },
        OwnerKind::Function => CatalogEntry::DeleteFunction {
            database_id: crate::types::DatabaseId::new(owner.database_id),
            tenant_id,
            name,
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        },
        OwnerKind::Procedure => CatalogEntry::DeleteProcedure {
            database_id: crate::types::DatabaseId::new(owner.database_id),
            tenant_id,
            name,
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        },
        OwnerKind::Trigger => CatalogEntry::DeleteTrigger {
            database_id: crate::types::DatabaseId::new(owner.database_id),
            tenant_id,
            name,
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        },
        OwnerKind::MaterializedView => CatalogEntry::DeleteMaterializedView {
            database_id: owner.database_id,
            tenant_id,
            name,
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        },
        OwnerKind::StreamingMaterializedView => CatalogEntry::DeleteStreamingMaterializedView {
            database_id: owner.database_id,
            tenant_id,
            name,
        },
        OwnerKind::Sequence => CatalogEntry::DeleteSequence {
            database_id: owner.database_id,
            tenant_id,
            name,
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        },
        OwnerKind::Schedule => CatalogEntry::DeleteSchedule {
            database_id: crate::types::DatabaseId::new(owner.database_id),
            tenant_id,
            name,
        },
        OwnerKind::ChangeStream => CatalogEntry::DeleteChangeStream {
            database_id: owner.database_id,
            tenant_id,
            name,
            target_hlc: nodedb_types::Hlc::ZERO,
        },
        OwnerKind::ContinuousAggregate => CatalogEntry::DeleteContinuousAggregate {
            database_id: owner.database_id,
            tenant_id,
            name,
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        },
        OwnerKind::Index => CatalogEntry::DeleteOwner {
            object_type: owner.object_type.clone(),
            database_id: owner.database_id,
            tenant_id,
            object_name: name,
        },
    }
}
