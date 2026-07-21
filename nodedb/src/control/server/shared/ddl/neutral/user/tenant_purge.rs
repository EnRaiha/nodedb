// SPDX-License-Identifier: BUSL-1.1

//! Terminal tenant-administrator object purge used only by `DROP TENANT`.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredOwner, SystemCatalog};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::DdlError;
use super::reassign_owned::{OwnerKind, ddl_err, propose, sweep_grants};

pub(super) fn purge_owned_for_tenant_teardown(
    state: &SharedState,
    username: &str,
    tenant: TenantId,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    let mut owned = catalog
        .owners_for_user(username, tenant.as_u64())
        .map_err(|e| ddl_err(format!("load owner rows: {e}")))?;
    owned.sort_by_key(|owner| owner.object_type == object_type::COLLECTION);

    for owner in owned {
        let kind = OwnerKind::from_object_type(&owner.object_type).ok_or_else(|| {
            ddl_err(format!(
                "cannot delete object of unknown owner type '{}' ('{}') during tenant teardown",
                owner.object_type, owner.object_name
            ))
        })?;
        if kind == OwnerKind::Collection {
            purge_collection_rls_policies(state, catalog, tenant, &owner.object_name)?;
        }
        let entry = teardown_delete_entry(kind, tenant, &owner);
        let log_index = propose(state, &entry)?;
        crate::control::catalog_entry::apply::local::apply_locally_if_needed(
            state, &entry, log_index,
        );
        if log_index == 0 {
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
    sweep_grants(state, catalog, username)
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

fn teardown_delete_entry(kind: OwnerKind, tenant: TenantId, owner: &StoredOwner) -> CatalogEntry {
    let tenant_id = tenant.as_u64();
    let name = owner.object_name.clone();
    match kind {
        OwnerKind::Collection => CatalogEntry::PurgeCollection {
            database_id: owner.database_id,
            tenant_id,
            name,
        },
        OwnerKind::Function => CatalogEntry::DeleteFunction { tenant_id, name },
        OwnerKind::Procedure => CatalogEntry::DeleteProcedure { tenant_id, name },
        OwnerKind::Trigger => CatalogEntry::DeleteTrigger { tenant_id, name },
        OwnerKind::MaterializedView => CatalogEntry::DeleteMaterializedView { tenant_id, name },
        OwnerKind::Sequence => CatalogEntry::DeleteSequence { tenant_id, name },
        OwnerKind::Schedule => CatalogEntry::DeleteSchedule { tenant_id, name },
        OwnerKind::ChangeStream => CatalogEntry::DeleteChangeStream { tenant_id, name },
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
