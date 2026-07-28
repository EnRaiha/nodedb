// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP SYNONYM GROUP` handler.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sync_dispatch::{
    SystemReason, SystemTask, dispatch_system,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::super::result::{DdlError, DdlResult};
use super::create::SYNONYM_SENTINEL_COLLECTION;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Handle `DROP SYNONYM GROUP [IF EXISTS] <name>`.
pub async fn drop_synonym_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    if_exists: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    super::super::auth_support::require_tenant_admin(identity, "drop synonym groups")?;

    let tenant_id = identity.tenant_id;
    let tenant_id_u64 = tenant_id.as_u64();

    if !state.synonym_registry.exists(tenant_id_u64, name) {
        if if_exists {
            return Ok(vec![DdlResult::Status {
                command: "DROP SYNONYM GROUP".to_string(),
                rows_affected: None,
            }]);
        }
        return Err(err(
            "42704",
            format!("synonym group '{name}' does not exist"),
        ));
    }

    // Remove from catalog.
    let catalog = state.credentials.catalog();

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteSynonymGroup {
        tenant_id: tenant_id_u64,
        name: name.to_string(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .delete_synonym_group(tenant_id_u64, name)
            .map_err(|e| err("XX000", format!("catalog delete: {e}")))?;
    }

    // Remove from in-memory registry.
    state.synonym_registry.unregister(tenant_id_u64, name);

    // Remove from Data Plane FTS backend.
    let plan = PhysicalPlan::Meta(MetaOp::DeleteSynonymGroup {
        tenant_id: tenant_id_u64,
        name: name.to_string(),
    });

    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    dispatch_system(
        state,
        SystemTask::new(
            SystemReason::CatalogMaintenance,
            tenant_id,
            database_id,
            SYNONYM_SENTINEL_COLLECTION,
            plan,
        ),
        timeout,
    )
    .await
    .map_err(|e| err("XX000", format!("data plane dispatch: {e}")))?;

    Ok(vec![DdlResult::Status {
        command: "DROP SYNONYM GROUP".to_string(),
        rows_affected: None,
    }])
}
