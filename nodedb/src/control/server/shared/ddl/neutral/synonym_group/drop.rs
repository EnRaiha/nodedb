// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP SYNONYM GROUP` handler.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
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

    let tenant_id_u64 = identity.tenant_id.as_u64();
    let database_id_u64 = database_id.as_u64();

    if !state
        .synonym_registry
        .exists(database_id_u64, tenant_id_u64, name)
    {
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

    let catalog = state.credentials.catalog();

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteSynonymGroup {
        database_id: database_id_u64,
        tenant_id: tenant_id_u64,
        name: name.to_string(),
    };
    let outcome = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;

    // Single node: no applier runs, so post-apply never fires. Run the two
    // per-node effects the applier runs everywhere else — the catalog delete,
    // and the fan-out that removes the group from every core's FTS backend.
    if outcome.needs_local_apply() {
        catalog
            .delete_synonym_group(database_id_u64, tenant_id_u64, name)
            .map_err(|e| err("XX000", format!("catalog delete: {e}")))?;
        crate::control::catalog_entry::post_apply::remove_synonym_group(
            database_id_u64,
            tenant_id_u64,
            name.to_string(),
            state,
        )
        .await;
    }

    // Idempotent: the applier's synchronous post-apply already removed the
    // group on the replicated path.
    state
        .synonym_registry
        .unregister(database_id_u64, tenant_id_u64, name);

    Ok(vec![DdlResult::Status {
        command: "DROP SYNONYM GROUP".to_string(),
        rows_affected: None,
    }])
}
