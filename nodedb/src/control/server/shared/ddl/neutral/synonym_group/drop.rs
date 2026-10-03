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

    // The apply unregisters the group and removes it from every core's FTS
    // backend in post-apply. A buffered drop removes nothing until COMMIT.
    let entry = crate::control::catalog_entry::CatalogEntry::DeleteSynonymGroup {
        database_id: database_id_u64,
        tenant_id: tenant_id_u64,
        name: name.to_string(),
        // Frozen by the proposer's stamp.
        target_hlc: nodedb_types::Hlc::ZERO,
    };
    crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;

    Ok(vec![DdlResult::Status {
        command: "DROP SYNONYM GROUP".to_string(),
        rows_affected: None,
    }])
}
