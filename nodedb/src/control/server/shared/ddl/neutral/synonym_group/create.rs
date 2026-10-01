// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE SYNONYM GROUP` handler.

use crate::control::security::catalog::StoredSynonymGroup;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Handle `CREATE SYNONYM GROUP <name> AS ('term1', ...)`.
pub async fn create_synonym_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    terms: &[String],
) -> Result<Vec<DdlResult>, DdlError> {
    super::super::auth_support::require_tenant_admin(identity, "create synonym groups")?;

    let tenant_id_u64 = identity.tenant_id.as_u64();
    let database_id_u64 = database_id.as_u64();

    // Duplicate check via in-memory registry, scoped to this database.
    if state
        .synonym_registry
        .exists(database_id_u64, tenant_id_u64, name)
    {
        return Err(err(
            "42710",
            format!("synonym group '{name}' already exists"),
        ));
    }

    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError::internal("system clock error"))?
        .as_secs();

    let stored = StoredSynonymGroup {
        database_id: database_id_u64,
        tenant_id: tenant_id_u64,
        name: name.to_string(),
        terms: terms.to_vec(),
        created_at,
        // Frozen by the proposer's stamp.
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    // The apply registers the group and installs it in every core's FTS
    // backend in post-apply. A buffered group registers nothing until COMMIT.
    let entry = crate::control::catalog_entry::CatalogEntry::PutSynonymGroup(Box::new(stored));
    crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;

    Ok(vec![DdlResult::Status {
        command: "CREATE SYNONYM GROUP".to_string(),
        rows_affected: None,
    }])
}
