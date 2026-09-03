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
        .map_err(|_| err("XX000", "system clock error".to_string()))?
        .as_secs();

    let stored = StoredSynonymGroup {
        database_id: database_id_u64,
        tenant_id: tenant_id_u64,
        name: name.to_string(),
        terms: terms.to_vec(),
        created_at,
    };

    let catalog = state.credentials.catalog();

    let entry =
        crate::control::catalog_entry::CatalogEntry::PutSynonymGroup(Box::new(stored.clone()));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;

    // Single node: no applier runs, so post-apply never fires. Run the two
    // per-node effects the applier runs everywhere else — the catalog write,
    // and the fan-out that installs the group in every core's FTS backend.
    if outcome.needs_local_apply() {
        catalog
            .put_synonym_group(&stored)
            .map_err(|e| err("XX000", format!("catalog write: {e}")))?;
        crate::control::catalog_entry::post_apply::install_synonym_group(stored.clone(), state)
            .await;
    }

    // Idempotent: the applier's synchronous post-apply already registered the
    // group on the replicated path.
    state.synonym_registry.register(stored);

    Ok(vec![DdlResult::Status {
        command: "CREATE SYNONYM GROUP".to_string(),
        rows_affected: None,
    }])
}
