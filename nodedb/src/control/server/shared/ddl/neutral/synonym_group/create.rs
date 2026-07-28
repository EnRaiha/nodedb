// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE SYNONYM GROUP` handler.

use std::time::Duration;

use nodedb_fts::SynonymGroupRecord;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::StoredSynonymGroup;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sync_dispatch::{
    SystemReason, SystemTask, dispatch_system,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::super::result::{DdlError, DdlResult};

/// Sentinel collection name used for routing synonym group MetaOp dispatches.
///
/// Synonym groups are global to the tenant (not collection-bound).
/// Routes via `VShardId::from_collection_in_database` on the default database;
/// any stable name works and `_synonym_groups` is descriptive.
pub(super) const SYNONYM_SENTINEL_COLLECTION: &str = "_synonym_groups";

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
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

    let tenant_id = identity.tenant_id;
    let tenant_id_u64 = tenant_id.as_u64();

    // Duplicate check via in-memory registry.
    if state.synonym_registry.exists(tenant_id_u64, name) {
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
        tenant_id: tenant_id_u64,
        name: name.to_string(),
        terms: terms.to_vec(),
        created_at,
    };

    // Persist to catalog.
    let catalog = state.credentials.catalog();

    let entry =
        crate::control::catalog_entry::CatalogEntry::PutSynonymGroup(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .put_synonym_group(&stored)
            .map_err(|e| err("XX000", format!("catalog write: {e}")))?;
    }

    // Update in-memory registry.
    state.synonym_registry.register(stored.clone());

    // Push to Data Plane FTS backend (all shards via collection-independent dispatch).
    let fts_record = SynonymGroupRecord {
        name: stored.name.clone(),
        terms: stored.terms.clone(),
        created_at: stored.created_at,
    };
    let record_json = sonic_rs::to_string(&fts_record)
        .map_err(|e| err("XX000", format!("serialize synonym group: {e}")))?;

    let plan = PhysicalPlan::Meta(MetaOp::PutSynonymGroup {
        tenant_id: tenant_id_u64,
        record_json,
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
        command: "CREATE SYNONYM GROUP".to_string(),
        rows_affected: None,
    }])
}
