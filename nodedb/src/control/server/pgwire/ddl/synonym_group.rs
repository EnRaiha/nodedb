// SPDX-License-Identifier: BUSL-1.1

//! pgwire handlers for synonym group DDL.
//!
//! - `CREATE SYNONYM GROUP <name> AS ('term1', 'term2', ...)`
//! - `DROP SYNONYM GROUP [IF EXISTS] <name>`
//! - `SHOW SYNONYM GROUPS`
//!
//! Synonym groups are tenant-scoped metadata that control query-time synonym
//! expansion in the FTS engine. When a query token matches any term in a
//! synonym group, all other terms in the group are added to the query
//! (OR-expansion semantics).

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use nodedb_fts::SynonymGroupRecord;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::StoredSynonymGroup;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::types::{require_tenant_admin, sqlstate_error, text_field};
use super::sync_dispatch::dispatch_async;

/// Handle `CREATE SYNONYM GROUP <name> AS ('term1', ...)`.
pub async fn create_synonym_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    terms: &[String],
) -> PgWireResult<Vec<Response>> {
    require_tenant_admin(identity, "create synonym groups")?;

    let tenant_id = identity.tenant_id;
    let tenant_id_u64 = tenant_id.as_u64();

    // Duplicate check via in-memory registry.
    if state.synonym_registry.exists(tenant_id_u64, name) {
        return Err(sqlstate_error(
            "42710",
            &format!("synonym group '{name}' already exists"),
        ));
    }

    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock error"))?
        .as_secs();

    let stored = StoredSynonymGroup {
        tenant_id: tenant_id_u64,
        name: name.to_string(),
        terms: terms.to_vec(),
        created_at,
    };

    // Persist to catalog.
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let entry =
        crate::control::catalog_entry::CatalogEntry::PutSynonymGroup(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .put_synonym_group(&stored)
            .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
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
        .map_err(|e| sqlstate_error("XX000", &format!("serialize synonym group: {e}")))?;

    let plan = PhysicalPlan::Meta(MetaOp::PutSynonymGroup {
        tenant_id: tenant_id_u64,
        record_json,
    });

    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    dispatch_async(
        state,
        tenant_id,
        database_id,
        SYNONYM_SENTINEL_COLLECTION,
        plan,
        timeout,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("data plane dispatch: {e}")))?;

    Ok(vec![Response::Execution(Tag::new("CREATE SYNONYM GROUP"))])
}

/// Handle `DROP SYNONYM GROUP [IF EXISTS] <name>`.
pub async fn drop_synonym_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    if_exists: bool,
) -> PgWireResult<Vec<Response>> {
    require_tenant_admin(identity, "drop synonym groups")?;

    let tenant_id = identity.tenant_id;
    let tenant_id_u64 = tenant_id.as_u64();

    if !state.synonym_registry.exists(tenant_id_u64, name) {
        if if_exists {
            return Ok(vec![Response::Execution(Tag::new("DROP SYNONYM GROUP"))]);
        }
        return Err(sqlstate_error(
            "42704",
            &format!("synonym group '{name}' does not exist"),
        ));
    }

    // Remove from catalog.
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteSynonymGroup {
        tenant_id: tenant_id_u64,
        name: name.to_string(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .delete_synonym_group(tenant_id_u64, name)
            .map_err(|e| sqlstate_error("XX000", &format!("catalog delete: {e}")))?;
    }

    // Remove from in-memory registry.
    state.synonym_registry.unregister(tenant_id_u64, name);

    // Remove from Data Plane FTS backend.
    let plan = PhysicalPlan::Meta(MetaOp::DeleteSynonymGroup {
        tenant_id: tenant_id_u64,
        name: name.to_string(),
    });

    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    dispatch_async(
        state,
        tenant_id,
        database_id,
        SYNONYM_SENTINEL_COLLECTION,
        plan,
        timeout,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("data plane dispatch: {e}")))?;

    Ok(vec![Response::Execution(Tag::new("DROP SYNONYM GROUP"))])
}

/// Handle `SHOW SYNONYM GROUPS`.
pub fn show_synonym_groups(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id_u64 = identity.tenant_id.as_u64();
    let groups = state.synonym_registry.list_for_tenant(tenant_id_u64);

    let schema = Arc::new(vec![text_field("name"), text_field("terms")]);
    let mut rows = Vec::new();
    for g in &groups {
        let mut enc = DataRowEncoder::new(schema.clone());
        enc.encode_field(&g.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        let terms_csv = g.terms.join(", ");
        enc.encode_field(&terms_csv)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(enc.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Sentinel collection name used for routing synonym group MetaOp dispatches.
///
/// Synonym groups are global to the tenant (not collection-bound).
/// Routes via `VShardId::from_collection_in_database` on the default database;
/// any stable name works and `_synonym_groups` is descriptive.
const SYNONYM_SENTINEL_COLLECTION: &str = "_synonym_groups";
