// SPDX-License-Identifier: BUSL-1.1

//! `CREATE MATERIALIZED VIEW` handler — replicates through the
//! metadata raft group via `CatalogEntry::PutMaterializedView`.

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::{StoredCollection, StoredMaterializedView};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// `CREATE MATERIALIZED VIEW <name> ON <source> AS SELECT ... [WITH (...)]`
pub async fn create_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    source: &str,
    query_sql: &str,
    refresh_mode: &str,
) -> PgWireResult<Vec<Response>> {
    let name = name.to_string();
    let source = source.to_string();
    let query_sql = query_sql.to_string();
    let refresh_mode = refresh_mode.to_string();

    let tenant_id = identity.tenant_id;

    // Validate source collection exists.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &source) {
            Ok(Some(_)) => {}
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("source collection '{source}' does not exist"),
                ));
            }
        }

        if let Ok(Some(_)) = catalog.get_materialized_view(tenant_id.as_u64(), &name) {
            return Err(sqlstate_error(
                "42P07",
                &format!("materialized view '{name}' already exists"),
            ));
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let view = StoredMaterializedView {
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
        source: source.clone(),
        query_sql,
        refresh_mode,
        owner: identity.username.clone(),
        created_at: now,
        // Stamped by the metadata applier at commit time.
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    // Propose through the metadata raft group. Every node's
    // applier writes the definition to local `SystemCatalog` redb
    // so the view is visible cluster-wide. The refresh loop picks
    // it up on its next tick.
    let entry =
        crate::control::catalog_entry::CatalogEntry::PutMaterializedView(Box::new(view.clone()));
    super::super::catalog_propose::propose_and_apply(state, &entry)?;

    // Create the view's target collection so REFRESH can insert into it
    // and clients can SELECT from it like any other collection. Skipped
    // when a collection of the same name already exists (idempotent).
    let target_exists = match state.credentials.catalog() {
        Some(catalog) => matches!(
            catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &name),
            Ok(Some(c)) if c.is_active
        ),
        None => false,
    };
    if !target_exists {
        let target = StoredCollection {
            tenant_id: tenant_id.as_u64(),
            name: name.clone(),
            owner: identity.username.clone(),
            created_at: now,
            descriptor_version: 0,
            constraint_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
            fields: Vec::new(),
            field_defs: Vec::new(),
            event_defs: Vec::new(),
            collection_type: nodedb_types::CollectionType::document(),
            timeseries_config: None,
            is_active: true,
            append_only: false,
            hash_chain: false,
            balanced: None,
            last_chain_hash: None,
            period_lock: None,
            retention_period: None,
            legal_holds: Vec::new(),
            state_constraints: Vec::new(),
            transition_checks: Vec::new(),
            type_guards: Vec::new(),
            check_constraints: Vec::new(),
            materialized_sums: Vec::new(),
            lvc_enabled: false,
            bitemporal: false,
            permission_tree_def: None,
            indexes: Vec::new(),
            size_bytes_estimate: 0,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            database_id: nodedb_types::DatabaseId::DEFAULT,
            cloned_from: None,
            clone_status: nodedb_types::CloneStatus::default(),
            has_implicit_edges: false,
        };
        let coll_entry =
            crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(target.clone()));
        super::super::catalog_propose::propose_and_apply(state, &coll_entry)?;
        // Register the target with this node's Data Plane so writes
        // encode correctly and scans can find the collection.
        super::super::collection::dispatch_register_from_stored(state, &target)
            .await
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    tracing::info!(
        view = name,
        source,
        tenant = tenant_id.as_u64(),
        "materialized view created"
    );

    Ok(vec![Response::Execution(Tag::new(
        "CREATE MATERIALIZED VIEW",
    ))])
}
