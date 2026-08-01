// SPDX-License-Identifier: BUSL-1.1

//! `CREATE TIMESERIES <name> [WITH (key = 'value', ...)]`

use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::helpers::{ddl_err, parse_column_defs, parse_with_clause};

/// CREATE TIMESERIES <name> [WITH (key = 'value', ...)]
pub fn create_timeseries(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    database_id: nodedb_types::DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 3 {
        return Err(ddl_err(
            "42601",
            "syntax: CREATE TIMESERIES <name> [WITH (key = 'value', ...)]",
        ));
    }

    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id;

    match state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), &name)
    {
        Ok(Some(_)) => {
            return Err(ddl_err(
                "42P07",
                format!("collection '{name}' already exists"),
            ));
        }
        Ok(None) => {}
        Err(error) => return Err(ddl_err("XX000", error.to_string())),
    }

    let config_json = parse_with_clause(parts);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Parse column definitions from CREATE TIMESERIES name (...) syntax.
    // Falls back to (timestamp, value) if no columns specified.
    let fields = parse_column_defs(parts).unwrap_or_else(|| {
        vec![
            ("timestamp".into(), "TIMESTAMP".into()),
            ("value".into(), "FLOAT".into()),
        ]
    });

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
        owner: identity.username.clone(),
        created_at: now,
        // Stamped by the metadata applier at commit time.
        descriptor_version: 0,
        constraint_version: 0,
        crdt_signing_required: false,
        modification_hlc: nodedb_types::Hlc::ZERO,
        fields,
        field_defs: Vec::new(),
        event_defs: Vec::new(),
        collection_type: nodedb_types::CollectionType::timeseries("timestamp", "1h"),
        timeseries_config: config_json,
        conflict_policy: None,
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
        crdt: false,
        permission_tree_def: None,
        indexes: Vec::new(),
        size_bytes_estimate: 0,
        primary: nodedb_types::PrimaryEngine::Columnar,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        database_id,
        cloned_from: None,
        clone_status: nodedb_types::CloneStatus::default(),
        has_implicit_edges: false,
        declared_primary_key: None,
    };

    crate::control::catalog_entry::persist_collection_replicated(state, database_id, &coll)
        .map_err(|e| ddl_err("XX000", e.to_string()))?;

    // Initialize partition registry for this timeseries collection.
    if let Some(registries) = state.timeseries_registries() {
        let config = nodedb_types::timeseries::TieredPartitionConfig::origin_defaults();
        let registry =
            crate::engine::timeseries::partition_registry::PartitionRegistry::new(config);
        let key = format!("{}:{}", tenant_id.as_u64(), name);
        let mut regs =
            crate::control::lock_utils::lock_or_recover(registries.lock(), "ts_registries");
        regs.insert(key, registry);
    }

    tracing::info!(
        collection = name,
        tenant = tenant_id.as_u64(),
        "timeseries collection created"
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE TIMESERIES".to_string(),
        rows_affected: None,
    }])
}
