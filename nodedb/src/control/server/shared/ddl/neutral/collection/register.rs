// SPDX-License-Identifier: BUSL-1.1

//! Dispatch `DocumentOp::Register` to this node's Data Plane
//! after a collection has been committed.
//!
//! Two entry points:
//! - [`dispatch_register_if_needed`] — leader-side, called from
//!   the pgwire handler path. Parses the FIELDS clause from
//!   `parts` to derive index paths.
//! - [`dispatch_register_from_stored`] — applier-side, called
//!   from the metadata applier's post-apply hook after a
//!   `CatalogEntry::PutCollection` commits. Derives index paths
//!   from `coll.fields`.
//!
//! Both funnel into [`dispatch_register_from_stored_inner`]
//! which builds the storage-mode + enforcement-options
//! `EnforcementOptions` value and dispatches to the Data Plane.
//!
//! Relocated verbatim from the pgwire
//! `pgwire::ddl::collection::create::register` module (now deleted) so the
//! neutral `continuous_agg` / `materialized_view` families, the
//! `catalog_entry::post_apply` hook, and the (still pgwire) `alter`
//! handlers can all depend on a protocol-neutral home instead of reaching
//! across the pgwire boundary.

use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TraceId;
use nodedb_types::DatabaseId;

use super::enforcement::{build_generated_column_specs, find_materialized_sum_bindings};

/// Dispatch a `DocumentOp::Register` to the Data Plane after
/// collection creation (leader-side pgwire path). Looks up the
/// just-created collection from catalog and parses the FIELDS
/// clause from `parts` for index paths.
///
/// Returns an error if any Data Plane core fails to acknowledge the
/// registration — the caller must not return DDL success to the client
/// until every core has applied the new schema.
pub async fn dispatch_register_if_needed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
    database_id: DatabaseId,
) -> crate::Result<()> {
    let name = parts.get(2).map(|s| s.to_lowercase()).unwrap_or_default();
    let tenant_id = identity.tenant_id;

    let catalog = state.credentials.catalog();
    let Ok(Some(coll)) = catalog.get_collection(database_id, tenant_id.as_u64(), &name) else {
        return Ok(());
    };
    let (fields, _serial_fields) =
        crate::control::server::shared::ddl::schema_validation::parse_fields_clause(parts);
    let mut indexes = derive_auto_indexes(fields.iter().map(|(n, _)| n.as_str()));
    extend_with_catalog_indexes(&mut indexes, &coll);
    // `sql` is unused on this leader-side path: index derivation reads
    // `parts`/the catalog row directly, and the `crdt` flag already
    // travels on `StoredCollection` (set at CREATE time from `WITH
    // (crdt=...)`), so no SQL re-parsing is needed here.
    let _ = sql;
    dispatch_register_from_stored_inner(state, tenant_id, &coll, indexes).await
}

/// Typed leader-side entry point: dispatch `DocumentOp::Register`
/// after collection creation when the collection name is known but
/// no raw SQL parts are available (typed AST path).
///
/// `database_id` must match the database the collection was created in so the
/// catalog lookup succeeds in non-default databases.
///
/// Returns an error if any Data Plane core fails to acknowledge the
/// registration.
pub async fn dispatch_register_by_name(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database_id: DatabaseId,
) -> crate::Result<()> {
    let tenant_id = identity.tenant_id;
    let catalog = state.credentials.catalog();
    let Ok(Some(coll)) = catalog.get_collection(database_id, tenant_id.as_u64(), name) else {
        return Ok(());
    };
    let mut indexes = derive_auto_indexes(coll.fields.iter().map(|(n, _)| n.as_str()));
    extend_with_catalog_indexes(&mut indexes, &coll);
    dispatch_register_from_stored_inner(state, tenant_id, &coll, indexes).await
}

/// Applier-side entry point: dispatch `DocumentOp::Register` using
/// a fully-populated [`StoredCollection`]. Called from the
/// production `MetadataCommitApplier` after it materializes a
/// replicated `CatalogEntry::PutCollection` into local
/// `SystemCatalog` redb, so every follower's Data Plane knows
/// about the collection before the first cross-node INSERT
/// arrives.
///
/// Returns an error if any Data Plane core fails to acknowledge the
/// registration — the post-apply hook must not bump the applied-index
/// watcher until every core carries the new schema.
pub async fn dispatch_register_from_stored(
    state: &SharedState,
    coll: &StoredCollection,
) -> crate::Result<()> {
    let tenant_id = crate::types::TenantId::new(coll.tenant_id);
    let mut indexes = derive_auto_indexes(coll.fields.iter().map(|(n, _)| n.as_str()));
    extend_with_catalog_indexes(&mut indexes, coll);
    dispatch_register_from_stored_inner(state, tenant_id, coll, indexes).await
}

/// Per-field auto-derived indexes (schemaless default: each declared field
/// becomes a non-unique `$.field` index). Always `Ready` — these exist
/// from the moment the collection is created.
///
/// `pub(crate)` so the boot-time `doc_configs` seed loader
/// ([`crate::bootstrap::data_plane::load_doc_config_registry`]) can derive
/// the same index set as the live-DDL path without drift.
pub(crate) fn derive_auto_indexes<'a>(
    field_names: impl IntoIterator<Item = &'a str>,
) -> Vec<nodedb_physical::physical_plan::RegisteredIndex> {
    field_names
        .into_iter()
        .map(|n| nodedb_physical::physical_plan::RegisteredIndex {
            name: n.to_string(),
            path: format!("$.{n}"),
            unique: false,
            case_insensitive: false,
            state: nodedb_physical::physical_plan::RegisteredIndexState::Ready,
            predicate: None,
        })
        .collect()
}

/// Append explicit `CREATE INDEX` entries from the catalog. When an
/// explicit catalog index shares a path with an auto-derived one, the
/// catalog entry supersedes the auto-derived one: UNIQUE/COLLATE
/// modifiers have to take effect.
pub(crate) fn extend_with_catalog_indexes(
    out: &mut Vec<nodedb_physical::physical_plan::RegisteredIndex>,
    coll: &StoredCollection,
) {
    for idx in &coll.indexes {
        let state = match idx.state {
            crate::control::security::catalog::IndexBuildState::Building => {
                nodedb_physical::physical_plan::RegisteredIndexState::Building
            }
            crate::control::security::catalog::IndexBuildState::Ready => {
                nodedb_physical::physical_plan::RegisteredIndexState::Ready
            }
        };
        let spec = nodedb_physical::physical_plan::RegisteredIndex {
            name: idx.name.clone(),
            path: idx.field.clone(),
            unique: idx.unique,
            case_insensitive: idx.case_insensitive,
            state,
            predicate: idx.predicate.clone(),
        };
        if let Some(existing) = out.iter_mut().find(|e| e.path == spec.path) {
            *existing = spec;
        } else {
            out.push(spec);
        }
    }
}

/// Extract the declared timeseries shape — column list plus designated
/// `TIME_KEY` — from a stored collection, or `None` for every other engine.
///
/// The time key is read from the persisted `ColumnarProfile::Timeseries`
/// rather than re-derived from the column list, so the name the Data Plane
/// uses is always the one DDL resolved and the catalog recorded.
fn build_timeseries_schema(
    coll: &StoredCollection,
) -> Option<Box<nodedb_physical::physical_plan::TimeseriesSchema>> {
    let nodedb_types::CollectionType::Columnar(nodedb_types::ColumnarProfile::Timeseries {
        time_key,
        ..
    }) = &coll.collection_type
    else {
        return None;
    };
    Some(Box::new(nodedb_physical::physical_plan::TimeseriesSchema {
        time_key: time_key.clone(),
        columns: coll.fields.clone(),
    }))
}

/// Build the `CollectionConfig` a `DocumentOp::Register` would install in
/// `doc_configs`, straight from the durable catalog — storage mode,
/// enforcement options, generated columns, and secondary indexes.
///
/// Shared by two callers that must never drift apart:
/// - [`dispatch_register_from_stored_inner`] (live DDL / applier path):
///   derives the same fields to build the `DocumentOp::Register` plan
///   broadcast to Data Plane cores.
/// - [`crate::bootstrap::data_plane::load_doc_config_registry`] (boot
///   path): seeds every core's `doc_configs` synchronously, before WAL
///   redo replay runs, so strict collections re-encode Binary Tuple
///   instead of falling through to the raw-MessagePack fallback.
pub(crate) fn build_doc_config_from_stored(
    catalog: &crate::control::security::catalog::SystemCatalog,
    tenant_id: crate::types::TenantId,
    coll: &StoredCollection,
    indexes: &[nodedb_physical::physical_plan::RegisteredIndex],
) -> crate::engine::document::store::CollectionConfig {
    let name = crate::control::planner::sql_plan_convert::convert::db_qualified(
        coll.database_id,
        &coll.name,
    );

    // Determine storage mode from collection type — exhaustive
    // match ensures new CollectionType variants get a compile
    // error here.
    let storage_mode = match &coll.collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            nodedb_physical::physical_plan::StorageMode::Strict {
                schema: schema.clone(),
            }
        }
        nodedb_types::CollectionType::KeyValue(config) => {
            nodedb_physical::physical_plan::StorageMode::Strict {
                schema: config.schema.clone(),
            }
        }
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        | nodedb_types::CollectionType::Columnar(_) => {
            nodedb_physical::physical_plan::StorageMode::Schemaless
        }
    };

    let enforcement = nodedb_physical::physical_plan::EnforcementOptions {
        append_only: coll.append_only,
        hash_chain: coll.hash_chain,
        balanced: coll
            .balanced
            .as_ref()
            .map(|b| nodedb_physical::physical_plan::BalancedDef {
                group_key_column: b.group_key_column.clone(),
                entry_type_column: b.entry_type_column.clone(),
                debit_value: b.debit_value.clone(),
                credit_value: b.credit_value.clone(),
                amount_column: b.amount_column.clone(),
            }),
        period_lock: coll.period_lock.as_ref().map(|pl| {
            nodedb_physical::physical_plan::PeriodLockConfig {
                period_column: pl.period_column.clone(),
                ref_table: pl.ref_table.clone(),
                ref_pk: pl.ref_pk.clone(),
                status_column: pl.status_column.clone(),
                allowed_statuses: pl.allowed_statuses.clone(),
            }
        }),
        retention: coll.retention_period.as_ref().and_then(|s| {
            crate::data::executor::enforcement::retention::parse_retention_period(s).ok()
        }),
        has_legal_hold: !coll.legal_holds.is_empty(),
        state_constraints: coll.state_constraints.clone(),
        transition_checks: coll.transition_checks.clone(),
        materialized_sum_sources: find_materialized_sum_bindings(
            catalog,
            tenant_id.as_u64(),
            &name,
            coll.database_id,
        ),
        generated_columns: build_generated_column_specs(coll),
    };

    let mut config = crate::engine::document::store::CollectionConfig::new(&name);
    config.crdt_enabled = coll.crdt;
    config.storage_mode = storage_mode;
    config.enforcement = enforcement;
    config.bitemporal = coll.bitemporal;
    config.conflict_policy = coll.conflict_policy.clone();
    config.timeseries = build_timeseries_schema(coll);
    config.index_paths = indexes
        .iter()
        .map(crate::engine::document::store::IndexPath::from_registered)
        .collect();
    config
}

async fn dispatch_register_from_stored_inner(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    coll: &StoredCollection,
    indexes: Vec<nodedb_physical::physical_plan::RegisteredIndex>,
) -> crate::Result<()> {
    let catalog = state.credentials.catalog();
    let config = build_doc_config_from_stored(catalog, tenant_id, coll, &indexes);

    let plan = crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::Register {
            collection: config.name.clone(),
            indexes,
            crdt_enabled: config.crdt_enabled,
            storage_mode: config.storage_mode,
            enforcement: Box::new(config.enforcement),
            bitemporal: config.bitemporal,
            conflict_policy: config.conflict_policy.clone(),
            timeseries: config.timeseries.clone(),
        },
    );

    crate::control::server::broadcast::broadcast_register_to_all_cores(
        state,
        tenant_id,
        coll.database_id,
        plan,
        TraceId::ZERO,
    )
    .await
}
