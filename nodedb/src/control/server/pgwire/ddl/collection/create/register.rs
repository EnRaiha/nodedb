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
) -> crate::Result<()> {
    let name = parts.get(2).map(|s| s.to_lowercase()).unwrap_or_default();
    let tenant_id = identity.tenant_id;

    let Some(catalog) = state.credentials.catalog() else {
        return Ok(());
    };
    let Ok(Some(coll)) = catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &name)
    else {
        return Ok(());
    };
    let (fields, _serial_fields) =
        super::super::super::schema_validation::parse_fields_clause(parts);
    let mut indexes = derive_auto_indexes(fields.iter().map(|(n, _)| n.as_str()));
    extend_with_catalog_indexes(&mut indexes, &coll);
    let _ = sql; // Reserved for future CRDT detection from SQL.
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
    let Some(catalog) = state.credentials.catalog() else {
        return Ok(());
    };
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
fn derive_auto_indexes<'a>(
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
fn extend_with_catalog_indexes(
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

async fn dispatch_register_from_stored_inner(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    coll: &StoredCollection,
    indexes: Vec<nodedb_physical::physical_plan::RegisteredIndex>,
) -> crate::Result<()> {
    let name = crate::control::planner::sql_plan_convert::convert::db_qualified(
        coll.database_id,
        &coll.name,
    );
    let Some(catalog) = state.credentials.catalog() else {
        return Ok(());
    };

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

    let crdt_enabled = false;

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
        ),
        generated_columns: build_generated_column_specs(coll),
    };

    let plan = crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::Register {
            collection: name.clone(),
            indexes,
            crdt_enabled,
            storage_mode,
            enforcement: Box::new(enforcement),
            bitemporal: coll.bitemporal,
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
