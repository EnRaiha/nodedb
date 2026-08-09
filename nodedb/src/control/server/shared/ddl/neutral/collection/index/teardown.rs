// SPDX-License-Identifier: BUSL-1.1

//! Per-kind index teardown.
//!
//! Each index kind establishes different state at CREATE time, so each needs
//! its own removal:
//!
//! | kind      | durable state created                              |
//! |-----------|----------------------------------------------------|
//! | secondary | `StoredCollection.indexes` entry + sparse-engine index entries |
//! | vector    | `_system.vector_index_params` row + Data Plane index + checkpoint |
//! | fulltext  | the collection's analyzer / fuzzy binding (per collection) |
//! | spatial   | none beyond the registry + ownership rows           |
//! | sparse    | none beyond the registry + ownership rows           |
//! | sorted    | an order-statistic tree on the core holding the collection's rows |
//!
//! Every failure here propagates. A teardown that logs and continues would
//! report a successful drop over state that is still live — the same silent
//! success that made a vector index undroppable in the first place.

use crate::control::security::catalog::{IndexKind, StoredIndexRecord};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

use super::super::super::super::result::DdlError;
use super::commit::{commit_collection_mutation, err};

/// Remove every piece of engine and catalog state belonging to `record`,
/// except the registry and ownership rows the caller removes afterwards.
pub(super) async fn teardown(
    state: &SharedState,
    record: &StoredIndexRecord,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    match record.kind {
        IndexKind::Secondary => secondary(state, record, database_id, tenant_id).await,
        IndexKind::Vector => vector(state, record, database_id, tenant_id).await,
        IndexKind::FullText => fulltext(state, record, database_id, tenant_id).await,
        // A spatial or sparse index declares no durable engine state of its
        // own: `CREATE SPATIAL INDEX` / `CREATE SPARSE INDEX` register the
        // index and bind nothing in the Data Plane, and the R-tree / sparse
        // posting structures are built per collection from the rows
        // themselves. Removing the registry and ownership rows is the whole
        // teardown.
        IndexKind::Spatial | IndexKind::Sparse => Ok(()),
        // A sorted index owns an order-statistic tree on the core that holds
        // its collection's rows — the same core that maintains it on every
        // write — so its removal goes through the same route
        // `DROP SORTED INDEX` uses.
        IndexKind::Sorted => {
            super::super::super::kv_sorted_index::drop_in_engine(
                state,
                &super::super::super::kv_sorted_index::SortedIndexTarget {
                    tenant_id,
                    database_id,
                    collection: &record.collection,
                },
                &record.name,
            )
            .await
        }
    }
}

/// Drop the `StoredIndex` entry from the owning collection and purge the
/// sparse engine's entries for the indexed path.
async fn secondary(
    state: &SharedState,
    record: &StoredIndexRecord,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    let Some(mut coll) = catalog
        .get_collection(database_id, tenant_id.as_u64(), &record.collection)
        .map_err(|e| err("XX000", e.to_string()))?
    else {
        // The registry outlived its collection — the collection teardown
        // path already reclaimed every engine surface, so there is nothing
        // left to remove here.
        return Ok(());
    };

    let dropped_field = coll
        .indexes
        .iter()
        .find(|i| i.name == record.name)
        .map(|i| i.field.clone());
    coll.indexes.retain(|i| i.name != record.name);
    commit_collection_mutation(state, &coll, database_id).await?;

    // Purge existing index entries from the sparse engine so stale rows
    // cannot leak into lookups on a re-created index of the same name.
    let Some(field) = dropped_field.or_else(|| record.fields.first().cloned()) else {
        return Ok(());
    };
    let plan = crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::DropIndex {
            collection: record.collection.clone(),
            field,
        },
    );
    dispatch(state, tenant_id, database_id, &record.collection, plan).await
}

/// Remove the vector index's durable build parameters and its Data Plane
/// state (graph, config, declared dimension, checkpoint).
async fn vector(
    state: &SharedState,
    record: &StoredIndexRecord,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    let field_name = record.primary_field().to_string();
    let plan = crate::bridge::envelope::PhysicalPlan::Vector(
        nodedb_physical::physical_plan::VectorOp::DropIndex {
            collection: record.collection.clone(),
            field_name: field_name.clone(),
        },
    );

    // WAL first: the `VectorParams` record that created this index is still
    // in the log, so without a durable drop record a restart rebuilds the
    // index the user just dropped.
    let vshard =
        crate::types::VShardId::from_collection_in_database(database_id, &record.collection);
    crate::control::server::wal_dispatch::wal_append_if_write(
        &state.wal,
        tenant_id,
        vshard,
        database_id,
        &plan,
    )
    .map_err(|e| err("XX000", format!("persist vector index drop to WAL: {e}")))?;

    dispatch(state, tenant_id, database_id, &record.collection, plan).await?;

    state
        .credentials
        .catalog()
        .delete_vector_index_params(tenant_id.as_u64(), &record.collection, &field_name)
        .map_err(|e| {
            err(
                "XX000",
                format!("remove vector index params from catalog: {e}"),
            )
        })?;
    Ok(())
}

/// Reset the collection's FTS binding once its last full-text index is gone.
///
/// `CREATE FULLTEXT INDEX ... ANALYZER '<name>'` binds an analyzer and a
/// fuzzy default on the collection. Those settings belong to the index that
/// asked for them, so when no full-text index remains they must go back to
/// the engine defaults; otherwise a dropped index keeps changing how the
/// collection's text is tokenized.
async fn fulltext(
    state: &SharedState,
    record: &StoredIndexRecord,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    let remaining = state
        .credentials
        .catalog()
        .list_index_records_for_collection(
            database_id.as_u64(),
            tenant_id.as_u64(),
            &record.collection,
        )
        .map_err(|e| err("XX000", e.to_string()))?
        .into_iter()
        .filter(|r| r.kind == IndexKind::FullText && r.name != record.name)
        .count();
    if remaining > 0 {
        return Ok(());
    }

    let plan = crate::bridge::envelope::PhysicalPlan::Text(
        nodedb_physical::physical_plan::TextOp::SetTextConfig {
            collection: record.collection.clone(),
            analyzer_name: Some("standard".to_string()),
            fuzzy_default: Some(false),
        },
    );
    dispatch(state, tenant_id, database_id, &record.collection, plan).await
}

/// Dispatch one teardown plan to the Data Plane, surfacing both transport and
/// handler-side failures.
async fn dispatch(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: crate::bridge::envelope::PhysicalPlan,
) -> Result<(), DdlError> {
    let vshard = crate::types::VShardId::from_collection_in_database(database_id, collection);
    let response = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| err("XX000", format!("index teardown dispatch failed: {e}")))?;

    if response.status == crate::bridge::envelope::Status::Error {
        let detail = match response.error_code.as_deref() {
            Some(crate::bridge::envelope::ErrorCode::Internal { detail, .. }) => detail.clone(),
            Some(other) => format!("{other:?}"),
            None => String::from_utf8_lossy(&response.payload).into_owned(),
        };
        return Err(err("XX000", format!("index teardown failed: {detail}")));
    }
    Ok(())
}
