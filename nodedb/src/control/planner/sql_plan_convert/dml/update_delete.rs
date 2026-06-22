// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::{EngineType, Filter, SqlExpr, SqlPlan, SqlValue};
use nodedb_types::Surrogate;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use super::super::convert::ConvertContext;
use super::super::filter::serialize_filters;
use super::super::value::{
    assignments_to_update_values, assignments_to_update_values_qualified, sql_value_to_bytes,
    sql_value_to_msgpack, sql_value_to_string,
};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

#[allow(clippy::too_many_arguments)]
pub(in super::super) fn convert_update(
    collection: &str,
    engine: &EngineType,
    assignments: &[(String, SqlExpr)],
    filters: &[Filter],
    target_keys: &[SqlValue],
    _returning: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let filter_bytes = serialize_filters(filters)?;
    let updates = assignments_to_update_values(assignments)?;

    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        if let Some((field, _)) = assignments
            .iter()
            .find(|(_, expr)| !matches!(expr, SqlExpr::Literal(_)))
        {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "UPDATE with non-literal RHS on KV engine (field '{field}') \
                     is not yet supported; use a literal value"
                ),
            });
        }
        let mut tasks = Vec::new();
        for key in target_keys {
            let field_updates: Vec<(String, Vec<u8>)> = assignments
                .iter()
                .filter_map(|(field, expr)| {
                    if let SqlExpr::Literal(val) = expr {
                        Some((field.clone(), sql_value_to_msgpack(val)))
                    } else {
                        None
                    }
                })
                .collect();
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                database_id: ctx.database_id,
                plan: PhysicalPlan::Kv(KvOp::FieldSet {
                    collection: collection.into(),
                    key: sql_value_to_bytes(key),
                    updates: field_updates,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        return Ok(tasks);
    }

    // Columnar and spatial engines have no document store; route to
    // ColumnarOp::Update regardless of whether the WHERE reduces to PK keys.
    if matches!(engine, EngineType::Columnar | EngineType::Spatial) {
        // ColumnarOp::Update carries raw msgpack bytes per field; extract
        // literals only (expressions require row-context eval not yet wired
        // into the columnar mutation handler).
        use nodedb_physical::physical_plan::UpdateValue;
        let mut columnar_updates: Vec<(String, Vec<u8>)> = Vec::with_capacity(updates.len());
        for (field, update_val) in &updates {
            match update_val {
                UpdateValue::Literal(bytes) => {
                    columnar_updates.push((field.clone(), bytes.clone()))
                }
                UpdateValue::Expr(_) => {
                    return Err(crate::Error::BadRequest {
                        detail: format!(
                            "UPDATE with non-literal RHS on columnar/spatial engine \
                             (field '{field}') is not yet supported; use a literal value"
                        ),
                    });
                }
            }
        }
        // When the planner resolved target_keys (PK-targeted WHERE), convert
        // them to an Eq filter on the PK column so the columnar UPDATE handler
        // can match and tombstone the right row.
        let effective_filter = pk_effective_filter(filter_bytes, target_keys)?;
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Columnar(ColumnarOp::Update {
                collection: collection.into(),
                filters: effective_filter,
                updates: columnar_updates,
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    if !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            let pk_string = sql_value_to_string(key);
            let pk_bytes = pk_string.clone().into_bytes();
            let surrogate = match ctx.surrogate_assigner.as_ref() {
                Some(a) => match a.lookup(ctx.database_id, ctx.tenant_id, collection, &pk_bytes)? {
                    Some(s) => s,
                    None => continue,
                },
                None => Surrogate::ZERO,
            };
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                database_id: ctx.database_id,
                plan: PhysicalPlan::Document(DocumentOp::PointUpdate {
                    collection: collection.into(),
                    document_id: pk_string,
                    surrogate,
                    pk_bytes,
                    updates: updates.clone(),
                    returning: None,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        Ok(tasks)
    } else {
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: collection.into(),
                filters: filter_bytes,
                updates,
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
            }),
            post_set_op: PostSetOp::None,
        }])
    }
}

pub(in super::super) fn convert_delete(
    collection: &str,
    engine: &EngineType,
    filters: &[Filter],
    target_keys: &[SqlValue],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);

    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        let keys: Vec<Vec<u8>> = target_keys.iter().map(sql_value_to_bytes).collect();
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Kv(KvOp::Delete {
                collection: collection.into(),
                keys,
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    // EDGE-BEARING GATE: a PK-equality delete on a schemaless-document
    // collection that carries implicit edges must NOT lower to a static
    // `PointDelete` — that op bypasses the dependent-predicate (OLLP) path
    // and leaks the implicit edge. Route it as a `BulkDelete` with an
    // equivalent filter so `execute.rs`'s edge-bearing gate sends it through
    // the Calvin/OLLP coordinator, which derives + drift-validates the routed
    // `EdgeDelete` (reusing all of O3a + O3a-drift). Non-edge-bearing
    // collections keep the fast `PointDelete` path below. Reached only for
    // document engines (the KV case returned above); strict/columnar/etc.
    // never set `has_implicit_edges`, so the flag naturally scopes this.
    if !target_keys.is_empty() && document_collection_is_edge_bearing(ctx, collection)? {
        let effective_filter = delete_effective_filter(filters, target_keys)?;
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: collection.into(),
                filters: effective_filter,
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    if !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            let pk_string = sql_value_to_string(key);
            let pk_bytes = pk_string.clone().into_bytes();
            let surrogate = match ctx.surrogate_assigner.as_ref() {
                Some(a) => match a.lookup(ctx.database_id, ctx.tenant_id, collection, &pk_bytes)? {
                    Some(s) => s,
                    None => continue,
                },
                None => Surrogate::ZERO,
            };
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                database_id: ctx.database_id,
                plan: PhysicalPlan::Document(DocumentOp::PointDelete {
                    collection: collection.into(),
                    document_id: pk_string,
                    surrogate,
                    pk_bytes,
                    returning: None,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        Ok(tasks)
    } else {
        let filter_bytes = serialize_filters(filters)?;
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: collection.into(),
                filters: filter_bytes,
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
            }),
            post_set_op: PostSetOp::None,
        }])
    }
}

/// Returns `true` when the schemaless-document `collection` (already
/// db-qualified by the caller) carries implicit edges, mirroring the
/// edge-bearing gate in `execute.rs`.
///
/// A genuine catalog READ error propagates (misrouting a delete on a real I/O
/// fault would silently skip edge cleanup → dangling edges). An ABSENT
/// credential store or catalog, or an absent collection row (`Ok(None)`), is
/// treated as non-edge-bearing (`Ok(false)`).
fn document_collection_is_edge_bearing(
    ctx: &ConvertContext,
    collection: &str,
) -> crate::Result<bool> {
    let Some(credentials) = ctx.credentials.as_ref() else {
        return Ok(false);
    };
    let Some(catalog) = credentials.catalog().as_ref() else {
        return Ok(false);
    };
    Ok(catalog
        .get_collection(ctx.database_id, ctx.tenant_id.as_u64(), collection)?
        .map(|c| c.has_implicit_edges)
        .unwrap_or(false))
}

/// Effective filter for a PK-pre-resolved write (shared by the columnar UPDATE
/// path and the edge-bearing PK-equality DELETE path).
///
/// Prefers the user's serialized `WHERE` predicate (`filter_bytes`) verbatim.
/// Only when it is empty AND the planner pre-resolved `target_keys` does it
/// synthesize one `id = <key>` `Eq` filter per key. When `target_keys` is also
/// empty (no WHERE at all) the empty `filter_bytes` is returned as-is (match
/// all) — so callers that must NEVER match all rows (the DELETE gate) must only
/// call this with a non-empty `target_keys`, which then guarantees a non-empty
/// result.
fn pk_effective_filter(filter_bytes: Vec<u8>, target_keys: &[SqlValue]) -> crate::Result<Vec<u8>> {
    if !filter_bytes.is_empty() || target_keys.is_empty() {
        return Ok(filter_bytes);
    }
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    let pk_filters: Vec<ScanFilter> = target_keys
        .iter()
        .map(|key| ScanFilter {
            field: "id".to_string(),
            op: FilterOp::Eq,
            value: nodedb_types::Value::String(sql_value_to_string(key)),
            clauses: Vec::new(),
            expr: None,
        })
        .collect();
    zerompk::to_msgpack_vec(&pk_filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("pk filter encode: {e}"),
    })
}

/// Build the filter bytes for an edge-bearing PK-equality DELETE routed as a
/// `BulkDelete`. Thin wrapper over [`pk_effective_filter`]: serializes the
/// user's `WHERE` predicate, then defers to the shared synthesis. The DELETE
/// gate only calls this with a non-empty `target_keys`, so the result is NEVER
/// an empty filter (which would match ALL rows).
fn delete_effective_filter(filters: &[Filter], target_keys: &[SqlValue]) -> crate::Result<Vec<u8>> {
    pk_effective_filter(serialize_filters(filters)?, target_keys)
}

/// Lower a `SqlPlan::UpdateFrom` to a `DocumentOp::UpdateFromJoin` physical task.
///
/// The source collection name and alias are extracted from the `source` plan.
/// Assignments are converted with table-qualified column references so the Data
/// Plane can resolve `src.col` against the merged `{target + "src.col": ...}` doc.
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn convert_update_from(
    collection: &str,
    source: &SqlPlan,
    target_join_col: &str,
    source_join_col: &str,
    assignments: &[(String, SqlExpr)],
    target_filters: &[Filter],
    _returning: bool,
    tenant_id: TenantId,
    ctx: &super::super::convert::ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    // Extract source collection name and alias from the source scan plan.
    let (source_collection, source_alias) = match source {
        SqlPlan::Scan {
            collection, alias, ..
        } => {
            let qualified = super::super::convert::db_qualified(ctx.database_id, collection);
            let alias_str = alias.as_deref().unwrap_or(collection.as_str()).to_string();
            (qualified, alias_str)
        }
        SqlPlan::DocumentIndexLookup {
            collection, alias, ..
        } => {
            let qualified = super::super::convert::db_qualified(ctx.database_id, collection);
            let alias_str = alias.as_deref().unwrap_or(collection.as_str()).to_string();
            (qualified, alias_str)
        }
        other => {
            return Err(crate::Error::PlanError {
                detail: format!("UpdateFrom source must be a Scan plan, got: {other:?}"),
            });
        }
    };

    let updates = assignments_to_update_values_qualified(assignments)?;
    let target_filter_bytes = serialize_filters(target_filters)?;
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: collection.into(),
            source_collection,
            source_alias,
            target_join_col: target_join_col.into(),
            source_join_col: source_join_col.into(),
            updates,
            target_filters: target_filter_bytes,
            returning: None,
        }),
        post_set_op: PostSetOp::None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;
    use std::sync::Arc;

    /// Build a `ConvertContext` whose credential store carries a catalog with
    /// two collections under tenant 0 / DEFAULT database: `edges`
    /// (`has_implicit_edges = true`) and `plain` (`false`). The returned
    /// `TempDir` must be kept alive for the lifetime of the context (it backs
    /// the catalog's redb file).
    fn ctx_with_catalog() -> (ConvertContext, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            CredentialStore::open(&dir.path().join("system.redb")).expect("open credential store");
        {
            let catalog = store
                .catalog()
                .as_ref()
                .expect("persistent store has a catalog");
            let mut edges = StoredCollection::new(0, "edges", "owner");
            edges.has_implicit_edges = true;
            catalog
                .put_collection(crate::types::DatabaseId::DEFAULT, &edges)
                .expect("put edges collection");
            let plain = StoredCollection::new(0, "plain", "owner");
            catalog
                .put_collection(crate::types::DatabaseId::DEFAULT, &plain)
                .expect("put plain collection");
        }

        let ctx = ConvertContext {
            retention_registry: None,
            array_catalog: None,
            credentials: Some(Arc::new(store)),
            wal: None,
            surrogate_assigner: None,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        };
        (ctx, dir)
    }

    #[test]
    fn pk_delete_on_edge_bearing_collection_routes_bulk_delete() {
        let (ctx, _dir) = ctx_with_catalog();
        let keys = vec![SqlValue::String("edge_3".to_string())];
        let tasks = convert_delete(
            "edges",
            &EngineType::DocumentSchemaless,
            &[],
            &keys,
            TenantId::new(0),
            &ctx,
        )
        .expect("convert_delete");
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::BulkDelete { filters, .. }) => {
                // Synthesized PK filter is never empty for a non-empty target_keys.
                assert!(
                    !filters.is_empty(),
                    "edge-bearing PK delete must carry a non-empty filter"
                );
            }
            other => panic!("expected BulkDelete, got {other:?}"),
        }
    }

    #[test]
    fn pk_delete_on_non_edge_collection_routes_point_delete() {
        let (ctx, _dir) = ctx_with_catalog();
        let keys = vec![SqlValue::String("row_1".to_string())];
        let tasks = convert_delete(
            "plain",
            &EngineType::DocumentSchemaless,
            &[],
            &keys,
            TenantId::new(0),
            &ctx,
        )
        .expect("convert_delete");
        assert_eq!(tasks.len(), 1);
        assert!(
            matches!(
                &tasks[0].plan,
                PhysicalPlan::Document(DocumentOp::PointDelete { .. })
            ),
            "non-edge-bearing PK delete must remain a PointDelete"
        );
    }

    #[test]
    fn delete_effective_filter_never_empty_for_non_empty_keys() {
        let keys = vec![
            SqlValue::String("a".to_string()),
            SqlValue::String("b".to_string()),
        ];
        let bytes = delete_effective_filter(&[], &keys).expect("synthesize filter");
        assert!(!bytes.is_empty());
    }
}
