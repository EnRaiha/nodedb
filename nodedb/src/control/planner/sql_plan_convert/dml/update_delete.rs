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
        let effective_filter = if !target_keys.is_empty() && !filter_bytes.is_empty() {
            filter_bytes
        } else if !target_keys.is_empty() {
            // Planner pre-resolved the PK — serialize as an Eq filter
            // so the columnar UPDATE handler can use the same scan path
            // as a WHERE-predicate update.
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
            })?
        } else {
            filter_bytes
        };
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
