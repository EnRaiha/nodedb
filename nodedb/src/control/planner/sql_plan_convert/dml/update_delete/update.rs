// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::Update` → `PhysicalTask` lowering.

use nodedb_sql::types::{EngineType, Filter, SqlExpr, SqlValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::control::planner::sql_plan_convert::filter::serialize_filters;
use crate::control::planner::sql_plan_convert::value::{
    assignments_to_update_values, sql_value_to_bytes, sql_value_to_msgpack, sql_value_to_string,
};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::shared::{document_collection_is_edge_bearing, pk_effective_filter};

/// Parameters for [`convert_update`], bundled to avoid an unwieldy argument
/// list. Fields borrow from the caller exactly as the individual arguments
/// did before this refactor — no new allocations.
pub(in crate::control::planner::sql_plan_convert) struct UpdateParams<'a> {
    pub collection: &'a str,
    pub engine: &'a EngineType,
    pub assignments: &'a [(String, SqlExpr)],
    pub filters: &'a [Filter],
    pub target_keys: &'a [SqlValue],
    pub returning: bool,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(in crate::control::planner::sql_plan_convert) fn convert_update(
    params: UpdateParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let UpdateParams {
        collection,
        engine,
        assignments,
        filters,
        target_keys,
        returning: _returning,
        tenant_id,
        ctx,
    } = params;
    let coll_qualified = crate::control::planner::sql_plan_convert::convert::db_qualified(
        ctx.database_id,
        collection,
    );
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let filter_bytes = serialize_filters(filters)?;
    let updates = assignments_to_update_values(assignments)?;

    if matches!(engine, EngineType::KeyValue) {
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
        // No document store: a WHERE with no PK still routes to KV, not `DocumentOp::BulkUpdate`.
        if target_keys.is_empty() {
            let literal_updates: Vec<(String, Vec<u8>)> = assignments
                .iter()
                .filter_map(|(field, expr)| match expr {
                    SqlExpr::Literal(val) => Some((field.clone(), sql_value_to_msgpack(val))),
                    _ => None,
                })
                .collect();
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                database_id: ctx.database_id,
                plan: PhysicalPlan::Kv(KvOp::PredicateUpdate {
                    collection: collection.into(),
                    filters: filter_bytes,
                    updates: literal_updates,
                    rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                }),
                post_set_op: PostSetOp::None,
                txn_id: None,
            }]);
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
            let key_bytes = sql_value_to_bytes(key);
            // Content-addressed identity: keeps the surrogate the original insert assigned.
            // `Surrogate::ZERO` only when no assigner is wired (test / embedded-without-catalog).
            let surrogate = ctx.surrogate_for_pk(collection, &key_bytes)?;
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                database_id: ctx.database_id,
                plan: PhysicalPlan::Kv(KvOp::FieldSet {
                    collection: collection.into(),
                    key: key_bytes,
                    updates: field_updates,
                    surrogate,
                    // Filled by the RLS injection pass, after plan conversion.
                    rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                }),
                post_set_op: PostSetOp::None,
                txn_id: None,
            });
        }
        return Ok(tasks);
    }

    // `TimeseriesRules::plan_update` already rejects this; guard keeps other
    // callers from falling through to `DocumentOp::BulkUpdate`.
    if matches!(engine, EngineType::Timeseries) {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "UPDATE is not supported on timeseries collection '{collection}'; \
                 timeseries data is append-only"
            ),
        });
    }

    // No document store: route to ColumnarOp::Update regardless of PK-reduced WHERE.
    if matches!(engine, EngineType::Columnar | EngineType::Spatial) {
        // Literals only: expressions need row-context eval, not wired into the columnar handler.
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
        // PK-targeted WHERE: convert target_keys to an Eq filter on the PK column.
        let effective_filter = pk_effective_filter(filter_bytes, target_keys)?;
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Columnar(ColumnarOp::Update {
                collection: collection.into(),
                filters: effective_filter,
                updates: columnar_updates,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // CRDT UPDATE never falls through to `DocumentOp`, which would bypass convergence.
    let is_crdt = super::super::crdt_gate::document_collection_is_crdt(ctx, collection)?;
    if is_crdt && target_keys.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "predicate (non-primary-key) UPDATE on CRDT collection '{collection}' is not \
                 supported; target rows by primary key"
            ),
        });
    }
    // CRDT partial-update payload, built once from the literal SET assignments.
    let crdt_fields_json = if is_crdt {
        Some(super::super::crdt_gate::literal_assignments_to_fields_json(
            assignments,
        )?)
    } else {
        None
    };

    // PK-equality UPDATE must not use `PointUpdate` here — it'd leave the mirrored edge stale.
    let edge_bearing = !is_crdt
        && !target_keys.is_empty()
        && document_collection_is_edge_bearing(ctx, collection)?;

    if edge_bearing {
        // Reject `Expr` RHS to a reserved edge field: reconciliation diffs against
        // literal SET values only (mirrors the KV/columnar `Expr`-RHS rejection).
        if let Some((field, _)) = assignments.iter().find(|(field, expr)| {
            matches!(field.as_str(), "_from" | "_to" | "_type")
                && !matches!(expr, SqlExpr::Literal(_))
        }) {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "expression updates to reserved edge fields (_from, _to, _type) \
                     are not supported on edge-bearing collections (field '{field}'); \
                     use a literal value"
                ),
            });
        }
        let effective_filter = pk_effective_filter(filter_bytes, target_keys)?;
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: collection.into(),
                filters: effective_filter,
                updates,
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                // Filled in by the materialized-sum resolution pass.
                resolved_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    if !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            let pk_string = sql_value_to_string(key);
            let pk_bytes = pk_string.clone().into_bytes();
            let plan = if let Some(fields_json) = crdt_fields_json.as_ref() {
                // An upsert CREATES the row when the key is absent, so it owns
                // a real identity and allocates one.
                let surrogate = ctx.surrogate_for_pk(collection, &pk_bytes)?;
                PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                    collection: collection.into(),
                    document_id: pk_string,
                    fields_json: fields_json.clone(),
                    surrogate,
                    partial: true,
                    returning: None,
                    rls_filters: Vec::new(),
                })
            } else {
                // Read-only resolution: a task always exists (the write hook
                // still runs, an unbound row_key affects 0 rows, and the clone
                // CoW resolver intercepts the ZERO sentinel), but an UPDATE
                // creates no row, so it must never mint a binding.
                let surrogate = ctx.surrogate_for_existing_pk(collection, &pk_bytes)?;
                PhysicalPlan::Document(DocumentOp::PointUpdate {
                    collection: collection.into(),
                    document_id: pk_string,
                    surrogate,
                    pk_bytes,
                    updates: updates.clone(),
                    returning: None,
                    rls_filters: Vec::new(),
                    rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                    resolved_sum_targets: Vec::new(),
                })
            };
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                database_id: ctx.database_id,
                plan,
                post_set_op: PostSetOp::None,
                txn_id: None,
            });
        }
        Ok(tasks)
    } else {
        // target_keys is empty so the edge-bearing gate above didn't run; re-check via catalog.
        if document_collection_is_edge_bearing(ctx, collection)?
            && let Some((field, _)) = assignments.iter().find(|(field, expr)| {
                matches!(field.as_str(), "_from" | "_to" | "_type")
                    && !matches!(expr, SqlExpr::Literal(_))
            })
        {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "expression updates to reserved edge fields (_from, _to, _type) \
                     are not supported on edge-bearing collections (field '{field}'); \
                     use a literal value"
                ),
            });
        }
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
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                // Filled in by the materialized-sum resolution pass.
                resolved_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }])
    }
}
