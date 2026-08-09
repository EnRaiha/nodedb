// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::Update` → `PhysicalTask` lowering.

use nodedb_sql::types::{EngineType, Filter, SqlExpr, SqlValue};
use nodedb_types::Surrogate;

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
            let key_bytes = sql_value_to_bytes(key);
            // Content-addressed cross-engine identity so the merged row keeps
            // the surrogate its original insert assigned. `Surrogate::ZERO`
            // only when no assigner is wired (test / embedded-without-catalog).
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
                    // Filled by the RLS injection pass, which runs after plan
                    // conversion.
                    rls_write_check: Vec::new(),
                }),
                post_set_op: PostSetOp::None,
                txn_id: None,
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
                rls_write_check: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // CRDT GATE: an UPDATE on a `crdt = true` document collection routes to
    // `CrdtOp::DocUpsert` with `partial = true` (LWW-per-field). Only PK-targeted
    // SET with literal RHS is representable. Predicate (non-PK) UPDATE and
    // non-literal RHS are rejected — there is NO silent fallthrough to a
    // `DocumentOp`, which would bypass CRDT convergence. `RETURNING` IS
    // supported: the response-only projection is injected into the plan later
    // and emitted by the Data Plane handler, exactly like `PointUpdate`. Read
    // the flag ONCE.
    let is_crdt = super::super::crdt_gate::document_collection_is_crdt(ctx, collection)?;
    if is_crdt && target_keys.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "predicate (non-primary-key) UPDATE on CRDT collection '{collection}' is not \
                 supported; target rows by primary key"
            ),
        });
    }
    // Partial-update payload for the CRDT path, built ONCE from the literal SET
    // assignments (non-literal RHS rejected inside the builder).
    let crdt_fields_json = if is_crdt {
        Some(super::super::crdt_gate::literal_assignments_to_fields_json(
            assignments,
        )?)
    } else {
        None
    };

    // EDGE-BEARING GATE: an UPDATE on a schemaless-document collection that
    // carries implicit edges must NOT lower to a static `PointUpdate` for a
    // PK-equality WHERE — that op bypasses the dependent-predicate (OLLP) path
    // and would leave the mirrored graph edge stale when `_from`/`_to`/`_type`
    // change. Route it as a `BulkUpdate` with an equivalent filter so the
    // Calvin/OLLP coordinator derives + drift-validates the routed edge
    // reconciliation (mirroring the edge-bearing DELETE gate). Non-edge-bearing
    // collections keep the fast `PointUpdate` path below. Reached only for
    // document engines (KV / columnar / spatial returned above); strict/etc.
    // never set `has_implicit_edges`, so the flag naturally scopes this.
    let edge_bearing = !is_crdt
        && !target_keys.is_empty()
        && document_collection_is_edge_bearing(ctx, collection)?;

    if edge_bearing {
        // Reject `Expr` RHS to a reserved edge field: the edge reconciliation
        // diffs against literal SET values (it cannot evaluate an expression
        // against per-row state on the Control Plane). Mirrors the KV /
        // columnar `Expr`-RHS rejection above. Expr to OTHER fields and literal
        // assignments to edge fields are allowed.
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
                rls_write_check: Vec::new(),
                // Filled in by the materialized-sum resolution pass, which
                // recon-scans the rows this predicate matches.
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
            let surrogate = match ctx.surrogate_assigner.as_ref() {
                Some(a) => match a.lookup(ctx.database_id, ctx.tenant_id, collection, &pk_bytes)? {
                    Some(s) => s,
                    None => continue,
                },
                None => Surrogate::ZERO,
            };
            let plan = if let Some(fields_json) = crdt_fields_json.as_ref() {
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
                PhysicalPlan::Document(DocumentOp::PointUpdate {
                    collection: collection.into(),
                    document_id: pk_string,
                    surrogate,
                    pk_bytes,
                    updates: updates.clone(),
                    returning: None,
                    rls_filters: Vec::new(),
                    rls_write_check: Vec::new(),
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
        // Predicate (non-PK) UPDATE: also reject `Expr` RHS to reserved edge
        // fields on an edge-bearing collection. `target_keys` is empty here so
        // the gate above did not run; re-check via the catalog flag.
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
                rls_write_check: Vec::new(),
                // Filled in by the materialized-sum resolution pass, which
                // recon-scans the rows this predicate matches.
                resolved_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }])
    }
}
