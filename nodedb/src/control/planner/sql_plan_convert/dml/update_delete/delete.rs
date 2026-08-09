// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::Delete` → `PhysicalTask` lowering.

use nodedb_sql::types::{EngineType, Filter, SqlValue};
use nodedb_types::Surrogate;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::control::planner::sql_plan_convert::filter::serialize_filters;
use crate::control::planner::sql_plan_convert::value::{sql_value_to_bytes, sql_value_to_string};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::shared::{
    delete_effective_filter, document_collection_is_edge_bearing, pk_effective_filter,
};

pub(in crate::control::planner::sql_plan_convert) fn convert_delete(
    collection: &str,
    engine: &EngineType,
    filters: &[Filter],
    target_keys: &[SqlValue],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = crate::control::planner::sql_plan_convert::convert::db_qualified(
        ctx.database_id,
        collection,
    );
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
                // Filled by the RLS injection pass, which runs after plan
                // conversion.
                rls_write_check: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // Columnar and spatial engines have no document store; route to
    // `ColumnarOp::Delete` regardless of whether the WHERE reduces to PK keys
    // (mirrors the columnar/spatial UPDATE routing in `convert_update`). Without
    // this a columnar/spatial DELETE falls through to `DocumentOp::BulkDelete`,
    // which scans the empty document store and matches nothing.
    if matches!(engine, EngineType::Columnar | EngineType::Spatial) {
        let filter_bytes = serialize_filters(filters)?;
        let effective_filter = pk_effective_filter(filter_bytes, target_keys)?;
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection: collection.into(),
                filters: effective_filter,
                rls_write_check: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // CRDT GATE: a DELETE on a `crdt = true` document collection routes to
    // `CrdtOp::DocDelete` (tombstone + sparse-store removal). Only a PK-targeted
    // DELETE is representable; a predicate (non-PK) DELETE is rejected — there is
    // NO silent fallthrough to a `DocumentOp`, which would bypass CRDT
    // convergence. Read the flag ONCE, before the PK loop.
    let is_crdt = super::super::crdt_gate::document_collection_is_crdt(ctx, collection)?;
    if is_crdt && target_keys.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "predicate (non-primary-key) DELETE on CRDT collection '{collection}' is not \
                 supported; target rows by primary key"
            ),
        });
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
    if !is_crdt && !target_keys.is_empty() && document_collection_is_edge_bearing(ctx, collection)?
    {
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
            let plan = if is_crdt {
                PhysicalPlan::Crdt(CrdtOp::DocDelete {
                    collection: collection.into(),
                    document_id: pk_string,
                    surrogate,
                    returning: None,
                    rls_filters: Vec::new(),
                })
            } else {
                PhysicalPlan::Document(DocumentOp::PointDelete {
                    collection: collection.into(),
                    document_id: pk_string,
                    surrogate,
                    pk_bytes,
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
