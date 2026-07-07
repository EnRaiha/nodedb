// SPDX-License-Identifier: BUSL-1.1

//! `CREATE GRAPH INDEX name ON collection (parent_col -> id_col)`
//!
//! Scans every document in the collection and materialises each
//! `parent → child` relation as a CSR edge under the graph-index name.
//!
//! Correctness contract:
//!
//! 1. **Atomic-ish**: the whole edge set is dispatched in a single
//!    `EdgePutBatch` per vshard. Any `Err` from the Data Plane causes
//!    the DDL to attempt a best-effort rollback via `EdgeDeleteBatch`
//!    on the shards that already succeeded, then surfaces SQLSTATE
//!    `XX000` to the client.
//! 2. **Loud on partial failure**: no `tracing::warn!` + continue. The
//!    reported `edges_created` count either matches the number of
//!    valid parent→child relations in the collection or the DDL fails.
//! 3. **schema_version gated**: `state.schema_version.bump()` runs only
//!    on success so consumers observing catalog version do not see a
//!    half-built index.
//! 4. **WAL-backed + replicated**: each batch is dispatched via
//!    `dispatch_sync_response`, which proposes the write through the
//!    destination shard's Raft group. The committed log entry is the
//!    durable record (so a mid-build crash replays the edges) and it
//!    reaches every replica under RF>1 — no separate WAL append, and no
//!    local-only dispatch that would strand the index on one node.
//! 5. **Broadcast scan**: documents live on hash-of-doc-id vshards, not
//!    the collection-name vshard. The scan uses `broadcast_to_all_cores`
//!    to see every document, then partitions the resulting edges by
//!    destination-shard for batched dispatch.

use std::collections::HashMap;

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::broadcast::broadcast_to_all_cores;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use super::super::super::result::{DdlError, DdlResult};
use super::parse::parse_edge_columns;
use super::support::ddl_err;

pub async fn create_graph_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // CREATE GRAPH INDEX <name> ON <collection> (<parent_col> -> <id_col>)
    let index_name = parts
        .get(3)
        .ok_or_else(|| ddl_err("42601", "missing graph index name"))?
        .to_lowercase();

    let on_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| ddl_err("42601", "CREATE GRAPH INDEX requires ON <collection>"))?;

    let collection = parts
        .get(on_idx + 1)
        .ok_or_else(|| ddl_err("42601", "missing collection name after ON"))?
        .to_lowercase();

    let (parent_col, id_col) = parse_edge_columns(sql)?;

    let catalog = state.credentials.catalog();
    if catalog
        .get_collection(database_id, tenant_id.as_u64(), &collection)
        .map_err(|e| ddl_err("XX000", e.to_string()))?
        .is_none()
    {
        return Err(ddl_err(
            "42P01",
            format!("collection '{collection}' not found"),
        ));
    }

    // ── Broadcast scan: collect documents from every vshard ──────────
    let scan_plan = PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
        collection: collection.clone(),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });
    let scan_resp = broadcast_to_all_cores(state, tenant_id, database_id, scan_plan, TraceId::ZERO)
        .await
        .map_err(|e| ddl_err("XX000", format!("scan failed: {e}")))?;

    let payload_json =
        crate::data::executor::response_codec::decode_payload_to_json(&scan_resp.payload);
    let docs: Vec<serde_json::Value> = sonic_rs::from_str(&payload_json)
        .map_err(|e| ddl_err("22P02", format!("invalid JSON in scan response: {e}")))?;

    // ── Build edge list partitioned by destination vshard ────────────
    //
    // Edges to the same vshard are batched into one `EdgePutBatch`. A
    // mixed-type `parent` field (e.g. an integer) surfaces SQLSTATE
    // `22P02` loudly; the earlier `.and_then(as_str).drop` behaviour
    // silently omitted the edge, leaving the index incomplete.
    let mut edges_by_shard: HashMap<VShardId, Vec<BatchEdge>> = HashMap::new();
    let mut total_edges = 0u64;
    for doc in &docs {
        // `DocumentOp::Scan` emits `{id, data: {...}}` per
        // `encode_raw_document_rows`. Anything else is a protocol bug,
        // not a shape we should silently accommodate.
        let Some(obj_outer) = doc.as_object() else {
            continue;
        };
        let Some(obj) = obj_outer.get("data").and_then(|v| v.as_object()) else {
            return Err(ddl_err(
                "XX000",
                format!(
                    "CREATE GRAPH INDEX: document scan returned a row without a `data` field: {doc}"
                ),
            ));
        };

        let doc_id = obj
            .get("id")
            .or_else(|| obj.get("_id"))
            .and_then(|v| v.as_str())
            .or_else(|| obj.get(&id_col).and_then(|v| v.as_str()));

        let parent_raw = obj.get(&parent_col);

        match (doc_id, parent_raw) {
            // Missing parent — legitimate root; skip.
            (Some(_), None) | (Some(_), Some(serde_json::Value::Null)) => {}
            (Some(child), Some(parent_v)) => {
                let parent = match parent_v.as_str() {
                    Some(s) => s,
                    None => {
                        return Err(ddl_err(
                            "22P02",
                            format!(
                                "collection '{collection}' doc '{child}': parent field '{parent_col}' \
                                 must be a string, got {parent_v:?}"
                            ),
                        ));
                    }
                };
                if parent.is_empty() || parent == child {
                    continue;
                }
                let shard = VShardId::from_key(parent.as_bytes());
                let src_surrogate = state
                    .surrogate_assigner
                    .assign(database_id, tenant_id, &collection, parent.as_bytes())
                    .map_err(|e| ddl_err("XX000", e.to_string()))?;
                let dst_surrogate = state
                    .surrogate_assigner
                    .assign(database_id, tenant_id, &collection, child.as_bytes())
                    .map_err(|e| ddl_err("XX000", e.to_string()))?;
                edges_by_shard.entry(shard).or_default().push(BatchEdge {
                    collection: collection.to_string(),
                    src_id: parent.to_string(),
                    label: index_name.clone(),
                    dst_id: child.to_string(),
                    src_surrogate,
                    dst_surrogate,
                });
                total_edges += 1;
            }
            _ => {}
        }
    }

    // ── Dispatch batches, WAL + rollback discipline ──────────────────
    let mut committed_shards: Vec<(VShardId, Vec<BatchEdge>)> = Vec::new();
    for (shard, edges) in edges_by_shard {
        let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch {
            edges: edges.clone(),
        });
        // Route the batch to its destination vShard and replicate via Raft.
        // `dispatch_sync_response` provides WAL durability + Raft replication
        // internally (the proposed log entry IS the durable record), so no
        // separate `wal_append_if_write` is needed — and under RF>1 the index
        // edges now reach every follower instead of landing local-only.
        match crate::control::server::sync::raft_dispatch::dispatch_sync_response(
            state,
            tenant_id,
            shard,
            plan,
            TraceId::ZERO,
            crate::event::EventSource::User,
        )
        .await
        {
            Ok(_) => committed_shards.push((shard, edges)),
            Err(e) => {
                return surface_failure(
                    state,
                    tenant_id,
                    &committed_shards,
                    format!("edge-insert dispatch failed on shard {shard:?}: {e}"),
                )
                .await;
            }
        }
    }

    // Only now is the index atomically complete.
    state.schema_version.bump();

    let mut row = Map::new();
    row.insert(
        "edges_created".to_string(),
        JsonValue::String(total_edges.to_string()),
    );

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns: vec!["edges_created".to_string()],
        column_types: ShapedRows::text_types(1),
        rows: vec![row],
        notice: None,
    })])
}

/// Surface a build-time failure.
///
/// Runs rollback in parallel across all committed shards. If **every**
/// shard's `EdgeDeleteBatch` succeeds, returns a clean
/// `XX000 CREATE GRAPH INDEX failed: <reason>; reverted N shards`.
///
/// If **any** shard's rollback itself fails, the CSR is now in an
/// inconsistent state across shards — some have the partial index,
/// others don't. This is surfaced loudly as
/// `XX001 GRAPH INDEX LEFT IN INCONSISTENT STATE` with the list of
/// shards that failed to revert. Clients MUST treat this as a hard
/// error requiring operator intervention; silently warn-logging the
/// failure would be the exact pattern the forward-path bug had.
async fn surface_failure(
    state: &SharedState,
    tenant_id: TenantId,
    committed: &[(VShardId, Vec<BatchEdge>)],
    cause: String,
) -> Result<Vec<DdlResult>, DdlError> {
    let committed_count = committed.len();
    let rollback_futures = committed.iter().map(|(shard, edges)| {
        let plan = PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch {
            edges: edges.clone(),
        });
        let shard = *shard;
        async move {
            (
                shard,
                // Same Raft-proposing path as the forward dispatch:
                // `dispatch_sync_response` carries WAL durability + replication
                // internally, so the rollback tombstones reach every replica.
                crate::control::server::sync::raft_dispatch::dispatch_sync_response(
                    state,
                    tenant_id,
                    shard,
                    plan,
                    TraceId::ZERO,
                    crate::event::EventSource::User,
                )
                .await,
            )
        }
    });

    let rollback_results = futures::future::join_all(rollback_futures).await;
    let failed: Vec<(VShardId, String)> = rollback_results
        .into_iter()
        .filter_map(|(shard, res)| res.err().map(|e| (shard, e.to_string())))
        .collect();

    if failed.is_empty() {
        Err(ddl_err(
            "XX000",
            format!(
                "CREATE GRAPH INDEX failed: {cause}; reverted {committed_count} committed shards"
            ),
        ))
    } else {
        // Distinct SQLSTATE so clients / operators can distinguish
        // "failed cleanly" from "failed and left the graph broken".
        Err(ddl_err(
            "XX001",
            format!(
                "CREATE GRAPH INDEX failed: {cause}; rollback also failed on {}/{} shards \
                 ({:?}); GRAPH INDEX LEFT IN INCONSISTENT STATE — operator intervention required",
                failed.len(),
                committed_count,
                failed
            ),
        ))
    }
}
