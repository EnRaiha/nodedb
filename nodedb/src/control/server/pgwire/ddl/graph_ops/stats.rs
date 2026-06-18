// SPDX-License-Identifier: BUSL-1.1

//! `SHOW GRAPH STATS` handler.
//!
//! Reads persistent graph-stats counters from every Data-Plane core via
//! `broadcast_to_all_cores`, aggregates the per-core
//! [`CollectionStats`](crate::engine::graph::edge_store::stats::CollectionStats)
//! payloads, and emits a pgwire result row set.
//!
//! Aggregation rules:
//! - `edge_count`: summed across cores (each core holds a disjoint partition).
//! - `distinct_node_count`: summed across cores. Per-core CSR partitions are
//!   hash-disjoint by node id, so the cross-core sum equals the global distinct
//!   count — no double-count.
//! - `distinct_label_count`: re-derived from the merged `labels` vec rather than
//!   summed (labels are NOT partition-disjoint — the same label name can appear
//!   in multiple cores).
//! - `labels`: merged by name; counts summed; output is sorted ascending by name.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream;
use nodedb_types::DatabaseId;
use nodedb_types::diagnostic::DiagnosticLayer;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use tracing::info_span;

/// Total number of `SHOW GRAPH STATS` calls served since process start.
/// Read by the metrics endpoint via [`graph_stats_calls_total`].
static GRAPH_STATS_CALLS: AtomicU64 = AtomicU64::new(0);

/// Counter for observability. Mirrors the `broadcast_call_count()` style
/// used elsewhere in the Control Plane. Exposed for metrics endpoints
/// and test harnesses to assert call counts.
#[allow(dead_code)]
pub fn graph_stats_calls_total() -> u64 {
    GRAPH_STATS_CALLS.load(Ordering::Relaxed)
}

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::broadcast::broadcast_to_all_cores;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::stats::CollectionStats;
use crate::types::TraceId;
use nodedb_physical::physical_plan::GraphOp;

/// `SHOW GRAPH STATS ['<collection>'] [VERBOSE] [AS OF SYSTEM TIME <ms>]`.
pub async fn show_graph_stats(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: Option<String>,
    verbose: bool,
    as_of: Option<i64>,
) -> PgWireResult<Vec<Response>> {
    GRAPH_STATS_CALLS.fetch_add(1, Ordering::Relaxed);
    let scope = if collection.is_some() {
        "collection"
    } else {
        "tenant"
    };
    let _span = info_span!(
        "graph.stats",
        layer = DiagnosticLayer::WritePath.as_str(),
        tenant_id = identity.tenant_id.as_u64(),
        scope = scope,
        collection = ?collection,
        verbose,
        as_of = ?as_of,
    );

    // Validate the collection exists if a name was supplied. We resolve
    // through the same catalog path used by SHOW COLLECTIONS / DESCRIBE,
    // so the same not-found / inactive semantics apply.
    if let Some(ref name) = collection {
        let catalog = match state.credentials.catalog() {
            Some(c) => c,
            None => return Err(sqlstate_error("XX000", "catalog not available")),
        };
        match catalog.get_collection(DatabaseId::DEFAULT, identity.tenant_id.as_u64(), name) {
            Ok(Some(c)) if c.is_active => {}
            Ok(Some(_)) => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{name}' is deactivated"),
                ));
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{name}' not found"),
                ));
            }
        }
    }

    let plan = PhysicalPlan::Graph(GraphOp::Stats {
        collection: collection.clone(),
        as_of,
    });

    let resp = broadcast_to_all_cores(
        state,
        identity.tenant_id,
        DatabaseId::DEFAULT,
        plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| sqlstate_error("58000", &format!("graph stats dispatch failed: {e}")))?;

    let merged: Vec<CollectionStats> = decode_merged_stats(resp.payload.as_bytes())
        .map_err(|e| sqlstate_error("XX000", &format!("graph stats decode failed: {e}")))?;

    let aggregated = aggregate_by_collection(merged);

    if verbose {
        encode_verbose_response(aggregated)
    } else {
        encode_compact_response(aggregated)
    }
}

/// Decode the merged msgpack array produced by `broadcast_to_all_cores`.
fn decode_merged_stats(payload: &[u8]) -> crate::Result<Vec<CollectionStats>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: e.to_string(),
    })
}

/// Aggregate per-core `CollectionStats` entries by `collection` name, merging
/// label counts and re-deriving `distinct_label_count` from the merged set.
fn aggregate_by_collection(entries: Vec<CollectionStats>) -> Vec<CollectionStats> {
    let mut label_acc: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    let mut by_name: BTreeMap<String, CollectionStats> = BTreeMap::new();

    for e in entries {
        let acc = by_name
            .entry(e.collection.clone())
            .or_insert_with(|| CollectionStats::zero(e.collection.clone()));
        acc.edge_count = acc.edge_count.saturating_add(e.edge_count);
        acc.distinct_node_count = acc
            .distinct_node_count
            .saturating_add(e.distinct_node_count);

        let labels = label_acc.entry(e.collection.clone()).or_default();
        for (label, count) in e.labels {
            let slot = labels.entry(label).or_insert(0);
            *slot = slot.saturating_add(count);
        }
    }

    let mut result: Vec<CollectionStats> = Vec::with_capacity(by_name.len());
    for (collection, mut acc) in by_name {
        let labels_map = label_acc.remove(&collection).unwrap_or_default();
        let labels: Vec<(String, u64)> = labels_map.into_iter().collect();
        acc.distinct_label_count = labels.len() as u64;
        acc.labels = labels;
        result.push(acc);
    }
    result
}

fn text_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_string(), None, None, Type::TEXT, FieldFormat::Text)
}

fn int8_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_string(), None, None, Type::INT8, FieldFormat::Text)
}

fn encode_compact_response(rows: Vec<CollectionStats>) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        text_field("collection"),
        int8_field("node_count"),
        int8_field("edge_count"),
        int8_field("distinct_label_count"),
        text_field("labels"),
    ]);

    let mut data_rows = Vec::with_capacity(rows.len());
    for r in rows {
        let labels_json = serde_json::Value::Array(
            r.labels
                .iter()
                .map(|(name, count)| {
                    let mut m = serde_json::Map::new();
                    m.insert("label".into(), serde_json::Value::String(name.clone()));
                    m.insert("count".into(), serde_json::Value::Number((*count).into()));
                    serde_json::Value::Object(m)
                })
                .collect(),
        )
        .to_string();

        let mut enc = DataRowEncoder::new(schema.clone());
        enc.encode_field(&r.collection)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        enc.encode_field(&(r.distinct_node_count as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        enc.encode_field(&(r.edge_count as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        enc.encode_field(&(r.distinct_label_count as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        enc.encode_field(&labels_json)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        data_rows.push(Ok(enc.take_row()));
    }
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(data_rows),
    ))])
}

fn encode_verbose_response(rows: Vec<CollectionStats>) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        text_field("collection"),
        text_field("label"),
        int8_field("edge_count"),
    ]);

    let mut data_rows = Vec::new();
    for r in &rows {
        for (label, count) in &r.labels {
            let mut enc = DataRowEncoder::new(schema.clone());
            enc.encode_field(&r.collection)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            enc.encode_field(label)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            enc.encode_field(&(*count as i64))
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            data_rows.push(Ok(enc.take_row()));
        }
    }
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(data_rows),
    ))])
}
