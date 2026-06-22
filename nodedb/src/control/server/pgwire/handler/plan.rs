// SPDX-License-Identifier: BUSL-1.1

//! Plan classification and response formatting.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::response_codec::{
    ArraySliceResponse, RowsPayload, decode_payload_to_json,
};
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, QueryOp, SpatialOp, TextOp,
    TimeseriesOp, VectorOp,
};
use zerompk;

use super::super::types::text_field;

#[derive(Debug, Clone, Copy)]
pub(super) enum PlanKind {
    SingleDocument,
    MultiRow,
    /// Array slice result — decoded via `ArraySliceResponse` to surface the
    /// `truncated_before_horizon` flag as a pgwire NOTICE when set.
    ArraySlice,
    Execution,
    /// DML operation that returns affected row count.
    /// The tag name is used in the pgwire `CommandComplete` message (e.g., "UPDATE", "DELETE").
    DmlResult(&'static str),
    /// DML with RETURNING clause — payload is a `RowsPayload` (msgpack).
    /// Decoded into one pgwire field per column.
    ReturningRows,
}

/// Extract the collection name from a physical plan (if applicable).
pub(super) fn extract_collection(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Search { collection, .. })
        | PhysicalPlan::Document(DocumentOp::RangeScan { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::Read { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::Apply { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Insert { collection, .. })
        | PhysicalPlan::Vector(VectorOp::BatchInsert { collection, .. })
        | PhysicalPlan::Vector(VectorOp::MultiSearch { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Delete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointPut { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointUpdate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Scan { collection, .. })
        | PhysicalPlan::Query(QueryOp::Aggregate { collection, .. })
        | PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: collection,
            ..
        })
        | PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: collection,
            ..
        })
        | PhysicalPlan::Graph(GraphOp::RagFusion { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::SetPolicy { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::GetPolicy { collection, .. })
        | PhysicalPlan::Vector(VectorOp::SetParams { collection, .. })
        | PhysicalPlan::Text(TextOp::Search { collection, .. })
        | PhysicalPlan::Text(TextOp::PhraseSearch { collection, .. })
        | PhysicalPlan::Text(TextOp::HybridSearch { collection, .. })
        | PhysicalPlan::Text(TextOp::HybridSearchTriple { collection, .. })
        | PhysicalPlan::Text(TextOp::BM25ScoreScan { collection, .. })
        | PhysicalPlan::Text(TextOp::FtsIndexDoc { collection, .. })
        | PhysicalPlan::Text(TextOp::FtsDeleteDoc { collection, .. })
        | PhysicalPlan::Query(QueryOp::PartialAggregate { collection, .. })
        | PhysicalPlan::Query(QueryOp::FacetCounts { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Upsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: collection,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::Truncate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::EstimateCount { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Scan { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Insert { collection, .. })
        | PhysicalPlan::Timeseries(TimeseriesOp::Scan { collection, .. })
        | PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. })
        | PhysicalPlan::Spatial(SpatialOp::Scan { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Register { collection, .. })
        | PhysicalPlan::Document(DocumentOp::IndexLookup { collection, .. })
        | PhysicalPlan::Document(DocumentOp::IndexedFetch { collection, .. })
        | PhysicalPlan::Document(DocumentOp::DropIndex { collection, .. }) => {
            Some(collection.as_str())
        }
        PhysicalPlan::Graph(GraphOp::EdgePut { .. })
        | PhysicalPlan::Graph(GraphOp::EdgeDelete { .. })
        | PhysicalPlan::Graph(GraphOp::Hop { .. })
        | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
        | PhysicalPlan::Graph(GraphOp::Path { .. })
        | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
        | PhysicalPlan::Meta(MetaOp::WalAppend { .. })
        | PhysicalPlan::Meta(MetaOp::Cancel { .. })
        | PhysicalPlan::Meta(MetaOp::TransactionBatch { .. })
        | PhysicalPlan::Meta(MetaOp::CreateSnapshot)
        | PhysicalPlan::Meta(MetaOp::Compact)
        | PhysicalPlan::Meta(MetaOp::Checkpoint)
        | PhysicalPlan::Graph(GraphOp::Algo { .. })
        | PhysicalPlan::Graph(GraphOp::Match { .. })
        | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. }) => None,
        // Exchange: recurse into the child plan to extract the collection.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => extract_collection(&op.child),
        // ProviderScan is a catalog/constant source — no user collection.
        PhysicalPlan::Query(QueryOp::ProviderScan { .. }) => None,
        _ => None,
    }
}

pub(super) fn describe_plan(plan: &PhysicalPlan) -> PlanKind {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { .. })
        | PhysicalPlan::Crdt(CrdtOp::Read { .. })
        | PhysicalPlan::Crdt(CrdtOp::GetPolicy { .. }) => PlanKind::SingleDocument,

        PhysicalPlan::Vector(VectorOp::Search { .. })
        | PhysicalPlan::Document(DocumentOp::RangeScan { .. })
        | PhysicalPlan::Graph(GraphOp::Hop { .. })
        | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
        | PhysicalPlan::Graph(GraphOp::Path { .. })
        | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
        | PhysicalPlan::Graph(GraphOp::RagFusion { .. })
        | PhysicalPlan::Document(DocumentOp::Scan { .. })
        | PhysicalPlan::Document(DocumentOp::IndexedFetch { .. })
        | PhysicalPlan::Columnar(ColumnarOp::Scan { .. })
        | PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. })
        | PhysicalPlan::Spatial(SpatialOp::Scan { .. })
        | PhysicalPlan::Kv(KvOp::Scan { .. })
        | PhysicalPlan::Kv(KvOp::BatchGet { .. })
        | PhysicalPlan::Query(QueryOp::Aggregate { .. })
        | PhysicalPlan::Query(QueryOp::FacetCounts { .. })
        | PhysicalPlan::Query(QueryOp::HashJoin { .. })
        | PhysicalPlan::Query(QueryOp::RecursiveScan { .. })
        | PhysicalPlan::Query(QueryOp::RecursiveValue { .. })
        | PhysicalPlan::Query(QueryOp::LateralTopK { .. })
        | PhysicalPlan::Query(QueryOp::LateralLoop { .. })
        | PhysicalPlan::Graph(GraphOp::Algo { .. })
        | PhysicalPlan::Graph(GraphOp::Match { .. })
        | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
        | PhysicalPlan::Text(TextOp::Search { .. })
        | PhysicalPlan::Text(TextOp::PhraseSearch { .. })
        | PhysicalPlan::Text(TextOp::HybridSearch { .. })
        | PhysicalPlan::Text(TextOp::HybridSearchTriple { .. })
        | PhysicalPlan::Text(TextOp::BM25ScoreScan { .. })
        | PhysicalPlan::Text(TextOp::FtsIndexDoc { .. })
        | PhysicalPlan::Text(TextOp::FtsDeleteDoc { .. }) => PlanKind::MultiRow,

        PhysicalPlan::Kv(KvOp::Get { .. }) | PhysicalPlan::Kv(KvOp::FieldGet { .. }) => {
            PlanKind::SingleDocument
        }

        // Constant-result or catalog-scan expressions (SELECT 1, SELECT 'hello',
        // catalog scans, etc.) are compiled to ProviderScan. Route through MultiRow
        // so each array element streams as its own pgwire row.
        PhysicalPlan::Query(QueryOp::ProviderScan { .. }) => PlanKind::MultiRow,

        // Exchange nodes at this point mean the plan was not yet resolved.
        // Recurse into the child to determine the plan kind.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => describe_plan(&op.child),

        // DML operations that return affected row count.
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
        | PhysicalPlan::Document(DocumentOp::BatchInsert { .. })
        | PhysicalPlan::Columnar(ColumnarOp::Insert { .. }) => DmlResult("INSERT"),

        PhysicalPlan::Document(DocumentOp::PointUpdate {
            returning: Some(_), ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
        PhysicalPlan::Document(DocumentOp::PointUpdate { .. })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate { .. }) => DmlResult("UPDATE"),

        PhysicalPlan::Document(DocumentOp::PointDelete {
            returning: Some(_), ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkDelete {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
        PhysicalPlan::Document(DocumentOp::PointDelete { .. })
        | PhysicalPlan::Document(DocumentOp::BulkDelete { .. }) => DmlResult("DELETE"),

        PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin { .. }) => DmlResult("UPDATE"),

        PhysicalPlan::Document(DocumentOp::Truncate { .. }) => DmlResult("TRUNCATE"),

        PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) => DmlResult("INSERT"),

        PhysicalPlan::Document(DocumentOp::Upsert { .. }) => DmlResult("UPSERT"),

        // Array engine read & maintenance ops produce a JSON-array
        // payload of rows; route to the multi-row decoder so each row
        // streams as its own pgwire `result` field. Aggregate's payload
        // is plain msgpack (decode_payload_to_json transcodes); Slice /
        // Project payloads use the tagged Value codec which transcodes
        // to a JSON array of arrays — clients receive JSON text per row.
        PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Slice { .. }) => {
            PlanKind::ArraySlice
        }
        PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Project { .. })
        | PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Aggregate { .. })
        | PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Elementwise { .. }) => {
            PlanKind::MultiRow
        }
        // Flush / Compact return `{flushed: 1}` / `{compacted: N}` —
        // route as SingleDocument so the row's `document` column
        // carries the status JSON.
        PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Flush { .. })
        | PhysicalPlan::Array(nodedb_physical::physical_plan::ArrayOp::Compact { .. }) => {
            PlanKind::SingleDocument
        }

        _ => PlanKind::Execution,
    }
}

// Bring the variant into scope for brevity in match arms above.
use PlanKind::DmlResult;

/// Returns `true` when a plan can produce a deterministic pgwire tag without
/// a round-trip to the Data Plane.
///
/// The Calvin multi-shard batch completes as a unit; the Data Plane does not
/// stream individual row counts back per task. For foldable plans we synthesise
/// a tag at plan time (INSERT 0 1, UPDATE 1, DELETE 1). Conservative list:
///
/// **Foldable** — plain point writes where the affected row count is always 1:
///   - `PointPut`, `PointInsert` (Document) → INSERT 0 1
///   - `PointUpdate` without RETURNING (Document) → UPDATE 1
///   - `PointDelete` without RETURNING (Document) → DELETE 1
///   - `KvOp::Put`, `KvOp::Insert`, `KvOp::InsertIfAbsent` → INSERT 0 1
///   - `KvOp::Delete` → DELETE 1
///
/// **Not foldable** (conservative defaults — do NOT expand without care):
///   - Any plan with `RETURNING` (response stream carries rows, not a tag)
///   - `InsertSelect` (row count from source query; unknown at plan time)
///   - `BatchInsert`, `BatchPut` (N rows; count in payload)
///   - `BulkUpdate`, `BulkDelete` (predicate-based; count in payload)
///   - `TimeseriesOp::Ingest` (separate path)
///   - `ColumnarOp::Insert` (batch path; count in payload)
///   - Any `Array`, `Spatial`, `Vector`, `Graph`, or `Text` write
///   - Any `SELECT` / `Query` plan (mixing read responses with a write tag
///     corrupts the response stream)
///   - Any other plan not explicitly listed above
pub(super) fn is_calvin_foldable(plan: &PhysicalPlan) -> bool {
    use nodedb_physical::physical_plan::KvOp;

    match plan {
        // Plain point document writes — always affects 1 row, no RETURNING.
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
        | PhysicalPlan::Document(DocumentOp::PointInsert { .. }) => true,

        // PointUpdate / PointDelete: foldable only when no RETURNING clause.
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            returning: None, ..
        })
        | PhysicalPlan::Document(DocumentOp::PointDelete {
            returning: None, ..
        }) => true,

        // Plain KV point writes.
        PhysicalPlan::Kv(KvOp::Put { .. })
        | PhysicalPlan::Kv(KvOp::Insert { .. })
        | PhysicalPlan::Kv(KvOp::InsertIfAbsent { .. })
        | PhysicalPlan::Kv(KvOp::Delete { .. }) => true,

        // Everything else: not foldable.
        _ => false,
    }
}

/// Synthesise the pgwire `CommandComplete` tag for a Calvin-foldable plan.
///
/// Caller invariant: `plan` must already have passed `is_calvin_foldable`.
/// The match arms here are kept in lockstep with that predicate so a desync
/// between the two is loud rather than silent.
pub(super) fn calvin_tag_for_plan(plan: &PhysicalPlan) -> Tag {
    use nodedb_physical::physical_plan::KvOp;

    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
        | PhysicalPlan::Document(DocumentOp::PointInsert { .. })
        | PhysicalPlan::Kv(KvOp::Put { .. })
        | PhysicalPlan::Kv(KvOp::Insert { .. })
        | PhysicalPlan::Kv(KvOp::InsertIfAbsent { .. }) => Tag::new("INSERT").with_rows(1),

        PhysicalPlan::Document(DocumentOp::PointUpdate {
            returning: None, ..
        }) => Tag::new("UPDATE").with_rows(1),

        PhysicalPlan::Document(DocumentOp::PointDelete {
            returning: None, ..
        })
        | PhysicalPlan::Kv(KvOp::Delete { .. }) => Tag::new("DELETE").with_rows(1),

        other => unreachable!(
            "calvin_tag_for_plan called on non-foldable plan; \
             is_calvin_foldable invariant broken: {other:?}"
        ),
    }
}

/// Extract affected row count from a JSON or MessagePack payload.
///
/// Looks for `"affected"`, `"truncated"`, `"inserted"`, or `"accepted"` fields.
fn extract_affected_count(payload: &[u8]) -> Option<u64> {
    if payload.is_empty() {
        return None;
    }
    let v: serde_json::Value = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice(payload).ok())?;
    v.get("affected")
        .or_else(|| v.get("truncated"))
        .or_else(|| v.get("inserted"))
        .or_else(|| v.get("accepted"))
        .and_then(|n| n.as_u64())
}

/// Outcome of shaping a Data Plane payload into a pgwire `Response`.
///
/// `notice` is set when the response shaper detected a condition the client
/// should know about (e.g. `truncated_before_horizon` on an array slice).
/// Callers forward it to the per-connection notice queue.
pub(super) struct ShapedResponse {
    pub response: Response,
    pub notice: Option<String>,
}

impl From<Response> for ShapedResponse {
    fn from(response: Response) -> Self {
        Self {
            response,
            notice: None,
        }
    }
}

/// NOTICE message emitted when a slice request's `system_as_of` cutoff fell
/// below the oldest tile version on at least one shard.
const TRUNCATED_BEFORE_HORIZON_NOTICE: &str = "AS OF SYSTEM TIME cutoff is older than the oldest retained tile version; \
     results may be incomplete";

pub(super) fn payload_to_response(payload: &[u8], kind: PlanKind) -> ShapedResponse {
    match kind {
        PlanKind::Execution => Response::Execution(Tag::new("OK")).into(),
        PlanKind::DmlResult(tag) => {
            let count = if payload.is_empty() {
                // Point operations with empty payload succeeded on exactly 1 row.
                1
            } else {
                extract_affected_count(payload).unwrap_or(1) as usize
            };
            Response::Execution(Tag::new(tag).with_rows(count)).into()
        }
        PlanKind::ArraySlice => {
            // Decode `ArraySliceResponse` envelope: extract rows and flag.
            // Fall back to treating the payload as a plain array when the
            // decode fails (e.g., empty payload or legacy shape).
            let schema = Arc::new(vec![text_field("result")]);
            if payload.is_empty() {
                return Response::Query(QueryResponse::new(schema, stream::empty())).into();
            }
            let (rows_json, truncated) =
                if let Ok(resp) = zerompk::from_msgpack::<ArraySliceResponse>(payload) {
                    let text = decode_payload_to_json(&resp.rows_msgpack);
                    (text, resp.truncated_before_horizon)
                } else {
                    // Fallback for any legacy or plain-array payloads.
                    (decode_payload_to_json(payload), false)
                };
            let notice = if truncated {
                Some(TRUNCATED_BEFORE_HORIZON_NOTICE.to_string())
            } else {
                None
            };
            let response = if let Ok(serde_json::Value::Array(items)) =
                sonic_rs::from_str::<serde_json::Value>(&rows_json)
            {
                let row_schema = schema.clone();
                let rows: Vec<_> = items
                    .iter()
                    .map(|item| {
                        let mut encoder = DataRowEncoder::new(row_schema.clone());
                        let _ = encoder.encode_field(&item.to_string());
                        Ok(encoder.take_row())
                    })
                    .collect();
                Response::Query(QueryResponse::new(schema, stream::iter(rows)))
            } else {
                let mut encoder = DataRowEncoder::new(schema.clone());
                if let Err(e) = encoder.encode_field(&rows_json) {
                    tracing::error!(error = %e, "failed to encode array slice field");
                    return Response::Execution(Tag::new("ERROR")).into();
                }
                let row = encoder.take_row();
                Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)])))
            };
            ShapedResponse { response, notice }
        }
        PlanKind::ReturningRows => {
            if payload.is_empty() {
                let schema = Arc::new(vec![text_field("result")]);
                return Response::Query(QueryResponse::new(schema, stream::empty())).into();
            }
            match zerompk::from_msgpack::<RowsPayload>(payload) {
                Ok(rp) => {
                    if rp.rows.is_empty() {
                        let schema = if rp.columns.is_empty() {
                            Arc::new(vec![text_field("result")])
                        } else {
                            Arc::new(rp.columns.iter().map(|c| text_field(c)).collect::<Vec<_>>())
                        };
                        return Response::Query(QueryResponse::new(schema, stream::empty())).into();
                    }
                    let schema: Arc<Vec<_>> =
                        Arc::new(rp.columns.iter().map(|c| text_field(c)).collect());
                    let row_schema = schema.clone();
                    let rows: Vec<_> = rp
                        .rows
                        .iter()
                        .map(|row_vals| {
                            let mut encoder = DataRowEncoder::new(row_schema.clone());
                            for cell in row_vals {
                                let _ = encoder.encode_field(cell);
                            }
                            Ok(encoder.take_row())
                        })
                        .collect();
                    Response::Query(QueryResponse::new(schema, stream::iter(rows))).into()
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        payload_len = payload.len(),
                        "ReturningRows msgpack decode failed; falling back to single-column JSON"
                    );
                    // Fall back to single-column JSON representation.
                    let schema = Arc::new(vec![text_field("result")]);
                    let text = decode_payload_to_json(payload);
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    let _ = encoder.encode_field(&text);
                    let row = encoder.take_row();
                    Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)]))).into()
                }
            }
        }
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            let col_name = if matches!(kind, PlanKind::SingleDocument) {
                "document"
            } else {
                "result"
            };
            let schema = Arc::new(vec![text_field(col_name)]);
            if payload.is_empty() {
                return Response::Query(QueryResponse::new(schema, stream::empty())).into();
            }
            let text = decode_payload_to_json(payload);

            // For multi-row results, parse the JSON array and stream each
            // element as a separate pgwire row. This avoids materializing
            // a single giant row for large result sets.
            if matches!(kind, PlanKind::MultiRow)
                && let Ok(serde_json::Value::Array(items)) =
                    sonic_rs::from_str::<serde_json::Value>(&text)
            {
                let row_schema = schema.clone();
                let rows: Vec<_> = items
                    .iter()
                    .map(|item| {
                        let mut encoder = DataRowEncoder::new(row_schema.clone());
                        let _ = encoder.encode_field(&item.to_string());
                        Ok(encoder.take_row())
                    })
                    .collect();
                return Response::Query(QueryResponse::new(schema, stream::iter(rows))).into();
            }

            // Single document or non-array: send as one row.
            let mut encoder = DataRowEncoder::new(schema.clone());
            if let Err(e) = encoder.encode_field(&text) {
                tracing::error!(error = %e, "failed to encode field");
                return Response::Execution(Tag::new("ERROR")).into();
            }
            let row = encoder.take_row();
            Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)]))).into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::extract_affected_count;

    #[test]
    fn extract_affected_count_reads_msgpack_payload() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": 3 })).unwrap();
        assert_eq!(extract_affected_count(&payload), Some(3));
    }
}
