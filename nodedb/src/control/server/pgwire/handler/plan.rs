// SPDX-License-Identifier: BUSL-1.1

//! Plan classification and response formatting.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::response_codec::decode_payload_to_json;
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, MetaOp, QueryOp, SpatialOp, TextOp, TimeseriesOp,
    VectorOp,
};

use crate::control::server::shared::sql::staging_predicates::{
    StagedTagKind, extract_affected_count,
};

use super::super::types::text_field;

pub(super) use crate::control::server::response_shape::types::{PlanKind, describe_plan};

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
        | PhysicalPlan::Document(DocumentOp::PointInsert { collection, .. })
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
        | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
        | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
        | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
        | PhysicalPlan::Graph(GraphOp::WccSuperstep(_)) => None,
        // Exchange: recurse into the child plan to extract the collection.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => extract_collection(&op.child),
        // ProviderScan is a catalog/constant source — no user collection.
        PhysicalPlan::Query(QueryOp::ProviderScan { .. }) => None,
        // All remaining ops carry no extractable user collection here. The
        // specific arms above take precedence; these inner wildcards catch the
        // unmatched ops of each engine plus the engines with no arms at all
        // (Kv, Array, ClusterArray). Exhaustive so a new PhysicalPlan variant
        // forces a decision rather than silently returning None.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_) => None,
    }
}

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

        // Everything else: not foldable. The foldable arms above take
        // precedence; these inner wildcards catch every remaining op of each
        // engine. Exhaustive so a new PhysicalPlan variant forces a decision.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_) => false,
    }
}

/// Render a neutral [`StagedTagKind`] (decided by the protocol-neutral
/// staging gate) as the pgwire `CommandComplete` tag, preserving the exact
/// tag strings the pre-refactor `point_write_tag` / `kv_write_tag` produced:
/// `INSERT 0 n` / `UPDATE n` / `DELETE n`, and for a KV
/// `InsertOnConflictUpdate` outcome, `UPDATE n` when the stage handler
/// resolved to an update or `INSERT 0 n` when it resolved to an insert.
pub(super) fn tag_from_staged(kind: StagedTagKind, affected: usize) -> Tag {
    match kind {
        StagedTagKind::Insert => Tag::new("INSERT").with_rows(affected),
        StagedTagKind::Update => Tag::new("UPDATE").with_rows(affected),
        StagedTagKind::Delete => Tag::new("DELETE").with_rows(affected),
        StagedTagKind::KvUpsert { updated: true } => Tag::new("UPDATE").with_rows(affected),
        StagedTagKind::KvUpsert { updated: false } => Tag::new("INSERT").with_rows(affected),
        // Matches the autocommit `DocumentOp::Upsert` tag exactly: always the
        // literal `UPSERT` command, regardless of insert-vs-update outcome
        // (see `response_shape::types::describe_plan`'s `DmlResult("UPSERT")`
        // arm and `payload_to_response`'s `PlanKind::DmlResult` rendering).
        StagedTagKind::DocUpsert => Tag::new("UPSERT").with_rows(affected),
        // KV `Incr` / `IncrFloat` / `Cas` / `GetSet` never reach pgwire's
        // generic tag-rendering path today: their sole SQL surface (`SELECT
        // KV_INCR(..)` and friends, in `ddl/neutral/kv_atomic.rs`) reads
        // `StagedWriteOutcome::payload` directly and never calls
        // `tag_from_staged`. This arm exists only so the match stays
        // exhaustive against a new `PhysicalPlan::Kv` caller; it renders the
        // same tag pgwire uses for a function-call `SELECT`.
        StagedTagKind::RawPayload => Tag::new("SELECT").with_rows(affected),
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
        PlanKind::ArraySlice | PlanKind::ReturningRows | PlanKind::SingleDocument => {
            unreachable!(
                "shaped via response_shape::compose; payload_to_response is only reached \
                 for Execution/DmlResult tags and MultiRow (facet)"
            )
        }
        PlanKind::MultiRow => {
            let schema = Arc::new(vec![text_field("result")]);
            if payload.is_empty() {
                return Response::Query(QueryResponse::new(schema, stream::empty())).into();
            }
            let text = decode_payload_to_json(payload);

            // For multi-row results, parse the JSON array and stream each
            // element as a separate pgwire row. This avoids materializing
            // a single giant row for large result sets.
            if let Ok(serde_json::Value::Array(items)) =
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
