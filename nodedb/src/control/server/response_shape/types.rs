// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral plan classification types.
//!
//! These operate purely on `PhysicalPlan` and carry no pgwire wire types,
//! so they are shared across any protocol-specific response shaper.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, QueryOp, SpatialOp, TextOp, TimeseriesOp,
    VectorOp,
};

#[derive(Debug, Clone, Copy)]
pub enum PlanKind {
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

pub fn describe_plan(plan: &PhysicalPlan) -> PlanKind {
    match plan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            returning: Some(_), ..
        })
        | PhysicalPlan::Crdt(CrdtOp::DocDelete {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,

        // A CRDT delete can legitimately remove nothing (the document was
        // already tombstoned), so its affected count is only knowable from the
        // write's own response — it must render as a DML count, not as a
        // document-shaped read whose count no consumer ever reads.
        PhysicalPlan::Crdt(CrdtOp::DocDelete { .. }) => DmlResult("DELETE"),

        PhysicalPlan::Document(DocumentOp::PointGet { .. })
        | PhysicalPlan::Crdt(CrdtOp::Read { .. })
        | PhysicalPlan::Crdt(CrdtOp::GetPolicy { .. })
        | PhysicalPlan::Crdt(CrdtOp::DocUpsert { .. }) => PlanKind::SingleDocument,

        PhysicalPlan::Vector(VectorOp::Search { .. })
        | PhysicalPlan::Vector(VectorOp::MultiSearch { .. })
        | PhysicalPlan::Vector(VectorOp::MultiVectorScoreSearch { .. })
        | PhysicalPlan::Vector(VectorOp::SparseSearch { .. })
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
        | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
        | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
        | PhysicalPlan::Graph(GraphOp::WccSuperstep(_))
        | PhysicalPlan::Text(TextOp::Search { .. })
        | PhysicalPlan::Text(TextOp::PhraseSearch { .. })
        | PhysicalPlan::Text(TextOp::HybridSearch { .. })
        | PhysicalPlan::Text(TextOp::HybridSearchTriple { .. })
        | PhysicalPlan::Text(TextOp::BM25ScoreScan { .. })
        | PhysicalPlan::Text(TextOp::FtsIndexDoc { .. })
        | PhysicalPlan::Text(TextOp::FtsDeleteDoc { .. }) => PlanKind::MultiRow,

        // Analyzer-binding DDL config write — opaque execution result, same
        // as `VectorOp::SetParams`.
        PhysicalPlan::Text(TextOp::SetTextConfig { .. })
        // Index teardown reports only success or failure, like `SetParams`.
        | PhysicalPlan::Vector(VectorOp::DropIndex { .. })
        // Preview results are an internal typed zerompk control-plane value,
        // never a client document row. Preserve the bytes for the admission
        // caller to decode as `CrdtPreviewResult`.
        | PhysicalPlan::Crdt(CrdtOp::PreviewApply { .. }) => PlanKind::Execution,

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

        // PostProcess reshapes a multi-row subquery result; its kind is the
        // child's. (Unresolved at this point; resolved to a ProviderScan =
        // MultiRow before dispatch.)
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => describe_plan(input),

        // DML operations that return affected row count.
        //
        // `PointInsert` and `KvOp::InsertIfAbsent` are here because
        // `ON CONFLICT DO NOTHING` makes them no-op-capable: the count is 0 when
        // the key was already present, so it has to be read from the write's
        // response instead of assumed from the plan.
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
        | PhysicalPlan::Document(DocumentOp::PointInsert { .. })
        | PhysicalPlan::Document(DocumentOp::BatchInsert { .. })
        | PhysicalPlan::Kv(KvOp::InsertIfAbsent { .. })
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

        // A MERGE carrying a projection returns real target rows, so it must be
        // decoded and redacted like every other RETURNING write. Without this
        // arm it fell through to `Execution`, whose passthrough forwards the
        // Data-Plane payload to the client with no redaction applied at all.
        PhysicalPlan::Document(DocumentOp::Merge {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
        // Postgres tags a plain MERGE `MERGE <total-rows-affected>`, matching
        // the in-transaction staged path's tag.
        PhysicalPlan::Document(DocumentOp::Merge { .. }) => DmlResult("MERGE"),

        PhysicalPlan::Document(DocumentOp::Truncate { .. }) => DmlResult("TRUNCATE"),

        // KV delete / truncate count the keys they removed. Classifying them as
        // opaque `Execution` discarded that count, so a KV delete reported no
        // rows however many keys it actually removed.
        PhysicalPlan::Kv(KvOp::Delete { .. }) => DmlResult("DELETE"),
        PhysicalPlan::Kv(KvOp::Truncate { .. }) => DmlResult("TRUNCATE"),

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

        // Vector write / config ops carry no row payload to shape — they
        // return an affected-count or status. Enumerated explicitly (not via
        // a `Vector(_)` wildcard) so a future *read* op like `SparseSearch`
        // cannot silently fall through to `Execution` and strand its hits.
        PhysicalPlan::Vector(VectorOp::Insert { .. })
        | PhysicalPlan::Vector(VectorOp::BatchInsert { .. })
        | PhysicalPlan::Vector(VectorOp::Delete { .. })
        | PhysicalPlan::Vector(VectorOp::DeleteBySurrogate { .. })
        | PhysicalPlan::Vector(VectorOp::SetParams { .. })
        | PhysicalPlan::Vector(VectorOp::QueryStats { .. })
        | PhysicalPlan::Vector(VectorOp::Seal { .. })
        | PhysicalPlan::Vector(VectorOp::CompactIndex { .. })
        | PhysicalPlan::Vector(VectorOp::Rebuild { .. })
        | PhysicalPlan::Vector(VectorOp::SparseInsert { .. })
        | PhysicalPlan::Vector(VectorOp::SparseDelete { .. })
        | PhysicalPlan::Vector(VectorOp::MultiVectorInsert { .. })
        | PhysicalPlan::Vector(VectorOp::MultiVectorDelete { .. })
        | PhysicalPlan::Vector(VectorOp::DirectUpsert { .. }) => PlanKind::Execution,

        // Document ops with no row payload to shape: index DDL, collection
        // registration, cardinality estimates, and the clone materializer's
        // cursor scan (whose payload is an internal typed tuple the
        // materializer decodes itself, never a client row). Enumerated
        // explicitly, not via a `Document(_)` wildcard: a wildcard here made
        // `Merge` default to unredacted passthrough for as long as it carried
        // no rows, and the next row-bearing op added would inherit exactly the
        // same silent leak.
        PhysicalPlan::Document(DocumentOp::Register { .. })
        | PhysicalPlan::Document(DocumentOp::IndexLookup { .. })
        | PhysicalPlan::Document(DocumentOp::DropIndex { .. })
        | PhysicalPlan::Document(DocumentOp::BackfillIndex { .. })
        | PhysicalPlan::Document(DocumentOp::EstimateCount { .. })
        | PhysicalPlan::Document(DocumentOp::MaterializeScan { .. })

        // Default: opaque execution result. The specific arms above take
        // precedence; these inner wildcards catch every unmatched op of each
        // engine (including the remaining `Crdt` ops not covered above) plus
        // the engines with no arms at all here (Meta, ClusterArray).
        // Exhaustive so a new PhysicalPlan variant forces a decision.
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => PlanKind::Execution,
    }
}

// Bring the variant into scope for brevity in match arms above.
use PlanKind::DmlResult;

/// Protocol-neutral SQL column type. Each server entrypoint maps this to its
/// own wire type (pgwire OID, native type tag, etc.). One variant per pgwire
/// field-builder in `pgwire::types::field`, so the mapping is lossless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DdlColType {
    #[default]
    Text,
    Int8,
    Int4,
    Int2,
    Float8,
    Float4,
    Bool,
    Bytea,
    Json,
    Jsonb,
    Timestamp,
    Timestamptz,
    Varchar,
    Float4Array,
    Float8Array,
}

/// Protocol-neutral shaped row set: columns + row objects + an optional
/// client-facing notice. Not yet constructed anywhere — a later relocation
/// unit wires this into a shared composed entry point.
#[derive(Debug, Clone)]
pub struct ShapedRows {
    pub columns: Vec<String>,
    /// Per-column SQL type, parallel to (same length/order as) `columns`.
    /// Only the pgwire encoder consumes this to reproduce exact RowDescription
    /// type OIDs; the native and http entrypoints ignore it. `Text` is used
    /// wherever the source type is unknown.
    pub column_types: Vec<DdlColType>,
    /// One map per row. Cells are keyed by [`ShapedRows::cell_keys`], NOT by
    /// `columns` directly — SQL output names may repeat (`SELECT w.id, b.id`
    /// displays both as `id`) and a map cannot hold two cells under one key.
    /// Read cells through `cell_keys` so each column reads its own value.
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub notice: Option<String>,
}

impl ShapedRows {
    /// Build a `column_types` vec of `n` `Text` entries, for the non-DDL
    /// construction sites whose consumers (native/http) ignore column types.
    pub fn text_types(n: usize) -> Vec<DdlColType> {
        vec![DdlColType::Text; n]
    }

    /// Per-column keys for reading cells out of [`ShapedRows::rows`], parallel
    /// to `columns`.
    ///
    /// This is the single source of truth every consumer shares — the pgwire
    /// encoders, the native converter, and the HTTP JSON serializers all key
    /// rows through it, so the row-map layout can never drift from what a
    /// consumer expects.
    ///
    /// Identical to `columns` unless two output columns share a name, in which
    /// case later duplicates take a `_<n>` suffix (see
    /// [`super::project::cell_keys`]). Because the HTTP transports serialize a
    /// row map directly to JSON, that suffix is user-visible there: a
    /// duplicate-name `SELECT w.id, b.id` emits `{"id": …, "id_1": …}`, since
    /// a JSON object likewise cannot carry the same key twice. pgwire and
    /// native are positional on the wire and still report both columns as
    /// `id`, matching PostgreSQL.
    pub fn cell_keys(&self) -> Vec<String> {
        super::project::cell_keys(&self.columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crdt_preview_is_an_opaque_execution_plan() {
        let plan = PhysicalPlan::Crdt(CrdtOp::PreviewApply {
            collection: "tasks".to_string(),
            document_id: "task-1".to_string(),
            delta: vec![0x92, 0x01],
        });

        assert!(matches!(describe_plan(&plan), PlanKind::Execution));
    }

    fn merge_plan(
        returning: Option<nodedb_physical::physical_plan::ReturningSpec>,
    ) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: "target".to_string(),
            source_collection: "source".to_string(),
            source_alias: "s".to_string(),
            target_join_col: "id".to_string(),
            source_join_col: "id".to_string(),
            clauses: Vec::new(),
            returning,
            resolve_only: false,
            resolved_inserts: None,
            source_rows: None,
        })
    }

    /// A `MERGE ... RETURNING` payload is a `RowsPayload` of real target rows.
    /// Classifying it as `Execution` would pass those rows straight to the
    /// client with no decode and no redaction — the leak this arm closes.
    #[test]
    fn merge_with_returning_is_returning_rows() {
        use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};

        let plan = merge_plan(Some(ReturningSpec {
            columns: ReturningColumns::Star,
        }));

        assert!(matches!(describe_plan(&plan), PlanKind::ReturningRows));
    }

    /// A plain MERGE reports its affected count under the Postgres `MERGE` tag,
    /// not an opaque `OK`.
    #[test]
    fn merge_without_returning_is_a_dml_result() {
        assert!(matches!(
            describe_plan(&merge_plan(None)),
            PlanKind::DmlResult("MERGE")
        ));
    }
}
