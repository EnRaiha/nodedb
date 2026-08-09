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

        // An insert carrying a projection returns real stored rows, so it must
        // be decoded and redacted like every other RETURNING write. Without
        // these arms it falls through to the count shape below, whose
        // passthrough forwards the Data-Plane payload with no redaction applied
        // at all — the same silent leak `Merge` had.
        PhysicalPlan::Kv(
            KvOp::Insert {
                returning: Some(_), ..
            }
            | KvOp::InsertIfAbsent {
                returning: Some(_), ..
            }
            | KvOp::InsertOnConflictUpdate {
                returning: Some(_), ..
            }
            | KvOp::Put {
                returning: Some(_), ..
            }
            | KvOp::BatchPut {
                returning: Some(_), ..
            },
        )
        | PhysicalPlan::Document(DocumentOp::PointPut {
            returning: Some(_), ..
        })
        | PhysicalPlan::Document(DocumentOp::PointInsert {
            returning: Some(_), ..
        })
        | PhysicalPlan::Document(DocumentOp::BatchInsert {
            returning: Some(_), ..
        })
        | PhysicalPlan::Columnar(ColumnarOp::Insert {
            returning: Some(_), ..
        })
        | PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            returning: Some(_), ..
        })
        | PhysicalPlan::Vector(VectorOp::DirectUpsert {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,

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

        PhysicalPlan::Document(DocumentOp::Upsert {
            returning: Some(_), ..
        }) => PlanKind::ReturningRows,
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
        // A derived balance write answers no client: it reports an affected
        // count to the planner that appended it and shapes no row.
        | PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { .. })

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

    /// Fold another shaped result into this one so the N tasks a single
    /// statement plans to answer with ONE result set.
    ///
    /// A statement is one result set on the wire. A multi-row
    /// `INSERT ... RETURNING` plans to one task per row, and emitting a
    /// RowDescription/DataRow sequence per task hands an extended-query client
    /// several results for one statement — which drivers that expect exactly
    /// one either mis-read or reject. Rows accumulate in task order, which is
    /// the order the statement listed them.
    ///
    /// The column set is the UNION of every contributor's columns, so a column
    /// that appears in any row is present in the result. Rows are read by
    /// column key rather than by position, so a row lacking one of those
    /// columns encodes as NULL instead of shifting its remaining cells left.
    /// Both halves are required: keeping only the first contributor's columns
    /// would silently discard a later row's extra field, since the encoder
    /// reads strictly through [`ShapedRows::cell_keys`] and never sees a key
    /// absent from `columns`.
    ///
    /// **Ordering rule.** The first contributor that has any columns fixes the
    /// leading positions and keeps them; a column that a later contributor
    /// introduces is appended after them, in the order it is first seen. Column
    /// order is therefore deterministic and append-only across the fold — a
    /// client reading positionally never sees an earlier column move.
    ///
    /// Assumes each contributor's own column names are unique, which every
    /// `RETURNING` shape satisfies: the names are either a stored row's object
    /// keys or a projection list. That makes each cell key equal to its column
    /// name, so rows merge by name with no re-keying. (Repeated output names
    /// are a SELECT-list concern — `SELECT w.id, b.id` — and no SELECT is ever
    /// folded here.)
    pub fn append(&mut self, other: ShapedRows) {
        if self.notice.is_none() {
            self.notice = other.notice;
        }
        if self.columns.is_empty() {
            self.columns = other.columns;
            self.column_types = other.column_types;
            self.rows.extend(other.rows);
            return;
        }
        for (index, name) in other.columns.iter().enumerate() {
            if self.columns.iter().any(|existing| existing == name) {
                continue;
            }
            self.columns.push(name.clone());
            self.column_types
                .push(other.column_types.get(index).copied().unwrap_or_default());
        }
        self.rows.extend(other.rows);
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

    fn shaped(columns: &[&str], rows: &[&[(&str, &str)]]) -> ShapedRows {
        ShapedRows {
            columns: columns.iter().map(|c| (*c).to_string()).collect(),
            column_types: ShapedRows::text_types(columns.len()),
            rows: rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|(k, v)| {
                            (
                                (*k).to_string(),
                                serde_json::Value::String((*v).to_string()),
                            )
                        })
                        .collect()
                })
                .collect(),
            notice: None,
        }
    }

    /// A column only a LATER contributor carries must survive the fold.
    ///
    /// The encoder reads every cell through `cell_keys()`, derived from the
    /// final `columns`, so a key absent from `columns` is never read — keeping
    /// only the first contributor's columns dropped that value from the
    /// response silently, for every row rather than just the row that had it.
    #[test]
    fn append_unions_a_column_only_a_later_row_carries() {
        let mut merged = shaped(&["id", "name"], &[&[("id", "r1"), ("name", "a")]]);
        merged.append(shaped(
            &["id", "name", "extra"],
            &[&[("id", "r2"), ("name", "b"), ("extra", "x")]],
        ));

        assert_eq!(merged.columns, vec!["id", "name", "extra"]);
        assert_eq!(
            merged.column_types.len(),
            merged.columns.len(),
            "column types must stay parallel to columns"
        );

        let keys = merged.cell_keys();
        assert_eq!(keys, vec!["id", "name", "extra"]);
        assert_eq!(
            merged.rows[1].get(keys[2].as_str()),
            Some(&serde_json::Value::String("x".to_string())),
            "the later row's extra value must be readable through the merged keys"
        );
        assert!(
            merged.rows[0].get(keys[2].as_str()).is_none(),
            "the row that lacks the column encodes as NULL, not a shifted cell"
        );
    }

    /// The first contributor's columns keep their positions and newly-seen
    /// columns are appended in first-seen order, so a positional client never
    /// sees a column move between rows.
    #[test]
    fn append_keeps_the_first_contributors_column_order_and_appends_the_rest() {
        let mut merged = shaped(&["b", "a"], &[&[("b", "1"), ("a", "2")]]);
        merged.append(shaped(&["a", "z"], &[&[("a", "3"), ("z", "4")]]));
        merged.append(shaped(&["y", "b"], &[&[("y", "5"), ("b", "6")]]));

        assert_eq!(
            merged.columns,
            vec!["b", "a", "z", "y"],
            "first contributor's positions are fixed; later columns append in \
             first-seen order"
        );
        assert_eq!(merged.rows.len(), 3);
    }

    /// A contributor with no columns at all — a task whose rows were entirely
    /// removed by a read policy, which shapes as `RETURNING *` with an empty
    /// column list — must not fix an empty shape for the statement.
    #[test]
    fn append_adopts_the_shape_of_the_first_contributor_that_has_columns() {
        let mut merged = shaped(&[], &[]);
        merged.append(shaped(&["id"], &[&[("id", "r1")]]));

        assert_eq!(merged.columns, vec!["id"]);
        assert_eq!(merged.rows.len(), 1);
    }

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
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
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

    /// Every insert-family op that can carry a projection must classify as
    /// row-returning. Falling through to the count arm forwards the Data-Plane
    /// payload to the client with no decode and no redaction — the leak the
    /// MERGE arm above closed, which any new row-bearing op would inherit.
    #[test]
    fn inserts_with_returning_are_returning_rows() {
        use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};

        let spec = || {
            Some(ReturningSpec {
                columns: ReturningColumns::Star,
            })
        };
        let plans = [
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: "c".into(),
                document_id: "d".into(),
                value: Vec::new(),
                if_absent: false,
                surrogate: nodedb_types::Surrogate::ZERO,
                returning: spec(),
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "c".into(),
                document_id: "d".into(),
                value: Vec::new(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                returning: spec(),
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection: "c".into(),
                documents: Vec::new(),
                surrogates: Vec::new(),
                returning: spec(),
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::Upsert {
                collection: "c".into(),
                document_id: "d".into(),
                value: Vec::new(),
                on_conflict_updates: Vec::new(),
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: Vec::new(),
                returning: spec(),
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        ];
        for plan in &plans {
            assert!(
                matches!(describe_plan(plan), PlanKind::ReturningRows),
                "{plan:?} must shape as rows"
            );
        }
    }

    /// Every KV insert-family op that can carry a projection must classify as
    /// row-returning too — the same passthrough leak, one engine over.
    #[test]
    fn kv_inserts_with_returning_are_returning_rows() {
        use nodedb_physical::physical_plan::{KvOp, ReturningColumns, ReturningSpec};

        let spec = || {
            Some(ReturningSpec {
                columns: ReturningColumns::Star,
            })
        };
        let plans = [
            PhysicalPlan::Kv(KvOp::Insert {
                collection: "c".into(),
                key: b"k".to_vec(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
                returning: spec(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::InsertIfAbsent {
                collection: "c".into(),
                key: b"k".to_vec(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
                returning: spec(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
                collection: "c".into(),
                key: b"k".to_vec(),
                value: Vec::new(),
                ttl_ms: 0,
                updates: Vec::new(),
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: Vec::new(),
                returning: spec(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::Put {
                collection: "c".into(),
                key: b"k".to_vec(),
                value: Vec::new(),
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
                returning: spec(),
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Kv(KvOp::BatchPut {
                collection: "c".into(),
                entries: Vec::new(),
                ttl_ms: 0,
                surrogates: Vec::new(),
                returning: spec(),
                rls_filters: Vec::new(),
            }),
        ];
        for plan in &plans {
            assert!(
                matches!(describe_plan(plan), PlanKind::ReturningRows),
                "{plan:?} must shape as rows"
            );
        }
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
