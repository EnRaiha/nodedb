// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral physical-plan classification helpers shared by every
//! server entrypoint (pgwire, native, http) and the transaction orchestrator.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::session::read_set::{EngineTag, ReadKey};
use crate::types::KeyRepr;
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, QueryOp, SpatialOp, TextOp,
    TimeseriesOp, VectorOp,
};

/// Extract the collection name from a physical plan (if applicable).
pub(crate) fn extract_collection(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Search { collection, .. })
        | PhysicalPlan::Document(DocumentOp::RangeScan { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::Read { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::PreviewApply { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::Apply { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::DocUpsert { collection, .. })
        | PhysicalPlan::Crdt(CrdtOp::DocDelete { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Insert { collection, .. })
        | PhysicalPlan::Vector(VectorOp::BatchInsert { collection, .. })
        | PhysicalPlan::Vector(VectorOp::MultiSearch { collection, .. })
        // A vector-primary collection stores its row HERE and nowhere else, so
        // this op is the only source a RETURNING projection over it can name.
        // Reporting `None` left those rows with no collection to key a
        // redaction policy on, and the masking pass ran inert.
        | PhysicalPlan::Vector(VectorOp::DirectUpsert { collection, .. })
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
        | PhysicalPlan::Text(TextOp::SetTextConfig { collection, .. })
        | PhysicalPlan::Query(QueryOp::PartialAggregate { collection, .. })
        | PhysicalPlan::Query(QueryOp::FacetCounts { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Upsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: collection,
            ..
        })
        // The joined source is read, but every row these two write — and every
        // row their RETURNING clause surfaces — belongs to the TARGET, so the
        // target is the collection whose policies and write version apply.
        // Reporting `None` here left their RETURNING rows with no source
        // collection to key a redaction policy on, so the masking pass ran
        // inert and shipped the rows in the clear.
        | PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: collection,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: collection,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::Truncate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::EstimateCount { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Scan { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Insert { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Update { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Delete { collection, .. })
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
        // PostProcess: recurse into the materialized child (twin of
        // `PhysicalPlan::collection`).
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => extract_collection(input),
        // ProviderScan is a catalog/constant source — no user collection.
        PhysicalPlan::Query(QueryOp::ProviderScan { .. }) => None,
        // KV ops carry their own collection (sorted-index-only ops return None).
        PhysicalPlan::Kv(op) => op.collection(),
        // All remaining ops carry no extractable user collection here: the
        // specific arms above take precedence; these inner wildcards catch the
        // unmatched ops of each engine plus the engines with no arms at all
        // (Array, ClusterArray). Exhaustive so a new PhysicalPlan variant
        // forces a decision rather than silently returning None.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}

/// Classify which peer engine a plan targets. Total over the top-level
/// [`PhysicalPlan`] variants (one-to-one with [`EngineTag`]) so a new engine
/// forces an explicit decision rather than a silent default.
pub(crate) fn plan_engine(plan: &PhysicalPlan) -> EngineTag {
    match plan {
        PhysicalPlan::Vector(_) => EngineTag::Vector,
        PhysicalPlan::Graph(_) => EngineTag::Graph,
        PhysicalPlan::Document(_) => EngineTag::Document,
        PhysicalPlan::Kv(_) => EngineTag::Kv,
        PhysicalPlan::Text(_) => EngineTag::Text,
        PhysicalPlan::Columnar(_) => EngineTag::Columnar,
        PhysicalPlan::Timeseries(_) => EngineTag::Timeseries,
        PhysicalPlan::Spatial(_) => EngineTag::Spatial,
        PhysicalPlan::Crdt(_) => EngineTag::Crdt,
        PhysicalPlan::Query(_) => EngineTag::Query,
        PhysicalPlan::Meta(_) => EngineTag::Meta,
        PhysicalPlan::Array(_) => EngineTag::Array,
        PhysicalPlan::ClusterArray(_) => EngineTag::ClusterArray,
        PhysicalPlan::ClusterEvent(_) => EngineTag::Meta,
    }
}

/// Classify a read plan's observed identity for the transaction read-set.
///
/// `found` reports whether the point read actually observed a present row
/// (`true` on a hit, `false` on a miss / absent row). It is meaningful only for
/// single-row keyed lookups; scans and predicate reads ignore it.
///
/// Secondary-index reads whose observation is confined to one indexed
/// dimension always record the narrower `IndexEq` / `IndexRange` variants:
/// `DocumentOp::IndexedFetch` / `IndexLookup` (equality) and
/// `DocumentOp::RangeScan` (range). These validate identically to the
/// collection floor today; per-value comparison lands later.
///
/// Single-row keyed lookups whose identity maps to exactly one [`KeyRepr`]
/// record [`ReadKey::Point`]:
/// - `DocumentOp::PointGet` that HIT — the row's cross-engine surrogate.
/// - `KvOp::Get` / `KvOp::FieldGet` — the raw KV key bytes (on hit AND miss).
///
/// A `DocumentOp::PointGet` that MISSED records [`ReadKey::Predicate`] instead.
/// A document surrogate is allocated from a monotonic counter unrelated to the
/// `document_id`, so the placeholder surrogate an absent read carries never
/// coincides with the fresh surrogate a concurrent INSERT of that `document_id`
/// will receive — a `Point` key would never collide with the phantom insert and
/// the stale read would be wrongly judged current. Degrading the miss to the
/// collection-scoped predicate makes any insert into the read collection advance
/// the floor and abort the reader (collection-granular phantom safety). KV is
/// unaffected: a KV read key IS the literal byte key any future write reuses, so
/// an absent-key read collides correctly — it keeps its precise `Point` key.
///
/// Everything else records [`ReadKey::Predicate`] (collection-scoped, phantom-
/// safe). This deliberately includes keyed ops whose observation cannot be
/// captured by a single `KeyRepr` without under-approximating — `KvOp::BatchGet`
/// (many keys). A single-key repr for those would MISS the other keys / rows
/// they observed, which is the one thing phantom safety forbids; the coarse
/// collection floor over-aborts instead, which is always safe. Secondary-index
/// reads are the one exception carved out above: their observation IS confined
/// to the indexed dimension, so `IndexEq` / `IndexRange` capture it precisely
/// without under-approximating. The match is total over [`PhysicalPlan`] so a
/// new variant forces a classification.
pub(crate) fn read_key_of(plan: &PhysicalPlan, found: bool) -> ReadKey {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { surrogate, .. }) => {
            if found {
                ReadKey::Point {
                    repr: KeyRepr::Surrogate(surrogate.as_u32()),
                }
            } else {
                ReadKey::Predicate
            }
        }
        PhysicalPlan::Kv(KvOp::Get { key, .. }) | PhysicalPlan::Kv(KvOp::FieldGet { key, .. }) => {
            ReadKey::Point {
                repr: KeyRepr::KvKey(key.clone().into_boxed_slice()),
            }
        }
        // Secondary-index equality: the observation is confined to the indexed
        // dimension, so record the indexed field + canonical stringified value.
        // `filters` (the residual compound predicate) is ignored: validating
        // the indexed dimension is sound (it never under-approximates the rows
        // a concurrent write must conflict against).
        PhysicalPlan::Document(
            DocumentOp::IndexedFetch { path, value, .. }
            | DocumentOp::IndexLookup { path, value, .. },
        ) => ReadKey::IndexEq {
            field: path.clone(),
            value: value.clone(),
        },
        // Secondary-index range: the bound bytes are interpreted as UTF-8
        // exactly as the scan itself interprets them. One-sided ranges keep the
        // present bound and leave the other `None`.
        PhysicalPlan::Document(DocumentOp::RangeScan {
            field,
            lower,
            upper,
            ..
        }) => ReadKey::IndexRange {
            field: field.clone(),
            lo: lower
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned()),
            hi: upper
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned()),
        },
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
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => ReadKey::Predicate,
    }
}
