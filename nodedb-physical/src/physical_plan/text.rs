// SPDX-License-Identifier: Apache-2.0

//! Full-text search operations dispatched to the Data Plane.

use nodedb_types::SurrogateBitmap;

/// Full-text search physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TextOp {
    /// BM25 full-text search on the inverted index.
    Search {
        collection: String,
        query: String,
        top_k: usize,
        /// Enable fuzzy matching (Levenshtein) for typo tolerance.
        fuzzy: bool,
        /// Pre-computed bitmap of eligible surrogates (from prefilter evaluation).
        /// `None` = no prefilter; all postings are eligible.
        prefilter: Option<SurrogateBitmap>,
        /// RLS post-score filters (serialized `Vec<ScanFilter>`).
        /// Applied after BM25 scoring, before returning to client.
        /// Result count may be less than requested `top_k`.
        rls_filters: Vec<u8>,
    },

    /// Full-collection scan with per-row BM25 score injection.
    ///
    /// Scans every document in the collection, runs FTS scoring for each
    /// document against `query`, and returns all documents with a score
    /// column appended under `score_alias`. Documents that do not match the
    /// query receive a `null` score. This is the physical plan used when
    /// `bm25_score(field, term)` appears as a SELECT projection without a
    /// restricting WHERE clause — all rows must be present in the result set
    /// so the query planner cannot emit the hit-only `TextOp::Search` shape.
    BM25ScoreScan {
        collection: String,
        query: String,
        /// Column name under which the BM25 score is injected into each row.
        score_alias: String,
        /// Enable fuzzy matching for the scoring pass.
        fuzzy: bool,
    },

    /// Exact phrase search: all terms must appear consecutively in the document.
    ///
    /// Unlike `Search` (BM25 scoring), phrase search returns only documents
    /// where the query terms appear as an exact contiguous sequence. Scoring
    /// is positional: documents with the phrase closer to the start rank higher.
    PhraseSearch {
        collection: String,
        /// Ordered sequence of terms to match as a phrase.
        terms: Vec<String>,
        top_k: usize,
        /// Pre-computed bitmap of eligible surrogates (from prefilter evaluation).
        prefilter: Option<nodedb_types::SurrogateBitmap>,
    },

    /// Hybrid search: vector similarity + BM25 text, fused via RRF.
    HybridSearch {
        collection: String,
        query_vector: Vec<f32>,
        query_text: String,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        /// Weight for vector results in RRF (0.0–1.0). Default: 0.5.
        vector_weight: f32,
        filter_bitmap: Option<SurrogateBitmap>,
        /// RLS post-fusion filters.
        rls_filters: Vec<u8>,
        /// SELECT-list alias the response should use for the RRF score
        /// column. `None` means the executor uses the fixed internal name
        /// `rrf_score`. Set by the planner from the SELECT alias for the
        /// `rrf_score(...)` call.
        score_alias: Option<String>,
    },

    /// Index a document into the inverted FTS index.
    ///
    /// Used by the sync path when a Lite client sends an `FtsIndex` frame.
    /// Origin assigns a surrogate for `(collection, doc_id)` on the Control
    /// Plane before dispatch; `surrogate` is the pre-assigned value.
    FtsIndexDoc {
        collection: String,
        /// Pre-assigned global surrogate for `(collection, doc_id)`.
        surrogate: nodedb_types::Surrogate,
        /// Concatenated text to index.
        text: String,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    },

    /// Remove a document from the inverted FTS index.
    ///
    /// Used by the sync path when a Lite client sends an `FtsDelete` frame.
    FtsDeleteDoc {
        collection: String,
        /// Pre-assigned global surrogate for `(collection, doc_id)`.
        surrogate: nodedb_types::Surrogate,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    },

    /// Three-source hybrid search: vector + BM25 text + graph BFS, fused via weighted RRF.
    ///
    /// Extends `HybridSearch` with an optional graph BFS leg. The graph leg
    /// performs a BFS from `graph_seed_id` up to `graph_depth` hops, filtering
    /// edges by `graph_edge_label` when set. All three ranked lists are passed
    /// to `reciprocal_rank_fusion_weighted` with per-source k-constants.
    HybridSearchTriple {
        collection: String,
        query_vector: Vec<f32>,
        query_text: String,
        /// Node id used as the BFS seed for the graph leg.
        graph_seed_id: String,
        /// Maximum BFS depth from the seed node.
        graph_depth: usize,
        /// Edge label filter for graph BFS. `None` = all edges.
        graph_edge_label: Option<String>,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        /// Per-source RRF k constants: (vector_k, text_k, graph_k).
        rrf_k: (f64, f64, f64),
        filter_bitmap: Option<SurrogateBitmap>,
        /// RLS post-fusion filters.
        rls_filters: Vec<u8>,
        /// SELECT-list alias for the fused RRF score column.
        score_alias: Option<String>,
    },

    /// Bind a collection's per-collection FTS analyzer.
    ///
    /// Persists to backend metadata (`InvertedIndex::set_collection_analyzer`)
    /// so `InvertedIndex::analyze_for_collection` resolves it for every
    /// subsequent tokenization of the collection's text — forward indexing,
    /// the in-transaction staged-write overlay, and query-time scoring alike.
    /// Config-only, single-node, non-WAL-durable — the same dispatch shape
    /// `VectorOp::SetParams` uses for `CREATE VECTOR INDEX`.
    SetTextConfig {
        collection: String,
        /// Analyzer name, already checked against
        /// `nodedb_fts::index::analyzer_config::analyzer_exists` by the DDL
        /// layer (e.g. "standard", "english", "japanese"). `None` leaves the
        /// collection's current analyzer in place.
        analyzer_name: Option<String>,
        /// Whether searches over this collection fall back to fuzzy matching
        /// by default. `None` leaves the current setting in place.
        fuzzy_default: Option<bool>,
    },
}
