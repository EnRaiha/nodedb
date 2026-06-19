// SPDX-License-Identifier: BUSL-1.1

//! Three-source hybrid search handler: vector + BM25 text + graph BFS, fused via weighted RRF.
//!
//! Pipeline:
//! 1. Vector search from the HNSW index — top-K by distance.
//! 2. BM25 full-text search from the inverted index — top-K by score.
//! 3. Graph BFS from `graph_seed_id` up to `graph_depth` hops — scored by hop distance.
//! 4. All three ranked lists are fused via `reciprocal_rank_fusion_weighted` with
//!    per-source k-constants `(vector_k, text_k, graph_k)`.
//! 5. Final top-K fused results are materialised with per-source rank diagnostics.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::graph_rag::graph_nodes_to_ranked_results;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::Direction;

impl CoreLoop {
    /// Execute a three-source hybrid search: vector + BM25 text + graph BFS, fused via RRF.
    ///
    /// `rrf_k` is `(vector_k, text_k, graph_k)`. Lower k → steeper rank discount → more
    /// influence from that source.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_hybrid_search_triple(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        query_vector: &[f32],
        query_text: &str,
        graph_seed_id: &str,
        graph_depth: usize,
        graph_edge_label: Option<&str>,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        rrf_k: (f64, f64, f64),
        filter_bitmap: Option<&nodedb_types::SurrogateBitmap>,
        rls_filters: &[u8],
        score_alias: Option<&str>,
    ) -> Response {
        let tenant_id = crate::types::TenantId::new(tid);
        debug!(
            core = self.core_id,
            tid,
            %collection,
            %query_text,
            %graph_seed_id,
            graph_depth,
            top_k,
            "hybrid search triple"
        );

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let fetch_k = top_k.saturating_mul(3).max(20);

        // 1. Vector search.
        let index_key =
            CoreLoop::vector_index_key(task.request.database_id.as_u64(), tid, collection, "");
        let vector_collection = self.vector_collections.get(&index_key);
        let vector_results = if let Some(index) = vector_collection {
            if index.is_empty() {
                Vec::new()
            } else {
                let ef = if ef_search > 0 {
                    ef_search.max(fetch_k)
                } else {
                    fetch_k.saturating_mul(4).max(64)
                };
                match filter_bitmap {
                    Some(surrogate_bm) => {
                        let mut buf = Vec::with_capacity(surrogate_bm.0.serialized_size());
                        if surrogate_bm.0.serialize_into(&mut buf).is_ok() {
                            index.search_with_bitmap_bytes(query_vector, fetch_k, ef, &buf)
                        } else {
                            index.search(query_vector, fetch_k, ef)
                        }
                    }
                    None => index.search(query_vector, fetch_k, ef),
                }
            }
        } else {
            Vec::new()
        };

        // 2. BM25 text search.
        let text_results = self
            .inverted
            .search(tenant_id, collection, query_text, fetch_k, fuzzy, None)
            .unwrap_or_default();

        // 3. Graph BFS from seed node.
        let edge_label_owned = graph_edge_label.map(str::to_string);
        let (graph_expanded, hop_distances, _bfs_truncated) = self.bfs_with_distances(
            task.request.database_id.as_u64(),
            tid,
            &[graph_seed_id],
            graph_edge_label,
            Direction::Out,
            graph_depth,
            self.query_tuning.bfs_memory_budget_bytes / self.query_tuning.bfs_bytes_per_node,
        );

        // 4. Build ranked lists.
        use crate::query::fusion::{RankedResult, reciprocal_rank_fusion_weighted};
        let _ = edge_label_owned; // consumed above

        let vector_ranked: Vec<RankedResult> = vector_results
            .iter()
            .enumerate()
            .map(|(rank, r)| {
                let document_id = vector_collection
                    .and_then(|c| c.get_surrogate(r.id))
                    .map(crate::engine::document::store::surrogate_to_doc_id)
                    .unwrap_or_else(|| format!("__local_{}", r.id));
                RankedResult {
                    document_id,
                    rank,
                    score: r.distance,
                    source: "vector",
                }
            })
            .collect();

        let text_ranked: Vec<RankedResult> = text_results
            .iter()
            .enumerate()
            .map(|(rank, r)| RankedResult {
                document_id: crate::engine::document::store::surrogate_to_doc_id(r.doc_id),
                rank,
                score: r.score,
                source: "text",
            })
            .collect();

        let graph_ranked = graph_nodes_to_ranked_results(&graph_expanded, &hop_distances);

        let (k_vector, k_text, k_graph) = rrf_k;
        let fused = reciprocal_rank_fusion_weighted(
            &[vector_ranked, text_ranked, graph_ranked],
            &[k_vector, k_text, k_graph],
            top_k,
        );

        // 5. Materialise results with per-engine rank diagnostics (reusing HybridSearchHit).
        let results: Vec<_> = fused
            .iter()
            .filter(|f| {
                if rls_filters.is_empty() {
                    return true;
                }
                match self.sparse.get(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    &f.document_id,
                ) {
                    Ok(Some(bytes)) => {
                        super::rls_eval::rls_check_msgpack_bytes(rls_filters, &bytes)
                    }
                    _ => false,
                }
            })
            .map(|f| {
                let vector_rank = vector_results.iter().position(|r| {
                    let doc_id = vector_collection
                        .and_then(|c| c.get_surrogate(r.id))
                        .map(crate::engine::document::store::surrogate_to_doc_id)
                        .unwrap_or_else(|| format!("__local_{}", r.id));
                    doc_id == f.document_id
                });
                let text_rank = text_results.iter().position(|r| {
                    crate::engine::document::store::surrogate_to_doc_id(r.doc_id) == f.document_id
                });

                super::super::response_codec::HybridSearchHit {
                    doc_id: &f.document_id,
                    score_field: score_alias.unwrap_or("rrf_score"),
                    rrf_score: f.rrf_score,
                    vector_rank,
                    text_rank,
                }
            })
            .collect();

        if let Some(ref m) = self.metrics {
            m.record_fts_search(0);
        }
        match super::super::response_codec::encode(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
