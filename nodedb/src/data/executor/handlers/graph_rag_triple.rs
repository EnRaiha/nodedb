// SPDX-License-Identifier: BUSL-1.1

//! Three-source GraphRAG fusion: vector search + BM25 text + graph expansion, fused via RRF.
//!
//! Pipeline:
//! 1. Vector search returns top-K semantically similar nodes.
//! 2. BM25 text search returns top-K text-relevant documents.
//! 3. Result vector node IDs feed into graph BFS as start nodes.
//! 4. All three ranked lists are fused by `reciprocal_rank_fusion_weighted` with
//!    per-source k-constants `(vector_k, text_k, graph_k)`.
//! 5. Final top-N results are materialised.

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::graph_rag::{
    RagResponseParams, graph_nodes_to_ranked_results,
};
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::Direction;
use crate::query::fusion::{RankedResult, reciprocal_rank_fusion_weighted};
use crate::types::TenantId;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_graph_rag_fusion_triple(
        &self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        query_vector: &[f32],
        vector_top_k: usize,
        edge_label: &Option<String>,
        direction: Direction,
        expansion_depth: usize,
        final_top_k: usize,
        rrf_k: (f64, f64, f64),
        vector_field: &str,
        max_visited: usize,
        bm25_query: &str,
        _bm25_field: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            vector_top_k,
            expansion_depth,
            final_top_k,
            %bm25_query,
            "graph rag fusion triple"
        );

        let tid_typed = TenantId::new(tenant_id);

        let (vector_results, vector_scores) = match self.vector_search_to_node_scores(
            task,
            tenant_id,
            collection,
            query_vector,
            vector_top_k,
            vector_field,
        ) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        // BM25 text search.
        let fetch_k = final_top_k.saturating_mul(3).max(20);
        let text_results = self
            .inverted
            .search(tid_typed, collection, bm25_query, fetch_k, true, None)
            .unwrap_or_default();

        // Graph BFS from vector-nearest nodes.
        let start_ids: Vec<&str> = vector_scores.keys().map(String::as_str).collect();
        let (expanded_nodes, hop_distances, bfs_truncated) = self.bfs_with_distances(
            task.request.database_id.as_u64(),
            tenant_id,
            &start_ids,
            edge_label.as_deref(),
            direction,
            expansion_depth,
            max_visited,
        );

        let (vector_k, text_k, graph_k) = rrf_k;

        let vector_list: Vec<RankedResult> = vector_scores
            .iter()
            .map(|(node_id, (rank, dist))| RankedResult {
                document_id: node_id.clone(),
                rank: *rank,
                score: *dist,
                source: "vector",
            })
            .collect();

        let text_list: Vec<RankedResult> = text_results
            .iter()
            .enumerate()
            .map(|(rank, r)| RankedResult {
                document_id: crate::engine::document::store::surrogate_to_doc_id(r.doc_id),
                rank,
                score: r.score,
                source: "text",
            })
            .collect();

        let graph_list = graph_nodes_to_ranked_results(&expanded_nodes, &hop_distances);

        let fused = reciprocal_rank_fusion_weighted(
            &[vector_list, text_list, graph_list],
            &[vector_k, text_k, graph_k],
            final_top_k,
        );

        self.build_rag_response(
            task,
            RagResponseParams {
                fused: &fused,
                vector_scores: &vector_scores,
                hop_distances: &hop_distances,
                vector_candidate_count: vector_results.len(),
                graph_expanded_count: expanded_nodes.len(),
                bfs_truncated,
                op_name: "graph rag fusion triple",
            },
        )
    }
}
