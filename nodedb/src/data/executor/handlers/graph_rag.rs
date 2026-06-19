// SPDX-License-Identifier: BUSL-1.1

//! GraphRAG fusion handler: vector search, graph expansion, RRF scoring.
//!
//! Pipeline:
//! 1. Vector engine returns top-K semantically similar nodes.
//! 2. Result node IDs feed into graph traversal as start nodes.
//! 3. Graph-expanded result set is scored by hop distance.
//! 4. RRF fuses vector_score and graph_score into unified ranking.
//! 5. Final top-N results are materialized.
//!
//! BFS expansion is bounded by a per-query memory budget derived from the
//! node count. If the budget is exceeded, expansion stops early and results
//! are marked as truncated.

use std::collections::{HashMap, HashSet, VecDeque};

use nodedb_vector::SearchResult;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::{
    GraphRagMetadata, GraphRagResponse, GraphRagResult, encode,
};
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::Direction;
use crate::query::fusion::{FusedResult, RankedResult, reciprocal_rank_fusion_weighted};

/// Result of a successful vector search + node-ID translation.
///
/// `Vec<SearchResult>` is the raw HNSW output (for reporting candidate counts).
/// `HashMap` maps graph node names to `(rank, distance)` pairs.
type VectorNodeScores = (Vec<SearchResult>, HashMap<String, (usize, f32)>);

/// Parameters for `build_rag_response`.
pub(in crate::data::executor) struct RagResponseParams<'a> {
    pub fused: &'a [FusedResult],
    pub vector_scores: &'a HashMap<String, (usize, f32)>,
    pub hop_distances: &'a HashMap<String, usize>,
    pub vector_candidate_count: usize,
    pub graph_expanded_count: usize,
    pub bfs_truncated: bool,
    pub op_name: &'a str,
}

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_graph_rag_fusion(
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
        rrf_k: (f64, f64),
        vector_field: &str,
        max_visited: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            vector_top_k,
            expansion_depth,
            final_top_k,
            "graph rag fusion"
        );

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

        let (vector_k, graph_k) = rrf_k;

        let vector_list: Vec<RankedResult> = vector_scores
            .iter()
            .map(|(node_id, (rank, dist))| RankedResult {
                document_id: node_id.clone(),
                rank: *rank,
                score: *dist,
                source: "vector",
            })
            .collect();

        let graph_list = graph_nodes_to_ranked_results(&expanded_nodes, &hop_distances);

        let fused = reciprocal_rank_fusion_weighted(
            &[vector_list, graph_list],
            &[vector_k, graph_k],
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
                op_name: "graph rag fusion",
            },
        )
    }

    /// Look up the HNSW index for `collection`, run the search, and translate
    /// local HNSW IDs to graph node names via surrogate mapping.
    ///
    /// Returns `Ok((vector_results, vector_scores))` on success, or
    /// `Err(response)` when the index is missing or the search returned no
    /// candidates. The caller should forward the pre-built response directly.
    pub(in crate::data::executor) fn vector_search_to_node_scores(
        &self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        query_vector: &[f32],
        vector_top_k: usize,
        vector_field: &str,
    ) -> Result<VectorNodeScores, Response> {
        let database_id = task.request.database_id.as_u64();
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, collection, vector_field);
        let Some(index) = self.vector_collections.get(&index_key) else {
            return Err(self.response_error(task, ErrorCode::NotFound));
        };
        if index.is_empty() {
            return Err(self.response_with_payload(task, b"[]".to_vec()));
        }

        let ef = vector_top_k.saturating_mul(4).max(64);
        let vector_results = index.search(query_vector, vector_top_k, ef);

        if vector_results.is_empty() {
            return Err(self.response_with_payload(task, b"[]".to_vec()));
        }

        // Translate local HNSW IDs to graph node names via the surrogate index.
        // Path: local_hnsw_id -> Surrogate (vector collection) -> graph node name
        // (CSR partition reverse map). Vectors without a surrogate binding, or
        // surrogates not bound to any graph node, emit a non-matching sentinel
        // that BFS will skip as a missing seed.
        let csr = self.csr_partition(database_id, tenant_id);
        let mut vector_scores: HashMap<String, (usize, f32)> = HashMap::new();
        for (rank, result) in vector_results.iter().enumerate() {
            let node_id = index
                .get_surrogate(result.id)
                .and_then(|s| {
                    csr.and_then(|c| c.node_id_for_surrogate(s))
                        .map(str::to_string)
                })
                .unwrap_or_else(|| format!("__local_{}", result.id));
            vector_scores.insert(node_id, (rank, result.distance));
        }

        Ok((vector_results, vector_scores))
    }

    /// Encode a `GraphRagResponse` from RRF-fused results.
    ///
    /// Shared by both 2-source (`execute_graph_rag_fusion`) and 3-source
    /// (`execute_graph_rag_fusion_triple`) fusion pipelines.
    pub(in crate::data::executor) fn build_rag_response(
        &self,
        task: &ExecutionTask,
        p: RagResponseParams<'_>,
    ) -> Response {
        let results: Vec<GraphRagResult> = p
            .fused
            .iter()
            .map(|f| {
                let (vector_rank, vector_distance) = p
                    .vector_scores
                    .get(f.document_id.as_str())
                    .map(|(rank, dist)| (Some(*rank), Some(*dist)))
                    .unwrap_or((None, None));
                let hop_distance = p.hop_distances.get(f.document_id.as_str()).copied();
                GraphRagResult {
                    node_id: f.document_id.clone(),
                    rrf_score: f.rrf_score,
                    vector_rank,
                    vector_distance,
                    hop_distance,
                }
            })
            .collect();

        let response_body = GraphRagResponse {
            results,
            metadata: GraphRagMetadata {
                vector_candidates: p.vector_candidate_count,
                graph_expanded: p.graph_expanded_count,
                truncated: p.bfs_truncated,
                watermark_lsn: self.watermark.as_u64(),
            },
        };

        match encode(&response_body) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "{} serialization failed", p.op_name);
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// BFS traversal that also tracks hop distances from start nodes.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn bfs_with_distances(
        &self,
        database_id: u64,
        tid: u64,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> (Vec<String>, HashMap<String, usize>, bool) {
        let budget_node_limit =
            self.query_tuning.bfs_memory_budget_bytes / self.query_tuning.bfs_bytes_per_node;
        let effective_limit = max_visited.min(budget_node_limit);

        let mut visited: HashSet<String> = HashSet::with_capacity(effective_limit.min(1024));
        let mut distances: HashMap<String, usize> =
            HashMap::with_capacity(effective_limit.min(1024));
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();
        let mut truncated = false;

        for &node in start_nodes {
            let owned = node.to_string();
            if visited.insert(owned.clone()) {
                distances.insert(owned.clone(), 0);
                queue.push_back((owned, 0));
            }
        }

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            if visited.len() >= effective_limit {
                truncated = true;
                break;
            }

            let neighbors = match self.csr_partition(database_id, tid) {
                Some(part) => part.neighbors(&node, label_filter, direction),
                None => Vec::new(),
            };
            for (_, neighbor) in &neighbors {
                if visited.len() >= effective_limit {
                    truncated = true;
                    break;
                }
                if !visited.contains(neighbor) {
                    visited.insert(neighbor.clone());
                    distances.insert(neighbor.clone(), depth + 1);
                    queue.push_back((neighbor.clone(), depth + 1));
                }
            }

            if truncated {
                break;
            }
        }

        if truncated {
            warn!(
                core = self.core_id,
                visited = visited.len(),
                limit = effective_limit,
                budget_limit = budget_node_limit,
                max_visited,
                "GraphRAG BFS truncated: memory budget or max_visited reached"
            );
        }

        let nodes: Vec<String> = visited.into_iter().collect();
        (nodes, distances, truncated)
    }
}

/// Sort expanded graph nodes by hop distance and convert to `RankedResult` list.
///
/// Used by 2-source GraphRAG, 3-source GraphRAG triple, and 3-source hybrid
/// text search to avoid duplicating the sort-and-rank pattern.
pub(super) fn graph_nodes_to_ranked_results(
    expanded_nodes: &[String],
    hop_distances: &HashMap<String, usize>,
) -> Vec<RankedResult> {
    let mut sorted: Vec<(&str, usize)> = expanded_nodes
        .iter()
        .map(|node| {
            let dist = hop_distances
                .get(node.as_str())
                .copied()
                .unwrap_or(usize::MAX);
            (node.as_str(), dist)
        })
        .collect();
    sorted.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)));

    sorted
        .into_iter()
        .enumerate()
        .map(|(rank, (node_id, hop_dist))| RankedResult {
            document_id: node_id.to_string(),
            rank,
            score: hop_dist as f32,
            source: "graph",
        })
        .collect()
}
