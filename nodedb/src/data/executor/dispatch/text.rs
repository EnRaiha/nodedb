// SPDX-License-Identifier: BUSL-1.1

//! Text (FTS) operation dispatch.

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::TextOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::text_search::TextSearchParams;
use crate::data::executor::handlers::text_search_hybrid::HybridSearchParams;
use crate::data::executor::handlers::text_search_triple::HybridSearchTripleParams;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_text(&mut self, task: &ExecutionTask, op: &TextOp) -> Response {
        let tid = task.request.tenant_id.as_u64();
        match op {
            TextOp::Search {
                collection,
                query,
                top_k,
                fuzzy,
                prefilter,
                rls_filters,
            } => self.execute_text_search(
                task,
                TextSearchParams {
                    tid,
                    collection,
                    query,
                    top_k: *top_k,
                    fuzzy: *fuzzy,
                    prefilter: prefilter.as_ref(),
                    rls_filters,
                },
            ),

            TextOp::BM25ScoreScan {
                collection,
                query,
                score_alias,
                fuzzy,
            } => self.execute_bm25_score_scan(task, tid, collection, query, score_alias, *fuzzy),

            TextOp::PhraseSearch {
                collection,
                terms,
                top_k,
                prefilter,
            } => {
                self.execute_phrase_search(task, tid, collection, terms, *top_k, prefilter.as_ref())
            }

            TextOp::HybridSearch {
                collection,
                query_vector,
                query_text,
                top_k,
                ef_search,
                fuzzy,
                vector_weight,
                filter_bitmap,
                rls_filters,
                score_alias,
            } => self.execute_hybrid_search(
                task,
                HybridSearchParams {
                    tid,
                    collection,
                    query_vector,
                    query_text,
                    top_k: *top_k,
                    ef_search: *ef_search,
                    fuzzy: *fuzzy,
                    vector_weight: *vector_weight,
                    filter_bitmap: filter_bitmap.as_ref(),
                    rls_filters,
                    score_alias: score_alias.as_deref(),
                },
            ),

            TextOp::FtsIndexDoc {
                collection,
                surrogate,
                text,
                provenance,
            } => self.execute_fts_index_doc(
                task,
                tid,
                collection,
                *surrogate,
                text,
                provenance.as_ref(),
            ),

            TextOp::FtsDeleteDoc {
                collection,
                surrogate,
                provenance,
            } => {
                self.execute_fts_delete_doc(task, tid, collection, *surrogate, provenance.as_ref())
            }

            TextOp::HybridSearchTriple {
                collection,
                query_vector,
                query_text,
                graph_seed_id,
                graph_depth,
                graph_edge_label,
                top_k,
                ef_search,
                fuzzy,
                rrf_k,
                filter_bitmap,
                rls_filters,
                score_alias,
            } => self.execute_hybrid_search_triple(
                task,
                HybridSearchTripleParams {
                    tid,
                    collection,
                    query_vector,
                    query_text,
                    graph_seed_id,
                    graph_depth: *graph_depth,
                    graph_edge_label: graph_edge_label.as_deref(),
                    top_k: *top_k,
                    ef_search: *ef_search,
                    fuzzy: *fuzzy,
                    rrf_k: *rrf_k,
                    filter_bitmap: filter_bitmap.as_ref(),
                    rls_filters,
                    score_alias: score_alias.as_deref(),
                },
            ),

            TextOp::SetTextConfig {
                collection,
                analyzer_name,
                fuzzy_default,
            } => self.execute_set_text_config(
                task,
                tid,
                collection,
                analyzer_name.as_deref(),
                *fuzzy_default,
            ),
        }
    }
}
