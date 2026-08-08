// SPDX-License-Identifier: BUSL-1.1

//! Vector operation dispatch.

use crate::bridge::envelope::Response;
use nodedb_mem;
use nodedb_physical::physical_plan::VectorOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_vector(&mut self, task: &ExecutionTask, op: &VectorOp) -> Response {
        let tid = task.request.tenant_id.as_u64();
        // Pressure guard for all write operations.
        let is_write = matches!(
            op,
            VectorOp::Insert { .. }
                | VectorOp::BatchInsert { .. }
                | VectorOp::SparseInsert { .. }
                | VectorOp::MultiVectorInsert { .. }
                | VectorOp::DirectUpsert { .. }
        );
        if is_write && let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Vector)
        {
            return r;
        }
        match op {
            VectorOp::Insert {
                collection,
                vector,
                dim,
                field_name,
                surrogate,
                pk_bytes: _,
                provenance,
            } => self.execute_vector_insert(super::super::handlers::vector::VectorInsertParams {
                task,
                tid,
                collection,
                vector,
                dim: *dim,
                field_name,
                surrogate: *surrogate,
                provenance: provenance.as_ref(),
            }),

            VectorOp::BatchInsert {
                collection,
                vectors,
                dim,
                surrogates,
            } => self.execute_vector_batch_insert(task, tid, collection, vectors, *dim, surrogates),

            VectorOp::MultiSearch {
                collection,
                query_vector,
                top_k,
                ef_search,
                filter_bitmap,
                rls_filters,
            } => self.execute_vector_multi_search(
                super::super::handlers::vector_search::VectorMultiSearchParams {
                    task,
                    tid,
                    collection,
                    query_vector,
                    top_k: *top_k,
                    ef_search: *ef_search,
                    filter_bitmap: filter_bitmap.as_ref(),
                    rls_filters,
                },
            ),

            VectorOp::Delete {
                collection,
                vector_id,
            } => self.execute_vector_delete(task, tid, collection, *vector_id),

            VectorOp::Search {
                collection,
                query_vector,
                top_k,
                ef_search,
                metric,
                filter_bitmap,
                field_name,
                rls_filters,
                inline_prefilter_plan,
                ann_options,
                skip_payload_fetch,
                payload_filters,
            } => self.execute_vector_search(
                super::super::handlers::vector_search::VectorSearchParams {
                    task,
                    tid,
                    collection,
                    query_vector,
                    top_k: *top_k,
                    ef_search: *ef_search,
                    metric: *metric,
                    filter_bitmap: filter_bitmap.as_ref(),
                    field_name,
                    rls_filters,
                    inline_prefilter_plan: inline_prefilter_plan.as_deref(),
                    ann_options,
                    skip_payload_fetch: *skip_payload_fetch,
                    payload_filters: payload_filters.as_slice(),
                },
            ),

            VectorOp::SetParams {
                collection,
                field_name,
                dim,
                m,
                ef_construction,
                metric,
                index_type,
                pq_m,
                ivf_cells,
                ivf_nprobe,
            } => self.execute_set_vector_params(
                super::super::handlers::vector::SetVectorParamsInput {
                    task,
                    tid,
                    collection,
                    field_name,
                    dim: *dim,
                    m: *m,
                    ef_construction: *ef_construction,
                    metric,
                    index_type,
                    pq_m: *pq_m,
                    ivf_cells: *ivf_cells,
                    ivf_nprobe: *ivf_nprobe,
                },
            ),

            VectorOp::DropIndex {
                collection,
                field_name,
            } => self.execute_drop_vector_index(task, tid, collection, field_name),

            VectorOp::QueryStats {
                collection,
                field_name,
            } => self.execute_vector_query_stats(task, tid, collection, field_name),

            VectorOp::Seal {
                collection,
                field_name,
            } => self.execute_vector_seal(task, tid, collection, field_name),

            VectorOp::CompactIndex {
                collection,
                field_name,
            } => self.execute_vector_compact_index(task, tid, collection, field_name),

            VectorOp::Rebuild {
                collection,
                field_name,
                m,
                m0,
                ef_construction,
            } => self.execute_vector_rebuild(
                super::super::handlers::vector_lifecycle::VectorRebuildParams {
                    task,
                    tid,
                    collection,
                    field_name,
                    m: *m,
                    m0: *m0,
                    ef_construction: *ef_construction,
                },
            ),

            VectorOp::SparseInsert {
                collection,
                field_name,
                doc_id,
                entries,
            } => self.execute_sparse_insert(task, tid, collection, field_name, doc_id, entries),

            VectorOp::SparseSearch {
                collection,
                field_name,
                query_entries,
                top_k,
            } => {
                self.execute_sparse_search(task, tid, collection, field_name, query_entries, *top_k)
            }

            VectorOp::SparseDelete {
                collection,
                field_name,
                doc_id,
            } => self.execute_sparse_delete(task, tid, collection, field_name, doc_id),

            VectorOp::MultiVectorInsert {
                collection,
                field_name,
                document_surrogate,
                vectors,
                count,
                dim,
            } => self.execute_multi_vector_insert(
                super::super::handlers::vector_multi::MultiVectorInsertParams {
                    task,
                    tid,
                    collection,
                    field_name,
                    document_surrogate: *document_surrogate,
                    vectors_flat: vectors,
                    count: *count,
                    dim: *dim,
                },
            ),

            VectorOp::MultiVectorDelete {
                collection,
                field_name,
                document_surrogate,
            } => self.execute_multi_vector_delete(
                task,
                tid,
                collection,
                field_name,
                *document_surrogate,
            ),

            VectorOp::MultiVectorScoreSearch {
                collection,
                field_name,
                query_vector,
                top_k,
                ef_search,
                mode,
            } => self.execute_multi_vector_score_search(
                super::super::handlers::vector_multi::MultiVectorScoreSearchParams {
                    task,
                    tid,
                    collection,
                    field_name,
                    query_vector,
                    top_k: *top_k,
                    ef_search: *ef_search,
                    mode_str: mode,
                },
            ),

            VectorOp::DirectUpsert {
                collection,
                field,
                surrogate,
                vector,
                payload,
                quantization,
                storage_dtype,
                payload_indexes,
                returning,
                rls_filters,
            } => self.execute_vector_direct_upsert(
                super::super::handlers::vector_upsert::VectorDirectUpsertParams {
                    task,
                    tid,
                    collection,
                    field,
                    surrogate: *surrogate,
                    vector,
                    payload,
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    payload_indexes,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),

            VectorOp::DeleteBySurrogate {
                collection,
                surrogate,
                field_name,
                provenance,
            } => self.execute_vector_delete_by_surrogate(
                task,
                tid,
                collection,
                *surrogate,
                field_name,
                provenance.as_ref(),
            ),
        }
    }
}
