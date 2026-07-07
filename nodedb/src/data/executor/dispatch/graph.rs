// SPDX-License-Identifier: BUSL-1.1

//! Graph operation dispatch.

use crate::bridge::envelope::Response;
use nodedb_mem;
use nodedb_physical::physical_plan::GraphOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::SystemTimeScope;

/// Resolve a graph temporal op's system-time selection to a point-in-time
/// cutoff. `AllVersions` (audit log) is not yet supported on the graph
/// engine and surfaces a typed `Unsupported` error.
fn graph_system_as_of(
    system_time: &SystemTimeScope,
) -> Result<Option<i64>, crate::bridge::envelope::ErrorCode> {
    match system_time {
        SystemTimeScope::Current => Ok(None),
        SystemTimeScope::AsOf(ms) => Ok(Some(*ms)),
        SystemTimeScope::AllVersions => Err(crate::bridge::envelope::ErrorCode::Unsupported {
            detail: "AS OF SYSTEM TIME NULL (all-versions) is not yet supported on the \
                     graph engine"
                .into(),
        }),
    }
}

impl CoreLoop {
    pub(super) fn dispatch_graph(&mut self, task: &ExecutionTask, op: &GraphOp) -> Response {
        let tid = task.request.tenant_id.as_u64();
        let database_id = task.request.database_id.as_u64();
        // Pressure guard for write operations.
        let is_write = matches!(
            op,
            GraphOp::EdgePut { .. }
                | GraphOp::EdgePutBatch { .. }
                | GraphOp::EdgeDelete { .. }
                | GraphOp::EdgeDeleteBatch { .. }
        );
        if is_write && let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Graph) {
            return r;
        }
        match op {
            GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
            } => self.execute_edge_put(
                task,
                crate::data::executor::handlers::graph::EdgePutParams {
                    tid,
                    collection,
                    src_id,
                    label,
                    dst_id,
                    properties,
                    src_surrogate: *src_surrogate,
                    dst_surrogate: *dst_surrogate,
                },
            ),

            GraphOp::EdgePutBatch { edges } => self.execute_edge_put_batch(task, tid, edges),

            GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
                ..
            } => self.execute_edge_delete(task, tid, collection, src_id, label, dst_id),

            GraphOp::EdgeDeleteBatch { edges } => self.execute_edge_delete_batch(task, tid, edges),

            GraphOp::Hop {
                start_nodes,
                edge_label,
                direction,
                depth,
                options: _,
                rls_filters: _,
                frontier_bitmap,
            } => self.execute_graph_hop(
                task,
                crate::data::executor::handlers::graph::GraphHopParams {
                    tid,
                    start_nodes,
                    edge_label,
                    direction: *direction,
                    depth: *depth,
                    frontier_bitmap: frontier_bitmap.as_ref(),
                },
            ),

            GraphOp::Neighbors {
                node_id,
                edge_label,
                direction,
                rls_filters: _,
            } => self.execute_graph_neighbors(task, tid, node_id, edge_label, *direction),

            GraphOp::NeighborsMulti {
                node_ids,
                edge_label,
                direction,
                max_results,
                rls_filters: _,
            } => self.execute_graph_neighbors_multi(
                task,
                tid,
                node_ids,
                edge_label,
                *direction,
                *max_results,
            ),

            GraphOp::Path {
                src,
                dst,
                edge_label,
                max_depth,
                options: _,
                rls_filters: _,
                frontier_bitmap,
            } => self.execute_graph_path(
                task,
                crate::data::executor::handlers::graph::graph_traversal::GraphPathParams {
                    tid,
                    src,
                    dst,
                    edge_label,
                    max_depth: *max_depth,
                    frontier_bitmap: frontier_bitmap.as_ref(),
                },
            ),

            GraphOp::Subgraph {
                start_nodes,
                edge_label,
                depth,
                options: _,
                rls_filters: _,
            } => self.execute_graph_subgraph(task, tid, start_nodes, edge_label, *depth),

            GraphOp::RagFusion {
                collection,
                query_vector,
                vector_top_k,
                edge_label,
                direction,
                expansion_depth,
                final_top_k,
                rrf_k,
                rrf_k_triple,
                vector_field,
                options,
                bm25_query,
                bm25_field,
            } => {
                if let (Some(bm25_q), Some(bm25_f), Some(triple_k)) =
                    (bm25_query.as_deref(), bm25_field.as_deref(), rrf_k_triple)
                {
                    self.execute_graph_rag_fusion_triple(
                        task,
                        crate::data::executor::handlers::graph_rag_triple::GraphRagFusionTripleParams {
                            tenant_id: tid,
                            collection,
                            query_vector,
                            vector_top_k: *vector_top_k,
                            edge_label,
                            direction: *direction,
                            expansion_depth: *expansion_depth,
                            final_top_k: *final_top_k,
                            rrf_k: *triple_k,
                            vector_field: vector_field.as_str(),
                            max_visited: options.max_visited,
                            bm25_query: bm25_q,
                            bm25_field: bm25_f,
                        },
                    )
                } else {
                    self.execute_graph_rag_fusion(
                        task,
                        crate::data::executor::handlers::graph_rag::GraphRagFusionParams {
                            tenant_id: tid,
                            collection,
                            query_vector,
                            vector_top_k: *vector_top_k,
                            edge_label,
                            direction: *direction,
                            expansion_depth: *expansion_depth,
                            final_top_k: *final_top_k,
                            rrf_k: *rrf_k,
                            vector_field: vector_field.as_str(),
                            max_visited: options.max_visited,
                        },
                    )
                }
            }

            GraphOp::Algo { algorithm, params } => {
                self.execute_graph_algo(task, tid, algorithm, params)
            }

            GraphOp::Match {
                query,
                frontier_bitmap,
                cluster_mode,
            } => {
                self.execute_graph_match(task, tid, query, frontier_bitmap.as_ref(), *cluster_mode)
            }

            GraphOp::MatchContinuation {
                query,
                resume_triple_idx,
                partial_row,
                source_node,
                source_binding,
            } => self.execute_graph_match_continuation(
                task,
                crate::data::executor::handlers::graph_match::GraphMatchContinuationParams {
                    tid,
                    query_bytes: query,
                    resume_triple_idx: *resume_triple_idx,
                    partial_row_bytes: partial_row,
                    source_node,
                    source_binding,
                },
            ),

            GraphOp::MatchVarLenResume { query, resume } => {
                self.execute_graph_match_varlen_resume(task, tid, query, resume)
            }

            GraphOp::SetNodeLabels { node_id, labels } => {
                let partition = self.csr_partition_mut(database_id, tid);
                for label in labels {
                    if let Err(e) = partition.add_node_label(node_id, label) {
                        return self.response_error(
                            task,
                            crate::bridge::envelope::ErrorCode::Internal {
                                detail: format!("set node label: {e}"),
                            },
                        );
                    }
                }
                self.response_ok(task)
            }

            GraphOp::RemoveNodeLabels { node_id, labels } => {
                let partition = self.csr_partition_mut(database_id, tid);
                for label in labels {
                    partition.remove_node_label(node_id, label);
                }
                self.response_ok(task)
            }

            GraphOp::TemporalNeighbors {
                collection,
                node_id,
                edge_label,
                direction,
                system_time,
                valid_at_ms,
                rls_filters: _,
            } => {
                let system_as_of_ms = match graph_system_as_of(system_time) {
                    Ok(v) => v,
                    Err(resp) => return self.response_error(task, resp),
                };
                self.execute_graph_temporal_neighbors(
                    task,
                    super::super::handlers::graph_temporal::TemporalNeighborsParams {
                        tid,
                        collection,
                        node_id,
                        edge_label,
                        direction: *direction,
                        system_as_of_ms,
                        valid_at_ms: *valid_at_ms,
                    },
                )
            }

            GraphOp::TemporalAlgorithm {
                algorithm,
                params,
                system_time,
            } => {
                let system_as_of_ms = match graph_system_as_of(system_time) {
                    Ok(v) => v,
                    Err(resp) => return self.response_error(task, resp),
                };
                self.execute_graph_temporal_algo(task, tid, algorithm, params, system_as_of_ms)
            }

            GraphOp::BspSuperstep(plan) => self.execute_bsp_superstep(
                task,
                tid,
                super::super::handlers::graph_bsp::BspSuperstepArgs {
                    algorithm: &plan.algorithm,
                    params: &plan.params,
                    superstep: plan.superstep,
                    global_n: plan.global_n,
                    owned_vshards: &plan.owned_vshards,
                    incoming_contributions: &plan.incoming_contributions,
                    rank_seed: &plan.rank_seed,
                    global_dangling: plan.global_dangling,
                    personalization_sum: plan.personalization_sum,
                },
            ),

            GraphOp::WccSuperstep(plan) => {
                self.execute_wcc_superstep(task, tid, &plan.params, &plan.owned_vshards)
            }

            GraphOp::Stats { collection, as_of } => {
                self.execute_graph_stats(task, tid, collection.as_deref(), *as_of)
            }
        }
    }
}
