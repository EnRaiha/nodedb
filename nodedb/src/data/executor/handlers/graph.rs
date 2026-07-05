// SPDX-License-Identifier: BUSL-1.1

//! Graph operation handlers: EdgePut, EdgeDelete, GraphHop, GraphNeighbors,
//! GraphPath, GraphSubgraph.
//!
//! ## Scoping at this layer
//!
//! The CSR index is partitioned structurally by tenant (see
//! `ShardedCsrIndex`). Handlers resolve the caller's partition once
//! via `self.csr_partition(_mut)(tid)` and then address node ids in
//! their raw, user-visible form — no `<tid>:` prefix, no post-hoc
//! stripping on the way out.
//!
//! `EdgeStore` now takes `(TenantId, name)` tuples and owns its
//! tenant encoding internally. Handlers pass raw user-visible names
//! throughout: to the CSR partition, to the edge store, and to the
//! `deleted_nodes` dangling-edge tracker via `mark_node_deleted` /
//! `is_node_deleted`. No `scoped_node()` wrapping at this layer.

use nodedb_types::diagnostic::DiagnosticLayer;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

#[path = "graph_traversal.rs"]
mod graph_traversal;
#[path = "graph_txn_merge.rs"]
mod graph_txn_merge;

use graph_txn_merge::merge_graph_txn_overlay_neighbors;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_edge_put(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        src_id: &str,
        label: &str,
        dst_id: &str,
        properties: &[u8],
        src_surrogate: nodedb_types::Surrogate,
        dst_surrogate: nodedb_types::Surrogate,
    ) -> Response {
        debug!(core = self.core_id, tid, %collection, %src_id, %label, %dst_id, "edge put");
        let database_id = task.request.database_id.as_u64();

        if self.is_node_deleted(database_id, tid, src_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: src_id.to_string(),
                },
            );
        }
        if self.is_node_deleted(database_id, tid, dst_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: dst_id.to_string(),
                },
            );
        }

        let ord = self.hlc.next_ordinal();
        // Under a Calvin batch, `epoch_system_ms` is the deterministic epoch
        // timestamp; outside Calvin (every path today — no Calvin edge writes
        // yet) it is None and we fall back to the HLC-derived wall time,
        // identical to the prior behavior.
        let valid_from_ms = match self.epoch_system_ms {
            Some(ms) => ms,
            None => nodedb_types::ordinal_to_ms(ord),
        };
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.put_edge_versioned(
            EdgeRef::new(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                src_id,
                label,
                dst_id,
            ),
            properties,
            ord,
            valid_from_ms,
            i64::MAX,
        ) {
            Ok(()) => {
                let weight = crate::engine::graph::csr::extract_weight_from_properties(properties);
                let partition = self.csr_partition_mut(database_id, tid);
                let csr_result = if weight != 1.0 {
                    partition.add_edge_weighted(src_id, label, dst_id, weight)
                } else {
                    partition.add_edge(src_id, label, dst_id)
                };
                match csr_result {
                    Ok(()) => {
                        // Populate the per-node surrogates so future bitmap-gated
                        // traversals can check membership without a separate lookup.
                        partition.set_node_surrogate(src_id, src_surrogate);
                        partition.set_node_surrogate(dst_id, dst_surrogate);
                        self.checkpoint_coordinator.mark_dirty("sparse", 1);
                        self.response_ok(task)
                    }
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Apply a batched edge insert in a single SPSC round-trip.
    pub(in crate::data::executor) fn execute_edge_put_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        edges: &[nodedb_physical::physical_plan::BatchEdge],
    ) -> Response {
        debug!(core = self.core_id, count = edges.len(), "edge put batch");
        let database_id = task.request.database_id.as_u64();
        for (idx, edge) in edges.iter().enumerate() {
            if self.is_node_deleted(database_id, tid, &edge.src_id) {
                return self.response_error(
                    task,
                    ErrorCode::RejectedDanglingEdge {
                        missing_node: edge.src_id.clone(),
                    },
                );
            }
            if self.is_node_deleted(database_id, tid, &edge.dst_id) {
                return self.response_error(
                    task,
                    ErrorCode::RejectedDanglingEdge {
                        missing_node: edge.dst_id.clone(),
                    },
                );
            }
            let ord = self.hlc.next_ordinal();
            let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
            use crate::engine::graph::edge_store::EdgeRef;
            match self.edge_store.put_edge_versioned(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    &edge.collection,
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                ),
                &[],
                ord,
                valid_from_ms,
                i64::MAX,
            ) {
                Ok(()) => {
                    let partition = self.csr_partition_mut(database_id, tid);
                    if let Err(e) = partition.add_edge(&edge.src_id, &edge.label, &edge.dst_id) {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("edge {idx} (label interning): {e}"),
                            },
                        );
                    }
                    partition.set_node_surrogate(&edge.src_id, edge.src_surrogate);
                    partition.set_node_surrogate(&edge.dst_id, edge.dst_surrogate);
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("edge {idx}: {e}"),
                        },
                    );
                }
            }
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        self.response_ok(task)
    }

    /// Apply a batched edge delete in a single SPSC round-trip.
    pub(in crate::data::executor) fn execute_edge_delete_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        edges: &[nodedb_physical::physical_plan::BatchEdge],
    ) -> Response {
        debug!(
            core = self.core_id,
            count = edges.len(),
            "edge delete batch"
        );
        let database_id = task.request.database_id.as_u64();
        for edge in edges {
            let ord = self.hlc.next_ordinal();
            use crate::engine::graph::edge_store::EdgeRef;
            let _ = self.edge_store.soft_delete_edge(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    &edge.collection,
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                ),
                ord,
            );
            let partition = self.csr_partition_mut(database_id, tid);
            partition.remove_edge(&edge.src_id, &edge.label, &edge.dst_id);
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_edge_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        src_id: &str,
        label: &str,
        dst_id: &str,
    ) -> Response {
        debug!(core = self.core_id, tid, %collection, %src_id, %label, %dst_id, "edge delete");
        let database_id = task.request.database_id.as_u64();
        let ord = self.hlc.next_ordinal();
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.soft_delete_edge(
            EdgeRef::new(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                src_id,
                label,
                dst_id,
            ),
            ord,
        ) {
            Ok(_) => {
                let partition = self.csr_partition_mut(database_id, tid);
                partition.remove_edge(src_id, label, dst_id);
                self.checkpoint_coordinator.mark_dirty("sparse", 1);
                self.response_ok(task)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_graph_hop(
        &self,
        task: &ExecutionTask,
        tid: u64,
        start_nodes: &[String],
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
        depth: usize,
        frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            ?start_nodes,
            ?edge_label,
            ?direction,
            depth,
            "graph hop"
        );
        let database_id = task.request.database_id.as_u64();
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        let result: Vec<String> = match self.csr_partition(database_id, tid) {
            Some(partition) => partition.traverse_bfs(
                &refs,
                edge_label.as_deref(),
                direction,
                depth,
                self.graph_tuning.max_visited,
                frontier_bitmap,
            ),
            None => Vec::new(),
        };
        // Read-your-own-writes only for the single-hop case (depth == 1),
        // matching `execute_graph_neighbors`. Multi-hop `Hop` (depth > 1)
        // stays durable-only -- see `merge_hop_single_hop`'s doc comment.
        let overlay = task
            .request
            .txn_id
            .and_then(|txn_id| self.graph_txn_overlays.get(&txn_id));
        let result: Vec<String> =
            graph_txn_merge::merge_hop_single_hop(graph_txn_merge::HopMergeParams {
                overlay,
                durable_neighbors_of: |start: &str| {
                    self.csr_partition(database_id, tid)
                        .map(|p| p.neighbors(start, edge_label.as_deref(), direction))
                        .unwrap_or_default()
                },
                starts: &refs,
                depth,
                database_id: task.request.database_id,
                tenant: TenantId::new(tid),
                edge_label: edge_label.as_deref(),
                direction,
                has_bitmap: frontier_bitmap.is_some(),
                durable_result: result,
            });
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, layer = DiagnosticLayer::WireShape.as_str(), error = %e, "graph hop serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_graph_neighbors(
        &self,
        task: &ExecutionTask,
        tid: u64,
        node_id: &str,
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
    ) -> Response {
        debug!(core = self.core_id, tid, %node_id, ?edge_label, ?direction, "graph neighbors");
        let database_id = task.request.database_id.as_u64();
        let durable: Vec<(String, String)> = match self.csr_partition(database_id, tid) {
            Some(partition) => partition.neighbors(node_id, edge_label.as_deref(), direction),
            None => Vec::new(),
        };
        // Read-your-own-writes: fold this transaction's staged edge writes
        // into the durable result (see `graph_txn_merge`).
        let overlay = task
            .request
            .txn_id
            .and_then(|txn_id| self.graph_txn_overlays.get(&txn_id));
        let neighbors = merge_graph_txn_overlay_neighbors(
            overlay,
            task.request.database_id,
            TenantId::new(tid),
            node_id,
            edge_label.as_deref(),
            direction,
            durable,
        );
        let result: Vec<_> = neighbors
            .iter()
            .map(
                |(label, node)| super::super::response_codec::NeighborEntry {
                    label: label.as_str(),
                    node: node.as_str(),
                },
            )
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, layer = DiagnosticLayer::WireShape.as_str(), error = %e, "graph neighbors serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_graph_neighbors_multi(
        &self,
        task: &ExecutionTask,
        tid: u64,
        node_ids: &[String],
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
        max_results: u32,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            count = node_ids.len(),
            ?edge_label,
            ?direction,
            max_results,
            "graph neighbors multi"
        );
        let cap: usize = if max_results == 0 {
            usize::MAX
        } else {
            max_results as usize
        };
        let database_id = task.request.database_id.as_u64();
        let mut owned: Vec<(String, String, String)> =
            Vec::with_capacity(node_ids.len().min(cap) * 4);
        let mut truncated = false;
        if let Some(partition) = self.csr_partition(database_id, tid) {
            'outer: for raw_src in node_ids {
                let neighbors = partition.neighbors(raw_src, edge_label.as_deref(), direction);
                for (label, node) in neighbors {
                    if owned.len() >= cap {
                        truncated = true;
                        break 'outer;
                    }
                    owned.push((raw_src.clone(), label, node));
                }
            }
        }
        let entries: Vec<super::super::response_codec::NeighborMultiEntry> = owned
            .iter()
            .map(
                |(src, label, node)| super::super::response_codec::NeighborMultiEntry {
                    src: src.as_str(),
                    label: label.as_str(),
                    node: node.as_str(),
                },
            )
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&entries) {
            Ok(payload) => {
                if truncated {
                    self.response_partial(task, payload)
                } else {
                    self.response_with_payload(task, payload)
                }
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    layer = DiagnosticLayer::WireShape.as_str(),
                    error = %e,
                    "graph neighbors-multi serialization failed"
                );
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }
}
