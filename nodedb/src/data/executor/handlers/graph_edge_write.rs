// SPDX-License-Identifier: BUSL-1.1

//! Edge write handlers: EdgePut, EdgePutBatch, EdgeDelete, EdgeDeleteBatch.
//!
//! Split out of `graph.rs` to keep that file under the file-size limit; see
//! its module doc for the scoping rules that also apply here.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// Bundled arguments for [`CoreLoop::execute_edge_put`].
pub(in crate::data::executor) struct EdgePutParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub src_id: &'a str,
    pub label: &'a str,
    pub dst_id: &'a str,
    pub properties: &'a [u8],
    pub src_surrogate: nodedb_types::Surrogate,
    pub dst_surrogate: nodedb_types::Surrogate,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_edge_put(
        &mut self,
        task: &ExecutionTask,
        params: EdgePutParams<'_>,
    ) -> Response {
        let EdgePutParams {
            tid,
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        } = params;
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
                    partition
                        .add_edge_weighted_in_collection(src_id, label, dst_id, collection, weight)
                } else {
                    partition.add_edge_in_collection(src_id, label, dst_id, collection)
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
                    if let Err(e) = partition.add_edge_in_collection(
                        &edge.src_id,
                        &edge.label,
                        &edge.dst_id,
                        &edge.collection,
                    ) {
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
            partition.remove_edge_in_collection(
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
                &edge.collection,
            );
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
                partition.remove_edge_in_collection(src_id, label, dst_id, collection);
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
}
