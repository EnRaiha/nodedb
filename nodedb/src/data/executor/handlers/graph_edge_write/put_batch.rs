// SPDX-License-Identifier: BUSL-1.1

//! `EdgePutBatch`: batched edge insert in a single SPSC round-trip.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::shared::owns_logical_edge_stats;

impl CoreLoop {
    /// Apply a batched edge insert in a single SPSC round-trip.
    ///
    /// Each edge unconditionally writes a new edge-store version and CSR
    /// entry (same no-op-free semantics as [`CoreLoop::execute_edge_put`]),
    /// so a successful batch always reports exactly `edges.len()` affected.
    pub(in crate::data::executor) fn execute_edge_put_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        edges: &[nodedb_physical::physical_plan::BatchEdge],
    ) -> Response {
        debug!(core = self.core_id, count = edges.len(), "edge put batch");
        let database_id = task.request.database_id.as_u64();
        // Every endpoint is checked before any edge is written, so a dangling
        // refusal applies nothing.
        if let Some(missing_node) = edges.iter().find_map(|edge| {
            [&edge.src_id, &edge.dst_id]
                .into_iter()
                .find(|node| self.is_node_deleted(database_id, tid, node))
                .cloned()
        }) {
            return self.response_error(task, ErrorCode::RejectedDanglingEdge { missing_node });
        }
        for (idx, edge) in edges.iter().enumerate() {
            let ord = self
                .active_graph_system_from
                .unwrap_or_else(|| self.hlc.next_ordinal());
            let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
            use crate::engine::graph::edge_store::EdgeRef;
            match self.edge_store.put_edge_versioned_with_stats(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                )
                .with_surrogates(edge.src_surrogate, edge.dst_surrogate),
                &[],
                ord,
                valid_from_ms,
                i64::MAX,
                owns_logical_edge_stats(task, &edge.src_id),
            ) {
                Ok(()) => {
                    let partition = self.csr_partition_mut(database_id, tid);
                    if let Err(e) = partition.add_edge_in_collection(
                        &edge.src_id,
                        &edge.label,
                        &edge.dst_id,
                        edge.collection.as_str(),
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
                    // Counted per edge as it is applied, not once for the
                    // batch: edges before a mid-batch failure are still
                    // written and must show up in the counter. A dual-homed
                    // edge is counted by its source home only.
                    if !self.boot_replaying_wal()
                        && owns_logical_edge_stats(task, &edge.src_id)
                        && let Some(metrics) = &self.metrics
                    {
                        metrics.record_graph_edge_written();
                    }
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
        for edge in edges {
            self.note_edge_write_lsn(
                task,
                tid,
                edge.collection.as_str(),
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
            );
            // CDC: batch edges are applied with empty properties (see
            // `execute_edge_put_batch`'s hardcoded `&[]`), so `new_value` is an
            // empty payload — a faithful pre-image of what was applied.
            self.emit_graph_edge_event(
                task,
                crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                    collection: edge.collection.as_str(),
                    src_id: &edge.src_id,
                    label: &edge.label,
                    dst_id: &edge.dst_id,
                    op: crate::event::WriteOp::Insert,
                    properties: Some(&[]),
                },
            );
        }
        self.response_affected(task, edges.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::control::metrics::SystemMetrics;
    use crate::types::TenantId;
    use nodedb_physical::physical_plan::BatchEdge;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::super::shared::test_support::{make_core, make_task_at_source, make_task_with_lsn};

    fn edge(src: &str, dst: &str) -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "knows"),
            src_id: src.to_string(),
            label: "KNOWS".to_string(),
            dst_id: dst.to_string(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        }
    }

    /// Every applied edge of a batch is one write, not one write for the
    /// batch: the counter has to describe the edges that reached storage.
    #[test]
    fn a_batch_counts_one_write_per_applied_edge() {
        let mut h = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        h.core.set_metrics(Arc::clone(&metrics));
        // One source home owns every edge of the batch, so each applied edge
        // is one counted write.
        let edges = vec![edge("owner", "b"), edge("owner", "d"), edge("owner", "f")];

        let resp = h
            .core
            .execute_edge_put_batch(&make_task_at_source(11, "owner"), 1, &edges);

        assert_eq!(resp.status, Status::Ok);
        assert_eq!(metrics.graph_edges_written.load(Ordering::Relaxed), 3);
    }

    /// A dual-homed edge runs on both endpoint homes. Only the source home
    /// owns the logical edge, so a batch running on a foreign home applies the
    /// edge without counting it — otherwise one cross-shard insert would count
    /// twice.
    #[test]
    fn a_batch_on_a_foreign_home_counts_no_write() {
        let mut h = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        h.core.set_metrics(Arc::clone(&metrics));

        // The task is homed on "b", the batch writes an edge whose source is
        // "a" — the destination-home replica of a dual-homed edge.
        let resp =
            h.core
                .execute_edge_put_batch(&make_task_at_source(13, "b"), 1, &[edge("a", "b")]);

        assert_eq!(resp.status, Status::Ok, "the replica is still applied");
        assert_eq!(
            metrics.graph_edges_written.load(Ordering::Relaxed),
            0,
            "the destination home does not own the logical edge"
        );
    }

    /// Replay re-enters this handler for batches a client ran before the
    /// restart, so it must not count them as new writes.
    #[test]
    fn a_replayed_batch_counts_no_write() {
        let mut h = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        h.core.set_metrics(Arc::clone(&metrics));
        h.core.boot_replaying_wal = true;

        let resp = h.core.execute_edge_put_batch(
            &make_task_at_source(14, "owner"),
            1,
            &[edge("owner", "b")],
        );

        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            metrics.graph_edges_written.load(Ordering::Relaxed),
            0,
            "a replayed write happened before the restart"
        );
    }

    /// A dangling endpoint refuses the whole batch before any edge is applied,
    /// so no write is counted — the counter and the affected count agree.
    #[test]
    fn a_refused_batch_counts_no_write() {
        let mut h = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        h.core.set_metrics(Arc::clone(&metrics));
        h.core
            .mark_node_deleted(DatabaseId::DEFAULT.as_u64(), 1, "gone");

        let resp = h.core.execute_edge_put_batch(
            &make_task_with_lsn(12),
            1,
            &[edge("a", "b"), edge("c", "gone")],
        );

        assert_eq!(resp.status, Status::Error);
        assert_eq!(metrics.graph_edges_written.load(Ordering::Relaxed), 0);
    }

    /// The funnel cancels the batch's record on a dangling refusal, so the
    /// refusal must leave no edge of the batch behind.
    #[test]
    fn a_dangling_edge_late_in_the_batch_writes_no_edge() {
        let mut h = make_core();
        h.core
            .mark_node_deleted(DatabaseId::DEFAULT.as_u64(), 1, "gone");
        let task = make_task_with_lsn(9);
        let edges = vec![edge("a", "b"), edge("c", "gone")];

        let resp = h.core.execute_edge_put_batch(&task, 1, &edges);

        assert_eq!(resp.status, Status::Error);
        assert!(matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RejectedDanglingEdge { missing_node }) if missing_node == "gone"
        ));
        let first = h
            .core
            .edge_store
            .get_edge(
                DatabaseId::DEFAULT.as_u64(),
                TenantId::new(1),
                "knows",
                "a",
                "KNOWS",
                "b",
            )
            .expect("read edge");
        assert!(
            first.is_none(),
            "the edge before the dangling one is not written"
        );
    }
}
