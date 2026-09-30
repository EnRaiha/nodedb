// SPDX-License-Identifier: BUSL-1.1

//! `EdgeDeleteBatch`: batched edge tombstone in a single SPSC round-trip.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::shared::{counts_logical_edge_delete, owns_logical_edge_stats};

impl CoreLoop {
    /// Apply a batched edge delete in a single SPSC round-trip.
    ///
    /// The bitemporal edge store always appends a tombstone version on
    /// success, whether or not a live edge existed, so each edge's pre-image
    /// is read before its tombstone is written and the affected count is the
    /// number that were actually live — never the batch length.
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
        let mut removed: u64 = 0;
        for edge in edges {
            let existed = self
                .edge_store
                .get_edge(
                    database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                )
                .ok()
                .flatten()
                .is_some();
            if existed {
                removed += 1;
            }
            let ord = self
                .active_graph_system_from
                .unwrap_or_else(|| self.hlc.next_ordinal());
            use crate::engine::graph::edge_store::EdgeRef;
            // A tombstone that fails to persist fails the statement, same as
            // the single-edge delete: a count that ignored it would report an
            // edge removed that is still live.
            if let Err(e) = self.edge_store.soft_delete_edge_with_stats(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                ),
                ord,
                owns_logical_edge_stats(task, &edge.src_id),
            ) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
            let partition = self.csr_partition_mut(database_id, tid);
            partition.remove_edge_in_collection(
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
                edge.collection.as_str(),
            );
            // Counted per edge as it is tombstoned, never once for the batch:
            // edges before a mid-batch failure are already removed.
            //
            // WIP, not yet correct on a multi-core cluster — see the note on
            // `execute_edge_delete_with_undo`. Counted per home.
            if existed
                && !self.boot_replaying_wal()
                && counts_logical_edge_delete(task, &edge.src_id, &edge.dst_id)
                && let Some(metrics) = &self.metrics
            {
                metrics.record_graph_edge_deleted();
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
            // CDC: one Delete event per edge on the edge's own collection.
            self.emit_graph_edge_event(
                task,
                crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                    collection: edge.collection.as_str(),
                    src_id: &edge.src_id,
                    label: &edge.label,
                    dst_id: &edge.dst_id,
                    op: crate::event::WriteOp::Delete,
                    properties: None,
                },
            );
        }
        self.response_affected(task, removed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use crate::bridge::envelope::Status;
    use crate::control::metrics::SystemMetrics;
    use crate::data::executor::handlers::graph::graph_edge_write::shared::EdgePutParams;
    use crate::data::executor::handlers::graph::graph_edge_write::shared::test_support::{
        affected_count, make_core, make_task_at_source,
    };
    use nodedb_physical::physical_plan::BatchEdge;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

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

    fn put(core: &mut crate::data::executor::core_loop::CoreLoop, src: &str, dst: &str, lsn: u64) {
        assert_eq!(
            core.execute_edge_put(
                &make_task_at_source(lsn, src),
                EdgePutParams {
                    tid: 1,
                    collection: "knows",
                    src_id: src,
                    label: "KNOWS",
                    dst_id: dst,
                    properties: b"w=1",
                    src_surrogate: Surrogate::new(1),
                    dst_surrogate: Surrogate::new(2),
                },
            )
            .status,
            Status::Ok
        );
    }

    /// The batch counts the edges it actually removed: two live edges count
    /// two, the absent third counts nothing, and the counter matches the
    /// affected count the response carries.
    #[test]
    fn a_delete_batch_counts_only_the_live_edges() {
        let mut h = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        h.core.set_metrics(Arc::clone(&metrics));
        put(&mut h.core, "owner", "b", 40);
        put(&mut h.core, "owner", "d", 41);

        let batch = vec![edge("owner", "b"), edge("owner", "d"), edge("owner", "f")];
        let resp = h
            .core
            .execute_edge_delete_batch(&make_task_at_source(42, "owner"), 1, &batch);

        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected_count(&resp), 2);
        assert_eq!(metrics.graph_edges_deleted.load(Ordering::Relaxed), 2);
    }

    /// A batch counts the live rows it removes on whichever home holds them,
    /// and the other home of a dual-homed edge then finds them absent and adds
    /// nothing — so the pair counts each logical edge once.
    #[test]
    fn a_delete_batch_counts_the_live_rows_any_home_removes() {
        let mut h = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        h.core.set_metrics(Arc::clone(&metrics));
        // Seed through the edge's source home so a live row exists.
        put(&mut h.core, "owner", "b", 43);

        // The task is homed on "b", not the edge's source home.
        let resp = h.core.execute_edge_delete_batch(
            &make_task_at_source(44, "b"),
            1,
            &[edge("owner", "b")],
        );

        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected_count(&resp), 1, "a live row was removed");
        assert_eq!(
            metrics.graph_edges_deleted.load(Ordering::Relaxed),
            1,
            "the home that held the live row counts the removal"
        );

        // The source home now finds the edge absent and adds nothing.
        let resp = h.core.execute_edge_delete_batch(
            &make_task_at_source(45, "owner"),
            1,
            &[edge("owner", "b")],
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected_count(&resp), 0);
        assert_eq!(
            metrics.graph_edges_deleted.load(Ordering::Relaxed),
            1,
            "the second home removes nothing, so the logical edge counts once"
        );
    }

    /// Replay re-enters this handler for batches a client ran before the
    /// restart, so it must not count them as new removals.
    #[test]
    fn a_replayed_delete_batch_counts_no_delete() {
        let mut h = make_core();
        let metrics = Arc::new(SystemMetrics::new());
        h.core.set_metrics(Arc::clone(&metrics));
        put(&mut h.core, "owner", "b", 47);

        h.core.boot_replaying_wal = true;
        let resp = h.core.execute_edge_delete_batch(
            &make_task_at_source(48, "owner"),
            1,
            &[edge("owner", "b")],
        );

        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            metrics.graph_edges_deleted.load(Ordering::Relaxed),
            0,
            "a replayed removal happened before the restart"
        );
    }
}
