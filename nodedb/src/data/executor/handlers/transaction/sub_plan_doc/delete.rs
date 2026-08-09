// SPDX-License-Identifier: BUSL-1.1

//! Document PointDelete helper for transaction sub-plans.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::tx_point_delete`].
pub(in crate::data::executor::handlers::transaction) struct TxPointDelete<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub user_roles: &'a [String],
}

impl CoreLoop {
    /// Execute a PointDelete within a transaction.
    pub(in crate::data::executor::handlers::transaction) fn tx_point_delete(
        &mut self,
        p: TxPointDelete<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxPointDelete {
            task: dummy_task,
            tid,
            collection,
            document_id,
            surrogate,
            user_roles,
        } = p;
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let database_id = dummy_task.request.database_id.as_u64();

        // Core delete path shared with the autocommit caller: bitemporal-vs-plain
        // primary tombstone/delete (including versioned index tombstones),
        // FTS/inverted removal, secondary-index cascade, graph-edge cascade,
        // spatial R-tree removal, `mark_node_deleted` bookkeeping, doc_cache
        // invalidation, and stateless DELETE enforcement. Every side-effect is
        // captured in the outcome and reversed via the undo log below, so the
        // transactional delete is identical to autocommit and fully
        // rollback-safe.
        //
        // Each transaction sub-plan owns its own per-row redb write txn; the
        // batch is stitched together by the undo log, not one big txn. A
        // failure inside `apply_point_delete` returns before the commit, so the
        // txn is dropped and every sparse-database write it staged is rolled
        // back.
        let txn = self.sparse.begin_write().map_err(|e| ErrorCode::Internal {
            detail: e.to_string(),
        })?;
        let outcome = self.apply_point_delete(
            &txn,
            PointDeleteParams {
                database_id,
                tid,
                collection,
                document_id,
                surrogate,
                user_roles,
                enforce: true,
            },
        )?;

        txn.commit().map_err(|e| ErrorCode::Internal {
            detail: format!("commit: {e}"),
        })?;
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Only push an undo entry when a row was actually removed — a delete
        // against a non-existent key has nothing to reverse.
        if let Some(old) = outcome.prior_value {
            undo_log.push(UndoEntry::DeleteDocument {
                collection: collection.to_string(),
                document_id: row_key.to_string(),
                surrogate,
                old_value: old,
                bitemporal_sys_from_ms: outcome.bitemporal_sys_from_ms,
                bitemporal_index_tuples: outcome.bitemporal_index_tuples,
                // NON-empty on non-bitemporal deletes: the cascade removed these
                // plain secondary-index entries, so a rolled-back DELETE restores
                // them (closes the pre-existing tx-DELETE rollback hole).
                secondary_index_tuples: outcome.secondary_index_tuples,
                chain_hash_prior: None,
            });
        }

        // The delete-cleanup soft-deleted this document's vectors unconditionally
        // (fixing the orphan leak even in autocommit). In the transactional path
        // a rollback must restore them, so push one `DeleteVector` undo per
        // soft-deleted vector — `apply_undo_vector` `undelete`s each on rollback.
        for delta in outcome.vector_deletes {
            undo_log.push(UndoEntry::DeleteVector {
                index_key: delta.index_key,
                vector_id: delta.vector_id,
                collection: delta.collection,
                field: delta.field,
                doc_id: delta.doc_id,
            });
        }

        // Reverse any spatial R-tree removals on rollback (one `SpatialDelete`
        // undo per per-field R-tree entry the delete removed, re-inserting it
        // with its captured bbox).
        for (key, entry_id, bbox, document_id) in outcome.spatial_deletes {
            undo_log.push(UndoEntry::SpatialDelete {
                key,
                entry_id,
                bbox,
                document_id,
            });
        }

        // Reverse the `mark_node_deleted` bookkeeping on rollback: un-mark the
        // node in the in-memory `deleted_nodes` tracker. `Some` only when this
        // delete NEWLY marked the node (a pre-existing tombstone from a prior
        // committed op is never resurrected — see `apply_point_delete`).
        if let Some(node_id) = outcome.mark_node_deleted {
            undo_log.push(UndoEntry::MarkNodeDeleted {
                database_id,
                tid,
                node_id,
            });
        }

        // The graph-edge cascade unconditionally removed every edge incident on
        // this document from BOTH the CSR partition and the persistent edge
        // store. In the transactional path a rollback must restore them, so push
        // one `DeleteEdge` undo per cascaded edge — `apply_undo_edge` re-inserts
        // each into both stores with its captured old properties. NON-empty
        // whenever the deleted document had edges: this closes the pre-existing
        // hole where a rolled-back tx DELETE permanently lost cascaded edges.
        for (collection, src_id, label, dst_id, old_properties) in outcome.edge_deletes {
            undo_log.push(UndoEntry::DeleteEdge {
                collection,
                src_id,
                label,
                dst_id,
                old_properties,
            });
        }
        Ok(self.response_ok(dummy_task))
    }
}
