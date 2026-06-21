// SPDX-License-Identifier: BUSL-1.1

//! PointInsert: write one document, probing existence under the same
//! write transaction so duplicate primary keys surface as
//! `unique_violation` (SQLSTATE 23505) instead of silently overwriting.
//!
//! Distinct from `PointPut` — that handler is by-design an upsert.
//! `PointInsert` is routed from SQL `INSERT` (and `INSERT ... ON CONFLICT
//! DO NOTHING` with `if_absent=true`).

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_types::Surrogate;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_point_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        document_id: &str,
        surrogate: Surrogate,
        value: &[u8],
        if_absent: bool,
    ) -> Response {
        let row_key = surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        debug!(
            core = self.core_id,
            %collection, %document_id, if_absent,
            "point insert"
        );

        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => return self.response_error(task, e),
        };

        // Existence probe inside the write transaction: linearizable with
        // the apply_point_put commit — no other writer can insert between
        // this check and our insert commit. Probe uses `document_id` as
        // the row key, which is how the primary key is encoded for strict
        // and schemaless collections alike (see `dml::convert_insert`).
        let bitemporal = self.is_bitemporal(tid, collection);
        let database_id = task.request.database_id.as_u64();
        let exists_result = if bitemporal {
            self.sparse
                .versioned_exists_current_in_txn(&txn, database_id, tid, collection, row_key)
        } else {
            self.sparse
                .exists_in_txn(&txn, database_id, tid, collection, row_key)
        };
        match exists_result {
            Ok(true) => {
                // Drop the txn without committing — no-op on redb.
                if if_absent {
                    // `INSERT ... ON CONFLICT DO NOTHING`: silent skip.
                    return self.response_ok(task);
                }
                return self.response_error(
                    task,
                    crate::Error::RejectedConstraint {
                        collection: collection.to_string(),
                        constraint: "unique".to_string(),
                        detail: format!(
                            "duplicate key value '{document_id}' violates primary-key \
                             uniqueness on '{collection}'"
                        ),
                    },
                );
            }
            Ok(false) => {}
            Err(e) => return self.response_error(task, e),
        }

        // `apply_point_put` returns prior bytes if any — for PointInsert this
        // must be `None` because the probe above already rejected the
        // conflict case. We intentionally drop it.
        if let Err(e) = self.apply_point_put(
            &txn,
            PointPutParams {
                database_id: task.request.database_id.as_u64(),
                tid,
                collection,
                document_id: row_key,
                surrogate,
                value,
            },
        ) {
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("commit: {e}"),
                },
            );
        }

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Implicit graph-edge extraction now lives on the Control Plane
        // (`control/planner/implicit_edges.rs`): a `_from`/`_to` document is
        // mirrored as a `GraphOp::EdgePut` task BEFORE dispatch, so the edge is
        // homed and surrogate-resolved per endpoint and routes through the same
        // single-home/Calvin path as an explicit edge. The PointInsert handler
        // only writes the document; it no longer derives edges (which mis-homed
        // cross-shard edges by the document's vShard).

        self.emit_put_event(task, tid, collection, row_key, value, None);

        self.response_ok(task)
    }
}
