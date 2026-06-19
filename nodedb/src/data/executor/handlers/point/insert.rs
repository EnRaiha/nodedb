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

        self.maybe_register_edge(database_id, tid, collection, surrogate, value);

        self.emit_put_event(task, tid, collection, row_key, value, None);

        self.response_ok(task)
    }

    /// Cross-engine graph overlay: when a schemaless document carries the
    /// reserved `_from` / `_to` (and optional `_type`) fields, mirror it as
    /// an edge in the CSR adjacency index and the edge store. Without this
    /// hook, `MATCH ...` and `GRAPH ALGO ...` would never see edges that
    /// were inserted via plain document INSERT.
    fn maybe_register_edge(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        value: &[u8],
    ) {
        let doc =
            match crate::data::executor::handlers::document::read::decode::decode_scanned_document(
                value, None,
            ) {
                serde_json::Value::Object(m) => m,
                _ => return,
            };
        let src = match doc.get("_from").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return,
        };
        let dst = match doc.get("_to").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return,
        };
        let label = doc
            .get("_type")
            .and_then(|v| v.as_str())
            .unwrap_or("edge")
            .to_string();
        let weight = doc.get("weight").and_then(|v| v.as_f64()).unwrap_or(1.0);

        let ord = self.hlc.next_ordinal();
        let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
        use crate::engine::graph::edge_store::EdgeRef;
        if let Err(e) = self.edge_store.put_edge_versioned(
            EdgeRef::new(
                nodedb_types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection,
                &src,
                &label,
                &dst,
            ),
            &[],
            ord,
            valid_from_ms,
            i64::MAX,
        ) {
            tracing::debug!(err = %e, %src, %dst, %label, %collection, "edge store write failed during graph overlay registration");
        }
        let partition = self.csr_partition_mut(database_id, tid);
        let csr_result = if (weight - 1.0).abs() > f64::EPSILON {
            partition.add_edge_weighted(&src, &label, &dst, weight)
        } else {
            partition.add_edge(&src, &label, &dst)
        };
        if let Err(e) = csr_result {
            tracing::debug!(err = %e, %src, %dst, %label, %collection, "CSR partition write failed during graph overlay registration");
        }
        partition.set_node_surrogate(&src, Surrogate::ZERO);
        partition.set_node_surrogate(&dst, surrogate);
    }
}
