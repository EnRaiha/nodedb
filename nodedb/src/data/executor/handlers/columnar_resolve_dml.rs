// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `ColumnarOp::ResolveDml`.
//!
//! Runs the SAME row-selection and assignment logic
//! `execute_columnar_update` / `execute_columnar_delete` use
//! (`columnar_resolve.rs`), but only reports the result instead of applying
//! it: the Control Plane runs this before proposing a governed predicate
//! `UPDATE`/`DELETE` through Raft, because a follower has no writing identity
//! to evaluate the predicate against, and it needs the exact row images the
//! predicate resolved to — not a JSON-lossy re-encoding of them — to decide
//! the write policy and build the `ColumnarOp::ResolvedUpdate` /
//! `ResolvedDelete` it then proposes.
//!
//! Read-only: this handler never calls `engine.update` / `engine.delete`, so
//! it takes the engine by shared reference. A rejected row refuses the whole
//! statement, exactly as `execute_columnar_update` / `execute_columnar_delete`
//! do — the caller never sees a partial resolved set.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_resolve::{
    ResolveUpdateRowsParams, require_pk_column_index, resolve_delete_rows, resolve_update_rows,
};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle `ColumnarOp::ResolveDml`: resolve the rows `filters` (and, for
    /// an update, `updates`) would touch, decide the write policy against
    /// each one, and report the resolved set. Mutates nothing.
    pub(in crate::data::executor) fn execute_columnar_resolve_dml(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        filters: &[u8],
        updates: &[(String, Vec<u8>)],
        is_update: bool,
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        debug!(core = self.core_id, %collection, is_update, "columnar resolve dml");

        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let engine = match self.columnar_engines.get(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        let schema = engine.schema().clone();
        let op_name = if is_update { "UPDATE" } else { "DELETE" };
        let pk_col_idx = match require_pk_column_index(&schema, op_name) {
            Ok(idx) => idx,
            Err(e) => return self.response_error(task, e),
        };

        let filter_predicates: Vec<ScanFilter> = if !filters.is_empty() {
            zerompk::from_msgpack(filters).unwrap_or_default()
        } else {
            Vec::new()
        };

        let tid = task.request.tenant_id.as_u64();
        let payload = if is_update {
            let rows = match resolve_update_rows(ResolveUpdateRowsParams {
                engine,
                schema: &schema,
                pk_col_idx,
                filter_predicates: &filter_predicates,
                updates,
                rls_write_check,
                tid,
                collection,
            }) {
                Ok(rows) => rows,
                Err(e) => return self.response_error(task, e),
            };
            response_codec::encode(&rows)
        } else {
            let pks = match resolve_delete_rows(
                engine,
                &schema,
                pk_col_idx,
                &filter_predicates,
                rls_write_check,
                tid,
                collection,
            ) {
                Ok(pks) => pks,
                Err(e) => return self.response_error(task, e),
            };
            response_codec::encode(&pks)
        };

        match payload {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(task, e),
        }
    }
}
