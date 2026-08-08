// SPDX-License-Identifier: BUSL-1.1

//! PointPut: insert or overwrite one document, committing storage + indexes
//! + stats in a single redb transaction via `apply_point_put`.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::Surrogate;

/// Dispatch-side arguments for [`CoreLoop::execute_point_put`].
pub(in crate::data::executor) struct PointPutExec<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    /// When `Some`, project the STORED post-image per spec instead of
    /// reporting a bare affected count.
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_put(
        &mut self,
        task: &ExecutionTask,
        params: PointPutExec<'_>,
    ) -> Response {
        let PointPutExec {
            tid,
            collection,
            document_id,
            surrogate,
            value,
            returning,
            rls_filters,
        } = params;
        let row_key = surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        debug!(core = self.core_id, %collection, %document_id, "point put");

        // Unified write transaction: document + inverted index + stats in one commit.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let mut prior = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id: task.request.database_id.as_u64(),
                tid,
                collection,
                document_id: row_key,
                surrogate,
                value,
                index_text: true,
                user_roles: &task.request.user_roles,
                enforce: true,
                wal_lsn: task.wal_lsn(),
            },
        ) {
            Ok(p) => p,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                },
            );
        }

        // Record the committed write's version against its surrogate + collection.
        self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());

        // Record the touched secondary-index values into the per-index
        // write-value substrate (added ∪ removed ∪ bitemporal tuples).
        if let Some(lsn) = task.wal_lsn() {
            let mut tuples = std::mem::take(&mut prior.secondary_index_added);
            tuples.append(&mut prior.secondary_index_removed);
            tuples.append(&mut prior.bitemporal_index_tuples);
            self.note_index_write_values(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
                &tuples,
                lsn,
            );
        }

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Emit write event to Event Plane. Insert vs Update is derived
        // from whether `prior` was present — a PointPut onto an existing
        // row is an Update from every downstream consumer's perspective.
        self.emit_put_event(
            task,
            tid,
            collection,
            row_key,
            value,
            prior.prior_value.as_deref(),
        );

        if let Some(spec) = returning {
            let strict_schema = self.strict_schema_for(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
            );
            return self.stored_returning_response(
                task,
                spec,
                rls_filters,
                strict_schema.as_ref(),
                &[(document_id, prior.stored_value.as_slice())],
            );
        }

        // An upsert always writes the row, whether or not one was there before.
        self.response_affected(task, 1)
    }
}
