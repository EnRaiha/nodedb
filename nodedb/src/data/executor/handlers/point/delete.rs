// SPDX-License-Identifier: BUSL-1.1

//! PointDelete: remove one document plus its cascading side-effects across
//! inverted, secondary, graph, and spatial indexes.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec, StorageMode};
use nodedb_types::Surrogate;

/// Borrowed arguments for [`CoreLoop::execute_point_delete`], grouped so the
/// handler stays within the argument-count limit.
pub(in crate::data::executor) struct PointDeleteExec<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled RLS read policy gating the `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Compiled RLS write policy gating the REMOVAL, decided against the row's
    /// pre-deletion image — the only image a delete has. A separate slot from
    /// `rls_filters`: that one bounds what may be shown back, this one bounds
    /// what may be removed. Empty = no write policy.
    pub rls_write_check: &'a [u8],
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this delete must debit, resolved on the Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_delete(
        &mut self,
        task: &ExecutionTask,
        args: PointDeleteExec<'_>,
    ) -> Response {
        let PointDeleteExec {
            tid,
            collection,
            document_id,
            surrogate,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = args;
        debug!(core = self.core_id, %collection, %document_id, "point delete");

        let database_id = task.request.database_id.as_u64();
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };

        // Gate the removal on the collection's write policy, decided against
        // the row's pre-deletion image. The read happens BEFORE the write: the
        // removal is staged into the transaction below, so checking a value
        // read back through it would decide a row already gone.
        if let Err(e) = self.gate_point_delete(task, tid, collection, surrogate, rls_write_check) {
            return self.response_error(task, e);
        }

        // Doc-store write + all index cascades, via `apply_point_delete`, in a
        // transaction this handler owns: every sparse-database write the delete
        // performs lands on commit below, or none of it does if any step fails
        // (the txn is dropped un-committed on every early return).
        let txn = match self.sparse.begin_write() {
            Ok(txn) => txn,
            Err(e) => return self.response_error(task, e),
        };
        let outcome = match self.apply_point_delete(
            &txn,
            PointDeleteParams {
                database_id,
                tid,
                collection,
                document_id,
                surrogate,
                user_roles: &task.request.user_roles,
                enforce: true,
            },
        ) {
            Ok(outcome) => outcome,
            Err(e) => return self.response_error(task, e),
        };
        // Image-folding enforcement, inside the SAME transaction the removal was
        // staged in: a materialized-sum target write is itself a document write,
        // so the debit and the row's removal land or roll back together. A
        // delete that matched nothing changes no total and folds nothing —
        // `apply_point_delete` reports that as a `None` pre-image.
        //
        // `outcome.prior_value` is the pre-image, which is the ONLY image a
        // delete has. An enforcement API that could report only a post-image
        // could not express this write at all, which is why a deleted row's
        // contribution used to stay on the total forever.
        let enforcement = match outcome.prior_value {
            Some(ref old) => match write_hook::run(
                self,
                &txn,
                &hook_ctx,
                WriteImages::Delete {
                    old: ImageBody::Stored(old),
                },
            ) {
                Ok(enforcement) => enforcement,
                Err(e) => {
                    // `apply_point_delete` already invalidated this row's cache
                    // entry, and dropping `txn` reverses every durable write it
                    // staged, so nothing else has to be undone here.
                    return self.response_error(task, e);
                }
            },
            None => Default::default(),
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);

        // A delete subtracts the removed row's amount, so removing one leg of a
        // balanced journal on its own is a violation. Settled before the commit,
        // and dropping `txn` un-committed reverses the removal.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                },
            );
        }
        let prior = outcome.prior_value;

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Record the committed delete's version against its surrogate +
        // collection, but only when a row was actually removed — a delete that
        // matched nothing changes no state and creates no OCC conflict.
        if prior.is_some() {
            self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());

            // Record the removed secondary-index values into the per-index
            // write-value substrate (plain cascade ∪ bitemporal tombstones).
            if let Some(lsn) = task.wal_lsn() {
                let mut tuples = outcome.secondary_index_tuples;
                tuples.extend(outcome.bitemporal_index_tuples);
                self.note_index_write_values(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                    &tuples,
                    lsn,
                );
            }
        }

        // Emit delete event to Event Plane if the row actually existed.
        // `apply_point_delete` returns the prior bytes — we thread them
        // through so CDC/trigger consumers see the pre-delete state as
        // `old_value`. A delete against a non-existent key is a true
        // no-op and emits nothing.
        if let Some(prior_bytes) = prior.as_deref() {
            let old_converted = self.resolve_event_payload(
                task.request.database_id.as_u64(),
                tid,
                collection,
                prior_bytes,
            );
            self.emit_write_event(
                task,
                collection,
                crate::event::WriteOp::Delete,
                document_id,
                None,
                Some(old_converted.as_deref().unwrap_or(prior_bytes)),
            );
        }

        let mut response = if let (Some(spec), Some(prior_bytes)) = (returning, prior.as_deref()) {
            // Decode the pre-deletion image with the collection's storage mode:
            // on a strict collection the prior bytes are a Binary Tuple, which
            // the MessagePack decoder accepts without erroring and turns into a
            // document with none of the row's real columns. The schema borrow is
            // scoped so the response build below can take `self` mutably.
            let doc = {
                let strict_schema = self
                    .doc_configs
                    .get(&(
                        task.request.database_id,
                        crate::types::TenantId::new(tid),
                        collection.to_string(),
                    ))
                    .and_then(|c| match &c.storage_mode {
                        StorageMode::Strict { schema } => Some(schema),
                        StorageMode::Schemaless => None,
                    });
                returning_doc::from_stored(prior_bytes, document_id, strict_schema)
            };
            let doc = match doc {
                Ok(doc) => doc,
                Err(e) => return self.response_error(task, e),
            };
            match returning_rows::build_rows_payload(spec, rls_filters, &[doc]) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else if let Some(spec) = returning {
            // Row did not exist — return empty rows payload.
            match returning_rows::build_rows_payload(spec, rls_filters, &[]) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else {
            // No RETURNING: report the count the doc-store write actually
            // produced. `prior` is `None` when the row was already gone, which
            // is a genuine no-op — the plan resolved a surrogate for the
            // primary key (surrogates outlive the row they were assigned to),
            // so the surrogate is no evidence a row was there to remove.
            self.response_affected(task, u64::from(prior.is_some()))
        };
        // Redo entries for the target rows this delete debited: the statement's
        // own redo names only the removed row, so without these a WAL-only
        // restart replays the removal and leaves every total still carrying the
        // contribution of a row that is gone.
        if !target_write_set.is_empty() {
            response.write_set = target_write_set;
        }
        response
    }

    /// Decide a single row's removal against the compiled write policy.
    ///
    /// A row that is already absent is admitted: the delete removes nothing, so
    /// there is no image for the policy to restrict and no state change to
    /// refuse. Reads through the same current-state view the delete cascade
    /// uses, so a bitemporal collection is decided on its live version rather
    /// than a superseded one.
    fn gate_point_delete(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        rls_write_check: &[u8],
    ) -> crate::Result<()> {
        if rls_write_check.is_empty() {
            return Ok(());
        }
        let database_id = task.request.database_id.as_u64();
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let stored = if self.is_bitemporal(database_id, tid, collection) {
            self.sparse
                .versioned_get_current(database_id, tid, collection, row_key)?
        } else {
            self.sparse.get(database_id, tid, collection, row_key)?
        };
        let Some(body) = stored else {
            return Ok(());
        };
        let strict_schema = self
            .doc_configs
            .get(&(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection.to_string(),
            ))
            .and_then(|c| match &c.storage_mode {
                StorageMode::Strict { schema } => Some(schema),
                StorageMode::Schemaless => None,
            });
        rls_write_gate::admit_stored_row(
            rls_write_check,
            &body,
            row_key,
            strict_schema,
            tid,
            collection,
        )
    }
}
