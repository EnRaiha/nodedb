// SPDX-License-Identifier: BUSL-1.1

//! PointPut: insert or overwrite one document, committing storage + indexes
//! + stats in a single redb transaction via `apply_point_put`.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_guard::{self, ChainGuard};
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec};
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
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this write may touch, resolved on the Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
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
            resolved_sum_targets,
        } = params;
        let row_key = surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        debug!(core = self.core_id, %collection, %document_id, "point put");

        let database_id = task.request.database_id.as_u64();
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };

        // A PUT is an upsert, so whether it is a chain link depends on whether a
        // row is already there. The chain rewrites the BODY, so that question
        // has to be answered BEFORE the write — `apply_point_put`'s outcome
        // comes too late for it. The probe is paid only by a collection that
        // actually declares `HASH_CHAIN`.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);
        let chained = if chain.enabled()
            && self
                .sparse
                .get(database_id, tid, collection, row_key)
                .ok()
                .flatten()
                .is_none()
        {
            match chain.chain_insert(self, database_id, tid, document_id, value) {
                Ok(chained) => chained,
                Err(e) => return self.response_error(task, e),
            }
        } else {
            None
        };
        let effective_value: &[u8] = chained.as_deref().unwrap_or(value);

        // Unified write transaction: document + inverted index + stats in one commit.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                chain.restore(self);
                return self.response_error(task, e);
            }
        };

        let mut prior = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid,
                collection,
                document_id: row_key,
                surrogate,
                value: effective_value,
                index_text: true,
                user_roles: &task.request.user_roles,
                enforce: true,
                wal_lsn: task.wal_lsn(),
            },
        ) {
            Ok(p) => p,
            Err(e) => {
                chain_guard::abort_after_apply(self, &chain, database_id, tid, collection, row_key);
                return self.response_error(task, e);
            }
        };

        if let Err(e) = chain.persist_head(self, &txn) {
            chain_guard::abort_after_apply(self, &chain, database_id, tid, collection, row_key);
            return self.response_error(task, e);
        }

        // `prior.prior_value` is what discriminates insert from update here —
        // the same bitemporal-aware pre-image that drives `emit_put_event`
        // below. An enforcement that only saw the post-image would account
        // every overwrite as a fresh insert and double-count its amount.
        let images = match prior.prior_value {
            Some(ref old) => WriteImages::Update {
                old: ImageBody::Stored(old),
                new: ImageBody::Submitted(value),
            },
            None => WriteImages::Insert {
                new: ImageBody::Submitted(value),
            },
        };
        let enforcement = match write_hook::run(self, &txn, &hook_ctx, images) {
            Ok(outcome) => outcome,
            Err(e) => {
                chain_guard::abort_after_apply(self, &chain, database_id, tid, collection, row_key);
                return self.response_error(task, e);
            }
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);

        // Settled before the commit: an autocommit statement is its own
        // transaction boundary, so a put that leaves a journal group unbalanced
        // is refused with nothing written.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            chain_guard::abort_after_apply(self, &chain, database_id, tid, collection, row_key);
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

        let mut response = if let Some(spec) = returning {
            let strict_schema = self.strict_schema_for(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
            );
            self.stored_returning_response(
                task,
                spec,
                rls_filters,
                strict_schema.as_ref(),
                &[(document_id, prior.stored_value.as_slice())],
            )
        } else {
            // An upsert always writes the row, whether or not one was there before.
            self.response_affected(task, 1)
        };
        if !target_write_set.is_empty() {
            response.write_set = target_write_set;
        }
        response
    }
}
