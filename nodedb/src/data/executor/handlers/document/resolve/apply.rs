// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `DocumentOp::ResolvedWrite`.
//!
//! Runs on every replica, leader included. The Control Plane already read the
//! rows this write depends on, computed every post-image, and decided the write
//! policy against them while the writing identity was live — so the plan carries
//! the verdict (`RlsWriteCheck::DecidedEarlierInRequest`) and the mutations, not
//! an operation to re-derive. Nothing is recomputed here.
//!
//! ## Drift check
//!
//! Between the resolve and this apply, the committed log may have advanced (a
//! concurrent write on another connection, replicated ahead of this one). Every
//! replica must reach the SAME verdict on a resolution that no longer matches
//! state, or replicas diverge. So every mutation's `precondition` — the exact
//! stored bytes the resolve read — is compared with `==` BEFORE the first
//! mutation runs; if any fails, nothing is mutated and the caller gets
//! `ErrorCode::OllpRetryRequired`.
//!
//! A surrogate-existence check would not do: it proves only that the row was not
//! deleted, and is blind to a concurrent write that changed the row's CONTENT
//! between resolve and apply. That is the lost update.

use nodedb_physical::physical_plan::DocumentResolvedMutation;
use tracing::debug;

use super::apply_row::{ApplyResolvedDelete, ApplyResolvedPut};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;

impl CoreLoop {
    /// Handle `DocumentOp::ResolvedWrite`: check every precondition, apply every
    /// mutation, and return the shipped payload verbatim.
    pub(in crate::data::executor) fn execute_document_resolved_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        mutations: &[DocumentResolvedMutation],
        response_payload: &[u8],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        debug!(
            core = self.core_id,
            count = mutations.len(),
            "document resolved write"
        );
        if let Err(code) = self.check_resolved_document_preconditions(task, tid, mutations) {
            return self.response_error(task, code);
        }
        // The gate stays on every write path even though
        // `DecidedEarlierInRequest` makes this a no-op — a single path that
        // skips it entirely is a hole future callers can fall into.
        for mutation in mutations {
            if let DocumentResolvedMutation::Put {
                collection,
                document_id,
                value,
                ..
            } = mutation
                && let Err(e) = rls_write_gate::admit_stored_row(
                    rls_write_check,
                    value,
                    document_id,
                    None,
                    tid,
                    collection,
                )
            {
                return self.response_error(task, e);
            }
        }

        let mut write_set = Vec::new();
        for mutation in mutations {
            let applied = match mutation {
                DocumentResolvedMutation::Put {
                    collection,
                    surrogate,
                    value,
                    precondition,
                    resolved_sum_targets,
                    document_id: _,
                    pk_bytes: _,
                } => self.apply_resolved_document_put(
                    task,
                    ApplyResolvedPut {
                        tid,
                        collection,
                        surrogate: *surrogate,
                        value,
                        precondition: precondition.as_deref(),
                        resolved_sum_targets,
                    },
                ),
                DocumentResolvedMutation::Delete {
                    collection,
                    document_id,
                    surrogate,
                    resolved_sum_targets,
                    pk_bytes: _,
                    precondition: _,
                } => self.apply_resolved_document_delete(
                    task,
                    ApplyResolvedDelete {
                        tid,
                        collection,
                        document_id,
                        surrogate: *surrogate,
                        resolved_sum_targets,
                    },
                ),
            };
            match applied {
                Ok(entries) => write_set.extend(entries),
                Err(code) => return self.response_error(task, code),
            }
        }

        let mut response = self.response_with_payload(task, response_payload.to_vec());
        response.write_set = write_set;
        response
    }

    /// Confirm every mutation still describes the row it was resolved against.
    ///
    /// Runs to completion BEFORE the first mutation applies, so the write stays
    /// all-or-nothing — the same contract `KvOp::ResolvedWrite` holds.
    fn check_resolved_document_preconditions(
        &self,
        task: &ExecutionTask,
        tid: u64,
        mutations: &[DocumentResolvedMutation],
    ) -> Result<(), ErrorCode> {
        let database_id = task.request.database_id.as_u64();
        for mutation in mutations {
            let row_key = surrogate_to_doc_id(mutation.surrogate());
            let current =
                self.doc_current_bytes(database_id, tid, mutation.collection(), row_key.as_str())?;
            if current.as_deref() != mutation.precondition() {
                return Err(ErrorCode::OllpRetryRequired);
            }
        }
        Ok(())
    }
}
