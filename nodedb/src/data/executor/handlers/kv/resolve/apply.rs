// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `KvOp::ResolvedWrite`. Runs on every replica; the
//! plan carries the decided verdict and mutations, nothing recomputed.
//!
//! Drift check: every mutation's `precondition` is checked before the first
//! mutation runs, all-or-nothing; a failure returns `OllpRetryRequired` —
//! same contract the columnar resolved-row apply uses.

use nodedb_physical::physical_plan::KvResolvedMutation;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle `KvOp::ResolvedWrite`: check every precondition, apply every
    /// mutation, and return the shipped payload verbatim.
    pub(in crate::data::executor) fn execute_kv_resolved_write(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        mutations: &[KvResolvedMutation],
        response_payload: &[u8],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        debug!(
            core = self.core_id,
            count = mutations.len(),
            "kv resolved write"
        );
        let now_ms = self.kv_ttl_now_ms(task);

        for mutation in mutations {
            let current = self.kv_engine.get(
                did,
                tid,
                mutation.collection().as_str(),
                mutation.key(),
                now_ms,
            );
            if current.as_deref() != mutation.precondition() {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
            // A still-absent key has nothing to expire/persist — same
            // `NotFound` the live handlers return, raised before any mutation.
            if current.is_none()
                && matches!(
                    mutation,
                    KvResolvedMutation::Expire { .. } | KvResolvedMutation::Persist { .. }
                )
            {
                return self.response_error(task, ErrorCode::NotFound);
            }
            // Gate stays on every write path even as a no-op here.
            if let KvResolvedMutation::Put {
                collection,
                key,
                value,
                ..
            } = mutation
                && let Err(error) =
                    admit_kv_row(rls_write_check, value, key, tid, collection.as_str())
            {
                return self.response_error(task, error);
            }
        }

        for mutation in mutations {
            self.apply_kv_resolved_mutation(task, did, tid, mutation, now_ms);
        }

        self.response_with_payload(task, response_payload.to_vec())
    }

    /// Apply one already-checked mutation and emit its change event. `old` on
    /// the event is the precondition the drift check just confirmed.
    fn apply_kv_resolved_mutation(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        mutation: &KvResolvedMutation,
        now_ms: u64,
    ) {
        match mutation {
            KvResolvedMutation::Put {
                collection,
                key,
                value,
                ttl_ms,
                expire_at_ms,
                surrogate,
                precondition,
            } => {
                self.kv_engine.put_with_absolute_expiry(
                    crate::engine::kv::KvPutParams {
                        database_id: did,
                        tenant_id: tid,
                        collection: collection.as_str(),
                        key,
                        value,
                        ttl_ms: *ttl_ms,
                        now_ms,
                        surrogate: *surrogate,
                    },
                    *expire_at_ms,
                );
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                let op = match precondition {
                    Some(_) => crate::event::WriteOp::Update,
                    None => crate::event::WriteOp::Insert,
                };
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection.as_str(),
                    op,
                    &key_str,
                    Some(value),
                    precondition.as_deref(),
                );
                self.note_kv_write_lsn(task, did, tid, collection.as_str(), key);
            }
            KvResolvedMutation::Delete {
                collection,
                key,
                precondition,
            } => {
                self.kv_engine
                    .delete(did, tid, collection.as_str(), &[key.to_vec()], now_ms);
                if let Some(ref m) = self.metrics {
                    m.record_kv_delete();
                }
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection.as_str(),
                    crate::event::WriteOp::Delete,
                    &key_str,
                    None,
                    precondition.as_deref(),
                );
                self.note_kv_write_lsn(task, did, tid, collection.as_str(), key);
            }
            // No body change, so no event — matches the live handlers.
            KvResolvedMutation::Expire {
                collection,
                key,
                ttl_ms,
                resolved_now_ms,
                precondition: _,
            } => {
                self.kv_engine.expire_with_absolute_expiry(
                    did,
                    tid,
                    collection.as_str(),
                    key,
                    resolved_now_ms.saturating_add(*ttl_ms),
                );
                self.note_kv_write_lsn(task, did, tid, collection.as_str(), key);
            }
            KvResolvedMutation::Persist {
                collection,
                key,
                precondition: _,
            } => {
                self.kv_engine.persist(did, tid, collection.as_str(), key);
                self.note_kv_write_lsn(task, did, tid, collection.as_str(), key);
            }
        }
    }
}
