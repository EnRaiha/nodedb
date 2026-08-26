// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `KvOp::ResolvedWrite`.
//!
//! Runs on every replica, leader included. The Control Plane already read the
//! rows this write depends on, computed every post-image, and decided the
//! write policy against them while the writing identity was live — so the plan
//! carries the verdict (`RlsWriteCheck::DecidedEarlierInRequest`) and the
//! mutations, not an operation to re-derive. Nothing is recomputed here.
//!
//! ## Drift check
//!
//! Between the resolve and this apply, the committed log may have advanced (a
//! concurrent write on another connection, replicated ahead of this one).
//! Every replica must reach the SAME verdict on a resolution that no longer
//! matches state, or replicas diverge. So every mutation's `precondition` is
//! checked BEFORE the first mutation runs; if any fails, nothing is mutated
//! and the caller gets `ErrorCode::OllpRetryRequired` — the same retry
//! contract the columnar resolved-row apply uses. The check runs
//! unconditionally on every replica, each against its own committed log
//! prefix, so leader and followers agree.

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
            let current =
                self.kv_engine
                    .get(did, tid, mutation.collection(), mutation.key(), now_ms);
            if current.as_deref() != mutation.precondition() {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
            // A TTL mutation on a key that was absent at resolve time and is
            // still absent has nothing to expire or persist — the same
            // `NotFound` `execute_kv_expire` / `execute_kv_persist` return.
            // Raised here, before any mutation, so the statement stays
            // all-or-nothing.
            if current.is_none()
                && matches!(
                    mutation,
                    KvResolvedMutation::Expire { .. } | KvResolvedMutation::Persist { .. }
                )
            {
                return self.response_error(task, ErrorCode::NotFound);
            }
            // The gate stays on every write path even though
            // `DecidedEarlierInRequest` makes this a no-op — a single path
            // that skips it entirely is a hole future callers can fall into.
            if let KvResolvedMutation::Put {
                collection,
                key,
                value,
                ..
            } = mutation
                && let Err(error) = admit_kv_row(rls_write_check, value, key, tid, collection)
            {
                return self.response_error(task, error);
            }
        }

        for mutation in mutations {
            self.apply_kv_resolved_mutation(task, did, tid, mutation, now_ms);
        }

        self.response_with_payload(task, response_payload.to_vec())
    }

    /// Apply one already-checked mutation and emit its change event.
    ///
    /// `old` on the event is the mutation's precondition — the exact image the
    /// resolve read and the drift check just confirmed is still stored.
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
                        collection,
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
                    collection,
                    op,
                    &key_str,
                    Some(value),
                    precondition.as_deref(),
                );
                self.note_kv_write_lsn(task, did, tid, collection, key);
            }
            KvResolvedMutation::Delete {
                collection,
                key,
                precondition,
            } => {
                self.kv_engine
                    .delete(did, tid, collection, &[key.to_vec()], now_ms);
                if let Some(ref m) = self.metrics {
                    m.record_kv_delete();
                }
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Delete,
                    &key_str,
                    None,
                    precondition.as_deref(),
                );
                self.note_kv_write_lsn(task, did, tid, collection, key);
            }
            // A TTL mutation leaves the body untouched, so there is no new
            // image to publish — the live `EXPIRE` / `PERSIST` handlers emit
            // no change event either.
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
                    collection,
                    key,
                    resolved_now_ms.saturating_add(*ttl_ms),
                );
                self.note_kv_write_lsn(task, did, tid, collection, key);
            }
            KvResolvedMutation::Persist {
                collection,
                key,
                precondition: _,
            } => {
                self.kv_engine.persist(did, tid, collection, key);
                self.note_kv_write_lsn(task, did, tid, collection, key);
            }
        }
    }
}
