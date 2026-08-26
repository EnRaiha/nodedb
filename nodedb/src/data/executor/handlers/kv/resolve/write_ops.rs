// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the KV writes whose image comes from a merge or from the
//! stored row itself: `InsertOnConflictUpdate`, `Delete`, `Expire`,
//! `Persist`, `FieldSet`.
//!
//! Each one reads what its live handler reads, computes the post-image by
//! calling the same function the live handler calls, and decides the write
//! policy with the same gate call. Re-deriving any of that here is exactly
//! the drift this protocol exists to prevent.

use nodedb_physical::physical_plan::{KvResolveOutcome, KvResolvedMutation};

use super::context::{
    ResolveResult, ResolvedPut, delete_mutation, expiry_from_ttl, one, put_mutation,
};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::atomic::KvAtomicCtx;
use crate::data::executor::handlers::kv::crud::KvInsertOnConflictUpdateParams;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::handlers::kv::ttl::KvTtlTarget;
use crate::data::executor::handlers::returning_rows::kv_stored_rows_payload;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    /// Resolve `INSERT ... ON CONFLICT (key) DO UPDATE SET ...`.
    ///
    /// Mirrors `execute_kv_insert_on_conflict_update`: the merge runs through
    /// `apply_on_conflict_updates`, and the gate decides whichever body would
    /// actually be persisted — the incoming row when the key is absent, the
    /// merge when it is present.
    pub(super) fn resolve_kv_insert_on_conflict_update(
        &self,
        params: KvInsertOnConflictUpdateParams<'_>,
        task: &ExecutionTask,
    ) -> ResolveResult {
        let KvInsertOnConflictUpdateParams {
            did,
            tid,
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
            rls_write_check,
            returning,
            rls_filters,
        } = params;

        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::Internal {
                detail: "KV memory budget exceeded, retry later".into(),
            });
        }

        let now_ms = self.kv_ttl_now_ms(task);
        let existing_bytes = self.kv_resolve_read(did, tid, collection, key, now_ms);

        let stored_bytes: Vec<u8> = match &existing_bytes {
            None => value.to_vec(),
            Some(existing_raw) => {
                let existing_val =
                    nodedb_types::value_from_msgpack(existing_raw).map_err(|_| {
                        ErrorCode::Internal {
                            detail: "failed to decode existing KV value for ON CONFLICT \
                                     DO UPDATE"
                                .into(),
                        }
                    })?;
                let excluded_val =
                    nodedb_types::value_from_msgpack(value).map_err(|_| ErrorCode::Internal {
                        detail: "failed to decode incoming KV value for ON CONFLICT DO UPDATE"
                            .into(),
                    })?;
                let merged = crate::data::executor::handlers::upsert::apply_on_conflict_updates(
                    existing_val,
                    &excluded_val,
                    updates,
                )?;
                nodedb_types::value_to_msgpack(&merged).map_err(|_| ErrorCode::Internal {
                    detail: "failed to encode merged KV value".into(),
                })?
            }
        };

        admit_kv_row(rls_write_check, &stored_bytes, key, tid, collection)?;

        let response_payload = match returning {
            Some(spec) => kv_stored_rows_payload(spec, rls_filters, &[(key, &stored_bytes)])?,
            None => Vec::new(),
        };

        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: stored_bytes,
                ttl_ms,
                expire_at_ms: expiry_from_ttl(ttl_ms, now_ms),
                surrogate,
                precondition: existing_bytes,
            }),
            response_payload,
        ))
    }

    /// Resolve a KV `DELETE`.
    ///
    /// An absent key removes no row, so it contributes no mutation and is
    /// counted as not-deleted — exactly what `execute_kv_delete` does. The
    /// resolved count is therefore the number of keys that were present.
    pub(super) fn resolve_kv_delete(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        keys: &[Vec<u8>],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> ResolveResult {
        let now_ms = current_ms();
        let mut mutations = Vec::new();
        for key in keys {
            let Some(body) = self.kv_resolve_read(did, tid, collection, key, now_ms) else {
                continue;
            };
            admit_kv_row(rls_write_check, &body, key, tid, collection)?;
            mutations.push(delete_mutation(collection, key, Some(body)));
        }

        let response_payload = response_codec::encode_count("deleted", mutations.len())?;
        Ok(KvResolveOutcome {
            mutations,
            response_payload,
        })
    }

    /// Resolve `EXPIRE`.
    ///
    /// The body does not change, so the stored row is both the pre- and the
    /// post-image and the gate decides it here, exactly as
    /// `execute_kv_expire` does. An absent key ships a mutation with
    /// `precondition: None`; the apply reports `NotFound` when it is still
    /// absent, matching today's reply.
    pub(super) fn resolve_kv_expire(
        &self,
        target: KvTtlTarget<'_>,
        ttl_ms: u64,
        task: &ExecutionTask,
    ) -> ResolveResult {
        let now_ms = self.kv_ttl_now_ms(task);
        let precondition = self.resolve_kv_ttl_precondition(&target, now_ms)?;
        Ok(one(
            KvResolvedMutation::Expire {
                collection: target.collection.to_owned(),
                key: target.key.to_vec(),
                ttl_ms,
                resolved_now_ms: now_ms,
                precondition,
            },
            Vec::new(),
        ))
    }

    /// Resolve `PERSIST`. See [`CoreLoop::resolve_kv_expire`] — same image,
    /// and the same clock `execute_kv_persist` reads for its policy check.
    pub(super) fn resolve_kv_persist(&self, target: KvTtlTarget<'_>) -> ResolveResult {
        let precondition = self.resolve_kv_ttl_precondition(&target, current_ms())?;
        Ok(one(
            KvResolvedMutation::Persist {
                collection: target.collection.to_owned(),
                key: target.key.to_vec(),
                precondition,
            },
            Vec::new(),
        ))
    }

    /// Read a TTL mutation's target row and decide it against the policy.
    ///
    /// Unlike `admit_kv_ttl_target`, the row is read even when the policy
    /// admits everything: the read is what pins the drift precondition, not
    /// only what feeds the gate.
    fn resolve_kv_ttl_precondition(
        &self,
        target: &KvTtlTarget<'_>,
        now_ms: u64,
    ) -> Result<Option<Vec<u8>>, ErrorCode> {
        let KvTtlTarget {
            did,
            tid,
            collection,
            key,
            rls_write_check,
        } = *target;
        let body = self.kv_resolve_read(did, tid, collection, key, now_ms);
        if let Some(bytes) = &body {
            admit_kv_row(rls_write_check, bytes, key, tid, collection)?;
        }
        Ok(body)
    }

    /// Resolve `FieldSet` (HSET-style field merge).
    ///
    /// The merge runs through `field_compute::merge_field_updates`, the same
    /// function `execute_kv_field_set` calls, and the merged body is the image
    /// the gate decides.
    pub(super) fn resolve_kv_field_set(
        &self,
        ctx: KvAtomicCtx<'_>,
        updates: &[(String, Vec<u8>)],
    ) -> ResolveResult {
        let KvAtomicCtx {
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
            ..
        } = ctx;
        let now_ms = current_ms();
        let current = self.kv_resolve_read(did, tid, collection, key, now_ms);
        let computed = crate::data::executor::handlers::kv::field_compute::merge_field_updates(
            current.as_deref(),
            updates,
        )?;
        admit_kv_row(rls_write_check, &computed.new_value, key, tid, collection)?;

        let response_payload = response_codec::encode_json_as_msgpack(
            &serde_json::json!({ "fields_added": computed.fields_added }),
        )?;
        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: computed.new_value,
                // `execute_kv_field_set` puts with `ttl_ms: 0`, which clears
                // any TTL the key held. Preserved verbatim here.
                ttl_ms: 0,
                expire_at_ms: 0,
                surrogate,
                precondition: current,
            }),
            response_payload,
        ))
    }
}
