// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `KvOp::ResolveWrite`.
//!
//! Reports what the wrapped write would apply, and what it would reply, while
//! mutating nothing. The Control Plane runs this before proposing a governed
//! state-dependent KV write through Raft: a follower has no writing identity
//! to evaluate the write predicate against, and a KV write whose image is
//! computed from the stored row cannot be re-derived after commit without
//! risking a different answer on every replica.
//!
//! Read-only, so this takes `&self`. The arms below are exactly the KV
//! ops whose stored image depends on state the Control Plane cannot see; every
//! other op either has no such dependence or is not a write.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::atomic::KvAtomicCtx;
use crate::data::executor::handlers::kv::crud::KvInsertOnConflictUpdateParams;
use crate::data::executor::handlers::kv::transfer::{TransferItemParams, TransferParams};
use crate::data::executor::handlers::kv::ttl::KvTtlTarget;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::KvOp;

impl CoreLoop {
    /// Handle `KvOp::ResolveWrite`: resolve `inner` against current state,
    /// decide every policy that governs it, and report the mutation list plus
    /// the reply. Mutates nothing.
    pub(in crate::data::executor) fn execute_kv_resolve_write(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        inner: &KvOp,
    ) -> Response {
        debug!(core = self.core_id, "kv resolve write");

        let outcome = match inner {
            KvOp::InsertOnConflictUpdate {
                collection,
                key,
                value,
                ttl_ms,
                updates,
                surrogate,
                rls_write_check,
                returning,
                rls_filters,
            } => self.resolve_kv_insert_on_conflict_update(
                KvInsertOnConflictUpdateParams {
                    did,
                    tid,
                    collection,
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    updates,
                    surrogate: *surrogate,
                    rls_write_check,
                    returning: returning.as_ref(),
                    rls_filters,
                },
                task,
            ),
            KvOp::Delete {
                collection,
                keys,
                rls_write_check,
            } => self.resolve_kv_delete(did, tid, collection, keys, rls_write_check),
            KvOp::Expire {
                collection,
                key,
                ttl_ms,
                rls_write_check,
            } => self.resolve_kv_expire(
                KvTtlTarget {
                    did,
                    tid,
                    collection,
                    key,
                    rls_write_check,
                },
                *ttl_ms,
                task,
            ),
            KvOp::Persist {
                collection,
                key,
                rls_write_check,
            } => self.resolve_kv_persist(KvTtlTarget {
                did,
                tid,
                collection,
                key,
                rls_write_check,
            }),
            KvOp::FieldSet {
                collection,
                key,
                updates,
                surrogate,
                rls_write_check,
            } => self.resolve_kv_field_set(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                updates,
            ),
            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
                surrogate,
                rls_write_check,
            } => self.resolve_kv_incr(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                *delta,
                *ttl_ms,
            ),
            KvOp::IncrFloat {
                collection,
                key,
                delta,
                surrogate,
                rls_write_check,
            } => self.resolve_kv_incr_float(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                *delta,
            ),
            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
                surrogate,
                rls_write_check,
            } => self.resolve_kv_cas(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                expected,
                new_value,
            ),
            KvOp::GetSet {
                collection,
                key,
                new_value,
                surrogate,
                rls_filters,
                rls_write_check,
            } => self.resolve_kv_getset(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                new_value,
                rls_filters,
            ),
            KvOp::Transfer {
                collection,
                source_key,
                dest_key,
                field,
                amount,
                debit_surrogate,
                credit_surrogate,
                rls_write_check,
            } => self.resolve_kv_transfer(TransferParams {
                did,
                tid,
                collection,
                source_key,
                dest_key,
                field,
                amount: *amount,
                debit_surrogate: *debit_surrogate,
                credit_surrogate: *credit_surrogate,
                rls_write_check,
            }),
            KvOp::TransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                surrogate,
                source_rls_write_check,
                dest_rls_write_check,
            } => self.resolve_kv_transfer_item(TransferItemParams {
                did,
                tid,
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                surrogate: *surrogate,
                source_rls_write_check,
                dest_rls_write_check,
            }),
            KvOp::PredicateUpdate {
                collection,
                filters,
                updates,
                rls_write_check,
            } => self.resolve_kv_predicate_update(
                did,
                tid,
                collection,
                filters,
                updates,
                rls_write_check,
            ),
            KvOp::PredicateDelete {
                collection,
                filters,
                rls_write_check,
            } => self.resolve_kv_predicate_delete(did, tid, collection, filters, rls_write_check),
            // Every other KV op either writes an image the Control Plane
            // already holds (a plain `Put`), writes nothing, or is itself a
            // resolve/resolved op. None of them is resolvable, and none is
            // ever wrapped: `resolver_for_plan` selects the ops above.
            other => Err(ErrorCode::Internal {
                detail: format!(
                    "kv resolve-write wraps an op with no state-dependent image: {}",
                    kv_op_name(other)
                ),
            }),
        };

        match outcome {
            Ok(resolved) => match response_codec::encode(&resolved) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, e),
            },
            Err(code) => self.response_error(task, code),
        }
    }
}

/// Variant name for the refusal above, so the message names the op without
/// putting a whole plan's bytes into an error string.
fn kv_op_name(op: &KvOp) -> &'static str {
    match op {
        KvOp::Get { .. } => "Get",
        KvOp::Put { .. } => "Put",
        KvOp::Insert { .. } => "Insert",
        KvOp::InsertIfAbsent { .. } => "InsertIfAbsent",
        KvOp::InsertOnConflictUpdate { .. } => "InsertOnConflictUpdate",
        KvOp::Delete { .. } => "Delete",
        KvOp::Scan { .. } => "Scan",
        KvOp::Expire { .. } => "Expire",
        KvOp::Persist { .. } => "Persist",
        KvOp::GetTtl { .. } => "GetTtl",
        KvOp::BatchGet { .. } => "BatchGet",
        KvOp::BatchPut { .. } => "BatchPut",
        KvOp::RegisterIndex { .. } => "RegisterIndex",
        KvOp::DropIndex { .. } => "DropIndex",
        KvOp::FieldGet { .. } => "FieldGet",
        KvOp::FieldSet { .. } => "FieldSet",
        KvOp::Truncate { .. } => "Truncate",
        KvOp::Incr { .. } => "Incr",
        KvOp::IncrFloat { .. } => "IncrFloat",
        KvOp::Cas { .. } => "Cas",
        KvOp::GetSet { .. } => "GetSet",
        KvOp::Transfer { .. } => "Transfer",
        KvOp::TransferItem { .. } => "TransferItem",
        KvOp::RegisterSortedIndex { .. } => "RegisterSortedIndex",
        KvOp::DropSortedIndex { .. } => "DropSortedIndex",
        KvOp::SortedIndexRank { .. } => "SortedIndexRank",
        KvOp::SortedIndexTopK { .. } => "SortedIndexTopK",
        KvOp::SortedIndexRange { .. } => "SortedIndexRange",
        KvOp::SortedIndexCount { .. } => "SortedIndexCount",
        KvOp::SortedIndexScore { .. } => "SortedIndexScore",
        KvOp::MaterializeScan { .. } => "MaterializeScan",
        KvOp::ResolveWrite(_) => "ResolveWrite",
        KvOp::ResolvedWrite { .. } => "ResolvedWrite",
        KvOp::PredicateUpdate { .. } => "PredicateUpdate",
        KvOp::PredicateDelete { .. } => "PredicateDelete",
    }
}
