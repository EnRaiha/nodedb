// SPDX-License-Identifier: BUSL-1.1

//! KV TTL (`Expire`/`Persist`) and sorted-index DDL (`RegisterSortedIndex`/
//! `DropSortedIndex`) execution for transaction batches.
//!
//! Split out of `sub_plan_kv_ops.rs` (the main `KvOp` dispatcher) once these
//! four arms pushed that file over this crate's per-file line budget. Each
//! handler here captures the prior state needed for `rollback_undo_log`
//! before delegating to the same live-path handler autocommit uses
//! (`execute_kv_expire` / `execute_kv_persist` / `sorted.rs`'s register/drop),
//! so a COMMIT-time replay and a live autocommit statement always produce the
//! identical result.

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::sorted::KvRegisterSortedIndexParams;
use crate::data::executor::handlers::kv::ttl::KvTtlTarget;
use crate::data::executor::task::ExecutionTask;

use super::undo::UndoEntry;

/// Parameters for [`CoreLoop::execute_tx_kv_register_sorted_index`], bundled
/// so the method stays under the `too_many_arguments` clippy threshold.
pub(super) struct TxRegisterSortedIndexParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub index_name: &'a str,
    pub sort_columns: &'a [(String, String)],
    pub key_column: &'a str,
    pub window_type: &'a str,
    pub window_timestamp_column: &'a str,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
}

impl CoreLoop {
    // ── TTL: Expire / Persist ────────────────────────────────────────────────

    /// Execute `EXPIRE` in a transaction context.
    ///
    /// Captures the key's prior TTL metadata (`has_ttl` + `expire_at_ms`)
    /// before the write so a later sibling sub-plan's failure can restore the
    /// exact prior instant via `UndoEntry::KvTtl`. The absolute expiry instant
    /// itself is resolved by `execute_kv_expire` via `kv_ttl_now_ms`, so a
    /// COMMIT-time replay (where `task.resolved_now_ms()` is absent) installs
    /// an instant resolved AT COMMIT rather than at original statement time --
    /// the correct semantics for when the write becomes visible, but one that
    /// can differ from what a same-transaction `GET_TTL` observed against the
    /// staging overlay before COMMIT.
    ///
    /// `target` is the same bundle the autocommit handler takes and is handed
    /// straight through, so the row this addresses and the row the write policy
    /// decides cannot drift apart. `undo_log` stays a separate trailing `&mut`
    /// parameter -- bundling a mutable borrow alongside borrowed fields of the
    /// same struct fights the borrow checker at the call site for no benefit.
    pub(super) fn execute_tx_kv_expire(
        &mut self,
        task: &ExecutionTask,
        target: KvTtlTarget<'_>,
        ttl_ms: u64,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let KvTtlTarget {
            did,
            tid,
            collection,
            key,
            ..
        } = target;
        let prior_meta = self.kv_engine.get_ttl_meta(did, tid, collection, key);
        let resp = self.execute_kv_expire(task, target, ttl_ms);
        if resp.status == Status::Error {
            // Mirrors the live handler: EXPIRE on an absent key returns
            // `ErrorCode::NotFound` (see `handlers/kv/ttl.rs`), not a
            // synthesized default.
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "kv expire failed".into(),
            }));
        }
        let prior_expiry = prior_meta.and_then(|m| m.has_ttl.then_some(m.expire_at_ms));
        undo_log.push(UndoEntry::KvTtl {
            collection: collection.to_string(),
            key: key.to_vec(),
            prior_expiry,
        });
        Ok(resp)
    }

    /// Execute `PERSIST` in a transaction context. See `execute_tx_kv_expire`
    /// for the undo-capture shape; `Persist` resolves no instant of its own
    /// (`KvEngine::persist` takes no `now_ms`), so there is no COMMIT-vs-
    /// statement-time divergence to note here.
    pub(super) fn execute_tx_kv_persist(
        &mut self,
        task: &ExecutionTask,
        target: KvTtlTarget<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let KvTtlTarget {
            did,
            tid,
            collection,
            key,
            ..
        } = target;
        let prior_meta = self.kv_engine.get_ttl_meta(did, tid, collection, key);
        let resp = self.execute_kv_persist(task, target);
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "kv persist failed".into(),
            }));
        }
        let prior_expiry = prior_meta.and_then(|m| m.has_ttl.then_some(m.expire_at_ms));
        undo_log.push(UndoEntry::KvTtl {
            collection: collection.to_string(),
            key: key.to_vec(),
            prior_expiry,
        });
        Ok(resp)
    }

    // ── Sorted index DDL: Register / Drop ───────────────────────────────────

    /// Execute `RegisterSortedIndex` in a transaction context.
    ///
    /// Captures whether an index already existed under this name (and its
    /// definition, if so) before registering, via the shared pure builder
    /// `build_sorted_index_def` (through `execute_kv_register_sorted_index`,
    /// the same path autocommit uses) -- never a hand-rolled `SortedIndexDef`.
    pub(super) fn execute_tx_kv_register_sorted_index(
        &mut self,
        task: &ExecutionTask,
        params: TxRegisterSortedIndexParams<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxRegisterSortedIndexParams {
            did,
            tid,
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } = params;
        let prior_def = self
            .kv_engine
            .sorted_index_def(did, tid, index_name)
            .cloned();
        let resp = self.execute_kv_register_sorted_index(
            task,
            KvRegisterSortedIndexParams {
                did,
                tid,
                collection,
                index_name,
                sort_columns,
                key_column,
                window_type,
                window_timestamp_column,
                window_start_ms,
                window_end_ms,
            },
        );
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "kv register sorted index failed".into(),
            }));
        }
        undo_log.push(UndoEntry::SortedIndexDdl {
            database_id: did,
            tenant_id: tid,
            index_name: index_name.to_string(),
            prior_def,
        });
        Ok(resp)
    }

    /// Execute `DropSortedIndex` in a transaction context.
    ///
    /// Captures the dropped index's definition so a later sibling sub-plan's
    /// failure can restore it -- `rollback_undo_log` re-registers it, which
    /// rebuilds the order-statistic tree by backfilling from the KV
    /// collection's CURRENT contents at undo time (see `UndoEntry::
    /// SortedIndexDdl` doc comment for why this is correct regardless of
    /// undo-log ordering).
    pub(super) fn execute_tx_kv_drop_sorted_index(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        index_name: &str,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let prior_def = self
            .kv_engine
            .sorted_index_def(did, tid, index_name)
            .cloned();
        let resp = self.execute_kv_drop_sorted_index(task, did, tid, index_name);
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "kv drop sorted index failed".into(),
            }));
        }
        undo_log.push(UndoEntry::SortedIndexDdl {
            database_id: did,
            tenant_id: tid,
            index_name: index_name.to_string(),
            prior_def,
        });
        Ok(resp)
    }
}
