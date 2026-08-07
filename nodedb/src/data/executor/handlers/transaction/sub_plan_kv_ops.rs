// SPDX-License-Identifier: BUSL-1.1

//! KV operation dispatch for transaction batches.

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::KvOp;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a KV operation in a transaction context.
    ///
    /// Write operations (including TTL `Expire`/`Persist` and sorted-index
    /// `RegisterSortedIndex`/`DropSortedIndex` DDL) capture prior state before
    /// executing and push an `UndoEntry`. Read-only operations execute
    /// without undo tracking. Secondary-index DDL and `Truncate` are
    /// rejected — see the reject arm below for why.
    pub(super) fn execute_tx_kv(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &KvOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let did = task.request.database_id.as_u64();
        match op {
            // ── Read-only KV ops — no undo needed ───────────────────────────
            KvOp::Get { .. }
            | KvOp::Scan { .. }
            | KvOp::MaterializeScan { .. }
            | KvOp::BatchGet { .. }
            | KvOp::GetTtl { .. }
            | KvOp::FieldGet { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. } => {
                let resp = self.execute_kv(task, did, tid, op);
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv read failed".into(),
                    }));
                }
                Ok(resp)
            }

            // ── DDL — reject inside TransactionBatch ─────────────────────────
            //
            // `plan_requires_txn_buffering`
            // (`control/server/shared/write_admission/predicate/txn_buffering.rs`)
            // classifies these three `false` (write-but-unbuffered): a
            // client statement never buffers them, so they never replay
            // through this arm at COMMIT via the `BEGIN ... COMMIT` path.
            // This arm is a defensive guard for a hypothetical direct-dispatch
            // route into `execute_tx_kv`, not dead code reachable today.
            KvOp::RegisterIndex { .. } | KvOp::DropIndex { .. } | KvOp::Truncate { .. } => {
                Err(ErrorCode::Internal {
                    detail: "KV secondary-index / truncate DDL is not permitted inside a \
                             TransactionBatch"
                        .into(),
                })
            }

            // ── TTL ops — capture prior expiry, execute, push undo ───────────
            KvOp::Expire {
                collection,
                key,
                ttl_ms,
                rls_write_check,
            } => self.execute_tx_kv_expire(
                task,
                crate::data::executor::handlers::kv::ttl::KvTtlTarget {
                    did,
                    tid,
                    collection,
                    key,
                    rls_write_check,
                },
                *ttl_ms,
                undo_log,
            ),

            KvOp::Persist {
                collection,
                key,
                rls_write_check,
            } => self.execute_tx_kv_persist(
                task,
                crate::data::executor::handlers::kv::ttl::KvTtlTarget {
                    did,
                    tid,
                    collection,
                    key,
                    rls_write_check,
                },
                undo_log,
            ),

            // ── Sorted-index DDL — capture prior def, execute, push undo ─────
            KvOp::RegisterSortedIndex {
                collection,
                index_name,
                sort_columns,
                key_column,
                window_type,
                window_timestamp_column,
                window_start_ms,
                window_end_ms,
            } => self.execute_tx_kv_register_sorted_index(
                task,
                super::sub_plan_kv_ttl_sorted::TxRegisterSortedIndexParams {
                    did,
                    tid,
                    collection,
                    index_name,
                    sort_columns,
                    key_column,
                    window_type,
                    window_timestamp_column,
                    window_start_ms: *window_start_ms,
                    window_end_ms: *window_end_ms,
                },
                undo_log,
            ),

            KvOp::DropSortedIndex { index_name } => {
                self.execute_tx_kv_drop_sorted_index(task, did, tid, index_name, undo_log)
            }

            // ── Write ops — delegated (capture prior value, execute, push
            // undo) to `sub_plan_kv_writes::execute_tx_kv_write`, moved out
            // once this file crossed the per-file line budget.
            KvOp::Put { .. }
            | KvOp::Insert { .. }
            | KvOp::InsertIfAbsent { .. }
            | KvOp::InsertOnConflictUpdate { .. }
            | KvOp::Delete { .. }
            | KvOp::BatchPut { .. }
            | KvOp::FieldSet { .. }
            | KvOp::Incr { .. }
            | KvOp::IncrFloat { .. }
            | KvOp::Cas { .. }
            | KvOp::GetSet { .. }
            | KvOp::Transfer { .. }
            | KvOp::TransferItem { .. } => self.execute_tx_kv_write(task, did, tid, op, undo_log),
        }
    }
}
