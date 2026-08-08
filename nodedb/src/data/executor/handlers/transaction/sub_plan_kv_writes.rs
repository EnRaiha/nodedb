// SPDX-License-Identifier: BUSL-1.1

//! KV read-modify-write op execution for transaction batches.
//!
//! Split out of `sub_plan_kv_ops.rs` (the main `KvOp` dispatcher) once the
//! TTL and sorted-index-DDL arms pushed that file over this crate's per-file
//! line budget. Each handler here captures the prior value(s) before the
//! write so a later sibling sub-plan's failure can restore them via
//! `rollback_undo_log`.

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use nodedb_physical::physical_plan::KvOp;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a KV read-modify-write operation in a transaction context.
    ///
    /// Only called for the `KvOp` write variants that capture-then-execute
    /// (see `sub_plan_kv_ops::execute_tx_kv`'s dispatch arm); every other
    /// variant is handled directly there.
    pub(super) fn execute_tx_kv_write(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        match op {
            KvOp::Put {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                ..
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(did, tid, collection, key, now_ms);
                let resp = self.execute_kv_put(
                    task,
                    crate::data::executor::handlers::kv::crud::KvWriteParams {
                        did,
                        tid,
                        collection,
                        key,
                        value,
                        ttl_ms: *ttl_ms,
                        surrogate: *surrogate,
                        returning: None,
                        rls_filters: &[],
                    },
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv put failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::Insert {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                ..
            } => {
                let resp = self.execute_kv_insert(
                    task,
                    crate::data::executor::handlers::kv::crud::KvWriteParams {
                        did,
                        tid,
                        collection,
                        key,
                        value,
                        ttl_ms: *ttl_ms,
                        surrogate: *surrogate,
                        returning: None,
                        rls_filters: &[],
                    },
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv insert failed".into(),
                    }));
                }
                // Insert only succeeds when key was absent; prior_value is None.
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: None,
                });
                Ok(resp)
            }

            KvOp::InsertIfAbsent {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                ..
            } => {
                let now_ms = current_ms();
                let was_absent = self
                    .kv_engine
                    .get(did, tid, collection, key, now_ms)
                    .is_none();
                let resp = self.execute_kv_insert_if_absent(
                    task,
                    crate::data::executor::handlers::kv::crud::KvWriteParams {
                        did,
                        tid,
                        collection,
                        key,
                        value,
                        ttl_ms: *ttl_ms,
                        surrogate: *surrogate,
                        returning: None,
                        rls_filters: &[],
                    },
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv insert-if-absent failed".into(),
                    }));
                }
                // Only push undo if the key was actually written (was absent).
                if was_absent {
                    undo_log.push(UndoEntry::KvPut {
                        collection: collection.clone(),
                        key: key.clone(),
                        prior_value: None,
                    });
                }
                Ok(resp)
            }

            KvOp::InsertOnConflictUpdate {
                collection, key, ..
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(did, tid, collection, key, now_ms);
                let resp = self.execute_kv(task, did, tid, op);
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv insert-on-conflict-update failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::Delete {
                collection,
                keys,
                rls_write_check,
            } => {
                let now_ms = current_ms();
                // Capture prior values for all keys that exist before deleting.
                let priors: Vec<(Vec<u8>, Vec<u8>)> = keys
                    .iter()
                    .filter_map(|k| {
                        let v = self.kv_engine.get(did, tid, collection, k, now_ms)?;
                        Some((k.clone(), v))
                    })
                    .collect();
                let resp =
                    self.execute_kv_delete(task, did, tid, collection, keys, rls_write_check);
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv delete failed".into(),
                    }));
                }
                for (key, prior_value) in priors {
                    undo_log.push(UndoEntry::KvDelete {
                        collection: collection.clone(),
                        key,
                        prior_value,
                    });
                }
                Ok(resp)
            }

            KvOp::BatchPut {
                collection,
                entries,
                ttl_ms,
                surrogates,
                ..
            } => {
                let now_ms = current_ms();
                let prior_entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries
                    .iter()
                    .map(|(k, _v)| {
                        let prior = self.kv_engine.get(did, tid, collection, k, now_ms);
                        (k.clone(), prior)
                    })
                    .collect();
                let resp = self.execute_kv_batch_put(
                    task,
                    crate::data::executor::handlers::kv::batch::KvBatchPutArgs {
                        did,
                        tid,
                        collection,
                        entries,
                        ttl_ms: *ttl_ms,
                        surrogates,
                        returning: None,
                        rls_filters: &[],
                    },
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv batch put failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvBatchPut {
                    collection: collection.clone(),
                    entries: prior_entries,
                });
                Ok(resp)
            }

            KvOp::FieldSet {
                collection,
                key,
                updates,
                surrogate,
                rls_write_check,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(did, tid, collection, key, now_ms);
                let resp = self.execute_kv_field_set(
                    crate::data::executor::handlers::kv::atomic::KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection,
                        key,
                        surrogate: *surrogate,
                        rls_write_check,
                    },
                    updates,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv field set failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            // The four read-modify-write atomics capture and restore their
            // prior value identically, so they share one handler in the
            // sibling `sub_plan_kv_atomics.rs`.
            KvOp::Incr { .. } | KvOp::IncrFloat { .. } | KvOp::Cas { .. } | KvOp::GetSet { .. } => {
                self.execute_tx_kv_atomic(task, did, tid, op, undo_log)
            }

            KvOp::Transfer {
                collection,
                source_key,
                dest_key,
                ..
            } => {
                let now_ms = current_ms();
                let source_prior = self.kv_engine.get(did, tid, collection, source_key, now_ms);
                let dest_prior = self.kv_engine.get(did, tid, collection, dest_key, now_ms);
                let resp = self.execute_kv(task, did, tid, op);
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv transfer failed".into(),
                    }));
                }
                let Some(source_bytes) = source_prior else {
                    // Transfer requires source to exist; it would have failed above.
                    return Err(ErrorCode::Internal {
                        detail: "kv transfer: source prior missing after success".into(),
                    });
                };
                undo_log.push(UndoEntry::KvTransfer {
                    collection: collection.clone(),
                    source_key: source_key.clone(),
                    source_prior: source_bytes,
                    dest_key: dest_key.clone(),
                    dest_prior,
                });
                Ok(resp)
            }

            KvOp::TransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                surrogate,
                source_rls_write_check,
                dest_rls_write_check,
            } => {
                let now_ms = current_ms();
                let source_prior =
                    self.kv_engine
                        .get(did, tid, source_collection, item_key, now_ms);
                let dest_prior = self
                    .kv_engine
                    .get(did, tid, dest_collection, dest_key, now_ms);
                let resp = self.execute_kv_transfer_item(
                    task,
                    crate::data::executor::handlers::kv::transfer::TransferItemParams {
                        did,
                        tid,
                        source_collection,
                        dest_collection,
                        item_key,
                        dest_key,
                        surrogate: *surrogate,
                        source_rls_write_check,
                        dest_rls_write_check,
                    },
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv transfer-item failed".into(),
                    }));
                }
                let Some(source_bytes) = source_prior else {
                    return Err(ErrorCode::Internal {
                        detail: "kv transfer-item: source prior missing after success".into(),
                    });
                };
                undo_log.push(UndoEntry::KvTransferItem {
                    source_collection: source_collection.clone(),
                    dest_collection: dest_collection.clone(),
                    item_key: item_key.clone(),
                    dest_key: dest_key.clone(),
                    source_prior: source_bytes,
                    dest_prior,
                });
                Ok(resp)
            }

            // Every non-write-RMW variant is handled directly by
            // `sub_plan_kv_ops::execute_tx_kv` before dispatching here.
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
            | KvOp::SortedIndexScore { .. }
            | KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::Truncate { .. }
            | KvOp::Expire { .. }
            | KvOp::Persist { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. } => Err(ErrorCode::Internal {
                detail: "execute_tx_kv_write called with a non-write-RMW KvOp variant".into(),
            }),
        }
    }
}
