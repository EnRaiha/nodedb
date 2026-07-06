// SPDX-License-Identifier: BUSL-1.1

//! KV operation dispatch for transaction batches.

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use nodedb_physical::physical_plan::KvOp;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a KV operation in a transaction context.
    ///
    /// Write operations capture prior state before executing and push an
    /// `UndoEntry`. Read-only operations execute without undo tracking.
    /// DDL/TTL operations are rejected — they do not belong in a multi-engine
    /// `TransactionBatch`.
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
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv read failed".into(),
                    }));
                }
                Ok(resp)
            }

            // ── DDL / TTL ops — reject inside TransactionBatch ───────────────
            KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::Truncate { .. }
            | KvOp::Expire { .. }
            | KvOp::Persist { .. } => Err(ErrorCode::Internal {
                detail: "KV DDL / TTL operations are not permitted inside a TransactionBatch"
                    .into(),
            }),

            // ── Write ops — capture prior value, execute, push undo ──────────
            KvOp::Put {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(did, tid, collection, key, now_ms);
                let resp = self
                    .execute_kv_put(task, did, tid, collection, key, value, *ttl_ms, *surrogate);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
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
            } => {
                let resp = self
                    .execute_kv_insert(task, did, tid, collection, key, value, *ttl_ms, *surrogate);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
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
            } => {
                let now_ms = current_ms();
                let was_absent = self
                    .kv_engine
                    .get(did, tid, collection, key, now_ms)
                    .is_none();
                let resp = self.execute_kv_insert_if_absent(
                    task, did, tid, collection, key, value, *ttl_ms, *surrogate,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
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
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
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

            KvOp::Delete { collection, keys } => {
                let now_ms = current_ms();
                // Capture prior values for all keys that exist before deleting.
                let priors: Vec<(Vec<u8>, Vec<u8>)> = keys
                    .iter()
                    .filter_map(|k| {
                        let v = self.kv_engine.get(did, tid, collection, k, now_ms)?;
                        Some((k.clone(), v))
                    })
                    .collect();
                let resp = self.execute_kv_delete(task, did, tid, collection, keys);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
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
                    },
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
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
                    },
                    updates,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
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

            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
                surrogate,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(did, tid, collection, key, now_ms);
                let resp = self.execute_kv_incr(
                    crate::data::executor::handlers::kv::atomic::KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection,
                        key,
                        surrogate: *surrogate,
                    },
                    *delta,
                    *ttl_ms,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv incr failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::IncrFloat {
                collection,
                key,
                delta,
                surrogate,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(did, tid, collection, key, now_ms);
                let resp = self.execute_kv_incr_float(
                    crate::data::executor::handlers::kv::atomic::KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection,
                        key,
                        surrogate: *surrogate,
                    },
                    *delta,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv incr float failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
                surrogate,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(did, tid, collection, key, now_ms);
                let resp = self.execute_kv_cas(
                    crate::data::executor::handlers::kv::atomic::KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection,
                        key,
                        surrogate: *surrogate,
                    },
                    expected,
                    new_value,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv cas failed".into(),
                    }));
                }
                // CAS only mutates on success (which we verified above).
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::GetSet {
                collection,
                key,
                new_value,
                surrogate,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(did, tid, collection, key, now_ms);
                let resp = self.execute_kv_getset(
                    crate::data::executor::handlers::kv::atomic::KvAtomicCtx {
                        task,
                        did,
                        tid,
                        collection,
                        key,
                        surrogate: *surrogate,
                    },
                    new_value,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv get-set failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
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
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
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
                    },
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
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
        }
    }
}
