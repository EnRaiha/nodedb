// SPDX-License-Identifier: BUSL-1.1

//! KV undo entry application logic.
//!
//! Split out of `apply.rs` (which grouped every engine family in one file)
//! once the `KvTtl` / `SortedIndexDdl` arms pushed the KV family over this
//! crate's per-file line budget.

use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::kv::current_ms;

use super::UndoEntry;

impl CoreLoop {
    pub(super) fn apply_undo_kv(
        &mut self,
        did: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::KvPut {
                collection,
                key,
                prior_value,
            } => {
                let now_ms = current_ms();
                if let Some(old) = prior_value {
                    self.kv_engine.put(crate::engine::kv::KvPutParams {
                        database_id: did,
                        tenant_id: tid,
                        collection: &collection,
                        key: &key,
                        value: &old,
                        ttl_ms: 0,
                        now_ms,
                        surrogate: nodedb_types::Surrogate::ZERO,
                    });
                } else {
                    self.kv_engine.delete(
                        did,
                        tid,
                        &collection,
                        std::slice::from_ref(&key),
                        now_ms,
                    );
                }
                Ok(())
            }
            UndoEntry::KvDelete {
                collection,
                key,
                prior_value,
            } => {
                let now_ms = current_ms();
                self.kv_engine.put(crate::engine::kv::KvPutParams {
                    database_id: did,
                    tenant_id: tid,
                    collection: &collection,
                    key: &key,
                    value: &prior_value,
                    ttl_ms: 0,
                    now_ms,
                    surrogate: nodedb_types::Surrogate::ZERO,
                });
                Ok(())
            }
            UndoEntry::KvBatchPut {
                collection,
                entries,
            } => {
                let now_ms = current_ms();
                for (key, prior_value) in entries {
                    if let Some(old) = prior_value {
                        self.kv_engine.put(crate::engine::kv::KvPutParams {
                            database_id: did,
                            tenant_id: tid,
                            collection: &collection,
                            key: &key,
                            value: &old,
                            ttl_ms: 0,
                            now_ms,
                            surrogate: nodedb_types::Surrogate::ZERO,
                        });
                    } else {
                        self.kv_engine.delete(did, tid, &collection, &[key], now_ms);
                    }
                }
                Ok(())
            }
            UndoEntry::KvTransfer {
                collection,
                source_key,
                source_prior,
                dest_key,
                dest_prior,
            } => {
                let now_ms = current_ms();
                self.kv_engine.put(crate::engine::kv::KvPutParams {
                    database_id: did,
                    tenant_id: tid,
                    collection: &collection,
                    key: &source_key,
                    value: &source_prior,
                    ttl_ms: 0,
                    now_ms,
                    surrogate: nodedb_types::Surrogate::ZERO,
                });
                if let Some(old) = dest_prior {
                    self.kv_engine.put(crate::engine::kv::KvPutParams {
                        database_id: did,
                        tenant_id: tid,
                        collection: &collection,
                        key: &dest_key,
                        value: &old,
                        ttl_ms: 0,
                        now_ms,
                        surrogate: nodedb_types::Surrogate::ZERO,
                    });
                } else {
                    self.kv_engine
                        .delete(did, tid, &collection, &[dest_key], now_ms);
                }
                Ok(())
            }
            UndoEntry::KvTransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                source_prior,
                dest_prior,
            } => {
                let now_ms = current_ms();
                // Cross-collection move: the forward op deleted `item_key` from
                // `source_collection` and wrote to `dest_key` in `dest_collection`
                // (e.g. inventory → archive). Reverse both halves: re-insert the
                // source row, then undo the destination write below. `source_prior`
                // is always Some because the forward op required the source to
                // exist; `dest_prior` is None when the dest key was a new insert
                // and Some(old) when it overwrote an existing row.
                self.kv_engine.put(crate::engine::kv::KvPutParams {
                    database_id: did,
                    tenant_id: tid,
                    collection: &source_collection,
                    key: &item_key,
                    value: &source_prior,
                    ttl_ms: 0,
                    now_ms,
                    surrogate: nodedb_types::Surrogate::ZERO,
                });
                // Undo the dest write.
                if let Some(old) = dest_prior {
                    self.kv_engine.put(crate::engine::kv::KvPutParams {
                        database_id: did,
                        tenant_id: tid,
                        collection: &dest_collection,
                        key: &dest_key,
                        value: &old,
                        ttl_ms: 0,
                        now_ms,
                        surrogate: nodedb_types::Surrogate::ZERO,
                    });
                } else {
                    self.kv_engine
                        .delete(did, tid, &dest_collection, &[dest_key], now_ms);
                }
                Ok(())
            }
            UndoEntry::KvTtl {
                collection,
                key,
                prior_expiry,
            } => {
                // The forward `Expire`/`Persist` only succeeds when the key
                // exists (mirrors the live handler's `NotFound` on an absent
                // key), so this undo entry is only ever pushed after that
                // precondition held. If a sibling undo already applied and
                // this key is now genuinely missing, that is a broken
                // invariant, not a soft "nothing to do" case.
                let restored = match prior_expiry {
                    Some(expire_at_ms) => self.kv_engine.expire_with_absolute_expiry(
                        did,
                        tid,
                        &collection,
                        &key,
                        expire_at_ms,
                    ),
                    None => self.kv_engine.persist(did, tid, &collection, &key),
                };
                if restored {
                    Ok(())
                } else {
                    let detail = format!(
                        "kv ttl undo: key missing in {collection} during rollback of Expire/Persist"
                    );
                    error!(
                        core = self.core_id,
                        entry_index,
                        error = %detail,
                        "transaction undo: kv ttl restore failed; shard state unknown"
                    );
                    Err((entry_index, detail))
                }
            }
            UndoEntry::SortedIndexDdl {
                database_id,
                tenant_id,
                index_name,
                prior_def,
            } => {
                match prior_def {
                    // An index existed under this name before the forward op
                    // (an overwritten `RegisterSortedIndex`, or the index a
                    // `DropSortedIndex` removed) -- restore it. `register`
                    // rebuilds the order-statistic tree by backfilling from
                    // the KV collection's CURRENT contents, which is correct
                    // regardless of where this undo entry falls relative to
                    // sibling KV-write undos in the log (see `UndoEntry`
                    // doc comment).
                    Some(def) => {
                        let collection = def.collection.clone();
                        self.kv_engine.register_sorted_index(
                            database_id,
                            tenant_id,
                            &collection,
                            def,
                        );
                        Ok(())
                    }
                    // No index existed under this name before the forward op
                    // (a fresh `RegisterSortedIndex`) -- undo removes it.
                    None => {
                        if self
                            .kv_engine
                            .drop_sorted_index(database_id, tenant_id, &index_name)
                        {
                            Ok(())
                        } else {
                            let detail = format!(
                                "sorted index undo: '{index_name}' missing during rollback of RegisterSortedIndex"
                            );
                            error!(
                                core = self.core_id,
                                entry_index,
                                error = %detail,
                                "transaction undo: sorted index drop failed; shard state unknown"
                            );
                            Err((entry_index, detail))
                        }
                    }
                }
            }
            _ => Err((
                entry_index,
                "apply_undo_kv called with non-kv entry".to_string(),
            )),
        }
    }
}
