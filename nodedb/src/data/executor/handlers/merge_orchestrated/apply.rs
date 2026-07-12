// SPDX-License-Identifier: BUSL-1.1

//! MERGE APPLY pass: verify the resolve→apply prediction, then atomically
//! apply every arm's writes with the Control-Plane-pre-assigned surrogates.

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::point::apply_put::{PointPutOutcome, PointPutParams};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::response_codec::encode_json;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_types::Surrogate;

use super::super::merge::MergeParams;

/// One committed Phase-A put captured for post-commit event emission:
/// `(row_key, new stored body borrowed from the plan, prior stored value)`.
/// The body borrows from the merge plan (owned for the whole apply) rather than
/// being cloned.
type MergePutEvent<'a> = (String, &'a [u8], Option<Vec<u8>>);

/// Record the in-memory index mutations a successful [`CoreLoop::apply_point_put`]
/// performed as undo entries. The HNSW vector index and the spatial R-tree live
/// OUTSIDE the shared redb transaction, so dropping that transaction on abort
/// does not reverse them — they must be undone explicitly. Drains the outcome's
/// insert deltas (leaving `prior_value` for the caller's event emission).
fn record_put_index_undo(undo_log: &mut Vec<UndoEntry>, outcome: &mut PointPutOutcome) {
    for d in std::mem::take(&mut outcome.vector_inserts) {
        undo_log.push(UndoEntry::InsertVector {
            index_key: d.index_key,
            vector_id: d.vector_id,
            collection: d.collection,
            field: d.field,
            doc_id: d.doc_id,
        });
    }
    for (key, entry_id) in std::mem::take(&mut outcome.spatial_inserts) {
        undo_log.push(UndoEntry::SpatialInsert { key, entry_id });
    }
}

/// Everything [`CoreLoop::abort_merge_apply`] needs to unwind a partially
/// applied MERGE and surface the terminating error.
struct MergeAbort<'a> {
    task: &'a ExecutionTask,
    database_id: u64,
    tid: u64,
    collection: &'a str,
    applied_keys: &'a [String],
    undo_log: Vec<UndoEntry>,
    err: ErrorCode,
}

impl CoreLoop {
    /// Evict cached document copies for rows written into a rolled-back apply
    /// transaction. `apply_point_put` populates the document cache BEFORE its
    /// UNIQUE check, so a row that fails the check — and every row rolled back
    /// when the shared txn is dropped — leaves a stale cache entry that a later
    /// point lookup would resurrect. Eviction is always safe: the worst case is
    /// a cache miss that falls through to the (correctly rolled-back) store.
    /// Mirrors the transaction-undo path's cache eviction.
    fn rollback_merge_cache(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        keys: &[String],
    ) {
        for key in keys {
            self.doc_cache.invalidate(database_id, tid, collection, key);
        }
    }

    /// Abort the apply pass: reverse the in-memory vector/spatial index deltas
    /// applied so far, evict the stale document-cache entries, and surface the
    /// error. The shared redb write transaction (dropped uncommitted once this
    /// returns) reverses the document store, secondary btree, FTS, and column
    /// stats; the HNSW and R-tree live outside it and are reversed here via the
    /// canonical undo driver. An undo failure leaves shard state unknown, so it
    /// escalates to `RollbackFailed` rather than the original error.
    fn abort_merge_apply(&mut self, p: MergeAbort<'_>) -> Response {
        let MergeAbort {
            task,
            database_id,
            tid,
            collection,
            applied_keys,
            undo_log,
            err,
        } = p;
        let final_err = match self.rollback_undo_log(database_id, tid, undo_log) {
            Ok(()) => err,
            Err((entry_index, detail)) => ErrorCode::RollbackFailed {
                entry_index,
                detail,
            },
        };
        self.rollback_merge_cache(database_id, tid, collection, applied_keys);
        self.response_error(task, final_err)
    }

    /// APPLY pass: verify the resolve→apply prediction, then atomically apply.
    pub(in crate::data::executor) fn execute_merge_apply(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        let resolved = match params.resolved_inserts {
            Some(r) => r,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "merge apply invoked without resolved inserts".into(),
                    },
                );
            }
        };
        let database_id = task.request.database_id.as_u64();

        let plan = match self.collect_merge_plan(database_id, tid, &params) {
            Ok(p) => p,
            Err(e) => return self.response_error(task, e),
        };

        // TOCTOU verification: the recomputed NOT-MATCHED insert-key set must
        // still equal the orchestrator's predicted set. Any drift (a target row
        // for a predicted-insert key appeared, or a matched row vanished) means
        // the pre-assigned surrogates no longer describe the merge — return
        // OllpRetryRequired WITHOUT writing so the orchestrator re-resolves.
        let mut actual_keys: Vec<&str> = plan.inserts.iter().map(|i| i.join_key.as_str()).collect();
        actual_keys.sort_unstable();
        let mut predicted_keys: Vec<&str> = resolved.iter().map(|(k, _)| k.as_str()).collect();
        predicted_keys.sort_unstable();
        if actual_keys != predicted_keys {
            return self.response_error(task, ErrorCode::OllpRetryRequired);
        }
        let surrogate_for: HashMap<&str, u32> =
            resolved.iter().map(|(k, s)| (k.as_str(), *s)).collect();

        // Whether the target maintains a secondary vector index. Gated ONCE here
        // (the schemaless half scans `vector_params` unindexed) and threaded into
        // the per-row UPDATE re-index below.
        let has_vectors = self.collection_has_vectors(database_id, tid, params.target_collection);

        // One post-apply redo entry per indexed row — a `Put` for each
        // UPDATE/INSERT post-image, a `Delete` for each removed row — carried
        // back so the Control Plane mints the durable WAL redo the vector index
        // needs to survive a WAL-only restart. Empty on non-vector targets.
        let mut write_set: Vec<WriteSetEntry> = Vec::new();

        // Phase A: matched UPDATE + NOT-MATCHED INSERT share ONE redb write
        // transaction. Any per-row error (including a UNIQUE violation from
        // `apply_point_put`) aborts, dropping the txn and rolling the whole set
        // back — the all-or-nothing guarantee the atomicity test pins.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => return self.response_error(task, e),
        };
        // Captured for post-commit event emission. The clone into `write_set`
        // below is the only owned body copy actually needed, since `plan`
        // doesn't outlive the function but does outlive this loop.
        let mut put_events: Vec<MergePutEvent<'_>> = Vec::new();
        let mut affected = 0u64;
        // Every row key written into `txn`, pushed BEFORE the write so a row that
        // fails mid-apply (its cache entry is populated before the UNIQUE check)
        // is evicted on abort too — see `rollback_merge_cache`.
        let mut applied_keys: Vec<String> = Vec::new();
        // In-memory (HNSW + R-tree) index deltas applied this pass, reversed on
        // any abort path — the redb txn drop only reverses store-backed state.
        let mut undo_log: Vec<UndoEntry> = Vec::new();

        for upd in &plan.updates {
            match upd.surrogate {
                Some(surrogate) => {
                    let row_key = surrogate_to_doc_id(surrogate);
                    applied_keys.push(row_key.clone());
                    // `apply_point_put`'s vector step APPENDS (it never replaces),
                    // so an in-place UPDATE must first soft-delete the surrogate's
                    // prior embedding or the stale vector keeps scoring in KNN
                    // search. Push each removal as a `DeleteVector` undo BEFORE the
                    // put's `InsertVector` undos so an abort undeletes the old
                    // vector after removing the new one (reverse order).
                    if has_vectors {
                        for d in self.remove_document_vector_indexes(
                            database_id,
                            tid,
                            params.target_collection,
                            &row_key,
                        ) {
                            undo_log.push(UndoEntry::DeleteVector {
                                index_key: d.index_key,
                                vector_id: d.vector_id,
                                collection: d.collection,
                                field: d.field,
                                doc_id: d.doc_id,
                            });
                        }
                    }
                    match self.apply_point_put(
                        &txn,
                        PointPutParams {
                            database_id,
                            tid,
                            collection: params.target_collection,
                            document_id: &row_key,
                            surrogate,
                            value: &upd.body,
                            index_text: true,
                            user_roles: &task.request.user_roles,
                            enforce: true,
                            wal_lsn: task.wal_lsn(),
                        },
                    ) {
                        Ok(mut outcome) => {
                            record_put_index_undo(&mut undo_log, &mut outcome);
                            if has_vectors {
                                write_set.push(WriteSetEntry {
                                    surrogate: surrogate.as_u32(),
                                    is_delete: false,
                                    value: upd.body.clone(),
                                });
                            }
                            put_events.push((row_key, upd.body.as_slice(), outcome.prior_value));
                            affected += 1;
                        }
                        Err(e) => {
                            return self.abort_merge_apply(MergeAbort {
                                task,
                                database_id,
                                tid,
                                collection: params.target_collection,
                                applied_keys: &applied_keys,
                                undo_log,
                                err: e.into(),
                            });
                        }
                    }
                }
                None => {
                    // Legacy non-surrogate target row: raw in-txn body rewrite
                    // (no cross-engine index — these rows predate surrogate
                    // keying and were never indexed).
                    applied_keys.push(upd.doc_id.clone());
                    if let Err(e) = self.sparse.put_in_txn(
                        &txn,
                        database_id,
                        tid,
                        params.target_collection,
                        &upd.doc_id,
                        &upd.body,
                    ) {
                        return self.abort_merge_apply(MergeAbort {
                            task,
                            database_id,
                            tid,
                            collection: params.target_collection,
                            applied_keys: &applied_keys,
                            undo_log,
                            err: e.into(),
                        });
                    }
                    affected += 1;
                }
            }
        }

        for ins in &plan.inserts {
            // The verify above proved every insert key has a pre-assigned
            // surrogate; the lookup cannot miss, but a missing entry is treated
            // as drift rather than unwrapped.
            let surrogate = match surrogate_for.get(ins.join_key.as_str()) {
                Some(s) => Surrogate(*s),
                None => {
                    return self.abort_merge_apply(MergeAbort {
                        task,
                        database_id,
                        tid,
                        collection: params.target_collection,
                        applied_keys: &applied_keys,
                        undo_log,
                        err: ErrorCode::OllpRetryRequired,
                    });
                }
            };
            let row_key = surrogate_to_doc_id(surrogate);
            applied_keys.push(row_key.clone());
            match self.apply_point_put(
                &txn,
                PointPutParams {
                    database_id,
                    tid,
                    collection: params.target_collection,
                    document_id: &row_key,
                    surrogate,
                    value: &ins.body,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    wal_lsn: task.wal_lsn(),
                },
            ) {
                Ok(mut outcome) => {
                    record_put_index_undo(&mut undo_log, &mut outcome);
                    if has_vectors {
                        write_set.push(WriteSetEntry {
                            surrogate: surrogate.as_u32(),
                            is_delete: false,
                            value: ins.body.clone(),
                        });
                    }
                    put_events.push((row_key, ins.body.as_slice(), None));
                    affected += 1;
                }
                Err(e) => {
                    return self.abort_merge_apply(MergeAbort {
                        task,
                        database_id,
                        tid,
                        collection: params.target_collection,
                        applied_keys: &applied_keys,
                        undo_log,
                        err: e.into(),
                    });
                }
            }
        }

        if let Err(e) = txn.commit() {
            return self.abort_merge_apply(MergeAbort {
                task,
                database_id,
                tid,
                collection: params.target_collection,
                applied_keys: &applied_keys,
                undo_log,
                err: ErrorCode::Internal {
                    detail: format!("merge apply commit: {e}"),
                },
            });
        }
        self.checkpoint_coordinator
            .mark_dirty("sparse", put_events.len());

        for (row_key, body, prior) in &put_events {
            self.emit_put_event(
                task,
                tid,
                params.target_collection,
                row_key,
                body,
                prior.as_deref(),
            );
        }

        // Phase B: DELETE arms. `apply_point_delete`'s cascade (document store,
        // FTS, spatial, HNSW vector, secondary indexes) opens its own
        // transactions, so it must run after the put commit rather than inside
        // the shared txn. These arms only ever hit existing registered rows.
        for del in &plan.deletes {
            match del.surrogate {
                Some(surrogate) => {
                    match self.apply_point_delete(PointDeleteParams {
                        database_id,
                        tid,
                        collection: params.target_collection,
                        document_id: &del.doc_id,
                        surrogate,
                        user_roles: &task.request.user_roles,
                        enforce: true,
                    }) {
                        Ok(outcome) => {
                            if outcome.prior_value.is_some() {
                                affected += 1;
                                if has_vectors {
                                    write_set.push(WriteSetEntry {
                                        surrogate: surrogate.as_u32(),
                                        is_delete: true,
                                        value: Vec::new(),
                                    });
                                }
                            }
                            let row_key = surrogate_to_doc_id(surrogate);
                            self.emit_write_event(
                                task,
                                params.target_collection,
                                crate::event::WriteOp::Delete,
                                &row_key,
                                None,
                                outcome.prior_value.as_deref(),
                            );
                        }
                        Err(e) => return self.response_error(task, e),
                    }
                }
                None => {
                    if let Err(e) =
                        self.sparse
                            .delete(database_id, tid, params.target_collection, &del.doc_id)
                    {
                        return self.response_error(task, e);
                    }
                    affected += 1;
                }
            }
        }

        let result = serde_json::json!({ "affected": affected });
        let mut response = match encode_json(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        };
        if !write_set.is_empty() {
            response.write_set = write_set;
        }
        response
    }
}
