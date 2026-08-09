// SPDX-License-Identifier: BUSL-1.1

//! MERGE APPLY pass: verify the resolve→apply prediction, then atomically
//! apply every arm's writes with the Control-Plane-pre-assigned surrogates.
//!
//! This file owns the drift verification and the single redb write transaction
//! the UPDATE and INSERT arms share. The two things that cannot live under that
//! transaction have their own files: unwinding a partial apply (`abort`) and
//! the DELETE arms, whose cascade opens transactions of its own and therefore
//! runs after the commit (`delete_arms`).

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::response_codec::encode_json_as_msgpack;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_types::Surrogate;

use super::super::merge::MergeParams;
use super::super::returning_rows;
use super::abort::MergeAbort;
use super::apply_support::{MergePutEvent, gate_merge_arms, record_put_index_undo, returning_doc};
use super::delete_arms::{MergeDeleteArms, MergeDeleteTally};

impl CoreLoop {
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

        let plan = match self.collect_merge_plan(database_id, tid, task.request.txn_id, &params) {
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

        // Gate every arm on the target's write policy BEFORE the apply
        // transaction opens, so a rejected row leaves nothing written and
        // nothing to unwind.
        if let Err(e) =
            gate_merge_arms(&plan, params.rls_write_check, tid, params.target_collection)
        {
            return self.response_error(task, e);
        }

        // One post-apply redo entry per indexed row — a `Put` for each
        // UPDATE/INSERT post-image, a `Delete` for each removed row — carried
        // back so the Control Plane mints the durable WAL redo the vector index
        // needs to survive a WAL-only restart. Empty on non-vector targets.
        let mut write_set: Vec<WriteSetEntry> = Vec::new();

        // The whole MERGE is ONE boundary, so its DELETE arms are accounted
        // here, before any phase runs: those arms apply in their own
        // transactions AFTER the phase-A commit, so entries collected as they
        // ran could only report a violation phase A had already made durable.
        // Their pre-images are the plan's captured bodies, which the classifier
        // already holds — nothing is re-read.
        let delete_bodies: Vec<&[u8]> = plan.deletes.iter().map(|d| d.body.as_slice()).collect();
        let mut balanced_entries = self.balanced_entries_for_submitted_deletes(
            database_id,
            tid,
            params.target_collection,
            &delete_bodies,
        );

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
        // RETURNING rows for THIS apply attempt: post-images for the UPDATE and
        // INSERT arms, pre-images for the DELETE arms. Built fresh here rather
        // than carried in, because an attempt that ends in `OllpRetryRequired`
        // is fully re-resolved and re-applied by the orchestrator — rows from a
        // failed attempt describe a snapshot that never committed.
        let mut returned_docs: Vec<serde_json::Value> = Vec::new();

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
                            // The arm's materialized-sum delta is folded inside
                            // the SAME transaction the arm's row lands in, so a
                            // moved total rolls back with the row that moved it.
                            // Both images come from the plan: the classifier held
                            // the pre-image already, so nothing is re-read.
                            match write_hook::run(
                                self,
                                &txn,
                                &write_hook::HookCtx {
                                    database_id,
                                    tid,
                                    collection: params.target_collection,
                                    resolved_targets: params.resolved_sum_targets,
                                    deferred_sum_targets: &[],
                                    wal_lsn: task.wal_lsn(),
                                },
                                write_hook::WriteImages::Update {
                                    old: write_hook::ImageBody::Submitted(&upd.old_body),
                                    new: write_hook::ImageBody::Submitted(&upd.body),
                                },
                            ) {
                                Ok(enforcement) => {
                                    write_set.extend(write_hook::target_write_set(
                                        &enforcement.target_writes,
                                    ));
                                    balanced_entries.extend(enforcement.balanced_entries);
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
                            if has_vectors {
                                write_set.push(WriteSetEntry {
                                    surrogate: surrogate.as_u32(),
                                    is_delete: false,
                                    value: upd.body.clone(),
                                    collection: None,
                                });
                            }
                            if params.returning.is_some() {
                                match returning_doc(&upd.body, &row_key) {
                                    Ok(doc) => returned_docs.push(doc),
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
                    if params.returning.is_some() {
                        match returning_doc(&upd.body, &upd.doc_id) {
                            Ok(doc) => returned_docs.push(doc),
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
                    // A NOT-MATCHED INSERT arm credits its target with the whole
                    // new row — post-image only, which is exactly what
                    // `RowImages::Insert` expresses.
                    match write_hook::run(
                        self,
                        &txn,
                        &write_hook::HookCtx {
                            database_id,
                            tid,
                            collection: params.target_collection,
                            resolved_targets: params.resolved_sum_targets,
                            deferred_sum_targets: &[],
                            wal_lsn: task.wal_lsn(),
                        },
                        write_hook::WriteImages::Insert {
                            new: write_hook::ImageBody::Submitted(&ins.body),
                        },
                    ) {
                        Ok(enforcement) => {
                            write_set
                                .extend(write_hook::target_write_set(&enforcement.target_writes));
                            balanced_entries.extend(enforcement.balanced_entries);
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
                    if has_vectors {
                        write_set.push(WriteSetEntry {
                            surrogate: surrogate.as_u32(),
                            is_delete: false,
                            value: ins.body.clone(),
                            collection: None,
                        });
                    }
                    if params.returning.is_some() {
                        match returning_doc(&ins.body, &row_key) {
                            Ok(doc) => returned_docs.push(doc),
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

        // Every arm of the statement — the UPDATE and INSERT arms folded above
        // and the DELETE arms accounted before phase A — is judged once here,
        // before the phase-A commit, so a MERGE that leaves a journal group
        // unbalanced writes nothing at all.
        if let Err(e) = self.settle_balanced_entries(
            database_id,
            tid,
            params.target_collection,
            balanced_entries,
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

        // Phase B: DELETE arms, applied after the put commit because their
        // cascade opens its own transactions.
        if let Err(response) = self.apply_merge_delete_arms(
            MergeDeleteArms {
                task,
                database_id,
                tid,
                collection: params.target_collection,
                deletes: &plan.deletes,
                has_vectors,
                returning: params.returning.is_some(),
                resolved_targets: params.resolved_sum_targets,
            },
            MergeDeleteTally {
                affected: &mut affected,
                write_set: &mut write_set,
                returned_docs: &mut returned_docs,
            },
        ) {
            return response;
        }

        let mut response = if let Some(spec) = params.returning {
            match returning_rows::build_rows_payload(spec, params.rls_filters, &returned_docs) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("RETURNING encode: {e}"),
                        },
                    );
                }
            }
        } else {
            let result = serde_json::json!({ "affected": affected });
            match encode_json_as_msgpack(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };
        if !write_set.is_empty() {
            response.write_set = write_set;
        }
        response
    }
}
