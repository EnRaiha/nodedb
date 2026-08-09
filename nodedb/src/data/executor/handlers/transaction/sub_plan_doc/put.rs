// SPDX-License-Identifier: BUSL-1.1

//! Document PointPut helper for transaction sub-plans.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::funnel::{self, WriteEnforcementOutcome};
use crate::data::executor::enforcement::hash_chain;
use crate::data::executor::enforcement::images::{EnforcementCtx, RowImages};
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId};

/// Parameters for [`CoreLoop::tx_point_put`].
pub(in crate::data::executor::handlers::transaction) struct TxPointPut<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub value: &'a [u8],
    pub user_roles: &'a [String],
    /// Insert-vs-upsert semantics. `None` = PUT/upsert (overwrite is allowed,
    /// no existence probe). `Some(if_absent)` = INSERT semantics: probe for an
    /// existing primary key under the same write txn and, if present, either
    /// silently skip (`if_absent = true`, `INSERT ... ON CONFLICT DO NOTHING`)
    /// or reject with a `unique` constraint violation (`if_absent = false`).
    pub insert_if_absent: Option<bool>,
    /// Join-key VALUE → target row surrogate, resolved on the Control Plane at
    /// plan time for every materialized-sum target this write may touch. The
    /// Data Plane addresses target rows with these and never derives them: the
    /// primary-key → surrogate map is Control-Plane catalog state.
    pub resolved_sum_targets: &'a [(String, nodedb_types::Surrogate)],
}

/// What an abort after `apply_point_put` has to reverse in memory.
struct PostApplyAbort<'a> {
    /// Whether this op advanced the hash-chain head.
    mutated_chain: bool,
    /// Key the chain head is tracked under.
    chain_key: &'a (DatabaseId, TenantId, String),
    /// Captured chain-head pre-image, per [`CoreLoop::restore_chain_head`].
    chain_prior: &'a Option<Option<String>>,
    database_id: u64,
    tid: u64,
    collection: &'a str,
    /// Storage key of the row the aborted put wrote.
    row_key: &'a str,
}

impl CoreLoop {
    /// Restore a hash-chain head pre-image after an aborted insert.
    ///
    /// `mutated` is whether this op actually advanced the chain head (only true
    /// on an insert into a hash-chain collection). `prior` is the captured
    /// pre-image: `None` = not a hash-chain collection; `Some(None)` = no prior
    /// head (genesis); `Some(Some(prev))` = restore this head.
    ///
    /// In-memory only, and correctly so: every caller aborts before the write
    /// transaction commits, so the persisted head was never written. Reversing
    /// a head that already reached disk is the rollback path's job
    /// (`undo_chain_hash`).
    fn restore_chain_head(
        &mut self,
        mutated: bool,
        config_key: &(DatabaseId, TenantId, String),
        prior: &Option<Option<String>>,
    ) {
        if !mutated {
            return;
        }
        match prior {
            Some(None) => {
                self.chain_hashes.remove(config_key);
            }
            Some(Some(prev)) => {
                self.chain_hashes.insert(config_key.clone(), prev.clone());
            }
            None => {}
        }
    }

    /// Undo the in-memory side-effects an abort AFTER `apply_point_put` leaves
    /// behind, before the caller drops its transaction uncommitted.
    ///
    /// `apply_point_put` populates the read-through document cache with the body
    /// it wrote. Dropping the redb transaction reverses the durable write but not
    /// that cache entry, so every subsequent read of the row would be served the
    /// post-image of a write that never landed — a row visible to readers and
    /// absent from storage. Restoring the hash-chain head is the same class of
    /// in-memory reversal, so both happen here rather than one being remembered
    /// at each abort site and the other forgotten.
    fn abort_after_apply(&mut self, abort: PostApplyAbort<'_>) {
        self.restore_chain_head(abort.mutated_chain, abort.chain_key, abort.chain_prior);
        self.doc_cache.invalidate(
            abort.database_id,
            abort.tid,
            abort.collection,
            abort.row_key,
        );
    }

    /// Execute a PointPut within a transaction.
    pub(in crate::data::executor::handlers::transaction) fn tx_point_put(
        &mut self,
        p: TxPointPut<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxPointPut {
            task: dummy_task,
            tid,
            collection,
            document_id,
            surrogate,
            value,
            user_roles,
            insert_if_absent,
            resolved_sum_targets,
        } = p;
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let database_id = dummy_task.request.database_id.as_u64();

        // Pre-read the plain-table value: it decides insert-vs-update for the
        // hash chain, and it is the PRE-IMAGE the enforcement funnel folds — an
        // enforcement that only sees the post-image cannot tell an update from
        // an insert, which is how a running total came to double-count one.
        // The authoritative prior value for the undo entry comes from
        // `apply_point_put`'s outcome, which is bitemporal-aware.
        let config_key = (
            dummy_task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );
        let chain_key = (
            dummy_task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );
        let prior_bytes = self
            .sparse
            .get(database_id, tid, collection, row_key)
            .ok()
            .flatten();
        let is_insert = prior_bytes.is_none();

        let hash_chain_enabled = self
            .doc_configs
            .get(&config_key)
            .is_some_and(|c| c.enforcement.hash_chain);

        // Capture the hash-chain head pre-image BEFORE `apply_chain_on_insert`
        // overwrites it, so the undo entry can restore it exactly.
        // `None` = not a hash-chain collection; `Some(None)` = no prior head
        // (genesis insert); `Some(Some(prev))` = prior head present.
        let chain_hash_prior: Option<Option<String>> = if hash_chain_enabled {
            Some(self.chain_hashes.get(&chain_key).cloned())
        } else {
            None
        };

        // Hash-chain wraps the document with a `_chain_hash` field on insert;
        // feed that wrapped value into `apply_point_put` so it stores/indexes
        // the chained form.
        let chained: Option<Vec<u8>> = if is_insert {
            hash_chain::apply_chain_on_insert(
                &mut self.chain_hashes,
                database_id,
                tid,
                collection,
                document_id,
                value,
                hash_chain_enabled,
            )
            .map_err(|e| ErrorCode::Internal {
                detail: format!("hash chain: {e}"),
            })?
        } else {
            None
        };
        let effective_value: &[u8] = chained.as_deref().unwrap_or(value);

        // Each transaction sub-plan owns its own per-row redb write txn; the
        // batch is stitched together by the undo log, not one big txn.
        let txn = self.sparse.begin_write().map_err(|e| ErrorCode::Internal {
            detail: e.to_string(),
        })?;

        // INSERT semantics: probe for an existing primary key under the SAME
        // write txn we will commit through — linearizable with the write, so no
        // concurrent writer can slip a row in between the probe and the commit.
        // Mirrors autocommit `execute_point_insert`. PUT/upsert (`None`) skips
        // this entirely and keeps overwrite behaviour.
        if let Some(if_absent) = insert_if_absent {
            let exists_result = if self.is_bitemporal(database_id, tid, collection) {
                self.sparse.versioned_exists_current_in_txn(
                    &txn,
                    database_id,
                    tid,
                    collection,
                    row_key,
                )
            } else {
                self.sparse
                    .exists_in_txn(&txn, database_id, tid, collection, row_key)
            };
            let exists = exists_result.map_err(|e| {
                // Restore any chain-head pre-image mutated above before bailing.
                self.restore_chain_head(chained.is_some(), &chain_key, &chain_hash_prior);
                ErrorCode::from(e)
            })?;
            if exists {
                // No write, no undo push — drop the txn without committing.
                self.restore_chain_head(chained.is_some(), &chain_key, &chain_hash_prior);
                if if_absent {
                    // `INSERT ... ON CONFLICT DO NOTHING`: silent skip.
                    return Ok(self.response_ok(dummy_task));
                }
                return Err(ErrorCode::from(crate::Error::RejectedConstraint {
                    collection: collection.to_string(),
                    constraint: "unique".to_string(),
                    detail: format!(
                        "duplicate key value '{document_id}' violates primary-key \
                         uniqueness on '{collection}'"
                    ),
                }));
            }
        }

        // Core write path shared with the autocommit callers: bitemporal-vs-plain
        // primary doc write, FTS/inverted, doc_cache, aggregate-cache
        // invalidation, UNIQUE enforcement, generated columns, stateless PUT
        // enforcement, and the side indexes (secondary/spatial/vector/stats).
        // Every side-effect is captured in the outcome and reversed via the undo
        // log below, so the transactional write is identical to autocommit and
        // fully rollback-safe.
        let outcome = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid,
                collection,
                document_id: row_key,
                surrogate,
                value: effective_value,
                index_text: true,
                user_roles,
                enforce: true,
                wal_lsn: dummy_task.wal_lsn(),
            },
        ) {
            Ok(o) => o,
            Err(e) => {
                // `apply_point_put` rejected the write (e.g. UNIQUE violation)
                // after we mutated the chain head and, on the later rejections,
                // after it had already cached the row. Reverse both so the
                // aborted op leaves no trace, then propagate the typed error.
                self.abort_after_apply(PostApplyAbort {
                    mutated_chain: chained.is_some(),
                    chain_key: &chain_key,
                    chain_prior: &chain_hash_prior,
                    database_id,
                    tid,
                    collection,
                    row_key,
                });
                return Err(e.into());
            }
        };

        // Persist the advanced chain head inside the SAME write transaction the
        // chained row lands in, so head and row commit or roll back as one
        // atomic unit. A head that can advance without its row (or a row that
        // lands without its head) is the same broken-chain bug persistence
        // exists to prevent. Every abort path above returns before this point
        // and drops `txn` uncommitted, so a rejected insert never leaves a head
        // behind on disk either.
        let advanced_head = if chained.is_some() {
            self.chain_hashes.get(&chain_key).cloned()
        } else {
            None
        };
        if let Some(head) = advanced_head
            && let Err(e) =
                self.sparse
                    .put_chain_head_in_txn(&txn, database_id, tid, collection, &head)
        {
            self.abort_after_apply(PostApplyAbort {
                mutated_chain: true,
                chain_key: &chain_key,
                chain_prior: &chain_hash_prior,
                database_id,
                tid,
                collection,
                row_key,
            });
            return Err(ErrorCode::from(e));
        }

        // Write-path enforcement runs one level ABOVE `apply_point_put`, and
        // inside THIS transaction: a materialized-sum target write is itself an
        // `apply_point_put`, so every derived write lands or rolls back with the
        // row that caused it. On failure the chain-head pre-image is restored
        // and `txn` is dropped uncommitted, leaving neither the row nor any
        // target it credited behind.
        //
        // Folding a write's images means DECODING its stored pre-image, and a
        // stored body is only guaranteed to be a readable document for a
        // collection that declares what its columns mean. An opaque body — one
        // written to a collection that registered no schema — is not one, and
        // reading it is a hard error by design. So the pre-image is decoded ONLY
        // when the collection declares enforcement that folds it. Deciding
        // otherwise fails every write to a constraint-free collection carrying
        // such a body, and fails it BEFORE the undo entry below is pushed, which
        // leaves a batch rollback with nothing to reverse for that row.
        let folds_images = self
            .doc_configs
            .get(&config_key)
            .is_some_and(|config| config.enforcement.has_image_enforcement());
        let enforcement = if folds_images {
            let source_format = self.sparse_body_format(
                dummy_task.request.database_id,
                TenantId::new(tid),
                collection,
            );
            // Here — where the collection HAS declared constraints over its
            // columns — a stored row that will not decode is corruption, not "no
            // pre-image": treating it as an INSERT would credit a target with the
            // row's whole new value on top of the contribution it already holds.
            let old_doc = match prior_bytes {
                Some(ref bytes) => {
                    match decode_scanned_document(bytes, source_format.as_format_ref()) {
                        Ok(doc) => Some(doc),
                        Err(e) => {
                            self.abort_after_apply(PostApplyAbort {
                                mutated_chain: chained.is_some(),
                                chain_key: &chain_key,
                                chain_prior: &chain_hash_prior,
                                database_id,
                                tid,
                                collection,
                                row_key,
                            });
                            return Err(ErrorCode::from(e));
                        }
                    }
                }
                None => None,
            };
            // The SUBMITTED body, not the chained one: `_chain_hash` is a wrapper
            // the hash chain adds around the row, and no constraint is declared
            // over it. An incoming body with no readable fields carries no column
            // any binding or BALANCED definition can read, so it folds to nothing.
            let new_doc = doc_format::decode_document(value).ok();
            let images = match (old_doc.as_ref(), new_doc.as_ref()) {
                (None, Some(new_doc)) => Some(RowImages::Insert { new_doc }),
                (Some(old_doc), Some(new_doc)) => Some(RowImages::Update { old_doc, new_doc }),
                (Some(_), None) | (None, None) => None,
            };
            match images {
                Some(images) => {
                    let ctx = EnforcementCtx {
                        database_id,
                        tid,
                        collection,
                        resolved_targets: resolved_sum_targets,
                        wal_lsn: dummy_task.wal_lsn(),
                    };
                    match funnel::run_write_enforcement(self, &txn, ctx, images) {
                        Ok(outcome) => outcome,
                        Err(e) => {
                            self.abort_after_apply(PostApplyAbort {
                                mutated_chain: chained.is_some(),
                                chain_key: &chain_key,
                                chain_prior: &chain_hash_prior,
                                database_id,
                                tid,
                                collection,
                                row_key,
                            });
                            return Err(ErrorCode::from(e));
                        }
                    }
                }
                None => WriteEnforcementOutcome::default(),
            }
        } else {
            WriteEnforcementOutcome::default()
        };
        let WriteEnforcementOutcome {
            target_writes,
            // The BALANCED check spans the whole transaction — debits and
            // credits arrive on different rows — so its entries belong to the
            // caller that owns transaction scope, which still recomputes them
            // from the committed rows.
            balanced_entries: _balanced_entries,
        } = enforcement;

        txn.commit().map_err(|e| ErrorCode::Internal {
            detail: format!("commit: {e}"),
        })?;
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Reverse every derived materialized-sum target write with the SAME set
        // of undo entries the source row uses: the target write is a full
        // document write, so it has index, vector, spatial and stats
        // side-effects of its own to reverse.
        for target in target_writes {
            undo_log.push(UndoEntry::PutDocument {
                collection: target.collection,
                document_id: target.document_id,
                surrogate: target.surrogate,
                old_value: target.outcome.prior_value,
                bitemporal_sys_from_ms: target.outcome.bitemporal_sys_from_ms,
                bitemporal_index_tuples: target.outcome.bitemporal_index_tuples,
                secondary_index_added: target.outcome.secondary_index_added,
                secondary_index_removed: target.outcome.secondary_index_removed,
                chain_hash_prior: None,
            });
            for delta in target.outcome.vector_inserts {
                undo_log.push(UndoEntry::InsertVector {
                    index_key: delta.index_key,
                    vector_id: delta.vector_id,
                    collection: delta.collection,
                    field: delta.field,
                    doc_id: delta.doc_id,
                });
            }
            for (key, entry_id) in target.outcome.spatial_inserts {
                undo_log.push(UndoEntry::SpatialInsert { key, entry_id });
            }
            for (key, prior) in target.outcome.stats_prior {
                undo_log.push(UndoEntry::StatsRestore { key, prior });
            }
        }

        undo_log.push(UndoEntry::PutDocument {
            collection: collection.to_string(),
            document_id: row_key.to_string(),
            surrogate,
            old_value: outcome.prior_value,
            bitemporal_sys_from_ms: outcome.bitemporal_sys_from_ms,
            bitemporal_index_tuples: outcome.bitemporal_index_tuples,
            // Plain secondary-index entries this put added/removed; reversed on
            // rollback so the index returns to its pre-tx state.
            secondary_index_added: outcome.secondary_index_added,
            secondary_index_removed: outcome.secondary_index_removed,
            chain_hash_prior,
        });

        // Reverse any HNSW vector inserts on rollback (one `InsertVector` undo
        // per vector this put added to a per-field index).
        for delta in outcome.vector_inserts {
            undo_log.push(UndoEntry::InsertVector {
                index_key: delta.index_key,
                vector_id: delta.vector_id,
                collection: delta.collection,
                field: delta.field,
                doc_id: delta.doc_id,
            });
        }

        // Reverse any spatial R-tree inserts on rollback (one `SpatialInsert`
        // undo per per-field R-tree entry this put added).
        for (key, entry_id) in outcome.spatial_inserts {
            undo_log.push(UndoEntry::SpatialInsert { key, entry_id });
        }

        // Reverse the column-stats read-modify-write on rollback by restoring
        // each captured pre-image.
        for (key, prior) in outcome.stats_prior {
            undo_log.push(UndoEntry::StatsRestore { key, prior });
        }

        Ok(self.response_ok(dummy_task))
    }
}
