// SPDX-License-Identifier: BUSL-1.1

//! Document PointPut and PointDelete helpers for transaction sub-plans.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::{hash_chain, materialized_sum};
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::undo::UndoEntry;

/// Parameters for [`CoreLoop::tx_point_put`].
pub(super) struct TxPointPut<'a> {
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
}

/// Parameters for [`CoreLoop::tx_point_delete`].
pub(super) struct TxPointDelete<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub user_roles: &'a [String],
}

impl CoreLoop {
    /// Restore a hash-chain head pre-image after an aborted insert.
    ///
    /// `mutated` is whether this op actually advanced the chain head (only true
    /// on an insert into a hash-chain collection). `prior` is the captured
    /// pre-image: `None` = not a hash-chain collection; `Some(None)` = no prior
    /// head (genesis); `Some(Some(prev))` = restore this head.
    fn restore_chain_head(
        &mut self,
        mutated: bool,
        config_key: &(TenantId, String),
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

    /// Execute a PointPut within a transaction.
    pub(super) fn tx_point_put(
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
        } = p;
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let database_id = dummy_task.request.database_id.as_u64();

        // Pre-read the plain-table value only to decide insert-vs-update for the
        // hash-chain and materialized-sum side-effects (both fire on insert).
        // The authoritative prior value for the undo entry comes from
        // `apply_point_put`'s outcome, which is bitemporal-aware.
        let config_key = (TenantId::new(tid), collection.to_string());
        let is_insert = self
            .sparse
            .get(database_id, tid, collection, row_key)
            .ok()
            .flatten()
            .is_none();

        let hash_chain_enabled = self
            .doc_configs
            .get(&config_key)
            .is_some_and(|c| c.enforcement.hash_chain);

        // Capture the hash-chain head pre-image BEFORE `apply_chain_on_insert`
        // overwrites it, so the undo entry can restore it exactly.
        // `None` = not a hash-chain collection; `Some(None)` = no prior head
        // (genesis insert); `Some(Some(prev))` = prior head present.
        let chain_hash_prior: Option<Option<String>> = if hash_chain_enabled {
            Some(self.chain_hashes.get(&config_key).cloned())
        } else {
            None
        };

        // Hash-chain wraps the document with a `_chain_hash` field on insert;
        // feed that wrapped value into `apply_point_put` so it stores/indexes
        // the chained form.
        let chained: Option<Vec<u8>> = if is_insert {
            hash_chain::apply_chain_on_insert(
                &mut self.chain_hashes,
                tid,
                collection,
                document_id,
                value,
                hash_chain_enabled,
            )
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
            let exists_result = if self.is_bitemporal(tid, collection) {
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
                self.restore_chain_head(chained.is_some(), &config_key, &chain_hash_prior);
                ErrorCode::from(e)
            })?;
            if exists {
                // No write, no undo push — drop the txn without committing.
                self.restore_chain_head(chained.is_some(), &config_key, &chain_hash_prior);
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
        // invalidation, UNIQUE enforcement, generated columns, and stateless PUT
        // enforcement. Side indexes (secondary/spatial/vector/stats) are disabled
        // in the transactional path — they have no undo variant yet, so enabling
        // them here would leave a rollback hole.
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
                enable_side_indexes: false,
            },
        ) {
            Ok(o) => o,
            Err(e) => {
                // `apply_point_put` rejected the write (e.g. UNIQUE violation)
                // after we mutated the chain head. Restore the pre-image so the
                // aborted op leaves no trace, then propagate the typed error.
                self.restore_chain_head(chained.is_some(), &config_key, &chain_hash_prior);
                return Err(e.into());
            }
        };

        txn.commit().map_err(|e| ErrorCode::Internal {
            detail: format!("commit: {e}"),
        })?;
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        undo_log.push(UndoEntry::PutDocument {
            collection: collection.to_string(),
            document_id: row_key.to_string(),
            surrogate,
            old_value: outcome.prior_value,
            bitemporal_sys_from_ms: outcome.bitemporal_sys_from_ms,
            bitemporal_index_tuples: outcome.bitemporal_index_tuples,
            // Empty today (this path passes `enable_side_indexes: false`, so
            // `apply_point_put` writes no plain secondary-index entries), but
            // wired now so rollback stays correct when that flag flips.
            secondary_index_added: outcome.secondary_index_added,
            secondary_index_removed: outcome.secondary_index_removed,
            chain_hash_prior,
        });

        // Reverse any HNSW vector inserts on rollback. Empty today (the
        // transactional path disables vector side-indexing via
        // `enable_side_indexes: false`), so this is a no-op until that flag
        // flips — wired now so it stays correct when it does.
        for (index_key, vector_id) in outcome.vector_inserts {
            undo_log.push(UndoEntry::InsertVector {
                index_key,
                vector_id,
            });
        }

        // Reverse any spatial R-tree inserts on rollback. Empty today (the
        // transactional path disables spatial side-indexing via
        // `enable_side_indexes: false`), so this is a no-op until that flag
        // flips — wired now so it stays correct when it does.
        for (key, entry_id) in outcome.spatial_inserts {
            undo_log.push(UndoEntry::SpatialInsert { key, entry_id });
        }

        if is_insert
            && let Some(config) = self.doc_configs.get(&config_key)
            && !config.enforcement.materialized_sum_sources.is_empty()
            && let Some(src_doc) = doc_format::decode_document(value)
        {
            let target_writes = materialized_sum::apply_materialized_sums(
                &self.sparse,
                database_id,
                tid,
                &config.enforcement.materialized_sum_sources,
                &src_doc,
            )?;
            for tw in target_writes {
                undo_log.push(UndoEntry::PutDocument {
                    collection: tw.collection,
                    document_id: tw.document_id,
                    surrogate: nodedb_types::Surrogate::ZERO,
                    old_value: tw.old_value,
                    bitemporal_sys_from_ms: None,
                    bitemporal_index_tuples: Vec::new(),
                    secondary_index_added: Vec::new(),
                    secondary_index_removed: Vec::new(),
                    chain_hash_prior: None,
                });
            }
        }

        Ok(self.response_ok(dummy_task))
    }

    /// Execute a PointDelete within a transaction.
    pub(super) fn tx_point_delete(
        &mut self,
        p: TxPointDelete<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxPointDelete {
            task: dummy_task,
            tid,
            collection,
            document_id,
            surrogate,
            user_roles,
        } = p;
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let database_id = dummy_task.request.database_id.as_u64();

        // Core delete path shared with the autocommit caller: bitemporal-vs-plain
        // primary tombstone/delete (including versioned index tombstones),
        // FTS/inverted removal, secondary-index cascade, graph-edge cascade,
        // doc_cache invalidation, and stateless DELETE enforcement. The EXTRA
        // cascades (spatial R-tree removal, mark_node_deleted) are disabled in
        // the transactional path — they have no undo variant yet, so enabling
        // them here would leave a rollback hole. `apply_point_delete` opens and
        // commits its own doc-store write txn internally.
        let outcome = self.apply_point_delete(PointDeleteParams {
            database_id,
            tid,
            collection,
            document_id,
            surrogate,
            user_roles,
            enforce: true,
            enable_extra_cascades: false,
        })?;

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Only push an undo entry when a row was actually removed — a delete
        // against a non-existent key has nothing to reverse.
        if let Some(old) = outcome.prior_value {
            undo_log.push(UndoEntry::DeleteDocument {
                collection: collection.to_string(),
                document_id: row_key.to_string(),
                old_value: old,
                bitemporal_sys_from_ms: outcome.bitemporal_sys_from_ms,
                bitemporal_index_tuples: outcome.bitemporal_index_tuples,
                // NON-empty on non-bitemporal deletes: the cascade removed these
                // plain secondary-index entries, so a rolled-back DELETE restores
                // them (closes the pre-existing tx-DELETE rollback hole).
                secondary_index_tuples: outcome.secondary_index_tuples,
                chain_hash_prior: None,
            });
        }

        // The delete-cleanup soft-deleted this document's vectors unconditionally
        // (fixing the orphan leak even in autocommit). In the transactional path
        // a rollback must restore them, so push one `DeleteVector` undo per
        // soft-deleted vector — `apply_undo_vector` `undelete`s each on rollback.
        for (index_key, vector_id) in outcome.vector_deletes {
            undo_log.push(UndoEntry::DeleteVector {
                index_key,
                vector_id,
            });
        }

        // Reverse any spatial R-tree removals on rollback. Empty today (the
        // transactional path disables the EXTRA cascade via
        // `enable_extra_cascades: false`, so `apply_point_delete` removes no
        // spatial entries), so this is a no-op until that flag flips — wired now
        // so it stays correct when it does.
        for (key, entry_id, bbox, document_id) in outcome.spatial_deletes {
            undo_log.push(UndoEntry::SpatialDelete {
                key,
                entry_id,
                bbox,
                document_id,
            });
        }
        Ok(self.response_ok(dummy_task))
    }
}
