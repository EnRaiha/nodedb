// SPDX-License-Identifier: BUSL-1.1

//! Document PointPut and PointDelete helpers for transaction sub-plans.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::{
    append_only, hash_chain, materialized_sum, period_lock, retention,
};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a PointPut within a transaction.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn tx_point_put(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        collection: &str,
        document_id: &str,
        surrogate: nodedb_types::Surrogate,
        value: &[u8],
        undo_log: &mut Vec<UndoEntry>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
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
                if chained.is_some() {
                    match &chain_hash_prior {
                        Some(None) => {
                            self.chain_hashes.remove(&config_key);
                        }
                        Some(Some(prev)) => {
                            self.chain_hashes.insert(config_key.clone(), prev.clone());
                        }
                        None => {}
                    }
                }
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
            chain_hash_prior,
        });

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
                    chain_hash_prior: None,
                });
            }
        }

        Ok(self.response_ok(dummy_task))
    }

    /// Execute a PointDelete within a transaction.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn tx_point_delete(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        collection: &str,
        document_id: &str,
        surrogate: nodedb_types::Surrogate,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let _ = document_id;
        let config_key = (TenantId::new(tid), collection.to_string());
        let database_id = dummy_task.request.database_id.as_u64();
        let old_value = self
            .sparse
            .get(database_id, tid, collection, row_key)
            .ok()
            .flatten();
        if let Some(config) = self.doc_configs.get(&config_key) {
            append_only::check_point_delete(collection, &config.enforcement)?;
            if let Some(ref pl) = config.enforcement.period_lock
                && let Some(ref old_bytes) = old_value
            {
                period_lock::check_period_lock(
                    &self.sparse,
                    database_id,
                    tid,
                    collection,
                    old_bytes,
                    pl,
                )?;
            }
            let created_at = old_value
                .as_ref()
                .and_then(|b| retention::extract_created_at_secs(b));
            retention::check_delete_allowed(collection, &config.enforcement, created_at)?;
        }
        match self.sparse.delete(database_id, tid, collection, row_key) {
            Ok(_) => {
                if let Some(s) = crate::engine::document::store::doc_id_to_surrogate(row_key) {
                    let _ = self.inverted.remove_document(
                        database_id,
                        TenantId::new(tid),
                        collection,
                        s,
                    );
                }
                let _ =
                    self.sparse
                        .delete_indexes_for_document(database_id, tid, collection, row_key);
                let edges_removed = self
                    .csr_partition_mut(database_id, tid)
                    .remove_node_edges(row_key);
                if edges_removed > 0 {
                    let cascade_ord = self.hlc.next_ordinal();
                    let _ = self.edge_store.delete_edges_for_node(
                        database_id,
                        nodedb_types::TenantId::new(tid),
                        row_key,
                        cascade_ord,
                    );
                }

                if let Some(old) = old_value {
                    undo_log.push(UndoEntry::DeleteDocument {
                        collection: collection.to_string(),
                        document_id: row_key.to_string(),
                        old_value: old,
                        bitemporal_sys_from_ms: None,
                        bitemporal_index_tuples: Vec::new(),
                        chain_hash_prior: None,
                    });
                }
                Ok(self.response_ok(dummy_task))
            }
            Err(e) => Err(ErrorCode::Internal {
                detail: e.to_string(),
            }),
        }
    }
}
