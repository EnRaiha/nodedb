// SPDX-License-Identifier: BUSL-1.1

//! Document PointPut and PointDelete helpers for transaction sub-plans.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::{
    append_only, hash_chain, materialized_sum, period_lock, retention, state_transition,
    transition_check,
};
use crate::data::executor::handlers::document::extract_indexable_text;
use crate::data::executor::strict_format;
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
        let old_value = self
            .sparse
            .get(database_id, tid, collection, row_key)
            .ok()
            .flatten();

        let config_key = (TenantId::new(tid), collection.to_string());
        if let Some(config) = self.doc_configs.get(&config_key) {
            append_only::check_point_put(collection, &config.enforcement, &old_value)?;
            if let Some(ref pl) = config.enforcement.period_lock {
                period_lock::check_period_lock(
                    &self.sparse,
                    database_id,
                    tid,
                    collection,
                    value,
                    pl,
                )?;
            }
            if old_value.is_some() {
                let old_json = old_value
                    .as_ref()
                    .and_then(|b| doc_format::decode_document(b));
                let new_json = doc_format::decode_document(value);
                if let (Some(old_doc), Some(new_doc)) = (&old_json, &new_json) {
                    if !config.enforcement.state_constraints.is_empty() {
                        state_transition::check_state_transitions(
                            collection,
                            &config.enforcement.state_constraints,
                            old_doc,
                            new_doc,
                            user_roles,
                        )?;
                    }
                    if !config.enforcement.transition_checks.is_empty() {
                        transition_check::check_transition_predicates(
                            collection,
                            &config.enforcement.transition_checks,
                            old_doc,
                            new_doc,
                        )?;
                    }
                }
            }
        }

        let encode_for_storage = |bytes: &[u8]| -> Result<Vec<u8>, ErrorCode> {
            if let Some(config) = self.doc_configs.get(&config_key)
                && let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                    config.storage_mode
            {
                strict_format::bytes_to_binary_tuple(bytes, schema).map_err(|e| {
                    ErrorCode::Internal {
                        detail: format!("strict encode: {e}"),
                    }
                })
            } else {
                Ok(doc_format::canonicalize_document_for_storage(bytes))
            }
        };

        let stored = if old_value.is_none() {
            let hash_chain_enabled = self
                .doc_configs
                .get(&config_key)
                .is_some_and(|c| c.enforcement.hash_chain);
            match hash_chain::apply_chain_on_insert(
                &mut self.chain_hashes,
                tid,
                collection,
                document_id,
                value,
                hash_chain_enabled,
            ) {
                Some(chained) => chained,
                None => encode_for_storage(value)?,
            }
        } else {
            encode_for_storage(value)?
        };
        match self
            .sparse
            .put(database_id, tid, collection, row_key, &stored)
        {
            Ok(_prior) => {
                if let Some(doc) = doc_format::decode_document(value) {
                    let text_content = extract_indexable_text(&doc);
                    if !text_content.is_empty() {
                        let _ = self.inverted.index_document(
                            database_id,
                            TenantId::new(tid),
                            collection,
                            surrogate,
                            &text_content,
                        );
                    }
                }

                undo_log.push(UndoEntry::PutDocument {
                    collection: collection.to_string(),
                    document_id: row_key.to_string(),
                    surrogate,
                    old_value: old_value.clone(),
                    bitemporal_sys_from_ms: None,
                    bitemporal_index_tuples: Vec::new(),
                    chain_hash_prior: None,
                });

                if old_value.is_none()
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
            Err(e) => Err(ErrorCode::Internal {
                detail: e.to_string(),
            }),
        }
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
