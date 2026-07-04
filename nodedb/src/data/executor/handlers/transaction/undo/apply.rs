// SPDX-License-Identifier: BUSL-1.1

//! Per-engine undo entry application logic.
//!
//! Each `apply_undo_*` method handles one engine family's undo entries.
//! All methods return `Err((entry_index, detail))` on fatal failure so the
//! caller can escalate to a typed `RollbackFailed` response.

use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::kv::current_ms;

use super::UndoEntry;

impl CoreLoop {
    // ── Vector ───────────────────────────────────────────────────────────────

    pub(super) fn apply_undo_vector(
        &mut self,
        _tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::InsertVector {
                index_key,
                vector_id,
                collection,
                field,
                doc_id,
            } => match self.vector_collections.get_mut(&index_key) {
                Some(index) => {
                    index.delete(vector_id);
                    // Reverse the forward insert's `vector_doc_map` write —
                    // without this a rolled-back insert leaves a stale
                    // doc→vector_id mapping behind (unbounded leak), mirroring
                    // `apply_undo_spatial`'s `spatial_doc_map.remove`. Empty
                    // `doc_id` marks the direct primary-vector write path
                    // (`PhysicalPlan::Vector`), which never populates
                    // `vector_doc_map` — skip the mutation for that path.
                    if !doc_id.is_empty() {
                        self.vector_doc_map.remove(&(
                            index_key.0,
                            index_key.1,
                            collection,
                            field,
                            doc_id,
                        ));
                    }
                    Ok(())
                }
                None => {
                    let detail = format!(
                        "vector index {:?} not found during undo of vector insert {}",
                        index_key, vector_id
                    );
                    error!(
                        core = self.core_id,
                        entry_index,
                        error = %detail,
                        "transaction undo: vector index missing; shard state unknown"
                    );
                    Err((entry_index, detail))
                }
            },
            UndoEntry::DeleteVector {
                index_key,
                vector_id,
                collection,
                field,
                doc_id,
            } => match self.vector_collections.get_mut(&index_key) {
                Some(index) => {
                    index.undelete(vector_id);
                    // Restore the `vector_doc_map` entry the forward delete
                    // removed — without this a rolled-back delete leaves the
                    // doc→vector reverse lookup missing, so a later delete of
                    // the same document can never find (and soft-delete) its
                    // vector: a permanent orphan. Mirrors
                    // `apply_undo_spatial`'s `spatial_doc_map.insert`. Empty
                    // `doc_id` marks the direct primary-vector write path,
                    // which never populates `vector_doc_map` — skip it there.
                    if !doc_id.is_empty() {
                        self.vector_doc_map.insert(
                            (index_key.0, index_key.1, collection, field, doc_id),
                            vector_id,
                        );
                    }
                    Ok(())
                }
                None => {
                    let detail = format!(
                        "vector index {:?} not found during undo of vector delete {}",
                        index_key, vector_id
                    );
                    error!(
                        core = self.core_id,
                        entry_index,
                        error = %detail,
                        "transaction undo: vector index missing; shard state unknown"
                    );
                    Err((entry_index, detail))
                }
            },
            _ => unreachable!("apply_undo_vector called with non-vector entry"),
        }
    }

    // ── Graph ────────────────────────────────────────────────────────────────

    pub(super) fn apply_undo_edge(
        &mut self,
        did: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        use crate::engine::graph::edge_store::EdgeRef;
        let database = nodedb_types::DatabaseId::new(did);
        match entry {
            UndoEntry::PutEdge {
                collection,
                src_id,
                label,
                dst_id,
                old_properties,
            } => {
                let tenant = nodedb_types::TenantId::new(tid);
                let ord = self.hlc.next_ordinal();
                let edge_ref =
                    EdgeRef::new(database, tenant, &collection, &src_id, &label, &dst_id);
                if let Some(old_props) = old_properties {
                    let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
                    self.edge_store
                        .put_edge_versioned(edge_ref, &old_props, ord, valid_from_ms, i64::MAX)
                        .map_err(|e| {
                            let detail = format!(
                                "edge restore {collection} {src_id}-[{label}]->{dst_id}: {e}"
                            );
                            error!(
                                core = self.core_id, entry_index,
                                error = %detail,
                                "transaction undo: edge restore failed; shard state unknown"
                            );
                            (entry_index, detail)
                        })?;
                } else {
                    self.edge_store
                        .soft_delete_edge(edge_ref, ord)
                        .map_err(|e| {
                            let detail = format!(
                                "edge tombstone {collection} {src_id}-[{label}]->{dst_id}: {e}"
                            );
                            error!(
                                core = self.core_id, entry_index,
                                error = %detail,
                                "transaction undo: edge tombstone failed; shard state unknown"
                            );
                            (entry_index, detail)
                        })?;
                    self.csr_partition_mut(did, tid)
                        .remove_edge(&src_id, &label, &dst_id);
                }
                Ok(())
            }
            UndoEntry::DeleteEdge {
                collection,
                src_id,
                label,
                dst_id,
                old_properties,
            } => {
                let tenant = nodedb_types::TenantId::new(tid);
                let ord = self.hlc.next_ordinal();
                let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
                self.edge_store
                    .put_edge_versioned(
                        EdgeRef::new(database, tenant, &collection, &src_id, &label, &dst_id),
                        &old_properties,
                        ord,
                        valid_from_ms,
                        i64::MAX,
                    )
                    .map_err(|e| {
                        let detail = format!(
                            "edge re-insert {collection} {src_id}-[{label}]->{dst_id}: {e}"
                        );
                        error!(
                            core = self.core_id, entry_index,
                            error = %detail,
                            "transaction undo: edge re-insert failed; shard state unknown"
                        );
                        (entry_index, detail)
                    })?;
                let weight =
                    crate::engine::graph::csr::extract_weight_from_properties(&old_properties);
                let partition = self.csr_partition_mut(did, tid);
                let csr_res = if weight != 1.0 {
                    partition.add_edge_weighted(&src_id, &label, &dst_id, weight)
                } else {
                    partition.add_edge(&src_id, &label, &dst_id)
                };
                csr_res.map_err(|e| {
                    let detail = format!("CSR re-insert {src_id}-[{label}]->{dst_id}: {e}");
                    error!(
                        core = self.core_id, entry_index,
                        error = %detail,
                        "transaction undo: CSR re-insert failed after edge_store restore; \
                         shard state unknown"
                    );
                    (entry_index, detail)
                })
            }
            _ => unreachable!("apply_undo_edge called with non-edge entry"),
        }
    }

    // ── KV ───────────────────────────────────────────────────────────────────

    pub(super) fn apply_undo_kv(
        &mut self,
        did: u64,
        tid: u64,
        _entry_index: usize,
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
                    self.kv_engine.put(
                        did,
                        tid,
                        &collection,
                        &key,
                        &old,
                        0,
                        now_ms,
                        nodedb_types::Surrogate::ZERO,
                    );
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
                self.kv_engine.put(
                    did,
                    tid,
                    &collection,
                    &key,
                    &prior_value,
                    0,
                    now_ms,
                    nodedb_types::Surrogate::ZERO,
                );
                Ok(())
            }
            UndoEntry::KvBatchPut {
                collection,
                entries,
            } => {
                let now_ms = current_ms();
                for (key, prior_value) in entries {
                    if let Some(old) = prior_value {
                        self.kv_engine.put(
                            did,
                            tid,
                            &collection,
                            &key,
                            &old,
                            0,
                            now_ms,
                            nodedb_types::Surrogate::ZERO,
                        );
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
                self.kv_engine.put(
                    did,
                    tid,
                    &collection,
                    &source_key,
                    &source_prior,
                    0,
                    now_ms,
                    nodedb_types::Surrogate::ZERO,
                );
                if let Some(old) = dest_prior {
                    self.kv_engine.put(
                        did,
                        tid,
                        &collection,
                        &dest_key,
                        &old,
                        0,
                        now_ms,
                        nodedb_types::Surrogate::ZERO,
                    );
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
                self.kv_engine.put(
                    did,
                    tid,
                    &source_collection,
                    &item_key,
                    &source_prior,
                    0,
                    now_ms,
                    nodedb_types::Surrogate::ZERO,
                );
                // Undo the dest write.
                if let Some(old) = dest_prior {
                    self.kv_engine.put(
                        did,
                        tid,
                        &dest_collection,
                        &dest_key,
                        &old,
                        0,
                        now_ms,
                        nodedb_types::Surrogate::ZERO,
                    );
                } else {
                    self.kv_engine
                        .delete(did, tid, &dest_collection, &[dest_key], now_ms);
                }
                Ok(())
            }
            _ => unreachable!("apply_undo_kv called with non-kv entry"),
        }
    }

    // ── Columnar ─────────────────────────────────────────────────────────────

    pub(super) fn apply_undo_columnar(
        &mut self,
        _entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::ColumnarInsert {
                collection_key,
                row_count_before,
                inserted_pks,
                displaced,
            } => {
                match self.columnar_engines.get_mut(&collection_key) {
                    Some(engine) => {
                        engine.rollback_memtable_inserts(
                            row_count_before,
                            &inserted_pks,
                            &displaced,
                        );
                        Ok(())
                    }
                    None => {
                        // Engine absent: no in-memory state to roll back.
                        // This is safe — if the engine was never created, no rows were inserted.
                        Ok(())
                    }
                }
            }
            _ => unreachable!("apply_undo_columnar called with non-columnar entry"),
        }
    }

    // ── Timeseries ───────────────────────────────────────────────────────────

    pub(super) fn apply_undo_timeseries(
        &mut self,
        _entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::TimeseriesIngest {
                collection_key,
                row_count_before,
            } => {
                if let Some(mt) = self.columnar_memtables.get_mut(&collection_key) {
                    mt.truncate_to(row_count_before);
                }
                // If memtable is absent, no rows were ingested — nothing to undo.
                Ok(())
            }
            _ => unreachable!("apply_undo_timeseries called with non-timeseries entry"),
        }
    }
}
