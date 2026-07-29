// SPDX-License-Identifier: BUSL-1.1

//! Per-engine undo entry application logic.
//!
//! Each `apply_undo_*` method handles one engine family's undo entries.
//! All methods return `Err((entry_index, detail))` on fatal failure so the
//! caller can escalate to a typed `RollbackFailed` response.

use nodedb_types::Surrogate;
use tracing::error;

use crate::data::executor::core_loop::CoreLoop;

use super::{TimeseriesIngestUndo, UndoEntry};

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
            _ => Err((
                entry_index,
                "apply_undo_vector called with non-vector entry".to_string(),
            )),
        }
    }

    // ── Graph ────────────────────────────────────────────────────────────────

    #[cfg(test)]
    pub(super) fn apply_undo_edge(
        &mut self,
        did: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        self.apply_undo_edge_with_stats(did, tid, entry_index, entry, true)
    }

    pub(super) fn apply_undo_edge_with_stats(
        &mut self,
        did: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
        account_stats: bool,
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
                        .put_edge_versioned_with_stats(
                            edge_ref,
                            &old_props,
                            ord,
                            valid_from_ms,
                            i64::MAX,
                            account_stats,
                        )
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
                    let weight =
                        crate::engine::graph::csr::extract_weight_from_properties(&old_props);
                    let partition = self.csr_partition_mut(did, tid);
                    partition.remove_edge_in_collection(&src_id, &label, &dst_id, &collection);
                    let csr_res = if weight != 1.0 {
                        partition.add_edge_weighted_in_collection(
                            &src_id,
                            &label,
                            &dst_id,
                            &collection,
                            weight,
                        )
                    } else {
                        partition.add_edge_in_collection(&src_id, &label, &dst_id, &collection)
                    };
                    csr_res.map_err(|e| {
                        let detail =
                            format!("CSR restore {collection} {src_id}-[{label}]->{dst_id}: {e}");
                        error!(
                            core = self.core_id, entry_index,
                            error = %detail,
                            "transaction undo: CSR restore failed after edge_store restore; \
                             shard state unknown"
                        );
                        (entry_index, detail)
                    })?;
                } else {
                    self.edge_store
                        .soft_delete_edge_with_stats(edge_ref, ord, account_stats)
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
                    self.csr_partition_mut(did, tid).remove_edge_in_collection(
                        &src_id,
                        &label,
                        &dst_id,
                        &collection,
                    );
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
                // The cascade that produced this entry dropped the endpoints'
                // durable identity bindings. The in-memory CSR still holds
                // them, so restoring the edge restores the binding with it —
                // otherwise a rolled-back delete would leave the graph intact
                // but invisible to every cross-engine read after a restart.
                let (src_surrogate, dst_surrogate) = self
                    .csr_partition(did, tid)
                    .map(|p| {
                        (
                            p.node_surrogate(&src_id).unwrap_or(Surrogate::ZERO),
                            p.node_surrogate(&dst_id).unwrap_or(Surrogate::ZERO),
                        )
                    })
                    .unwrap_or((Surrogate::ZERO, Surrogate::ZERO));
                self.edge_store
                    .put_edge_versioned_with_stats(
                        EdgeRef::new(database, tenant, &collection, &src_id, &label, &dst_id)
                            .with_surrogates(src_surrogate, dst_surrogate),
                        &old_properties,
                        ord,
                        valid_from_ms,
                        i64::MAX,
                        account_stats,
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
                    partition.add_edge_weighted_in_collection(
                        &src_id,
                        &label,
                        &dst_id,
                        &collection,
                        weight,
                    )
                } else {
                    partition.add_edge_in_collection(&src_id, &label, &dst_id, &collection)
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
            _ => Err((
                entry_index,
                "apply_undo_edge called with non-edge entry".to_string(),
            )),
        }
    }

    // ── Columnar ─────────────────────────────────────────────────────────────

    pub(super) fn apply_undo_columnar(
        &mut self,
        entry_index: usize,
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
            UndoEntry::ColumnarUpdate {
                collection_key,
                row_count_before,
                inserted_pks,
                displaced,
                restored,
            } => {
                if let Some(engine) = self.columnar_engines.get_mut(&collection_key) {
                    // 1. Remove the appended replacement rows (mirrors ColumnarInsert).
                    engine.rollback_memtable_inserts(row_count_before, &inserted_pks, &displaced);
                    // 2. Restore the tombstoned originals.
                    engine.restore_deleted_rows(&restored);
                }
                // Engine absent: no in-memory state to roll back.
                Ok(())
            }
            UndoEntry::ColumnarDelete {
                collection_key,
                restored,
            } => {
                if let Some(engine) = self.columnar_engines.get_mut(&collection_key) {
                    engine.restore_deleted_rows(&restored);
                }
                // Engine absent: no in-memory state to roll back.
                Ok(())
            }
            _ => Err((
                entry_index,
                "apply_undo_columnar called with non-columnar entry".to_string(),
            )),
        }
    }

    // ── Timeseries ───────────────────────────────────────────────────────────

    pub(super) fn apply_undo_timeseries(
        &mut self,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::TimeseriesIngest(token) => {
                self.restore_timeseries_ingest_preimage(entry_index, token)
            }
            _ => Err((
                entry_index,
                "apply_undo_timeseries called with non-timeseries entry".to_string(),
            )),
        }
    }

    fn restore_timeseries_ingest_preimage(
        &mut self,
        entry_index: usize,
        token: TimeseriesIngestUndo,
    ) -> Result<(), (usize, String)> {
        let TimeseriesIngestUndo {
            collection_key,
            memtable_before,
            memtable_config_before,
            memtable_memory_bytes_before,
            last_value_cache_before,
            max_ingested_lsn_before,
            last_ts_ingest_before,
            reservation_bytes_before,
        } = token;

        // Commit-deferred ingest must not touch reservations. Treat a mismatch
        // as fatal rather than dropping/recharging a token and corrupting the
        // governor's accounting during a failed transaction.
        let reservation_now = self
            .columnar_memtable_mem
            .get(&collection_key)
            .map(nodedb_mem::ReservationToken::size);
        if reservation_now != reservation_bytes_before {
            return Err((
                entry_index,
                format!(
                    "timeseries reservation changed during deferred ingest for {:?}: before {:?}, now {:?}",
                    collection_key, reservation_bytes_before, reservation_now
                ),
            ));
        }

        match (
            memtable_before,
            memtable_config_before,
            memtable_memory_bytes_before,
        ) {
            (Some(snapshot), Some(config), Some(memory_bytes)) => {
                let mut restored =
                    crate::engine::timeseries::columnar_memtable::ColumnarMemtable::from_snapshot(
                        snapshot, config,
                    )
                    .map_err(|error| {
                        (
                            entry_index,
                            format!("timeseries memtable snapshot restore failed: {error}"),
                        )
                    })?;
                restored.restore_memory_bytes_for_undo(memory_bytes);
                self.columnar_memtables
                    .insert(collection_key.clone(), restored);
            }
            (None, None, None) => {
                self.columnar_memtables.remove(&collection_key);
            }
            _ => {
                return Err((
                    entry_index,
                    "timeseries undo token has inconsistent memtable pre-image fields".into(),
                ));
            }
        }

        match last_value_cache_before {
            Some(cache) => {
                self.ts_last_value_caches
                    .insert(collection_key.clone(), cache);
            }
            None => {
                self.ts_last_value_caches.remove(&collection_key);
            }
        }
        match max_ingested_lsn_before {
            Some(lsn) => {
                self.ts_max_ingested_lsn.insert(collection_key, lsn);
            }
            None => {
                self.ts_max_ingested_lsn.remove(&collection_key);
            }
        }
        self.last_ts_ingest = last_ts_ingest_before;
        Ok(())
    }
}
