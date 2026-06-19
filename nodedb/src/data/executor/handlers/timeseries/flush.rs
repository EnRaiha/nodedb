// SPDX-License-Identifier: BUSL-1.1

//! Timeseries flush and partition registry management.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;
use crate::engine::timeseries::partition_registry::PartitionRegistry;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Ensure the partition registry is loaded for a timeseries collection.
    ///
    /// On first access, scans the scoped segment directory for existing
    /// partition directories and populates the registry from partition metadata.
    pub(in crate::data::executor) fn ensure_ts_registry(
        &mut self,
        tid: TenantId,
        database_id: DatabaseId,
        collection: &str,
    ) -> crate::Result<()> {
        let key = (database_id, tid, collection.to_string());
        if self.ts_registries.contains_key(&key) {
            return Ok(());
        }
        let ts_dir = super::paths::ts_collection_dir(
            &self.data_dir,
            database_id.as_u64(),
            tid.as_u64(),
            collection,
        );
        if !ts_dir.exists() {
            return Ok(());
        }

        let mut registry = PartitionRegistry::new(
            nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
        );

        // Scan for existing partition directories.
        if let Ok(entries) = std::fs::read_dir(&ts_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name_str) = name.to_str() else {
                    continue;
                };
                if !name_str.starts_with("ts-") || !entry.path().is_dir() {
                    continue;
                }
                let meta_path = entry.path().join("partition.meta");
                if let Ok(meta_bytes) = std::fs::read(&meta_path)
                    && let Ok(meta) =
                        sonic_rs::from_slice::<nodedb_types::timeseries::PartitionMeta>(&meta_bytes)
                {
                    let pe = crate::engine::timeseries::partition_registry::PartitionEntry {
                        meta,
                        dir_name: name_str.to_string(),
                    };
                    registry.import(vec![(pe.meta.min_ts, pe)]);
                }
            }
        }

        if registry.partition_count() > 0 {
            tracing::info!(
                collection,
                partitions = registry.partition_count(),
                "loaded partition registry from disk"
            );
        }
        self.ts_registries.insert(key, registry);
        Ok(())
    }

    /// Flush a timeseries collection's memtable to L1 segments.
    ///
    /// Drains the columnar memtable, writes segments via `ColumnarSegmentWriter`,
    /// registers the new partition in `ts_registries`, and fires the continuous
    /// aggregate hook.
    pub(in crate::data::executor) fn flush_ts_collection(
        &mut self,
        tid: TenantId,
        database_id: DatabaseId,
        collection: &str,
        now_ms: i64,
    ) {
        let key = (database_id, tid, collection.to_string());
        let Some(mt) = self.columnar_memtables.get_mut(&key) else {
            return;
        };
        if mt.is_empty() {
            return;
        }

        let drain = mt.drain();

        // The memtable is now empty — drop its memory reservation. The
        // reservation tracked the full resident footprint (kept current by
        // `recharge_ts_memtable_budget` after every ingest), so dropping the
        // token here releases exactly what was reserved. This replaces the
        // old `gov.release(Timeseries, memtable_bytes)` call, which released
        // the memtable footprint against a budget that ingest had only ever
        // charged a tiny per-batch estimate — an over-release on every flush.
        self.columnar_memtable_mem.remove(&key);

        // Write to L1 segments.
        let segment_dir = super::paths::ts_collection_dir(
            &self.data_dir,
            database_id.as_u64(),
            tid.as_u64(),
            collection,
        );
        let writer = ColumnarSegmentWriter::new(&segment_dir);
        let partition_name = format!("ts-{}_{}", drain.min_ts, drain.max_ts);

        // Use the max ingested WAL LSN for this collection so the partition
        // records which WAL records have been flushed.
        let flush_wal_lsn = self.ts_max_ingested_lsn.get(&key).copied().unwrap_or(0);
        let ts_kek = self.ts_segment_kek.as_ref();
        match writer.write_partition(&partition_name, &drain, 0, flush_wal_lsn, ts_kek) {
            Ok(meta) => {
                tracing::info!(
                    collection,
                    rows = meta.row_count,
                    "timeseries columnar flush complete"
                );

                let registry = self.ts_registries.entry(key).or_insert_with(|| {
                    PartitionRegistry::new(
                        nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
                    )
                });
                let mut reg_meta = meta;
                reg_meta.min_ts = drain.min_ts;
                reg_meta.max_ts = drain.max_ts;
                reg_meta.state = nodedb_types::timeseries::PartitionState::Sealed;
                let pe = crate::engine::timeseries::partition_registry::PartitionEntry {
                    meta: reg_meta,
                    dir_name: partition_name,
                };
                registry.import(vec![(drain.min_ts, pe)]);
            }
            Err(e) => {
                tracing::error!(
                    collection,
                    error = %e,
                    "timeseries columnar flush failed"
                );
                return;
            }
        }

        // Fire continuous aggregate hook.
        let refreshed = self.continuous_agg_mgr.on_flush(collection, &drain, now_ms);
        if !refreshed.is_empty() {
            tracing::debug!(
                collection,
                aggregates = ?refreshed,
                "continuous aggregates refreshed on flush"
            );
        }
    }

    /// Re-charge the engine memory budget for a timeseries memtable's
    /// current resident footprint.
    ///
    /// Called after every ingest into `collection`'s memtable (ILP/JSON/
    /// msgpack ingest and WAL replay). Drops the previous reservation — so
    /// the budget tracks the memtable's net `memory_bytes()`, not the sum
    /// of every recharge — then takes a fresh one. If the reservation
    /// can't be granted (budget exhausted), the memtable runs un-accounted
    /// until the next flush: an under-count, never an over-release. The
    /// pre-flush-on-pressure check in the ingest path already tries to
    /// drain before reaching here, and `flush_ts_collection` drops the
    /// reservation when it drains the memtable.
    pub(in crate::data::executor) fn recharge_ts_memtable_budget(
        &mut self,
        tid: TenantId,
        db_id: DatabaseId,
        collection: &str,
    ) {
        let gov = match &self.governor {
            Some(g) => g.clone(),
            None => return,
        };
        let key = (db_id, tid, collection.to_string());
        let bytes = match self.columnar_memtables.get(&key) {
            Some(mt) => mt.memory_bytes(),
            None => {
                self.columnar_memtable_mem.remove(&key);
                return;
            }
        };
        // Release the prior reservation first so a recharge of an
        // unchanged memtable nets to zero rather than double-counting.
        self.columnar_memtable_mem.remove(&key);
        if bytes == 0 {
            return;
        }
        if let Ok(token) = gov.try_reserve(db_id, tid, nodedb_mem::EngineId::Timeseries, bytes) {
            self.columnar_memtable_mem.insert(key, token);
        }
    }
}
