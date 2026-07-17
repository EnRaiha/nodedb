// SPDX-License-Identifier: BUSL-1.1

//! Timeseries memtable flush to L1 partition segments.
//!
//! The boot-side counterpart — rebuilding `ts_registries` from the partitions
//! this writes — lives in `data::executor::timeseries_checkpoint`.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;
use crate::engine::timeseries::partition_registry::PartitionRegistry;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Flush a timeseries collection's memtable to L1 segments.
    ///
    /// Writes the partition via `ColumnarSegmentWriter`, drains the columnar
    /// memtable, registers the new partition in `ts_registries`, and fires the
    /// continuous aggregate hook.
    ///
    /// Returns `Ok(())` on success (including when the memtable is empty or
    /// absent — both are no-ops). Returns `Err` if the segment write fails;
    /// the caller is responsible for surfacing or propagating the error.
    ///
    /// ## Why the segment is written BEFORE the memtable is drained
    ///
    /// These rows have no durable copy but the WAL, and the coordinated
    /// checkpoint calls this flush and then reports the LSN that authorises
    /// deleting it. Draining first — as this did while its only callers were the
    /// ingest-path thresholds and the idle timer — meant an encode or write
    /// failure took the rows out of memory without putting them anywhere: the
    /// scan stopped returning them for the rest of the process's life, and only
    /// a restart's WAL replay brought them back. The partition is therefore
    /// written from a BORROW (`ColumnarMemtable::flush_view`) and the drain
    /// happens only once `write_partition` has returned `Ok` — its
    /// `partition.meta` write being the commit point. Every failure path now
    /// leaves the memtable exactly as it was, so a failed flush costs a retry
    /// while the caller's clamped checkpoint LSN keeps the WAL records behind
    /// it.
    pub(in crate::data::executor) fn flush_ts_collection(
        &mut self,
        tid: TenantId,
        database_id: DatabaseId,
        collection: &str,
        now_ms: i64,
    ) -> crate::Result<()> {
        let key = (database_id, tid, collection.to_string());
        let Some(mt) = self.columnar_memtables.get(&key) else {
            return Ok(());
        };
        if mt.is_empty() {
            return Ok(());
        }

        // Write to L1 segments.
        let segment_dir = super::paths::ts_collection_dir(
            &self.data_dir,
            database_id.as_u64(),
            tid.as_u64(),
            collection,
        );
        let writer = ColumnarSegmentWriter::new(&segment_dir);
        let view = mt.flush_view();
        let partition_name = format!("ts-{}_{}", view.min_ts, view.max_ts);

        // Use the max ingested WAL LSN for this collection so the partition
        // records which WAL records have been flushed. Read before the write and
        // never advanced by it: the ingest path stamps this only AFTER its own
        // flush calls return, so every row in the view is at or below it.
        let flush_wal_lsn = self.ts_max_ingested_lsn.get(&key).copied().unwrap_or(0);
        let ts_kek = self.segment_keks.ts_segment_kek.as_ref();
        let meta = writer
            .write_partition(&partition_name, &view, 0, flush_wal_lsn, ts_kek)
            .map_err(|e| crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!("columnar flush failed for collection {collection}: {e}"),
            })?;

        // ── Commit point passed: the rows are on disk and reachable ──────────
        let Some(mt) = self.columnar_memtables.get_mut(&key) else {
            return Err(crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!(
                    "timeseries memtable for collection {collection} vanished between the \
                     segment write and the drain"
                ),
            });
        };
        let drain = mt.drain();

        // The memtable is now empty — drop its memory reservation. The
        // reservation tracked the full resident footprint (kept current by
        // `recharge_ts_memtable_budget` after every ingest), so dropping the
        // token here releases exactly what was reserved. This replaces the
        // old `gov.release(Timeseries, memtable_bytes)` call, which released
        // the memtable footprint against a budget that ingest had only ever
        // charged a tiny per-batch estimate — an over-release on every flush.
        self.columnar_memtable_mem.remove(&key);

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

        // Fire continuous aggregate hook.
        let refreshed =
            self.continuous_agg_mgr
                .on_flush(database_id.as_u64(), collection, &drain, now_ms);
        if !refreshed.is_empty() {
            tracing::debug!(
                collection,
                aggregates = ?refreshed,
                "continuous aggregates refreshed on flush"
            );
        }

        Ok(())
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
