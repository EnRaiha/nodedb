// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for timeseries records.
//!
//! On startup, replays `TimeseriesBatch` records into the per-core
//! columnar memtable. Only replays records with LSN > `last_flushed_wal_lsn`
//! per partition (not max_ts — safe with out-of-order data).

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::TimeseriesIngestExec;
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::engine::timeseries::columnar_memtable::{
    ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use crate::types::DatabaseId;
use crate::types::ReadConsistency;
use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp, TimeseriesOp};
use nodedb_types::timeseries::MetricSample;

/// Default timeseries memtable configuration for replay and auto-creation.
fn default_ts_config() -> ColumnarMemtableConfig {
    ColumnarMemtableConfig {
        max_memory_bytes: 64 * 1024 * 1024,
        hard_memory_limit: 80 * 1024 * 1024,
        max_tag_cardinality: 100_000,
    }
}

impl CoreLoop {
    fn replay_task(
        tenant_id: crate::types::TenantId,
        database_id: DatabaseId,
        vshard_id: crate::types::VShardId,
        plan: PhysicalPlan,
    ) -> ExecutionTask {
        ExecutionTask {
            request: Request {
                request_id: crate::types::RequestId::new(0),
                tenant_id,
                database_id,
                vshard_id,
                plan,
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
                priority: Priority::Normal,
                trace_id: crate::types::TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
            },
            state: TaskState::Running,
        }
    }

    /// Ensure a timeseries memtable exists for the given collection, creating if needed.
    fn ensure_columnar_memtable(
        &mut self,
        key: (DatabaseId, crate::types::TenantId, String),
        schema: ColumnarSchema,
    ) {
        self.columnar_memtables
            .entry(key)
            .or_insert_with(|| ColumnarMemtable::new(schema, default_ts_config()));
    }

    fn replay_timeseries_payload(
        &mut self,
        tid: crate::types::TenantId,
        db_id: DatabaseId,
        collection: &str,
        payload: &[u8],
        record_lsn: u64,
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    ) -> usize {
        if let Ok(batch) =
            zerompk::from_msgpack::<nodedb_types::timeseries::TimeseriesWalBatch>(payload)
        {
            let key = (db_id, tid, collection.to_string());
            self.ensure_columnar_memtable(key.clone(), ColumnarSchema::metric_default());

            let Some(mt) = self.columnar_memtables.get_mut(&key) else {
                return 0;
            };
            for (series_id, timestamp_ms, value) in &batch.samples {
                mt.ingest_metric(
                    *series_id,
                    MetricSample {
                        timestamp_ms: *timestamp_ms,
                        value: *value,
                    },
                );
            }
            let sample_count = batch.samples.len();
            // Re-charge the engine memory budget to the memtable's resident
            // footprint after replaying these samples. The reservation is
            // held until the memtable is drained on flush, so a replay-driven
            // flush balances its release instead of over-releasing.
            self.recharge_ts_memtable_budget(tid, db_id, collection);
            return sample_count;
        }

        let format = if std::str::from_utf8(payload).is_ok() {
            "ilp"
        } else {
            "msgpack"
        };
        let task = Self::replay_task(
            tid,
            db_id,
            crate::types::VShardId::from_collection_in_database(db_id, collection),
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: collection.to_string(),
                payload: payload.to_vec(),
                format: format.to_string(),
                wal_lsn: Some(record_lsn),
                surrogates: Vec::new(),
                provenance: provenance.clone(),
            }),
        );
        let response = self.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid,
            collection,
            payload,
            format,
            wal_lsn: Some(record_lsn),
            provenance: provenance.as_ref(),
        });
        if response.status != crate::bridge::envelope::Status::Ok {
            tracing::warn!(
                "timeseries WAL replay failed for collection={collection} lsn={record_lsn}: {:?}",
                response.error_code
            );
            return 0;
        }
        match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(rows)) => rows.len(),
            Ok(nodedb_types::Value::Object(_)) => 1,
            _ => 0,
        }
    }

    fn replay_columnar_payload(
        &mut self,
        tid: crate::types::TenantId,
        db_id: DatabaseId,
        collection: &str,
        payload: &[u8],
        record_lsn: u64,
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    ) -> usize {
        let task = Self::replay_task(
            tid,
            db_id,
            crate::types::VShardId::from_collection_in_database(db_id, collection),
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: collection.to_string(),
                payload: payload.to_vec(),
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: provenance.clone(),
                wal_lsn: Some(record_lsn),
            }),
        );
        let response = self.execute_columnar_insert(
            &task,
            collection,
            payload,
            "msgpack",
            ColumnarInsertIntent::Insert,
            &[],
            &[],
            &[],
            provenance.as_ref(),
        );
        if response.status != crate::bridge::envelope::Status::Ok {
            tracing::warn!(
                "columnar WAL replay failed for collection={collection} lsn={record_lsn}: {:?}",
                response.error_code
            );
            return 0;
        }
        match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(rows)) => rows.len(),
            Ok(nodedb_types::Value::Object(_)) => 1,
            _ => 0,
        }
    }

    /// Replay WAL timeseries records to rebuild in-memory memtable state after crash.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Processes `TimeseriesBatch` records, ignoring records for other vShards.
    /// Uses LSN-based skip: only replays records with LSN > last flushed LSN.
    pub fn replay_timeseries_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::RecordType;

        let mut replayed = 0usize;
        let mut skipped = 0usize;

        for record in records {
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);

            let is_ts_batch = record_type == Some(RecordType::TimeseriesBatch);
            if !is_ts_batch {
                continue;
            }

            // Route by vShard to the correct core.
            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                skipped += 1;
                continue;
            }

            // Decode 4-tuple (with provenance), falling back to 3-tuple then
            // 2-tuple for backwards compatibility with older WAL records.
            // Records iterate in LSN order (guaranteed by the WAL segment layout),
            // so provenance-aware replay naturally processes seq in order.
            let decoded = zerompk::from_msgpack::<(
                String,
                String,
                Vec<u8>,
                Option<nodedb_types::sync::wire::SyncProvenance>,
            )>(&record.payload)
            .map(|(kind, collection, payload, prov)| (Some(kind), collection, payload, prov))
            .or_else(|_| {
                zerompk::from_msgpack::<(String, String, Vec<u8>)>(&record.payload)
                    .map(|(kind, collection, payload)| (Some(kind), collection, payload, None))
            })
            .or_else(|_| {
                zerompk::from_msgpack::<(String, Vec<u8>)>(&record.payload)
                    .map(|(collection, payload)| (None, collection, payload, None))
            });
            let Ok((kind, raw_collection, payload, record_provenance)) = decoded else {
                tracing::warn!(
                    core = self.core_id,
                    lsn = record.header.lsn,
                    "skipping malformed TimeseriesBatch WAL record"
                );
                continue;
            };

            let tenant_id = record.header.tenant_id;
            let tid_id = crate::types::TenantId::new(tenant_id);
            let db_id = DatabaseId::new(record.header.database_id);
            let collection = raw_collection.as_str();
            let key = (db_id, tid_id, raw_collection.clone());

            let record_lsn = record.header.lsn;

            // Skip records for collections that were hard-deleted after
            // this write. Otherwise the purged memtable would resurrect.
            if tombstones.is_tombstoned(tenant_id, collection, record_lsn) {
                skipped += 1;
                continue;
            }

            // Check if this record was already flushed (LSN-based skip).
            if let Some(registry) = self.ts_registries.get(&key) {
                // Find the max flushed LSN across all partitions.
                let max_flushed_lsn = registry
                    .iter()
                    .map(|(_, e)| e.meta.last_flushed_wal_lsn)
                    .max()
                    .unwrap_or(0);
                if record_lsn <= max_flushed_lsn {
                    skipped += 1;
                    continue;
                }
            }

            // Track the max WAL LSN ingested per collection for flush metadata.
            if let Some(entry) = self.ts_max_ingested_lsn.get_mut(&key) {
                *entry = (*entry).max(record_lsn);
            } else {
                self.ts_max_ingested_lsn.insert(key.clone(), record_lsn);
            }

            let accepted = match kind.as_deref() {
                Some("columnar") => self.replay_columnar_payload(
                    tid_id,
                    db_id,
                    collection,
                    &payload,
                    record_lsn,
                    record_provenance,
                ),
                Some("timeseries") | None => self.replay_timeseries_payload(
                    tid_id,
                    db_id,
                    collection,
                    &payload,
                    record_lsn,
                    record_provenance,
                ),
                Some(other) => {
                    tracing::warn!(
                        core = self.core_id,
                        lsn = record_lsn,
                        kind = other,
                        "skipping unknown TimeseriesBatch WAL kind"
                    );
                    0
                }
            };
            if accepted == 0 {
                continue;
            }
            replayed += accepted;
        }

        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                skipped,
                collections = self.columnar_memtables.len(),
                "WAL timeseries replay complete"
            );
        }
    }
}
