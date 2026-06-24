// SPDX-License-Identifier: BUSL-1.1

//! Timeseries ILP ingest handler.
//!
//! msgpack / JSON row ingests that normalize into ILP text live in the
//! sibling `ingest_formats` module.

use std::collections::HashMap;

use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnarMemtable, ColumnarMemtableConfig,
};
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;

/// Parameters for a timeseries ingest operation on the Data Plane.
///
/// Bundles the non-`self` arguments to `execute_timeseries_ingest` so the
/// method stays within the argument-count limit.
pub(in crate::data::executor) struct TimeseriesIngestExec<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    pub wal_lsn: Option<u64>,
    pub provenance: Option<&'a SyncProvenance>,
}

impl CoreLoop {
    /// Execute a timeseries ingest.
    ///
    /// When `provenance` is `Some`, the sync idempotency gate runs first:
    /// - Duplicate / Fenced / Gap → return `SyncAckResult` via `response_with_payload`
    ///   without re-applying engine state.
    /// - Apply → continue; after the memtable write call `sync_commit` to
    ///   advance the HWM, then return `SyncAckResult{Applied}` via payload.
    ///
    /// When `provenance` is `None` (SQL / ILP paths), behave exactly as
    /// before: no gate, no `SyncAckResult` in the payload.
    ///
    /// `wal_lsn` deduplication (last-flushed skip) is preserved on the Apply
    /// branch: if the record is already on disk the memtable write is skipped,
    /// but `sync_commit` still advances the HWM because the record WAS
    /// applied (durably flushed to a segment).
    pub(in crate::data::executor) fn execute_timeseries_ingest(
        &mut self,
        args: TimeseriesIngestExec<'_>,
    ) -> Response {
        let TimeseriesIngestExec {
            task,
            tid,
            collection,
            payload,
            format,
            wal_lsn,
            provenance,
        } = args;
        // ── Sync idempotency gate (Data-Plane side) ──────────────────────────
        if let Some(prov) = provenance {
            let admit = self.sync_admit(prov);
            match admit {
                SyncAdmit::Apply => {
                    // Fall through to the ingest path below.
                    // sync_commit is called AFTER the memtable write.
                }
                non_apply => {
                    let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&non_apply),
                        current_hwm,
                    );
                }
            }
        }

        let key = (task.request.database_id, tid, collection.to_string());

        // ── LSN-based deduplication (last-flushed skip) ──────────────────────
        // Skip memtable re-apply if the record was already flushed to disk.
        // On the sync path we still advance the HWM after this check because
        // the record IS durably applied (on the flushed segment).
        let already_flushed = if let Some(lsn) = wal_lsn
            && let Some(registry) = self.ts_registries.get(&key)
        {
            let max_flushed = registry
                .iter()
                .map(|(_, e)| e.meta.last_flushed_wal_lsn)
                .max()
                .unwrap_or(0);
            max_flushed > 0 && lsn <= max_flushed
        } else {
            false
        };

        if already_flushed {
            // Advance the HWM even though the memtable write is skipped — the
            // record is durable on disk, so the seq counts as applied.
            if let Some(prov) = provenance {
                self.sync_commit(prov);
                let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
            }

            // Non-sync path: return original dedup_skipped JSON shape.
            let result = serde_json::json!({
                "accepted": 0,
                "rejected": 0,
                "collection": collection,
                "dedup_skipped": true,
            });
            let json = match response_codec::encode_json(&result) {
                Ok(b) => b,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };
            return Response {
                request_id: task.request.request_id,
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload: Payload::from_vec(json),
                watermark_lsn: self.watermark,
                error_code: None,
            };
        }

        // Use the epoch's deterministic timestamp when executing inside a Calvin
        // txn; fall back to wall clock for single-shard (non-Calvin) paths.
        let now_ms: i64 = self.epoch_system_ms.unwrap_or_else(|| {
            // no-determinism: fallback only reached outside Calvin path; epoch_system_ms is set for Calvin
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });

        let ingest_response = match format {
            "ilp" => self.execute_ilp_ingest(task, tid, collection, payload, wal_lsn, now_ms),
            "json" => self.execute_json_ingest(task, tid, collection, payload, wal_lsn, now_ms),
            "msgpack" => {
                self.execute_msgpack_ingest(task, tid, collection, payload, wal_lsn, now_ms)
            }
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("unknown ingest format: {format}"),
                    },
                );
            }
        };

        // On the sync path, advance the HWM after a successful ingest and
        // return a SyncAckResult payload instead of the normal JSON body.
        if let Some(prov) = provenance
            && ingest_response.status == Status::Ok
        {
            self.sync_commit(prov);
            let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
            return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
        }

        // Either no provenance, or ingest failed on the Apply path — surface
        // the response as-is; the HWM is NOT advanced (record not applied).
        ingest_response
    }

    pub(super) fn execute_ilp_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        payload: &[u8],
        wal_lsn: Option<u64>,
        now_ms: i64,
    ) -> Response {
        let key = (task.request.database_id, tid, collection.to_string());
        let input = match std::str::from_utf8(payload) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid UTF-8 in ILP: {e}"),
                    },
                );
            }
        };

        let lines: Vec<_> = ilp::parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        if lines.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid ILP lines in payload".into(),
                },
            );
        }

        let bitemporal = self.is_bitemporal(tid.as_u64(), collection);
        // Ensure memtable exists (auto-create on first write).
        let is_new_memtable = !self.columnar_memtables.contains_key(&key);
        if is_new_memtable {
            let mut schema = ilp_ingest::infer_schema(&lines);
            if bitemporal {
                ilp_ingest::ensure_bitemporal_columns(&mut schema);
            }
            let config = ColumnarMemtableConfig {
                max_memory_bytes: 64 * 1024 * 1024,
                hard_memory_limit: 80 * 1024 * 1024,
                max_tag_cardinality: 100_000,
            };
            let mt = ColumnarMemtable::new(schema, config);
            self.columnar_memtables.insert(key.clone(), mt);
        }

        // Schema evolution: detect new fields and expand memtable schema.
        let cols_before = if !is_new_memtable {
            self.columnar_memtables
                .get(&key)
                .map(|mt| mt.schema().columns.len())
                .unwrap_or(0)
        } else {
            0
        };
        if !is_new_memtable && let Some(mt) = self.columnar_memtables.get_mut(&key) {
            ilp_ingest::evolve_schema(mt, &lines);
        }
        let schema_changed = !is_new_memtable
            && self
                .columnar_memtables
                .get(&key)
                .is_some_and(|mt| mt.schema().columns.len() != cols_before);

        // Pre-flush: flush BEFORE ingesting if memtable is at the soft limit
        // OR if the timeseries engine budget is exhausted (governor pressure).
        let governor_pressure = self.governor.as_ref().is_some_and(|g| {
            g.try_reserve(
                task.request.database_id,
                tid,
                nodedb_mem::EngineId::Timeseries,
                0,
            )
            .is_err()
        });
        let needs_flush = self
            .columnar_memtables
            .get(&key)
            .is_some_and(|mt| mt.memory_bytes() >= 64 * 1024 * 1024 || governor_pressure);
        if needs_flush
            && let Err(e) =
                self.flush_ts_collection(tid, task.request.database_id, collection, now_ms)
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("pre-ingest ts flush failed: {e}"),
                },
            );
        }

        let Some(mt) = self.columnar_memtables.get_mut(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after init: {collection}"),
                },
            );
        };

        let stamps = if bitemporal {
            Some(ilp_ingest::BitempStamps { system_ms: now_ms })
        } else {
            None
        };
        let lvc = self.ts_last_value_caches.get_mut(&key);
        let mut series_keys = HashMap::new();
        let (mut accepted, rejected) =
            ilp_ingest::ingest_batch_with_lvc(mt, &lines, &mut series_keys, now_ms, lvc, stamps);

        // If rows were rejected (memtable hit hard limit), flush and re-ingest.
        if rejected > 0 {
            tracing::warn!(
                collection,
                accepted,
                rejected,
                "ILP batch rows rejected by hard limit, flushing and retrying"
            );
            if let Err(e) =
                self.flush_ts_collection(tid, task.request.database_id, collection, now_ms)
            {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("hard-limit ts flush failed: {e}"),
                    },
                );
            }
            if let Some(mt) = self.columnar_memtables.get_mut(&key) {
                let mut retry_keys = HashMap::new();
                let retry_lines = &lines[accepted..];
                let retry_lvc = self.ts_last_value_caches.get_mut(&key);
                let (retry_accepted, _) = ilp_ingest::ingest_batch_with_lvc(
                    mt,
                    retry_lines,
                    &mut retry_keys,
                    now_ms,
                    retry_lvc,
                    stamps,
                );
                accepted += retry_accepted;
            }
        }

        // Post-flush: standard 64MB threshold check.
        let Some(mt) = self.columnar_memtables.get(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after ingest: {collection}"),
                },
            );
        };
        let needs_flush = mt.memory_bytes() >= 64 * 1024 * 1024;
        if needs_flush
            && let Err(e) =
                self.flush_ts_collection(tid, task.request.database_id, collection, now_ms)
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("post-ingest ts flush failed: {e}"),
                },
            );
        }

        // Track WAL LSN and last ingest time for dedup + idle flush.
        if accepted > 0 {
            if let Some(lsn) = wal_lsn {
                let entry = self.ts_max_ingested_lsn.entry(key.clone()).or_insert(0);
                *entry = (*entry).max(lsn);
            }
            // no-determinism: last_ts_ingest is a flush-trigger timer, not Calvin row data
            self.last_ts_ingest = Some(std::time::Instant::now());
        }

        self.checkpoint_coordinator
            .mark_dirty("timeseries", accepted);

        // Re-charge the engine memory budget to the memtable's current
        // resident footprint. The reservation is held (in
        // `columnar_memtable_mem`) until the memtable is drained on flush,
        // so the Timeseries budget reflects what the memtable holds and the
        // flush release is balanced — never `release()`-ing bytes that were
        // never reserved.
        self.recharge_ts_memtable_budget(tid, task.request.database_id, collection);

        // Include schema_columns when schema is new OR evolved.
        let include_schema = is_new_memtable || schema_changed;
        let result = if include_schema && let Some(mt) = self.columnar_memtables.get(&key) {
            let schema_columns: Vec<serde_json::Value> = mt
                .schema()
                .columns
                .iter()
                .map(|(name, col_type)| {
                    let type_str = match col_type {
                        ColumnType::Timestamp => "TIMESTAMP",
                        ColumnType::Float64 => "FLOAT",
                        ColumnType::Int64 => "BIGINT",
                        ColumnType::Symbol => "VARCHAR",
                    };
                    serde_json::json!([name, type_str])
                })
                .collect();
            serde_json::json!({
                "accepted": accepted,
                "rejected": rejected,
                "collection": collection,
                "schema_columns": schema_columns,
            })
        } else {
            serde_json::json!({
                "accepted": accepted,
                "rejected": rejected,
                "collection": collection,
            })
        };
        let json = match response_codec::encode_json(&result) {
            Ok(b) => b,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        Response {
            request_id: task.request.request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(json),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }
}
