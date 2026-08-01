// SPDX-License-Identifier: BUSL-1.1

//! Timeseries ILP ingest handler.
//!
//! Every ingest format funnels through here: msgpack / JSON row ingests
//! normalize into ILP text in the sibling `ingest_formats` module and then call
//! `execute_ilp_ingest`, so the record-boundary admission gate below covers
//! them all. The checks the gate runs live in the sibling `admission` module.

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnarMemtable, ColumnarMemtableConfig,
};
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;

use super::admission;
use super::ingest_dispatch::{TimeseriesApplyMode, TimeseriesIngestParams};

impl CoreLoop {
    /// Schema for a collection's very first memtable.
    ///
    /// A collection created through DDL declares its columns and its
    /// `TIME_KEY`; that declaration is the schema, so the time key keeps its
    /// name and position and every declared column exists from the first row
    /// on. Only a collection with no declaration — raw ILP protocol ingest
    /// into a measurement that was never created — falls back to inferring a
    /// shape from the batch itself.
    fn initial_ts_schema(
        &self,
        task: &crate::data::executor::task::ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        lines: &[ilp::IlpLine<'_>],
    ) -> crate::engine::timeseries::columnar_memtable::ColumnarSchema {
        self.declared_ts_memtable_schema(task.request.database_id, tid, collection)
            .unwrap_or_else(|| ilp_ingest::infer_schema(lines))
    }

    /// Check every condition that could reject a commit-deferred ILP ingest
    /// before it is allowed to cast a Calvin commit vote. The simulation is
    /// deliberately isolated from live state: schema evolution and dictionary
    /// probes run against an exact snapshot clone, so this cannot publish a
    /// schema, consume tag IDs, or create a memtable.
    pub(in crate::data::executor) fn prevalidate_deferred_ilp_ingest(
        &self,
        task: &crate::data::executor::task::ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        lines: &[ilp::IlpLine<'_>],
    ) -> Result<(), ErrorCode> {
        let key = (task.request.database_id, tid, collection.to_string());
        let bitemporal =
            self.is_bitemporal(task.request.database_id.as_u64(), tid.as_u64(), collection);
        let existing = self.columnar_memtables.get(&key);
        let live_resident = existing.map(ColumnarMemtable::memory_bytes);
        let mut simulation = match existing {
            Some(memtable) => {
                ColumnarMemtable::from_snapshot(memtable.export_snapshot(), memtable.config())
                    .map_err(|error| ErrorCode::Internal {
                        detail: format!(
                            "failed to clone timeseries memtable for admission: {error}"
                        ),
                    })?
            }
            None => {
                let mut schema = self.initial_ts_schema(task, tid, collection, lines);
                if bitemporal {
                    ilp_ingest::ensure_bitemporal_columns(&mut schema);
                }
                ColumnarMemtable::new(schema, ColumnarMemtableConfig::from_tuning(&self.ts_tuning))
            }
        };
        // Snapshots retain rows and dictionaries but not spare vector capacity,
        // so their baseline footprint can be lower than the live memtable.
        // Simulate only the schema change, then apply that delta to the live
        // resident bytes; otherwise a full live memtable could vote yes.
        let simulation_baseline = simulation.memory_bytes();
        if existing.is_some() {
            ilp_ingest::evolve_schema(&mut simulation, lines);
        }

        if !admission::has_tag_headroom(&simulation, lines, self.ts_tuning.max_tag_cardinality) {
            return Err(ErrorCode::RejectedPrevalidation {
                reason: "transactional timeseries ingest exceeds tag dictionary headroom".into(),
            });
        }

        let governor_pressure = self.governor.as_ref().is_some_and(|governor| {
            governor
                .try_reserve(
                    task.request.database_id,
                    tid,
                    nodedb_mem::EngineId::Timeseries,
                    0,
                )
                .is_err()
        });
        let resident = match live_resident {
            Some(live) => live.saturating_add(
                simulation
                    .memory_bytes()
                    .saturating_sub(simulation_baseline),
            ),
            None => simulation.memory_bytes(),
        };
        if resident >= self.ts_tuning.memtable_budget_bytes
            || resident >= self.ts_tuning.memtable_hard_limit_bytes
            || governor_pressure
        {
            return Err(ErrorCode::RejectedPrevalidation {
                reason: "transactional timeseries ingest requires a flush before mutation".into(),
            });
        }
        Ok(())
    }

    pub(super) fn execute_ilp_ingest(&mut self, params: TimeseriesIngestParams<'_>) -> Response {
        let TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload,
            wal_lsn,
            now_ms,
            mode,
        } = params;
        let key = (task.request.database_id, tid, collection.to_string());
        let input = match std::str::from_utf8(payload) {
            Ok(s) => s,
            Err(_) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: "ILP payload is not valid UTF-8".into(),
                    },
                );
            }
        };

        let lines = match ilp::parse_batch(input) {
            Ok(batch) => batch.into_lines(),
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: error.to_string(),
                    },
                );
            }
        };

        if lines.is_empty() {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: "no ILP data lines in payload".into(),
                },
            );
        }
        if lines
            .iter()
            .any(|line| line.measurement.as_ref() != collection)
        {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: "ILP measurements must match the routed collection".into(),
                },
            );
        }

        if mode == TimeseriesApplyMode::CommitDeferred
            && let Err(error) = self.prevalidate_deferred_ilp_ingest(task, tid, collection, &lines)
        {
            return self.response_error(task, error);
        }

        let bitemporal =
            self.is_bitemporal(task.request.database_id.as_u64(), tid.as_u64(), collection);
        let is_new_memtable = !self.columnar_memtables.contains_key(&key);
        if is_new_memtable {
            let mut schema = self.initial_ts_schema(task, tid, collection, &lines);
            if bitemporal {
                ilp_ingest::ensure_bitemporal_columns(&mut schema);
            }
            let config = ColumnarMemtableConfig::from_tuning(&self.ts_tuning);
            let mt = ColumnarMemtable::new(schema, config);
            self.columnar_memtables.insert(key.clone(), mt);
        }

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

        // The WAL has already committed this record, so the admission gate
        // resolves every possible mid-record stop before the first row lands.
        let governor_pressure = self.governor.as_ref().is_some_and(|g| {
            g.try_reserve(
                task.request.database_id,
                tid,
                nodedb_mem::EngineId::Timeseries,
                0,
            )
            .is_err()
        });
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        let hard_limit = self.ts_tuning.memtable_hard_limit_bytes;
        let max_tag_cardinality = self.ts_tuning.max_tag_cardinality;
        let needs_flush = self.columnar_memtables.get(&key).is_some_and(|mt| {
            let resident = mt.memory_bytes();
            resident >= soft_limit
                || resident >= hard_limit
                || governor_pressure
                || !admission::has_tag_headroom(mt, &lines, max_tag_cardinality)
        });
        if needs_flush {
            if mode == TimeseriesApplyMode::CommitDeferred {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: "transactional timeseries ingest requires a flush before mutation"
                            .into(),
                    },
                );
            }
            if let Err(e) =
                self.flush_ts_collection(tid, task.request.database_id, collection, now_ms)
            {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("pre-ingest ts flush failed: {e}"),
                    },
                );
            }
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
        let (accepted, rejected) =
            ilp_ingest::ingest_batch_with_lvc(mt, &lines, &mut series_keys, now_ms, lvc, stamps);

        if rejected > 0 {
            tracing::warn!(
                collection,
                accepted,
                rejected,
                "ILP batch rows rejected as invalid rows"
            );
        }

        if accepted > 0
            && let Some(lsn) = wal_lsn
        {
            let entry = self.ts_max_ingested_lsn.entry(key.clone()).or_insert(0);
            *entry = (*entry).max(lsn);
        }

        let Some(mt) = self.columnar_memtables.get(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after ingest: {collection}"),
                },
            );
        };
        let needs_flush = mt.memory_bytes() >= soft_limit;
        if mode == TimeseriesApplyMode::Immediate {
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

            if accepted > 0 {
                // no-determinism: Instant::now runs only for the operational idle/checkpoint timer in Immediate mode and is skipped in Calvin staged apply.
                self.last_ts_ingest = Some(std::time::Instant::now());
            }

            self.checkpoint_coordinator
                .mark_dirty("timeseries", accepted);
            self.recharge_ts_memtable_budget(tid, task.request.database_id, collection);
        }

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
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }
}
