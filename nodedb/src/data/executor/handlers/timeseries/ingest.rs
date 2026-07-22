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
            && self.columnar_memtables.get(&key).is_some_and(|memtable| {
                !admission::has_tag_headroom(memtable, &lines, self.ts_tuning.max_tag_cardinality)
            })
        {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: "transactional timeseries ingest exceeds tag dictionary headroom"
                        .into(),
                },
            );
        }

        let bitemporal =
            self.is_bitemporal(task.request.database_id.as_u64(), tid.as_u64(), collection);
        let is_new_memtable = !self.columnar_memtables.contains_key(&key);
        if is_new_memtable {
            let mut schema = ilp_ingest::infer_schema(&lines);
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
