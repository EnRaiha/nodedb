// SPDX-License-Identifier: BUSL-1.1

//! Columnar and Timeseries write tracking for transaction batches.
//!
//! These handlers capture prior state before each write so the undo log
//! can reverse the operation on batch failure.
//!
//! KV operation dispatch lives in `sub_plan_kv_ops`.

use nodedb_columnar::pk_index::RowLocation;

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::TimeseriesIngestExec;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;
use nodedb_physical::physical_plan::ColumnarInsertIntent;
use nodedb_physical::physical_plan::document::UpdateValue;

use super::undo::UndoEntry;

/// Captured undo state for a pending columnar insert: the list of new PK bytes
/// to insert, paired with the prior `RowLocation` of any displaced memtable rows.
type ColumnarUndoState = (Vec<Vec<u8>>, Vec<(Vec<u8>, RowLocation)>);

/// Parameters for [`CoreLoop::execute_tx_columnar_insert`].
pub(super) struct TxColumnarInsertParams<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    pub intent: ColumnarInsertIntent,
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub surrogates: &'a [nodedb_types::Surrogate],
    pub schema_bytes: &'a [u8],
}

/// Parameters for [`CoreLoop::execute_tx_timeseries_ingest`].
pub(super) struct TxTimeseriesIngestParams<'a> {
    pub tid: TenantId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    pub wal_lsn: Option<u64>,
}

impl CoreLoop {
    // ── Columnar insert ──────────────────────────────────────────────────────

    /// Execute a columnar insert in a transaction context.
    ///
    /// Captures `row_count_before`, inserted PK bytes, and displaced prior-row
    /// locations before the insert so the undo log can reverse the operation.
    pub(super) fn execute_tx_columnar_insert(
        &mut self,
        task: &ExecutionTask,
        params: TxColumnarInsertParams<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxColumnarInsertParams {
            collection,
            payload,
            format,
            intent,
            on_conflict_updates,
            surrogates,
            schema_bytes,
        } = params;
        let collection_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );

        let row_count_before = self
            .columnar_engines
            .get(&collection_key)
            .map(|e| e.memtable().row_count())
            .unwrap_or(0);

        let (inserted_pks, displaced) =
            self.capture_columnar_insert_undo_state(&collection_key, payload, intent);

        let resp = self.execute_columnar_insert(
            task,
            crate::data::executor::handlers::columnar_write::ColumnarInsertParams {
                collection,
                payload,
                format,
                intent,
                on_conflict_updates,
                surrogates,
                schema_bytes,
                provenance: None,
            },
        );
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "columnar insert failed".into(),
            }));
        }

        undo_log.push(UndoEntry::ColumnarInsert {
            collection_key,
            row_count_before,
            inserted_pks,
            displaced,
        });
        Ok(resp)
    }

    /// Capture the PK bytes and displaced prior-row locations for a pending
    /// columnar insert, without executing the insert.
    fn capture_columnar_insert_undo_state(
        &self,
        collection_key: &(nodedb_types::DatabaseId, TenantId, String),
        payload: &[u8],
        intent: ColumnarInsertIntent,
    ) -> ColumnarUndoState {
        let mut inserted_pks: Vec<Vec<u8>> = Vec::new();
        let mut displaced: Vec<(Vec<u8>, RowLocation)> = Vec::new();

        let Some(engine) = self.columnar_engines.get(collection_key) else {
            // Engine doesn't exist yet; execute_columnar_insert will create it.
            // row_count_before will be 0, so truncate_to(0) handles rollback.
            return (inserted_pks, displaced);
        };

        let ndb_rows: Vec<nodedb_types::Value> = match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(arr)) => arr,
            Ok(v @ nodedb_types::Value::Object(_)) => vec![v],
            _ => return (inserted_pks, displaced),
        };

        let schema = engine.schema().clone();
        for row in &ndb_rows {
            let obj = match row {
                nodedb_types::Value::Object(m) => m,
                _ => continue,
            };

            let values: Vec<nodedb_types::Value> = schema
                .columns
                .iter()
                .map(|col| {
                    obj.get(&col.name)
                        .cloned()
                        .unwrap_or(nodedb_types::Value::Null)
                })
                .collect();

            let Ok(pk_bytes) = engine.encode_pk_from_row(&values) else {
                continue;
            };

            match intent {
                ColumnarInsertIntent::InsertIfAbsent => {
                    if !engine.pk_index().contains(&pk_bytes) {
                        inserted_pks.push(pk_bytes);
                    }
                }
                ColumnarInsertIntent::Insert | ColumnarInsertIntent::Put => {
                    if let Some(prior_loc) = engine.pk_index().get(&pk_bytes).copied()
                        && prior_loc.segment_id == engine.memtable_segment_id()
                    {
                        displaced.push((pk_bytes.clone(), prior_loc));
                    }
                    inserted_pks.push(pk_bytes);
                }
            }
        }

        (inserted_pks, displaced)
    }

    // ── Timeseries ingest ────────────────────────────────────────────────────

    /// Execute a timeseries ingest in a transaction context.
    ///
    /// Captures the memtable row count before ingest so the undo log can
    /// truncate back to that point on batch failure.
    pub(super) fn execute_tx_timeseries_ingest(
        &mut self,
        task: &ExecutionTask,
        params: TxTimeseriesIngestParams<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxTimeseriesIngestParams {
            tid,
            collection,
            payload,
            format,
            wal_lsn,
        } = params;
        let collection_key = (task.request.database_id, tid, collection.to_string());

        let row_count_before = self
            .columnar_memtables
            .get(&collection_key)
            .map(|mt| mt.row_count())
            .unwrap_or(0);

        let resp = self.execute_timeseries_ingest(TimeseriesIngestExec {
            task,
            tid,
            collection,
            payload,
            format,
            wal_lsn,
            provenance: None,
        });
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "timeseries ingest failed".into(),
            }));
        }

        undo_log.push(UndoEntry::TimeseriesIngest {
            collection_key,
            row_count_before,
        });
        Ok(resp)
    }
}
