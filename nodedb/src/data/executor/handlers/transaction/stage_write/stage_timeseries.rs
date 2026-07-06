// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for `TimeseriesOp::Ingest`.
//!
//! A timeseries INSERT issued inside a `BEGIN..COMMIT` block is staged here,
//! one overlay `Put` per row, so a later same-transaction RAW timeseries
//! SELECT observes the newly inserted rows (read-your-own-writes) before
//! COMMIT. COMMIT durable replay is unchanged: the buffered
//! `TimeseriesOp::Ingest` plan is still replayed through
//! `execute_timeseries_ingest` inside the COMMIT `TransactionBatch`, which
//! remains the sole durable apply.
//!
//! No memtable mutation at statement time: staging writes ONLY into the
//! per-transaction overlay (`txn_overlays`), never into `columnar_memtables`.
//! ROLLBACK is therefore handled entirely by dropping / rewinding the
//! transaction overlay (`TxnOverlay`'s journal, `MetaOp::DropTxnOverlay`) —
//! no undo-log entry is required, exactly like the columnar statement-time
//! staging path.
//!
//! Row identity: a timeseries row is identified internally by its `series_id`
//! (a hash of measurement + tags), which is not a cross-engine surrogate. For
//! staging, each row is keyed by the per-row `Surrogate` the planner minted
//! via `assign_fresh` (`convert_timeseries_ingest`) — a fresh unique id per
//! row so every staged INSERT occupies its own overlay slot. `surrogate_to_doc_id`
//! (hex) is used only for the overlay's doc-id side-map.
//!
//! Row body encoding: each row's `{field => value}` map is stored VERBATIM
//! (the exact column names the INSERT used) and encoded via
//! `nodedb_types::value_to_msgpack` — the same shape
//! `merge_overlay_into_timeseries_scan` decodes and re-emits. Verbatim keys are
//! deliberate: the residual time predicate the planner extracts into the
//! scan's `time_range` is NOT stripped from the serialized field-filters, so a
//! `WHERE ts >= …` scan still carries a `ts` `ScanFilter`. `ScanFilter::matches_binary`
//! returns `false` for a field absent from the row, so renaming the timestamp
//! column would make that residual filter drop every staged row. Keeping the
//! original key lets both the field-filter (`matches_binary` on `ts`) and the
//! merge's alias-aware time-range prune resolve correctly.

use nodedb_types::Surrogate;
use nodedb_types::value::Value;

use super::context::StageCtx;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::TxnId;

/// Inputs for [`CoreLoop::stage_timeseries_insert`]. Bundled because the raw
/// parameter list exceeds the project's too-many-arguments bound.
pub(in crate::data::executor) struct StageTimeseriesInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub surrogates: &'a [Surrogate],
}

impl CoreLoop {
    /// Stage a `TimeseriesOp::Ingest` batch: decode the msgpack row maps and
    /// stage one overlay `Put` per row keyed by its surrogate, body stored
    /// verbatim. Returns the shared `stage_count_response` shape
    /// (`{"affected": N}`).
    pub(in crate::data::executor) fn stage_timeseries_insert(
        &mut self,
        params: StageTimeseriesInsertParams<'_>,
    ) -> Response {
        let StageTimeseriesInsertParams {
            task,
            tid,
            txn_id,
            collection,
            payload,
            surrogates,
        } = params;

        let rows: Vec<Value> = match nodedb_types::value_from_msgpack(payload) {
            Ok(Value::Array(arr)) => arr,
            Ok(v @ Value::Object(_)) => vec![v],
            Ok(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "timeseries insert: payload must be array or object".into(),
                    },
                );
            }
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("timeseries insert: invalid payload: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty payload".into(),
                },
            );
        }

        let mut staged = 0usize;
        for (row_idx, row) in rows.iter().enumerate() {
            if !matches!(row, Value::Object(_)) {
                continue;
            }

            let surrogate = match surrogates.get(row_idx).copied() {
                Some(s) => s,
                None => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "timeseries insert: missing surrogate for staged row".into(),
                        },
                    );
                }
            };

            let body = match nodedb_types::value_to_msgpack(row) {
                Ok(b) => b,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("timeseries insert: row encode failed: {e}"),
                        },
                    );
                }
            };

            let doc_id = surrogate_to_doc_id(surrogate);
            let ctx = StageCtx::new(task, tid, txn_id, collection, doc_id, surrogate);
            if let Err(e) = self.stage_put_capped(&ctx, body) {
                return self.response_error(task, e);
            }
            staged += 1;
        }

        self.stage_count_response(task, staged)
    }
}
