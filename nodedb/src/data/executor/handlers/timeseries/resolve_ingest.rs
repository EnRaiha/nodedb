// SPDX-License-Identifier: BUSL-1.1

//! Read-only resolve pass for a governed `TimeseriesOp::Ingest`.
//!
//! A follower has no writing identity, so an ingest carrying a live write
//! predicate cannot be replicated. This normalizes the payload into the line
//! protocol the ingest handler would have stored, stamps every line's
//! timestamp, decides the policy against those exact lines, and reports them
//! back. The Control Plane then proposes an `ilp-msgpack` ingest carrying them
//! with a decided check.
//!
//! Normalization belongs here and not in the Control Plane: the time column of
//! a measurement with no DDL behind it comes from the resident memtable's
//! schema, and the default timestamp comes from this core's clock — both
//! Data-Plane state.

use nodedb_physical::physical_plan::TimeseriesOp;

use super::normalize;
use super::rls_gate;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// The measurement an ingest writes to, taken from its routing collection.
fn measurement_of(collection: &str) -> &str {
    collection
        .split_once(':')
        .map(|(_, name)| name)
        .unwrap_or(collection)
}

impl CoreLoop {
    /// Resolve the wrapped ingest to the canonical lines it would store.
    ///
    /// Answers with a MessagePack `Vec<String>` — the same canonical shape the
    /// `"ilp-msgpack"` format decodes — or the policy's own refusal.
    pub(in crate::data::executor) fn execute_timeseries_resolve_ingest(
        &mut self,
        task: &ExecutionTask,
        inner: &TimeseriesOp,
    ) -> Response {
        let TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            rls_write_check,
            ..
        } = inner
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "timeseries resolve pass wraps a plan that is not an ingest".into(),
                },
            );
        };

        let tid = task.request.tenant_id;
        let time_key = self
            .declared_ts_time_key(task.request.database_id, tid, collection)
            .map(str::to_string);
        let batch =
            match Self::normalized_ilp_batch(collection, payload, format, time_key.as_deref()) {
                Ok(batch) => batch,
                Err(error) => return self.response_error(task, error),
            };

        let now_ms = self.ingest_now_ms();
        let lines = match normalize::stamp_timestamps(&batch, now_ms) {
            Ok(lines) => lines,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("timeseries resolve: unparsable line protocol: {error}"),
                    },
                );
            }
        };
        if lines.is_empty() {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: format!("timeseries resolve: '{collection}' payload holds no rows"),
                },
            );
        }

        // Decide the policy against the stamped lines — the exact images the
        // proposed ingest will store on every replica.
        let joined = lines.join("\n");
        let parsed = match crate::engine::timeseries::ilp::parse_batch(&joined) {
            Ok(parsed) => parsed,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("timeseries resolve: unparsable line protocol: {error}"),
                    },
                );
            }
        };
        if let Err(error) = rls_gate::admit_ilp_lines(
            rls_write_check,
            parsed.lines(),
            time_key.as_deref(),
            now_ms,
            tid.as_u64(),
            collection,
        ) {
            return self.response_error(task, error);
        }

        match zerompk::to_msgpack_vec(&lines) {
            Ok(encoded) => self.response_with_payload(task, encoded),
            Err(error) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("timeseries resolve: could not encode resolved lines: {error}"),
                },
            ),
        }
    }

    /// Rewrite `payload` into line protocol, whichever shape it arrived in.
    ///
    /// Mirrors the format dispatch the ingest handler runs, so the lines this
    /// returns are the lines that ingest would have parsed.
    fn normalized_ilp_batch(
        collection: &str,
        payload: &[u8],
        format: &str,
        time_key: Option<&str>,
    ) -> Result<String, ErrorCode> {
        let measurement = measurement_of(collection);
        match format {
            "ilp" => String::from_utf8(payload.to_vec()).map_err(|error| {
                ErrorCode::RejectedPrevalidation {
                    reason: format!("timeseries resolve: line protocol is not UTF-8: {error}"),
                }
            }),
            "ilp-msgpack" => {
                let lines: Vec<String> = zerompk::from_msgpack(payload).map_err(|error| {
                    ErrorCode::RejectedPrevalidation {
                        reason: format!(
                            "timeseries resolve: invalid canonical ILP payload: {error}"
                        ),
                    }
                })?;
                Ok(lines.join("\n"))
            }
            "msgpack" => {
                let rows =
                    super::msgpack_decode::decode_msgpack_rows(payload).map_err(|error| {
                        ErrorCode::RejectedPrevalidation {
                            reason: format!("timeseries resolve: msgpack decode error: {error}"),
                        }
                    })?;
                Ok(normalize::msgpack_rows_to_ilp(&rows, measurement, time_key))
            }
            "json" => {
                let rows: sonic_rs::Array = sonic_rs::from_slice(payload).map_err(|error| {
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("timeseries resolve: JSON parse error: {error}"),
                    }
                })?;
                Ok(normalize::json_rows_to_ilp(&rows, measurement, time_key))
            }
            other => Err(ErrorCode::Internal {
                detail: format!("timeseries resolve: unknown ingest format: {other}"),
            }),
        }
    }
}
