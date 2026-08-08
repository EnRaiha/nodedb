// SPDX-License-Identifier: BUSL-1.1

//! MessagePack + JSON ingest formats for timeseries.

use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use super::ingest_dispatch::TimeseriesIngestParams;
use super::msgpack_decode::{self, MsgpackValue};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;

/// Decode the canonical ILP row envelope used by the Calvin path.
///
/// Caller-provided malformed bytes are rejected before ingest. An inability to
/// re-encode an already decoded value is an internal serialization failure.
fn decode_canonical_ilp_lines(payload: &[u8]) -> Result<Vec<String>, ErrorCode> {
    let lines: Vec<String> =
        zerompk::from_msgpack(payload).map_err(|error| ErrorCode::RejectedPrevalidation {
            reason: format!("invalid canonical ILP payload: {error}"),
        })?;
    if lines.is_empty() {
        return Err(ErrorCode::RejectedPrevalidation {
            reason: "empty canonical ILP payload".into(),
        });
    }
    let canonical = zerompk::to_msgpack_vec(&lines).map_err(|error| ErrorCode::Internal {
        detail: format!("canonical ILP payload re-encode failed: {error}"),
    })?;
    if canonical != payload
        || lines
            .iter()
            .any(|line| line.contains('\n') || line.contains('\r'))
    {
        return Err(ErrorCode::RejectedPrevalidation {
            reason: "malformed canonical ILP line payload".into(),
        });
    }
    Ok(lines)
}

/// Is `column` the time column of the row being ingested?
///
/// A collection created through DDL designates its time column explicitly, so
/// the match is against that declared name and nothing else — a column called
/// `timestamp` in a collection whose `TIME_KEY` is `captured_at` is an
/// ordinary column and must keep its value.
///
/// `declared` is `None` only for a measurement with no DDL behind it (raw ILP
/// protocol ingest). There the conventional names are the only signal
/// available, so they remain the fallback.
fn is_time_column(column: &str, declared: Option<&str>) -> bool {
    match declared {
        Some(time_key) => column.eq_ignore_ascii_case(time_key),
        None => {
            let lower = column.to_lowercase();
            lower == "ts" || lower == "timestamp" || lower == "time"
        }
    }
}

/// Normalize decoded MessagePack rows into line protocol.
///
/// This is where a structured ingest's values become the values that are
/// STORED: the declared time column moves into the line's timestamp (and thence
/// into the schema's millisecond time column), and a numeric-looking string
/// becomes a number. Anything that needs to reason about the row a MessagePack
/// ingest will actually persist — the row-level-security gate in particular —
/// has to go through here rather than reading the submitted values, or it is
/// reasoning about an image the collection never holds.
pub(super) fn msgpack_rows_to_ilp(
    rows: &[Vec<(String, MsgpackValue)>],
    measurement: &str,
    time_key: Option<&str>,
) -> String {
    let mut ilp_buf = String::new();
    for row in rows {
        let mut fields = Vec::new();
        let mut timestamp_ns: Option<i64> = None;

        for (key, val) in row {
            if is_time_column(key, time_key) {
                match val {
                    MsgpackValue::Str(s) => {
                        timestamp_ns = parse_ts_string_to_nanos(s);
                    }
                    MsgpackValue::Int(n) => {
                        timestamp_ns = Some(*n * 1_000_000);
                    }
                    MsgpackValue::Float(f) => {
                        timestamp_ns = Some(*f as i64 * 1_000_000);
                    }
                    _ => {}
                }
                continue;
            }

            match val {
                MsgpackValue::Float(f) => fields.push(format!("{key}={f}")),
                MsgpackValue::Int(n) => fields.push(format!("{key}={n}i")),
                MsgpackValue::Str(s) => {
                    // SQL parser routes numeric literals with `.`/`e`/`E` through
                    // `SqlValue::Decimal`, which the standard msgpack writer encodes
                    // as a string. Recover the numeric type here so timeseries
                    // schema inference picks `Float64` / `Int64` instead of `Symbol`.
                    if let Ok(i) = s.parse::<i64>() {
                        fields.push(format!("{key}={i}i"));
                    } else if let Ok(f) = s.parse::<f64>()
                        && f.is_finite()
                    {
                        fields.push(format!("{key}={f}"));
                    } else {
                        fields.push(format!("{key}=\"{}\"", s.replace('\"', "\\\"")));
                    }
                }
                MsgpackValue::Bool(b) => fields.push(format!("{key}={b}")),
                _ => {}
            }
        }

        if fields.is_empty() {
            continue;
        }

        ilp_buf.push_str(measurement);
        ilp_buf.push(' ');
        ilp_buf.push_str(&fields.join(","));
        if let Some(ts) = timestamp_ns {
            ilp_buf.push(' ');
            ilp_buf.push_str(&ts.to_string());
        }
        ilp_buf.push('\n');
    }
    ilp_buf
}

impl CoreLoop {
    /// Decode the canonical Calvin ILP representation without reformatting any
    /// identifiers, unsigned values, escaped tags, or nanosecond timestamps.
    pub(super) fn execute_ilp_msgpack_ingest(
        &mut self,
        params: TimeseriesIngestParams<'_>,
    ) -> Response {
        let TimeseriesIngestParams { task, payload, .. } = &params;
        // Calvin produces canonical zerompk. This rejects trailing bytes and
        // alternate encodings before any memtable mutation.
        let lines = match decode_canonical_ilp_lines(payload) {
            Ok(lines) => lines,
            Err(error) => return self.response_error(task, error),
        };
        let joined = lines.join("\n");
        self.execute_ilp_ingest(TimeseriesIngestParams {
            payload: joined.as_bytes(),
            ..params
        })
    }

    /// Payload is a msgpack array of maps (same schema as JSON ingest but in msgpack).
    /// Converts each row to an ILP line and delegates to the ILP ingest path.
    pub(super) fn execute_msgpack_ingest(
        &mut self,
        params: TimeseriesIngestParams<'_>,
    ) -> Response {
        let TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload,
            wal_lsn,
            now_ms,
            mode,
            rls_write_check,
            // Carried through, never blanked: this function only rewrites the
            // payload into line protocol and delegates. Dropping the slot here
            // would leave the clause working on the ILP path and silently doing
            // nothing on this one — a difference no caller could see.
            returning,
            rls_filters,
        } = params;
        let measurement = collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(collection);

        // The measurement name carries an optional `<db_id>/` db-qualifier for
        // non-default databases (`db_qualified()` in the planner emits this
        // shape). The slash is part of the wire-level routing key, not part of
        // the user-facing measurement, so allow it alongside the original
        // `[a-zA-Z0-9_-]` set.
        if !measurement
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '/')
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "invalid measurement name '{measurement}': only [a-zA-Z0-9_-/] allowed"
                    ),
                },
            );
        }

        let rows = match msgpack_decode::decode_msgpack_rows(payload) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("msgpack decode error: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty msgpack rows array".into(),
                },
            );
        }

        // The declared TIME_KEY is resolved once per batch: it is a property
        // of the collection, not of any individual row.
        let time_key = self
            .declared_ts_time_key(task.request.database_id, tid, collection)
            .map(str::to_string);

        let ilp_buf = msgpack_rows_to_ilp(&rows, measurement, time_key.as_deref());

        if ilp_buf.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid rows in msgpack payload".into(),
                },
            );
        }

        self.execute_ilp_ingest(TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload: ilp_buf.as_bytes(),
            wal_lsn,
            now_ms,
            mode,
            rls_write_check,
            // Forwarded so the projection is resolved in `execute_ilp_ingest`,
            // on the far side of this conversion — the stored point exists only
            // after the rewrite above, so projecting any earlier would report
            // the submitted values instead.
            returning,
            rls_filters,
        })
    }

    /// Payload is a JSON array like: `[{"id":"e1","ts":"2024-01-01T00:00:00Z","value":42.0}]`.
    /// Converts each row to an ILP line and delegates to the ILP ingest path.
    pub(super) fn execute_json_ingest(&mut self, params: TimeseriesIngestParams<'_>) -> Response {
        let TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload,
            wal_lsn,
            now_ms,
            mode,
            rls_write_check,
            // Carried through, never blanked: this function only rewrites the
            // payload into line protocol and delegates. Dropping the slot here
            // would leave the clause working on the ILP path and silently doing
            // nothing on this one — a difference no caller could see.
            returning,
            rls_filters,
        } = params;
        let rows: sonic_rs::Array = match sonic_rs::from_slice(payload) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("JSON parse error: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty JSON rows array".into(),
                },
            );
        }

        let measurement = collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(collection);

        // The measurement name carries an optional `<db_id>/` db-qualifier for
        // non-default databases (`db_qualified()` in the planner emits this
        // shape). The slash is part of the wire-level routing key, not part of
        // the user-facing measurement, so allow it alongside the original
        // `[a-zA-Z0-9_-]` set.
        if !measurement
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '/')
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "invalid measurement name '{measurement}': only [a-zA-Z0-9_-/] allowed"
                    ),
                },
            );
        }

        // The declared TIME_KEY is resolved once per batch: it is a property
        // of the collection, not of any individual row.
        let time_key = self
            .declared_ts_time_key(task.request.database_id, tid, collection)
            .map(str::to_string);

        let mut ilp_buf = String::new();
        for row_val in rows.iter() {
            let obj = match row_val.as_object() {
                Some(o) => o,
                None => continue,
            };

            let mut fields = Vec::new();
            let mut timestamp_ns: Option<i64> = None;

            for (key, val) in obj.iter() {
                if is_time_column(key, time_key.as_deref()) {
                    if let Some(s) = val.as_str() {
                        timestamp_ns = parse_ts_string_to_nanos(s);
                    } else if let Some(n) = val.as_i64() {
                        timestamp_ns = Some(n * 1_000_000);
                    } else if let Some(f) = val.as_f64() {
                        timestamp_ns = Some(f as i64 * 1_000_000);
                    }
                    continue;
                }

                if let Some(f) = val.as_f64() {
                    fields.push(format!("{key}={f}"));
                } else if let Some(n) = val.as_i64() {
                    fields.push(format!("{key}={n}i"));
                } else if let Some(s) = val.as_str() {
                    fields.push(format!("{key}=\"{}\"", s.replace('\"', "\\\"")));
                } else if let Some(b) = val.as_bool() {
                    fields.push(format!("{key}={b}"));
                }
            }

            if fields.is_empty() {
                continue;
            }

            ilp_buf.push_str(measurement);
            ilp_buf.push(' ');
            ilp_buf.push_str(&fields.join(","));
            if let Some(ts) = timestamp_ns {
                ilp_buf.push(' ');
                ilp_buf.push_str(&ts.to_string());
            }
            ilp_buf.push('\n');
        }

        if ilp_buf.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid rows in JSON payload".into(),
                },
            );
        }

        self.execute_ilp_ingest(TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload: ilp_buf.as_bytes(),
            wal_lsn,
            now_ms,
            mode,
            rls_write_check,
            // Forwarded so the projection is resolved in `execute_ilp_ingest`,
            // on the far side of this conversion — the stored point exists only
            // after the rewrite above, so projecting any earlier would report
            // the submitted values instead.
            returning,
            rls_filters,
        })
    }
}

/// Parse a datetime string to nanoseconds since Unix epoch.
///
/// Accepts RFC3339 / ISO8601 with timezone (e.g., "2024-01-01T00:00:00Z"),
/// and common datetime formats without timezone (treated as UTC).
/// Returns nanoseconds since Unix epoch, or `None` if the string cannot be parsed.
fn parse_ts_string_to_nanos(s: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_nanos_opt();
    }

    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];
    for fmt in &formats {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Utc.from_utc_datetime(&ndt).timestamp_nanos_opt();
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::decode_canonical_ilp_lines;
    use crate::bridge::envelope::ErrorCode;

    #[test]
    fn canonical_ilp_msgpack_roundtrip_preserves_raw_protocol_values() {
        let lines =
            vec![r"cpu,host=west\,1 u=18446744073709551615u 1234567890123456789".to_owned()];
        let payload = zerompk::to_msgpack_vec(&lines).expect("encode");
        assert_eq!(decode_canonical_ilp_lines(&payload).expect("decode"), lines);
    }

    #[test]
    fn canonical_ilp_msgpack_rejects_trailing_or_multiline_values() {
        let mut payload =
            zerompk::to_msgpack_vec(&vec!["cpu value=1i".to_owned()]).expect("encode");
        payload.push(0);
        assert!(matches!(
            decode_canonical_ilp_lines(&payload),
            Err(ErrorCode::RejectedPrevalidation { .. })
        ));
        let newline = zerompk::to_msgpack_vec(&vec!["cpu value=1i\nmem value=2i".to_owned()])
            .expect("encode");
        assert!(matches!(
            decode_canonical_ilp_lines(&newline),
            Err(ErrorCode::RejectedPrevalidation { .. })
        ));
    }
}
