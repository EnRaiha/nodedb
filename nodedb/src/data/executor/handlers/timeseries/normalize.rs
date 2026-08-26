// SPDX-License-Identifier: BUSL-1.1

//! Rewriting every timeseries ingest payload shape into line protocol.
//!
//! This is where a structured ingest's values become the values that are
//! STORED: the declared time column moves into the line's timestamp (and thence
//! into the schema's millisecond time column), and a numeric-looking string
//! becomes a number. Anything that needs to reason about the row an ingest will
//! actually persist — the row-level-security gate, the resolve pass — has to go
//! through here rather than reading the submitted values, or it is reasoning
//! about an image the collection never holds.

use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use super::msgpack_decode::MsgpackValue;
use crate::engine::timeseries::ilp::{self, IlpError};

/// Nanoseconds per millisecond — line protocol timestamps are nanoseconds, the
/// stored time column is milliseconds.
const NANOS_PER_MILLI: i64 = 1_000_000;

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
pub(super) fn is_time_column(column: &str, declared: Option<&str>) -> bool {
    match declared {
        Some(time_key) => column.eq_ignore_ascii_case(time_key),
        None => {
            let lower = column.to_lowercase();
            lower == "ts" || lower == "timestamp" || lower == "time"
        }
    }
}

/// Parse a datetime string to nanoseconds since Unix epoch.
///
/// Accepts RFC3339 / ISO8601 with timezone (e.g., "2024-01-01T00:00:00Z"),
/// and common datetime formats without timezone (treated as UTC).
/// Returns nanoseconds since Unix epoch, or `None` if the string cannot be parsed.
pub(super) fn parse_ts_string_to_nanos(s: &str) -> Option<i64> {
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

/// One line's accumulated fields and timestamp, rendered into `buf`.
fn push_line(buf: &mut String, measurement: &str, fields: &[String], timestamp_ns: Option<i64>) {
    if fields.is_empty() {
        return;
    }
    buf.push_str(measurement);
    buf.push(' ');
    buf.push_str(&fields.join(","));
    if let Some(ts) = timestamp_ns {
        buf.push(' ');
        buf.push_str(&ts.to_string());
    }
    buf.push('\n');
}

/// Normalize decoded MessagePack rows into line protocol.
pub(in crate::data::executor) fn msgpack_rows_to_ilp(
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
                        timestamp_ns = Some(*n * NANOS_PER_MILLI);
                    }
                    MsgpackValue::Float(f) => {
                        timestamp_ns = Some(*f as i64 * NANOS_PER_MILLI);
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

        push_line(&mut ilp_buf, measurement, &fields, timestamp_ns);
    }
    ilp_buf
}

/// Normalize decoded JSON rows into line protocol.
///
/// The JSON value model carries no decimal-as-string case, so a numeric field
/// arrives already typed and no string is re-parsed into a number here.
pub(in crate::data::executor) fn json_rows_to_ilp(
    rows: &sonic_rs::Array,
    measurement: &str,
    time_key: Option<&str>,
) -> String {
    let mut ilp_buf = String::new();
    for row_val in rows.iter() {
        let Some(obj) = row_val.as_object() else {
            continue;
        };

        let mut fields = Vec::new();
        let mut timestamp_ns: Option<i64> = None;

        for (key, val) in obj.iter() {
            if is_time_column(key, time_key) {
                if let Some(s) = val.as_str() {
                    timestamp_ns = parse_ts_string_to_nanos(s);
                } else if let Some(n) = val.as_i64() {
                    timestamp_ns = Some(n * NANOS_PER_MILLI);
                } else if let Some(f) = val.as_f64() {
                    timestamp_ns = Some(f as i64 * NANOS_PER_MILLI);
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

        push_line(&mut ilp_buf, measurement, &fields, timestamp_ns);
    }
    ilp_buf
}

/// Split `batch` into lines, giving every line that carries no timestamp the
/// batch's `default_timestamp_ms`.
///
/// A replicated ingest must store the same row on every replica, and a line
/// with no timestamp otherwise takes each applying node's own clock. Stamping
/// it once, here, is what makes the resolved form replica-independent.
pub(in crate::data::executor) fn stamp_timestamps(
    batch: &str,
    default_timestamp_ms: i64,
) -> Result<Vec<String>, IlpError> {
    let mut stamped = Vec::new();
    for raw in batch.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let parsed = ilp::parse_batch(line)?;
        let has_timestamp = parsed
            .lines()
            .first()
            .is_some_and(|l| l.timestamp_ns.is_some());
        if has_timestamp {
            stamped.push(line.to_string());
        } else {
            stamped.push(format!("{line} {}", default_timestamp_ms * NANOS_PER_MILLI));
        }
    }
    Ok(stamped)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A line without a timestamp takes the batch default; one that carries its
    /// own keeps it byte for byte.
    #[test]
    fn stamping_fills_only_the_lines_that_carry_no_timestamp() {
        let batch = "cpu,owner=alice value=1i\ncpu,owner=alice value=2i 1700000000000000000\n";
        let stamped = stamp_timestamps(batch, 7).expect("stamp");
        assert_eq!(
            stamped,
            vec![
                "cpu,owner=alice value=1i 7000000".to_string(),
                "cpu,owner=alice value=2i 1700000000000000000".to_string(),
            ]
        );
    }

    /// A malformed line is reported rather than passed through unstamped.
    #[test]
    fn a_malformed_line_is_reported() {
        assert!(stamp_timestamps("cpu\n", 0).is_err());
    }
}
