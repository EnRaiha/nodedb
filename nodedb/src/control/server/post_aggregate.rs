// SPDX-License-Identifier: BUSL-1.1

//! Post-join aggregation in the Control Plane.
//!
//! When a query has `GROUP BY` over a `JOIN` result, the Data Plane cores
//! return raw join rows. This module aggregates them in the Control Plane.
//! All processing stays in msgpack — no JSON intermediary.

use std::collections::HashMap;

use crate::bridge::envelope::{Payload, Response};
use nodedb_query::agg_key::canonical_agg_key;

/// Apply GROUP BY + aggregate functions on a join response payload.
///
/// The input response payload is a msgpack array of maps (merged from all cores).
/// Returns a new response with aggregated results (also msgpack).
pub fn apply_post_aggregation(
    resp: Response,
    group_by: &[String],
    aggregates: &[(String, String)],
) -> crate::Result<Response> {
    let payload_bytes = resp.payload.as_bytes();

    // Parse the msgpack array of row-maps.
    let rows = parse_msgpack_rows(payload_bytes)?;

    // Group rows by the GROUP BY columns.
    let mut groups: HashMap<Vec<String>, Vec<&[u8]>> = HashMap::new();
    for row in &rows {
        let key: Vec<String> = group_by
            .iter()
            .map(|col| extract_field_str(row, col).unwrap_or_default())
            .collect();
        groups.entry(key).or_default().push(row);
    }

    // Build result as msgpack array.
    use nodedb_query::msgpack_scan::writer;
    let mut buf = Vec::with_capacity(payload_bytes.len());
    writer::write_array_header(&mut buf, groups.len());

    for (key, group_rows) in &groups {
        let field_count = group_by.len() + aggregates.len();
        writer::write_map_header(&mut buf, field_count);

        // Write GROUP BY columns.
        for (i, col) in group_by.iter().enumerate() {
            writer::write_kv_str(&mut buf, col, &key[i]);
        }

        // Compute and write each aggregate.
        for (op, field) in aggregates {
            let agg_key = canonical_agg_key(op, field);
            let value = compute_aggregate(op, field, group_rows);
            match value {
                AggValue::Int(n) => writer::write_kv_i64(&mut buf, &agg_key, n),
                AggValue::Float(f) => writer::write_kv_f64(&mut buf, &agg_key, f),
                AggValue::Null => writer::write_kv_null(&mut buf, &agg_key),
            }
        }
    }

    Ok(Response {
        payload: Payload::from_vec(buf),
        ..resp
    })
}

enum AggValue {
    Int(i64),
    Float(f64),
    Null,
}

/// Parse a msgpack array payload into individual row slices.
fn parse_msgpack_rows(bytes: &[u8]) -> crate::Result<Vec<&[u8]>> {
    use nodedb_query::msgpack_scan::reader;

    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let (count, mut pos) =
        reader::array_header(bytes, 0).ok_or_else(|| crate::Error::PlanError {
            detail: "post-aggregation: invalid msgpack array header".into(),
        })?;

    // The msgpack array header is untrusted; reserve only after each row has
    // been proven present by `skip_value`.
    let mut rows = Vec::new();
    for _ in 0..count {
        let start = pos;
        pos = reader::skip_value(bytes, pos).ok_or_else(|| crate::Error::PlanError {
            detail: "post-aggregation: truncated msgpack row".into(),
        })?;
        rows.push(&bytes[start..pos]);
    }
    Ok(rows)
}

/// Extract a field value as string from a msgpack map row.
/// Handles "collection.field" suffix matching.
fn extract_field_str(row: &[u8], field: &str) -> Option<String> {
    use nodedb_query::msgpack_scan::reader;

    // Try exact match first.
    if let Some((start, end)) = nodedb_query::msgpack_scan::extract_field(row, 0, field) {
        return Some(read_value_as_string(row, start, end));
    }

    // Suffix match: iterate map keys looking for "*.{field}".
    let suffix = format!(".{field}");
    let (count, mut pos) = reader::map_header(row, 0)?;
    for _ in 0..count {
        let key = reader::read_str(row, pos)?;
        let key_end = reader::skip_value(row, pos)?;
        let val_end = reader::skip_value(row, key_end)?;
        if key.ends_with(&suffix) {
            return Some(read_value_as_string(row, key_end, val_end));
        }
        pos = val_end;
    }
    None
}

/// Extract a numeric field value from a msgpack map row.
fn extract_number(row: &[u8], field: &str) -> Option<f64> {
    use nodedb_query::msgpack_scan::reader;

    let try_field = |name: &str| -> Option<f64> {
        let (start, _end) = nodedb_query::msgpack_scan::extract_field(row, 0, name)?;
        if let Some(i) = reader::read_i64(row, start) {
            return Some(i as f64);
        }
        if let Some(f) = reader::read_f64(row, start) {
            return Some(f);
        }
        // Try parsing string as number.
        reader::read_str(row, start).and_then(|s| s.parse().ok())
    };

    if field == "*" {
        return Some(1.0);
    }

    // Exact match.
    if let Some(v) = try_field(field) {
        return Some(v);
    }

    // Suffix match.
    let suffix = format!(".{field}");
    let (count, mut pos) = nodedb_query::msgpack_scan::reader::map_header(row, 0)?;
    for _ in 0..count {
        let key = nodedb_query::msgpack_scan::reader::read_str(row, pos)?;
        let key_end = nodedb_query::msgpack_scan::reader::skip_value(row, pos)?;
        let val_end = nodedb_query::msgpack_scan::reader::skip_value(row, key_end)?;
        if key.ends_with(&suffix) {
            if let Some(i) = nodedb_query::msgpack_scan::reader::read_i64(row, key_end) {
                return Some(i as f64);
            }
            if let Some(f) = nodedb_query::msgpack_scan::reader::read_f64(row, key_end) {
                return Some(f);
            }
            if let Some(s) = nodedb_query::msgpack_scan::reader::read_str(row, key_end) {
                return s.parse().ok();
            }
        }
        pos = val_end;
    }
    None
}

/// Read a msgpack value at [start..end) as a display string.
fn read_value_as_string(bytes: &[u8], start: usize, end: usize) -> String {
    use nodedb_query::msgpack_scan::reader;
    if let Some(s) = reader::read_str(bytes, start) {
        return s.to_string();
    }
    if let Some(i) = reader::read_i64(bytes, start) {
        return i.to_string();
    }
    if let Some(f) = reader::read_f64(bytes, start) {
        return f.to_string();
    }
    if let Some(b) = reader::read_bool(bytes, start) {
        return b.to_string();
    }
    if reader::read_null(bytes, start) {
        return String::new();
    }
    // Complex value — transcode slice to JSON string.
    nodedb_types::msgpack_to_json_string(&bytes[start..end]).unwrap_or_default()
}

/// Compute a single aggregate over a group of msgpack rows.
fn compute_aggregate(op: &str, field: &str, rows: &[&[u8]]) -> AggValue {
    match op {
        "count" => AggValue::Int(rows.len() as i64),
        "sum" => {
            let sum: f64 = rows.iter().filter_map(|r| extract_number(r, field)).sum();
            AggValue::Float(sum)
        }
        "avg" => {
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|r| extract_number(r, field))
                .collect();
            if values.is_empty() {
                AggValue::Null
            } else {
                AggValue::Float(values.iter().sum::<f64>() / values.len() as f64)
            }
        }
        "min" => rows
            .iter()
            .filter_map(|r| extract_number(r, field))
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(AggValue::Float)
            .unwrap_or(AggValue::Null),
        "max" => rows
            .iter()
            .filter_map(|r| extract_number(r, field))
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(AggValue::Float)
            .unwrap_or(AggValue::Null),
        _ => AggValue::Null,
    }
}
