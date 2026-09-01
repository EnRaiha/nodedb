// SPDX-License-Identifier: BUSL-1.1

//! Column statistics collector using SIMD-accelerated kernels.
//!
//! Extracts typed slices from scan results and computes per-column
//! statistics (min, max, count, null_count, distinct_count) using
//! the existing SIMD aggregation kernels from nodedb-query.

use std::collections::HashSet;

use crate::control::security::catalog::column_stats::StoredColumnStats;
use sonic_rs;

/// Collect column statistics from JSON scan results.
///
/// Parses each row as JSON, extracts field values, and computes
/// aggregate statistics. For numeric columns, delegates to SIMD
/// kernels for min/max/sum computation.
pub fn collect_stats_from_json_rows(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    columns: &[String],
    rows: &[String],
    analyzed_at: u64,
) -> Vec<StoredColumnStats> {
    let row_count = rows.len() as u64;
    let mut results = Vec::with_capacity(columns.len());

    for col_name in columns {
        let mut null_count = 0u64;
        let mut f64_values = Vec::new();
        let mut i64_values = Vec::new();
        let mut string_values = Vec::new();
        let mut distinct_set: HashSet<String> = HashSet::new();
        let mut total_len: u64 = 0;

        for row_json in rows {
            let value = extract_field_from_json(row_json, col_name);
            match value {
                FieldValue::Null => null_count += 1,
                FieldValue::Int(v) => {
                    i64_values.push(v);
                    distinct_set.insert(v.to_string());
                }
                FieldValue::Float(v) => {
                    f64_values.push(v);
                    distinct_set.insert(format!("{v:.6}"));
                }
                FieldValue::Text(s) => {
                    total_len += s.len() as u64;
                    distinct_set.insert(s.clone());
                    string_values.push(s);
                }
            }
        }

        // Compute min/max using SIMD kernels for numeric columns.
        let (min_value, max_value) = if !f64_values.is_empty() {
            let rt = nodedb_query::simd_agg::ts_runtime();
            let min = (rt.min_f64)(&f64_values);
            let max = (rt.max_f64)(&f64_values);
            (Some(format!("{min}")), Some(format!("{max}")))
        } else if !i64_values.is_empty() {
            let rt = nodedb_query::simd_agg_i64::i64_runtime();
            let min = (rt.min_i64)(&i64_values);
            let max = (rt.max_i64)(&i64_values);
            (Some(format!("{min}")), Some(format!("{max}")))
        } else if !string_values.is_empty() {
            let min = string_values.iter().min().cloned();
            let max = string_values.iter().max().cloned();
            (min, max)
        } else {
            (None, None)
        };

        let non_null = row_count - null_count;
        let avg_len = if non_null > 0 && total_len > 0 {
            Some((total_len / non_null) as u32)
        } else {
            None
        };

        results.push(StoredColumnStats {
            database_id,
            tenant_id,
            collection: collection.to_string(),
            column: col_name.clone(),
            row_count,
            null_count,
            distinct_count: distinct_set.len() as u64,
            min_value,
            max_value,
            avg_value_len: avg_len,
            analyzed_at,
        });
    }

    results
}

enum FieldValue {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
}

/// The map holding one scan row's user columns.
///
/// A document scan row is the two-key map `{"id": …, "data": {…}}`, and its
/// `data` member holds the columns. Every other row is the column map itself.
fn row_columns(row: &serde_json::Value) -> Option<&serde_json::Map<String, serde_json::Value>> {
    let map = row.as_object()?;
    let wrapped = map.len() == 2 && map.contains_key("id");
    if wrapped && let Some(serde_json::Value::Object(data)) = map.get("data") {
        return Some(data);
    }
    Some(map)
}

/// Extract a field value from a JSON row string.
///
/// A scan row arrives in one of two shapes. A document scan wraps each row as
/// `{"id": "<doc_id>", "data": {…}}`, and the user's columns live under `data`.
/// A columnar, KV, spatial, or aggregate scan emits the column map directly.
/// Reading the wrapper as the column map finds no column and scores every
/// row null. The wrapper is unwrapped first.
fn extract_field_from_json(json_str: &str, field_name: &str) -> FieldValue {
    let parsed: Result<serde_json::Value, _> = sonic_rs::from_str(json_str);
    let Ok(obj) = parsed else {
        return FieldValue::Null;
    };

    let val = match row_columns(&obj) {
        Some(map) => map.get(field_name),
        None => None,
    };

    match val {
        None | Some(serde_json::Value::Null) => FieldValue::Null,
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                FieldValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                FieldValue::Float(f)
            } else {
                FieldValue::Text(n.to_string())
            }
        }
        Some(serde_json::Value::String(s)) => FieldValue::Text(s.clone()),
        Some(serde_json::Value::Bool(b)) => FieldValue::Text(b.to_string()),
        Some(other) => FieldValue::Text(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_numeric_stats() {
        let rows: Vec<String> = (0..100)
            .map(|i| format!("{{\"id\": {i}, \"value\": {:.2}}}", i as f64 * 1.5))
            .collect();
        let stats = collect_stats_from_json_rows(9, 1, "test", &["id".into()], &rows, 0);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].row_count, 100);
        assert_eq!(stats[0].null_count, 0);
        assert_eq!(stats[0].min_value, Some("0".to_string()));
        assert_eq!(stats[0].max_value, Some("99".to_string()));
        assert_eq!(stats[0].distinct_count, 100);
    }

    #[test]
    fn collect_with_nulls() {
        let rows = vec![
            r#"{"name": "alice"}"#.to_string(),
            r#"{"name": null}"#.to_string(),
            r#"{"other": 1}"#.to_string(),
        ];
        let stats = collect_stats_from_json_rows(9, 1, "t", &["name".into()], &rows, 0);
        assert_eq!(stats[0].null_count, 2); // null + missing field
        assert_eq!(stats[0].distinct_count, 1);
    }

    #[test]
    fn collect_reads_columns_under_the_document_scan_wrapper() {
        let rows: Vec<String> = (0..12)
            .map(|i| format!(r#"{{"id":"{i:08}","data":{{"k":"k{i}","v":{}}}}}"#, i * 10))
            .collect();

        let stats = collect_stats_from_json_rows(9, 1, "t", &["k".into(), "v".into()], &rows, 0);

        let k = stats.iter().find(|s| s.column == "k").expect("column k");
        assert_eq!(k.null_count, 0, "every wrapped row carries a k value");
        assert_eq!(k.distinct_count, 12);
        assert_eq!(k.min_value, Some("k0".to_string()));

        let v = stats.iter().find(|s| s.column == "v").expect("column v");
        assert_eq!(v.min_value, Some("0".to_string()));
        assert_eq!(v.max_value, Some("110".to_string()));
    }

    #[test]
    fn the_wrapper_id_never_masks_the_user_column() {
        let rows = vec![r#"{"id":"00000001","data":{"id":"r0","v":1}}"#.to_string()];

        let stats = collect_stats_from_json_rows(9, 1, "t", &["id".into()], &rows, 0);

        assert_eq!(
            stats[0].min_value,
            Some("r0".to_string()),
            "the column named id is the row's own id, not the surrogate doc id",
        );
    }

    #[test]
    fn a_two_key_row_without_a_data_object_stays_the_column_map() {
        let rows = vec![r#"{"id":7,"data":"not-an-object"}"#.to_string()];

        let stats = collect_stats_from_json_rows(9, 1, "t", &["data".into()], &rows, 0);

        assert_eq!(stats[0].null_count, 0);
        assert_eq!(stats[0].min_value, Some("not-an-object".to_string()));
    }
}
