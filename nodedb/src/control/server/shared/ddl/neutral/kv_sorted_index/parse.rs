// SPDX-License-Identifier: BUSL-1.1

//! Statement and argument parsing for the sorted-index family.

use super::super::super::result::DdlError;

pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

pub(super) fn parse_sort_columns(cols_str: &str) -> Result<Vec<(String, String)>, DdlError> {
    let mut columns = Vec::new();
    for part in cols_str.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.is_empty() {
            continue;
        }
        let name = tokens[0].to_lowercase();
        let dir = if tokens.len() > 1 {
            tokens[1].to_uppercase()
        } else {
            "ASC".into()
        };
        if dir != "ASC" && dir != "DESC" {
            return Err(ddl_err(
                "42601",
                format!("invalid sort direction '{dir}', expected ASC or DESC"),
            ));
        }
        columns.push((name, dir));
    }
    if columns.is_empty() {
        return Err(ddl_err("42601", "at least one sort column required"));
    }
    Ok(columns)
}

pub(super) fn parse_key_column(upper: &str) -> Result<String, DdlError> {
    // Look for "KEY <column_name>" after the closing paren.
    let key_pos = upper
        .find(") KEY ")
        .or_else(|| upper.find(")KEY "))
        .ok_or_else(|| ddl_err("42601", "missing KEY clause"))?;
    let after_key = upper[key_pos..].trim_start_matches(')').trim();
    let after_key = after_key
        .strip_prefix("KEY ")
        .ok_or_else(|| ddl_err("42601", "missing KEY clause"))?;
    let key_col = after_key
        .split_whitespace()
        .next()
        .ok_or_else(|| ddl_err("42601", "missing key column name"))?
        .trim_end_matches(';')
        .to_lowercase();
    Ok(key_col)
}

pub(super) fn parse_window_clause(upper: &str) -> (String, String, u64, u64) {
    // Look for "WINDOW <type> ON <ts_col>" or "WINDOW CUSTOM START '...' END '...'"
    let Some(win_pos) = upper.find(" WINDOW ") else {
        return ("none".into(), String::new(), 0, 0);
    };

    let after_window = &upper[win_pos + 8..];
    let tokens: Vec<&str> = after_window.split_whitespace().collect();

    if tokens.is_empty() {
        return ("none".into(), String::new(), 0, 0);
    }

    let win_type = tokens[0].to_lowercase();

    match win_type.as_str() {
        "daily" | "weekly" | "monthly" => {
            // WINDOW DAILY ON ts_col
            let ts_col = if tokens.len() >= 3 && tokens[1] == "ON" {
                tokens[2].to_lowercase()
            } else {
                "updated_at".into()
            };
            (win_type, ts_col, 0, 0)
        }
        "custom" => {
            // WINDOW CUSTOM START '2026-01-01' END '2026-03-31'
            let mut start_ms = 0u64;
            let mut end_ms = 0u64;
            let mut ts_col = "updated_at".to_string();

            for i in 1..tokens.len() {
                if tokens[i] == "START" && i + 1 < tokens.len() {
                    start_ms = tokens[i + 1].trim_matches('\'').parse().unwrap_or(0);
                }
                if tokens[i] == "END" && i + 1 < tokens.len() {
                    end_ms = tokens[i + 1].trim_matches('\'').parse().unwrap_or(0);
                }
                if tokens[i] == "ON" && i + 1 < tokens.len() {
                    ts_col = tokens[i + 1].to_lowercase();
                }
            }
            ("custom".into(), ts_col, start_ms, end_ms)
        }
        _ => ("none".into(), String::new(), 0, 0),
    }
}

pub(super) fn parse_function_args(sql: &str) -> Result<Vec<String>, DdlError> {
    let start = sql
        .find('(')
        .ok_or_else(|| ddl_err("42601", "expected '(' in function call"))?;
    let end = sql
        .rfind(')')
        .ok_or_else(|| ddl_err("42601", "expected ')' in function call"))?;
    if start >= end {
        return Ok(Vec::new());
    }
    let inner = &sql[start + 1..end];
    Ok(super::super::kv_atomic::split_args(inner))
}

pub(super) fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// A `RANGE(...)` score bound, as the raw value bytes of the index's leading
/// sort column.
///
/// The bytes must match what the engine derives from a stored row for that
/// column (`json_value_to_index_bytes` / `field_value_to_sort_bytes`), because
/// the Data Plane frames these into a tree bound with the index's own encoder
/// and compares them against keys built from stored values. So an integer
/// literal encodes as a sign-flipped big-endian i64, a decimal literal as an
/// order-preserving f64, and anything else as its UTF-8 bytes — the same three
/// shapes a stored number / number / string produce.
pub(super) fn parse_score_arg(s: &str) -> Option<Vec<u8>> {
    let t = unquote(s);
    if t.eq_ignore_ascii_case("NULL") || t.eq_ignore_ascii_case("NONE") || t == "*" {
        return None;
    }
    use crate::engine::kv::sorted_index::key::SortKeyEncoder;
    if let Ok(v) = t.parse::<i64>() {
        return Some(SortKeyEncoder::encode_i64(v).to_vec());
    }
    if let Ok(v) = t.parse::<f64>() {
        return Some(SortKeyEncoder::encode_f64(v).to_vec());
    }
    Some(t.into_bytes())
}
