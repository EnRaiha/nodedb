// SPDX-License-Identifier: Apache-2.0

use sqlparser::ast::Value;

use crate::error::{Result, SqlError};
use crate::types::*;

/// Convert a sqlparser `Value` to our `SqlValue`.
///
/// Number literal routing:
/// - Pure integers → `SqlValue::Int`.
/// - Numbers with `.`, `e`, or `E` → `SqlValue::Decimal` (exact arithmetic).
/// - If decimal parse fails → fallback to `SqlValue::Float`, then `SqlValue::String`.
pub fn convert_value(val: &Value) -> Result<SqlValue> {
    match val {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(SqlValue::Int(i))
            } else if n.contains('.') || n.contains('e') || n.contains('E') {
                // Fractional or scientific notation: prefer exact Decimal.
                if let Ok(d) = rust_decimal::Decimal::from_str_exact(n) {
                    Ok(SqlValue::Decimal(d))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(SqlValue::Float(f))
                } else {
                    Ok(SqlValue::String(n.clone()))
                }
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(SqlValue::Float(f))
            } else {
                Ok(SqlValue::String(n.clone()))
            }
        }
        Value::SingleQuotedString(s) => Ok(SqlValue::String(s.clone())),
        Value::Boolean(b) => Ok(SqlValue::Bool(*b)),
        Value::Null => Ok(SqlValue::Null),
        // `X'...'` hex-string literal → raw bytes. Enables byte-typed
        // arguments such as WKB geometry (`ST_GeomFromWKB(X'01...')`).
        Value::HexStringLiteral(s) => Ok(SqlValue::Bytes(decode_hex_literal(s)?)),
        _ => Err(SqlError::Unsupported {
            detail: format!("value literal: {val}"),
        }),
    }
}

/// Decode an even-length ASCII hex string (the inner text of an `X'...'`
/// literal) into its byte sequence.
fn decode_hex_literal(s: &str) -> Result<Vec<u8>> {
    parse_hex_bytes(s).map_err(|reason| SqlError::Parse {
        detail: format!("hex literal {reason}: X'{s}'"),
    })
}

/// Parse an even-length ASCII hex string into bytes. Shared by `X'...'`
/// literal decoding here and plain hex-string WKB arguments in
/// `planner::spatial_ctor`, so the two surfaces never drift. Returns the
/// failure reason as a fragment; callers wrap it into their own contextual
/// `SqlError` variant.
pub(crate) fn parse_hex_bytes(s: &str) -> std::result::Result<Vec<u8>, &'static str> {
    if !s.len().is_multiple_of(2) {
        return Err("must have an even number of digits");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| "contains an invalid hex digit"))
        .collect()
}

/// Parse an interval string to microseconds.
///
/// Delegates to `nodedb_types::kv_parsing::parse_interval_to_ms` (ms → μs)
/// and `NdbDuration::parse` for compound shorthand forms.
pub(super) fn parse_interval_to_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Try NdbDuration::parse first (handles compound "1h30m", "500ms", "2d").
    if let Some(dur) = nodedb_types::NdbDuration::parse(s) {
        return Some(dur.micros);
    }

    // Delegate to shared interval parser (handles all forms including compound).
    if let Ok(ms) = nodedb_types::kv_parsing::parse_interval_to_ms(s) {
        return Some(ms as i64 * 1000); // ms → μs
    }

    None
}
