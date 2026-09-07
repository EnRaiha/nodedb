// SPDX-License-Identifier: Apache-2.0

//! Column DEFAULT expression evaluation at insert time.
//!
//! Supports ID generation functions (UUIDv4/v7, ULID, CUID2, NANOID), `NOW()`,
//! and literal values. More complex defaults (arbitrary expressions) fall
//! through to the plan-time const-folder.
//!
//! Lives in the SQL crate rather than beside one engine's converter because
//! every engine that materializes a DEFAULT has to produce the SAME value for
//! the same expression — a `DEFAULT now()` that means one thing on a document
//! collection and another on a key-value one would be a difference nobody
//! declared. The key-value planner also needs it BEFORE its declared-type
//! coercion and range checks run, so a materialized default is validated
//! exactly like a supplied one.

use nodedb_types::NodeDbError;

pub fn evaluate_default_expr(expr: &str) -> Result<Option<nodedb_types::Value>, NodeDbError> {
    let upper = expr.trim().to_uppercase();
    match upper.as_str() {
        "UUID_V7" | "UUIDV7" | "GEN_UUID_V7()" | "UUID_V7()" => Ok(Some(
            nodedb_types::Value::String(nodedb_types::id_gen::uuid_v7()),
        )),
        "UUID_V4" | "UUIDV4" | "UUID" | "GEN_UUID_V4()" | "UUID_V4()" => Ok(Some(
            nodedb_types::Value::String(nodedb_types::id_gen::uuid_v4()),
        )),
        "ULID" | "GEN_ULID()" | "ULID()" => Ok(Some(nodedb_types::Value::String(
            nodedb_types::id_gen::ulid(),
        ))),
        "CUID2" | "CUID2()" => Ok(Some(nodedb_types::Value::String(
            nodedb_types::id_gen::cuid2(),
        ))),
        "NANOID" | "NANOID()" => Ok(Some(nodedb_types::Value::String(
            nodedb_types::id_gen::nanoid(),
        ))),
        "NOW()" => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            Ok(Some(nodedb_types::Value::String(
                chrono::DateTime::from_timestamp_millis(now.as_millis() as i64)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| now.as_millis().to_string()),
            )))
        }
        _ => parse_parametric_or_literal(expr, &upper),
    }
}

fn parse_parametric_or_literal(
    expr: &str,
    upper: &str,
) -> Result<Option<nodedb_types::Value>, NodeDbError> {
    // NANOID(N) — custom length.
    if upper.starts_with("NANOID(") && upper.ends_with(')') {
        let len_str = &upper[7..upper.len() - 1];
        if let Ok(len) = len_str.parse::<usize>() {
            return Ok(Some(nodedb_types::Value::String(
                nodedb_types::id_gen::nanoid_with_length(len),
            )));
        }
    }
    // CUID2(N) — custom length; validates length range and surfaces planning errors.
    if upper.starts_with("CUID2(") && upper.ends_with(')') {
        let len_str = &upper[6..upper.len() - 1];
        if let Ok(len) = len_str.parse::<usize>() {
            let id = nodedb_types::id_gen::cuid2_with_length(len).map_err(|e| {
                NodeDbError::plan_error_at(
                    "defaults",
                    format!("CUID2({len}) default expression is invalid: {e}"),
                )
            })?;
            return Ok(Some(nodedb_types::Value::String(id)));
        }
    }
    // Numeric literal.
    if let Ok(i) = expr.trim().parse::<i64>() {
        return Ok(Some(nodedb_types::Value::Integer(i)));
    }
    if let Ok(f) = expr.trim().parse::<f64>() {
        return Ok(Some(nodedb_types::Value::Float(f)));
    }
    // Quoted string literal.
    let trimmed = expr.trim();
    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        return Ok(Some(nodedb_types::Value::String(
            trimmed[1..trimmed.len() - 1].to_string(),
        )));
    }

    // Fallback: try the plan-time const-folder for arbitrary expressions
    // (e.g. `upper('x')`, `1 + 2`, `concat('a', 'b')`).
    Ok(try_const_fold_default(expr))
}

/// Attempt to parse the DEFAULT expression as SQL, then const-fold it.
fn try_const_fold_default(expr: &str) -> Option<nodedb_types::Value> {
    let sql_expr = crate::parse_expr_string(expr).ok()?;
    let folded = crate::planner::const_fold::fold_constant_default(&sql_expr).ok()??;
    Some(sql_value_to_ndb(folded))
}

fn sql_value_to_ndb(v: crate::types::SqlValue) -> nodedb_types::Value {
    use crate::types::SqlValue;
    match v {
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Bool(b) => nodedb_types::Value::Bool(b),
        SqlValue::Int(i) => nodedb_types::Value::Integer(i),
        SqlValue::Float(f) => nodedb_types::Value::Float(f),
        SqlValue::Decimal(d) => nodedb_types::Value::Decimal(d),
        SqlValue::String(s) => nodedb_types::Value::String(s),
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b),
        SqlValue::Array(a) => {
            nodedb_types::Value::Array(a.into_iter().map(sql_value_to_ndb).collect())
        }
        SqlValue::Timestamp(dt) => nodedb_types::Value::NaiveDateTime(dt),
        SqlValue::Timestamptz(dt) => nodedb_types::Value::DateTime(dt),
    }
}

/// A sequence accessor appearing in a DEFAULT expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceAccessor {
    Nextval,
    Currval,
    Setval,
}

/// Whether a DEFAULT expression *starts like* a sequence accessor
/// (`nextval(`/`currval(`/`setval(` … `)`) even when the body does not parse.
/// Malformed accessor-looking defaults must raise loudly — never fall back
/// to the pure evaluator and silently vanish (#294 class).
pub fn looks_like_sequence_accessor(expr: &str) -> bool {
    let t = expr.trim();
    let bytes = t.as_bytes();
    if bytes.len() < 8 || !t.ends_with(')') {
        return false;
    }
    let head = |n: usize, name: &[u8]| bytes[..n].eq_ignore_ascii_case(name) && bytes[n] == b'(';
    head(7, b"nextval") || head(7, b"currval") || head(6, b"setval")
}

/// One canonical sequence-accessor recognizer for DEFAULT expressions,
/// shared by the SQL planner (which must skip these — the pure evaluator
/// cannot run them) and the convert layer (which advances the CP-side
/// registry). Matched on the ORIGINAL bytes, ASCII case-insensitive — never
/// by slicing the original with a length taken from a case-folded copy.
pub fn sequence_accessor(expr: &str) -> Option<(SequenceAccessor, String)> {
    let t = expr.trim();
    let bytes = t.as_bytes();
    if bytes.len() < 8 || !t.ends_with(')') {
        return None;
    }
    let (accessor, prefix_len) = if bytes[..7].eq_ignore_ascii_case(b"nextval") {
        (SequenceAccessor::Nextval, 7)
    } else if bytes[..7].eq_ignore_ascii_case(b"currval") {
        (SequenceAccessor::Currval, 7)
    } else if bytes[..6].eq_ignore_ascii_case(b"setval") {
        (SequenceAccessor::Setval, 6)
    } else {
        return None;
    };
    if bytes[prefix_len] != b'(' {
        return None;
    }
    let inner = &t[prefix_len + 1..t.len() - 1];
    let inner = inner.trim();
    let name = inner
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| inner.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(inner);
    if name.is_empty() {
        return None;
    }
    Some((accessor, name.to_string()))
}
