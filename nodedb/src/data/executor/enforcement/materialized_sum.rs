// SPDX-License-Identifier: BUSL-1.1

//! Materialized sum: on INSERT to source collection, atomically update balance
//! on the target collection within the same Data Plane transaction.
//!
//! The balance column on the target is maintained as:
//! `target.column += eval(value_expr, new_source_row)`
//!
//! This fires synchronously in the write path (not via Event Plane) to ensure
//! atomicity — the source INSERT and target balance update succeed or fail together.

use rust_decimal::Decimal;

use crate::bridge::envelope::ErrorCode;
use crate::engine::sparse::btree::SparseEngine;
use nodedb_physical::physical_plan::MaterializedSumBinding;

/// Apply materialized sum updates for all bindings on a source INSERT.
///
/// For each binding:
/// 1. Evaluate `value_expr` against the new source document → delta
/// 2. Extract join key from source doc → target document ID
/// 3. Read target document from sparse engine
/// 4. Add delta to the target column
/// 5. Write updated target document back
///
/// Returns a list of (collection, doc_id, old_value) for rollback tracking.
pub fn apply_materialized_sums(
    sparse: &SparseEngine,
    database_id: u64,
    tid: u64,
    bindings: &[MaterializedSumBinding],
    source_doc: &serde_json::Value,
) -> Result<Vec<TargetWrite>, ErrorCode> {
    let mut writes = Vec::new();

    for binding in bindings {
        let write = apply_single_binding(sparse, database_id, tid, binding, source_doc)?;
        if let Some(w) = write {
            writes.push(w);
        }
    }

    Ok(writes)
}

/// A target write performed by materialized sum, tracked for rollback.
pub struct TargetWrite {
    /// Target collection name.
    pub collection: String,
    /// Target document ID.
    pub document_id: String,
    /// Old value of the target document (before the balance update).
    /// `None` if the target document didn't exist.
    pub old_value: Option<Vec<u8>>,
}

/// Apply a single materialized sum binding.
fn apply_single_binding(
    sparse: &SparseEngine,
    database_id: u64,
    tid: u64,
    binding: &MaterializedSumBinding,
    source_doc: &serde_json::Value,
) -> Result<Option<TargetWrite>, ErrorCode> {
    // 1. Evaluate value_expr against the source document to get the delta.
    // A materialized-sum binding fires on the write path (nodedb issue
    // #216): a division/modulo-by-zero fails the write instead of silently
    // skipping the balance update.
    let source_val = nodedb_types::Value::from(source_doc.clone());
    let delta_val = binding
        .value_expr
        .eval(&source_val)
        .map_err(|_e| ErrorCode::DivisionByZero)?;
    let delta_json = serde_json::Value::from(delta_val);
    let delta = json_to_decimal(&delta_json);
    let Some(delta) = delta else {
        // value_expr evaluated to NULL or non-numeric → skip (no balance change).
        return Ok(None);
    };
    if delta == Decimal::ZERO {
        return Ok(None);
    }

    // 2. Extract the join key from the source document.
    let join_key = source_doc
        .get(&binding.join_column)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ErrorCode::Internal {
            detail: format!(
                "materialized_sum: join column '{}' missing or not a string in source document",
                binding.join_column
            ),
        })?;

    // 3. Read the target document.
    let old_bytes = sparse
        .get(database_id, tid, &binding.target_collection, join_key)
        .map_err(|e| ErrorCode::Internal {
            detail: format!(
                "materialized_sum: failed to read {}/{}: {e}",
                binding.target_collection, join_key
            ),
        })?;

    let old_bytes = old_bytes.ok_or_else(|| ErrorCode::Internal {
        detail: format!(
            "materialized_sum: target row {}/{} not found",
            binding.target_collection, join_key
        ),
    })?;

    // 4. Decode, update balance, re-encode.
    let mut target_doc =
        super::super::doc_format::decode_document(&old_bytes).ok_or_else(|| {
            ErrorCode::Internal {
                detail: format!(
                    "materialized_sum: failed to decode target {}/{}",
                    binding.target_collection, join_key
                ),
            }
        })?;

    let current_balance = target_doc
        .get(&binding.target_column)
        .and_then(json_to_decimal)
        .unwrap_or(Decimal::ZERO);

    let new_balance = current_balance + delta;

    // Update the balance field in the JSON document.
    // Always store as string to preserve exact Decimal precision — f64 is lossy
    // for values with >15 significant digits (e.g. 123456789012345.67).
    if let Some(obj) = target_doc.as_object_mut() {
        let json_val = serde_json::json!(new_balance.to_string());
        obj.insert(binding.target_column.clone(), json_val);
    }

    // 5. Write back.
    let new_bytes = super::super::doc_format::encode_to_msgpack(&target_doc);
    sparse
        .put(
            database_id,
            tid,
            &binding.target_collection,
            join_key,
            &new_bytes,
        )
        .map_err(|e| ErrorCode::Internal {
            detail: format!(
                "materialized_sum: failed to write {}/{}: {e}",
                binding.target_collection, join_key
            ),
        })?;

    Ok(Some(TargetWrite {
        collection: binding.target_collection.clone(),
        document_id: join_key.to_string(),
        old_value: Some(old_bytes),
    }))
}

/// Convert a JSON value to `rust_decimal::Decimal`.
fn json_to_decimal(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Decimal::from(i))
            } else {
                n.as_f64().and_then(|f| Decimal::try_from(f).ok())
            }
        }
        serde_json::Value::String(s) => s.parse::<Decimal>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn json_to_decimal_integer() {
        assert_eq!(json_to_decimal(&serde_json::json!(100)), Some(d("100")));
    }

    #[test]
    fn json_to_decimal_float() {
        let val = json_to_decimal(&serde_json::json!(99.5));
        assert!(val.is_some());
    }

    #[test]
    fn json_to_decimal_string() {
        assert_eq!(
            json_to_decimal(&serde_json::json!("1500.75")),
            Some(d("1500.75"))
        );
    }

    #[test]
    fn json_to_decimal_null() {
        assert_eq!(json_to_decimal(&serde_json::Value::Null), None);
    }

    #[test]
    fn json_to_decimal_negative() {
        assert_eq!(json_to_decimal(&serde_json::json!(-250)), Some(d("-250")));
    }
}
