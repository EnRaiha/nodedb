// SPDX-License-Identifier: BUSL-1.1

//! Shared msgpack-row sorting utilities used by scan handlers.

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_query::{EvalError, compare_json, eval_expr_on_json};

/// Sort msgpack-map rows by the ORDER BY terms. Decodes each row to JSON,
/// evaluates each sort expression against it, and reorders the original
/// msgpack bytes.
///
/// A sort key is an expression, so evaluating it can fail — `ORDER BY 1/qty`
/// over a row with `qty = 0` returns `Err(EvalError::DivisionByZero)` and the
/// statement fails with SQLSTATE `22012`, the same as it would in a
/// projection. Returning the rows unsorted instead would answer a different
/// query than the client asked for.
///
/// Decode failures for individual rows are logged at debug level and treated
/// as `null` for comparison purposes, so they sort to the start rather than
/// failing the entire scan.
pub(in crate::data::executor) fn sort_msgpack_rows(
    rows: &mut [Vec<u8>],
    sort_keys: &[SortKeySpec],
) -> Result<(), EvalError> {
    if sort_keys.is_empty() {
        return Ok(());
    }

    let decoded: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| match nodedb_types::json_from_msgpack(r) {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(err = %e, "msgpack decode failed during sort; treating row as null");
                serde_json::Value::Null
            }
        })
        .collect();

    let keyed = sort_key_values(&decoded, sort_keys)?;

    let mut indices: Vec<usize> = (0..rows.len()).collect();
    indices.sort_by(|&a, &b| compare_key_rows(&keyed[a], &keyed[b], sort_keys));

    let original: Vec<Vec<u8>> = rows.to_vec();
    for (dst, src) in indices.iter().enumerate() {
        rows[dst] = original[*src].clone();
    }
    Ok(())
}

/// Evaluate every sort expression against every row, up front.
///
/// Sorting compares a pair at a time and `sort_by` cannot report an error, so
/// the fallible work happens here where it can propagate.
pub(in crate::data::executor) fn sort_key_values(
    rows: &[serde_json::Value],
    sort_keys: &[SortKeySpec],
) -> Result<Vec<Vec<serde_json::Value>>, EvalError> {
    rows.iter()
        .map(|row| {
            sort_keys
                .iter()
                .map(|k| eval_expr_on_json(&k.expr, row))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect()
}

/// Compare two rows' pre-evaluated sort values.
pub(in crate::data::executor) fn compare_key_rows(
    a: &[serde_json::Value],
    b: &[serde_json::Value],
    sort_keys: &[SortKeySpec],
) -> std::cmp::Ordering {
    for (idx, key) in sort_keys.iter().enumerate() {
        let (Some(va), Some(vb)) = (a.get(idx), b.get(idx)) else {
            continue;
        };
        // NULL placement follows the key's NULLS FIRST/LAST setting, which the
        // sort direction never flips.
        let ord = match key.order_nulls(va.is_null(), vb.is_null()) {
            Some(ord) => ord,
            None => key.direct(compare_json(va, vb)),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}
