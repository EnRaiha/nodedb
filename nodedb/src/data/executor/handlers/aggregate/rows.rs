// SPDX-License-Identifier: BUSL-1.1

//! Post-aggregate row helpers: user-alias renaming and ORDER BY sorting.

use nodedb_physical::physical_plan::AggregateSpec;

pub(super) fn apply_user_aliases_to_rows(
    rows: &mut [serde_json::Value],
    aggregates: &[AggregateSpec],
) {
    let renames: Vec<(&str, &str)> = aggregates
        .iter()
        .filter_map(|agg| {
            agg.user_alias
                .as_deref()
                .filter(|alias| *alias != agg.alias)
                .map(|alias| (agg.alias.as_str(), alias))
        })
        .collect();

    if renames.is_empty() {
        return;
    }

    for row in rows {
        if let Some(obj) = row.as_object_mut() {
            for (from, to) in &renames {
                if let Some(value) = obj.remove(*from) {
                    obj.insert((*to).to_string(), value);
                }
            }
        }
    }
}

/// Sort finalized group rows by the post-aggregate ORDER BY terms.
///
/// Each row is a `serde_json::Value::Object` keyed by output column name. A
/// key naming one of those columns reads straight out of the row; a computed
/// key (`ORDER BY 1000 / SUM(amount)`) is evaluated against it, with the
/// planner having already bound each aggregate call to the column it lands in.
///
/// Evaluation is fallible — a zero divisor in a sort key fails the statement
/// with SQLSTATE `22012` — so every row's keys are evaluated up front, where
/// the error can propagate, rather than inside the comparator. Keys missing
/// from a row sort as NULL, placed by the key's NULLS FIRST/LAST setting. The
/// sort is stable to preserve relative order of equal-key rows.
pub(super) fn sort_aggregated_rows(
    rows: &mut [serde_json::Value],
    sort_keys: &[nodedb_physical::physical_plan::SortKeySpec],
) -> crate::Result<()> {
    if sort_keys.is_empty() {
        return Ok(());
    }

    let keyed: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| {
            sort_keys
                .iter()
                .map(|k| nodedb_query::eval_expr_on_json(&k.expr, row).map_err(crate::Error::from))
                .collect::<crate::Result<Vec<_>>>()
        })
        .collect::<crate::Result<Vec<_>>>()?;

    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by(|&a, &b| {
        for (idx, key) in sort_keys.iter().enumerate() {
            let av = keyed[a].get(idx);
            let bv = keyed[b].get(idx);
            let ord = match key.order_nulls(
                matches!(av, None | Some(serde_json::Value::Null)),
                matches!(bv, None | Some(serde_json::Value::Null)),
            ) {
                Some(ord) => ord,
                None => key.direct(compare_json_values(av, bv)),
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    let original = rows.to_vec();
    for (dst, &src) in order.iter().enumerate() {
        rows[dst] = original[src].clone();
    }
    Ok(())
}

/// Compare two `Option<&serde_json::Value>` for sort. Nulls / absent
/// keys sort last; numbers compare numerically; everything else falls
/// back to string comparison.
fn compare_json_values(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    use serde_json::Value as V;
    use std::cmp::Ordering;
    let a_is_null = matches!(a, None | Some(V::Null));
    let b_is_null = matches!(b, None | Some(V::Null));
    if a_is_null && b_is_null {
        return Ordering::Equal;
    }
    if a_is_null {
        return Ordering::Greater;
    }
    if b_is_null {
        return Ordering::Less;
    }
    match (a.unwrap(), b.unwrap()) {
        (V::Number(x), V::Number(y)) => {
            let xf = x.as_f64().unwrap_or(0.0);
            let yf = y.as_f64().unwrap_or(0.0);
            xf.partial_cmp(&yf).unwrap_or(Ordering::Equal)
        }
        (V::String(x), V::String(y)) => x.cmp(y),
        (V::Bool(x), V::Bool(y)) => x.cmp(y),
        (x, y) => x.to_string().cmp(&y.to_string()),
    }
}
