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

/// Sort aggregated rows by `sort_keys = [(column, ascending), ...]`.
///
/// Each row is a `serde_json::Value::Object`; for every key, the
/// extracted value is converted to a comparable form (numbers compared
/// numerically, strings lexically, nulls last). Keys missing from a
/// row sort as null. The sort is stable to preserve relative order of
/// equal-key rows.
pub(super) fn sort_aggregated_rows(rows: &mut [serde_json::Value], sort_keys: &[(String, bool)]) {
    if sort_keys.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
        for (column, ascending) in sort_keys {
            let av = a.get(column);
            let bv = b.get(column);
            let ord = compare_json_values(av, bv);
            if ord != std::cmp::Ordering::Equal {
                return if *ascending { ord } else { ord.reverse() };
            }
        }
        std::cmp::Ordering::Equal
    });
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
