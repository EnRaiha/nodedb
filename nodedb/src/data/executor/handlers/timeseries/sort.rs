// SPDX-License-Identifier: BUSL-1.1

//! `ORDER BY` for timeseries results.
//!
//! Both timeseries result shapes — raw rows from the memtable / partitions and
//! encoded aggregate rows — are `rmpv::Value::Map`s keyed by the name the
//! client sees, so one comparator orders either. Sorting happens before the
//! row limit is applied: `ORDER BY … LIMIT n` must return the first `n` rows
//! of the requested order, not `n` arbitrary rows in that order.

use std::cmp::Ordering;

/// Sort materialized result rows by the planner's `(column, ascending)` list.
///
/// Rows are compared on the named fields in significance order. A field that
/// is absent from a row compares as NULL, and NULL sorts last ascending —
/// PostgreSQL's default. An unknown column name leaves the order untouched
/// rather than shuffling rows arbitrarily.
pub(in crate::data::executor) fn sort_rows(rows: &mut [rmpv::Value], sort_keys: &[(String, bool)]) {
    if sort_keys.is_empty() {
        return;
    }
    rows.sort_by(|a, b| compare_rows(a, b, sort_keys));
}

fn compare_rows(a: &rmpv::Value, b: &rmpv::Value, sort_keys: &[(String, bool)]) -> Ordering {
    for (field, ascending) in sort_keys {
        let ord = compare_values(field_of(a, field), field_of(b, field));
        if ord != Ordering::Equal {
            return if *ascending { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

/// Look up a field in an rmpv map row. `None` for a non-map row or a missing
/// field — both treated as NULL by the comparator.
fn field_of<'a>(row: &'a rmpv::Value, field: &str) -> Option<&'a rmpv::Value> {
    let rmpv::Value::Map(entries) = row else {
        return None;
    };
    entries
        .iter()
        .find(|(key, _)| match key {
            rmpv::Value::String(name) => name.as_str() == Some(field),
            _ => false,
        })
        .map(|(_, value)| value)
        .filter(|value| !matches!(value, rmpv::Value::Nil))
}

fn compare_values(a: Option<&rmpv::Value>, b: Option<&rmpv::Value>) -> Ordering {
    // NULL / absent sorts last in ascending order (PostgreSQL default).
    let (a, b) = match (a, b) {
        (Some(a), Some(b)) => (a, b),
        (None, None) => return Ordering::Equal,
        (None, Some(_)) => return Ordering::Greater,
        (Some(_), None) => return Ordering::Less,
    };
    if let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) {
        return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
    }
    match (a, b) {
        (rmpv::Value::String(x), rmpv::Value::String(y)) => {
            x.as_str().unwrap_or("").cmp(y.as_str().unwrap_or(""))
        }
        (rmpv::Value::Boolean(x), rmpv::Value::Boolean(y)) => x.cmp(y),
        // Exotic shapes never appear in a timeseries result row; keep the
        // order stable rather than inventing one.
        _ => Ordering::Equal,
    }
}

/// Numeric view of a value, so an integer column and a float column compare
/// against each other the way SQL expects.
fn as_f64(value: &rmpv::Value) -> Option<f64> {
    match value {
        rmpv::Value::Integer(n) => n.as_i64().map(|i| i as f64).or_else(|| n.as_f64()),
        rmpv::Value::F32(f) => Some(*f as f64),
        rmpv::Value::F64(f) => Some(*f),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, rmpv::Value)]) -> rmpv::Value {
        rmpv::Value::Map(
            pairs
                .iter()
                .map(|(k, v)| (rmpv::Value::String((*k).into()), v.clone()))
                .collect(),
        )
    }

    fn ints(field: &str, rows: &[rmpv::Value]) -> Vec<i64> {
        rows.iter()
            .map(|r| match field_of(r, field) {
                Some(rmpv::Value::Integer(n)) => n.as_i64().unwrap_or(0),
                _ => i64::MIN,
            })
            .collect()
    }

    #[test]
    fn descending_order_reverses() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Integer(100.into()))]),
            row(&[("ts", rmpv::Value::Integer(300.into()))]),
            row(&[("ts", rmpv::Value::Integer(200.into()))]),
        ];
        sort_rows(&mut rows, &[("ts".to_string(), false)]);
        assert_eq!(ints("ts", &rows), vec![300, 200, 100]);
    }

    #[test]
    fn ascending_order_sorts_up() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Integer(300.into()))]),
            row(&[("ts", rmpv::Value::Integer(100.into()))]),
        ];
        sort_rows(&mut rows, &[("ts".to_string(), true)]);
        assert_eq!(ints("ts", &rows), vec![100, 300]);
    }

    #[test]
    fn secondary_key_breaks_ties() {
        let mut rows = vec![
            row(&[
                ("host", rmpv::Value::String("a".into())),
                ("ts", rmpv::Value::Integer(200.into())),
            ]),
            row(&[
                ("host", rmpv::Value::String("a".into())),
                ("ts", rmpv::Value::Integer(100.into())),
            ]),
        ];
        sort_rows(
            &mut rows,
            &[("host".to_string(), true), ("ts".to_string(), true)],
        );
        assert_eq!(ints("ts", &rows), vec![100, 200]);
    }

    #[test]
    fn nulls_and_missing_fields_sort_last_ascending() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Nil)]),
            row(&[("other", rmpv::Value::Integer(1.into()))]),
            row(&[("ts", rmpv::Value::Integer(5.into()))]),
        ];
        sort_rows(&mut rows, &[("ts".to_string(), true)]);
        assert_eq!(ints("ts", &rows)[0], 5);
    }

    #[test]
    fn integers_and_floats_compare_numerically() {
        let mut rows = vec![
            row(&[("v", rmpv::Value::F64(2.5))]),
            row(&[("v", rmpv::Value::Integer(2.into()))]),
            row(&[("v", rmpv::Value::F64(1.5))]),
        ];
        sort_rows(&mut rows, &[("v".to_string(), true)]);
        let vs: Vec<f64> = rows
            .iter()
            .map(|r| as_f64(field_of(r, "v").unwrap()).unwrap())
            .collect();
        assert_eq!(vs, vec![1.5, 2.0, 2.5]);
    }

    #[test]
    fn an_unknown_column_leaves_the_order_untouched() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Integer(2.into()))]),
            row(&[("ts", rmpv::Value::Integer(1.into()))]),
        ];
        sort_rows(&mut rows, &[("nope".to_string(), true)]);
        assert_eq!(ints("ts", &rows), vec![2, 1]);
    }

    #[test]
    fn no_sort_keys_is_a_no_op() {
        let mut rows = vec![
            row(&[("ts", rmpv::Value::Integer(2.into()))]),
            row(&[("ts", rmpv::Value::Integer(1.into()))]),
        ];
        sort_rows(&mut rows, &[]);
        assert_eq!(ints("ts", &rows), vec![2, 1]);
    }
}
