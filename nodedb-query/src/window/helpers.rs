// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for window-function evaluation.

use std::collections::HashMap;

use crate::expr::types::SqlExpr;

/// Group row indices by partition key, preserving first-seen partition order.
pub(super) fn build_partitions(
    rows: &[(String, serde_json::Value)],
    partition_by: &[SqlExpr],
) -> Vec<Vec<usize>> {
    if partition_by.is_empty() {
        return vec![(0..rows.len()).collect()];
    }

    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    let mut order = Vec::new();

    for (i, (_id, doc)) in rows.iter().enumerate() {
        let key: String = partition_by
            .iter()
            .map(|expr| eval_expr_on_json(expr, doc).to_string())
            .collect::<Vec<_>>()
            .join("\x00");
        let entry = groups.entry(key.clone()).or_default();
        if entry.is_empty() {
            order.push(key);
        }
        entry.push(i);
    }

    order.iter().filter_map(|k| groups.remove(k)).collect()
}

pub(super) fn set_window_col(row: &mut serde_json::Value, alias: &str, val: serde_json::Value) {
    if let serde_json::Value::Object(map) = row {
        map.insert(alias.to_string(), val);
    }
}

pub(super) fn get_field(doc: &serde_json::Value, field: &str) -> serde_json::Value {
    doc.get(field).cloned().unwrap_or(serde_json::Value::Null)
}

/// Evaluate a `SqlExpr` against a serde_json document, returning a serde_json value.
///
/// `build_partitions`/`order_keys_equal` (this function's only callers) feed
/// `evaluate_window_functions`, which is itself infallible (`fn(..)`) and is
/// reached, in the live server path, through
/// `nodedb/.../document/read/scan.rs`'s `CoreLoop::execute_document_scan`,
/// which returns the bridge `Response` envelope rather than `crate::Result`.
/// Giving a PARTITION BY / ORDER BY expression the same `22012`
/// statement-failure treatment as WHERE/projection expressions (nodedb
/// issue #216) would mean threading a `Result` through the window
/// subsystem's several still-infallible function implementations
/// (`row_number`/`rank`/`dense_rank`/`percent_rank`/`cume_dist`/aggregate
/// window functions) and extending `Response`/`ErrorCode` to carry it —
/// a substantially larger, separate change. For now a division/modulo-by-
/// zero here folds to `NULL`, matching prior behavior; see the unit-2
/// completion report.
pub(super) fn eval_expr_on_json(expr: &SqlExpr, doc: &serde_json::Value) -> serde_json::Value {
    match expr {
        SqlExpr::Column(name) => get_field(doc, name),
        SqlExpr::Literal(v) => serde_json::Value::from(v.clone()),
        other => {
            let ndb_doc = nodedb_types::Value::from(doc.clone());
            let result = other.eval(&ndb_doc).unwrap_or(nodedb_types::Value::Null);
            serde_json::Value::from(result)
        }
    }
}

pub(super) fn as_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Returns true when row at index `b` has the same ORDER BY key as row at
/// index `a` (used by peer-aware ranking like RANK and PERCENT_RANK).
pub(super) fn order_keys_equal(
    rows: &[(String, serde_json::Value)],
    a: usize,
    b: usize,
    order_by: &[(SqlExpr, bool)],
) -> bool {
    order_by.iter().all(|(expr, _)| {
        let va = eval_expr_on_json(expr, &rows[a].1);
        let vb = eval_expr_on_json(expr, &rows[b].1);
        va == vb
    })
}
