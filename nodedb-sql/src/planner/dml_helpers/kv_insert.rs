// SPDX-License-Identifier: Apache-2.0

//! Plan construction for the KV engine's `VALUES`-clause insert paths
//! (plain `INSERT`, `UPSERT`, and `INSERT ... ON CONFLICT DO UPDATE`).

use sqlparser::ast;

use super::range_check::check_declared_int_ranges;
use super::value_convert::expr_to_sql_value;
use crate::error::{Result, SqlError};
use crate::planner::declared_type_coerce::{
    coerce_assignments_to_declared_types, coerce_row_to_declared_types,
};
use crate::types::*;

/// Build a `SqlPlan::KvInsert` from a VALUES clause. Shared by plain INSERT,
/// UPSERT, and `INSERT ... ON CONFLICT (key) DO UPDATE` — the three paths
/// differ only in `intent` and `on_conflict_updates`, never in how entries
/// are extracted from the row exprs.
///
/// `pk_col` is the schema-defined primary-key column name from
/// `CollectionInfo::primary_key`.  When supplied, that column is used as
/// the KV key regardless of whether it is named `"key"`.  Falls back to
/// the literal name `"key"` when `pk_col` is `None` (legacy / generic
/// KV collections that use the built-in key/value column convention).
pub(crate) fn build_kv_insert_plan(
    table_name: String,
    columns: &[String],
    rows_ast: &[Vec<ast::Expr>],
    intent: KvInsertIntent,
    mut on_conflict_updates: Vec<(String, SqlExpr)>,
    pk_col: Option<&str>,
    declared_columns: &[ColumnInfo],
) -> Result<Vec<SqlPlan>> {
    // Positional KV insert (no column list): the key/value split below is
    // driven entirely by matching column *names* against `key_col_name`/
    // `"ttl"`. With an empty `columns` list there is no key to bind to, so
    // every row would silently become an empty-keyed, empty-valued entry
    // (all colliding). Reject rather than corrupt.
    if columns.is_empty() {
        return Err(SqlError::PositionalKvInsertUnsupported {
            collection: table_name,
        });
    }
    let key_col_name = pk_col.unwrap_or("key");
    let key_idx = columns.iter().position(|c| c == key_col_name);
    let ttl_idx = columns.iter().position(|c| c == "ttl");
    // When using a named primary-key column (e.g. `k STRING PRIMARY KEY`), we
    // store the key bytes in the KV key slot AND also keep the column in the
    // value map.  This allows scan filters on the primary-key column (e.g.
    // `WHERE k = 'x'`) and projection (e.g. `SELECT k FROM ...`) to work
    // without teaching the KV scan handler to inspect the raw key bytes.
    // The only column we exclude from the value map is the built-in `"key"`
    // sentinel (used by raw key/value KV collections) and `"ttl"`.
    let exclude_from_value: std::collections::HashSet<usize> = {
        let mut s = std::collections::HashSet::new();
        // Exclude the raw "key" sentinel column (not a named PK column).
        if key_col_name == "key"
            && let Some(idx) = key_idx
        {
            s.insert(idx);
        }
        if let Some(idx) = ttl_idx {
            s.insert(idx);
        }
        s
    };
    // Resolve every row's literals once, then coerce each cell to its declared
    // column type. Unlike the strict, columnar, and timeseries engines, KV has
    // no typed write path: its engine stores the bytes it is handed and the
    // declared schema exists only here, in the catalog. Without this the
    // stored cell's type is whatever the literal happened to resolve to — a
    // fractional literal is an exact `Decimal`, which serializes as a msgpack
    // string — while `RowDescription` advertises the declared numeric type, and
    // the read path can only encode SQL NULL for it. See
    // `declared_type_coerce` for the full rationale.
    let mut coerced_rows: Vec<Vec<(String, SqlValue)>> = Vec::with_capacity(rows_ast.len());
    for row_exprs in rows_ast {
        let mut row: Vec<(String, SqlValue)> = Vec::with_capacity(columns.len());
        for (i, col) in columns.iter().enumerate() {
            let Some(expr) = row_exprs.get(i) else { break };
            row.push((col.clone(), expr_to_sql_value(expr)?));
        }
        // The key column is exempt — see `coerce_rows_to_declared_types`.
        coerce_row_to_declared_types(declared_columns, &mut row, Some(key_col_name))?;
        coerced_rows.push(row);
    }
    // KV returns early from every INSERT/UPSERT entry point, so the declared
    // width check lives here rather than at the call sites — otherwise a
    // fourth KV entry point could be added without it. It runs on the coerced
    // values so a literal that only becomes an integer through coercion is
    // still range-checked against its declared width.
    check_declared_int_ranges(declared_columns, &coerced_rows)?;
    // `ON CONFLICT DO UPDATE SET col = <literal>` writes through the same
    // untyped KV path as the inserted row, so its literals need the same
    // declared-type coercion.
    coerce_assignments_to_declared_types(
        declared_columns,
        &mut on_conflict_updates,
        Some(key_col_name),
    )?;

    let mut entries = Vec::with_capacity(coerced_rows.len());
    let mut ttl_secs: u64 = 0;
    for row in &coerced_rows {
        let key_val = match key_idx.and_then(|idx| row.get(idx)) {
            Some((_, value)) => value.clone(),
            None => SqlValue::String(String::new()),
        };
        if let Some((_, value)) = ttl_idx.and_then(|idx| row.get(idx)) {
            match value {
                SqlValue::Int(n) => ttl_secs = (*n).max(0) as u64,
                SqlValue::Float(f) => ttl_secs = f.max(0.0) as u64,
                _ => {}
            }
        }
        let value_cols: Vec<(String, SqlValue)> = row
            .iter()
            .enumerate()
            .filter(|(i, _)| !exclude_from_value.contains(i))
            .map(|(_, cell)| cell.clone())
            .collect();
        entries.push((key_val, value_cols));
    }
    Ok(vec![SqlPlan::KvInsert {
        collection: table_name,
        entries,
        ttl_secs,
        intent,
        on_conflict_updates,
    }])
}
