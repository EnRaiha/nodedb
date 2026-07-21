// SPDX-License-Identifier: Apache-2.0

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::resolver::expr::convert_value;
use crate::types::*;

pub(super) fn convert_value_rows(
    columns: &[String],
    rows: &[Vec<ast::Expr>],
) -> Result<Vec<Vec<(String, SqlValue)>>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, expr)| {
                    let col = columns.get(i).cloned().unwrap_or_else(|| format!("col{i}"));
                    let val = expr_to_sql_value(expr)?;
                    Ok((col, val))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect()
}

/// Resolve the effective column list for a `VALUES`-clause INSERT/UPSERT.
///
/// A *positional* insert — `INSERT INTO t VALUES (...)` with no explicit
/// column list — must still bind each value to the collection's declared
/// column names. Left alone, `convert_value_rows` falls back to synthetic
/// `col0`, `col1`, ... names for every value (see its `col{i}` fallback
/// below): the row stores fine, but named projections and WHERE predicates
/// can never find it again (#202).
///
/// Named inserts (`columns` already non-empty) and schemaless collections
/// (`info.columns` empty — there is no declared order to bind to) pass
/// through unchanged; the `col{i}` fallback remains the last resort for
/// those.
///
/// Fewer values than declared columns binds by position against the
/// leading columns, consistent with a partial named insert (e.g.
/// `INSERT INTO t (id) VALUES (1)` on a wider table also only binds
/// `id`). More values than declared columns is rejected outright:
/// inventing a `colN` slot for the overflow would reproduce the exact
/// unaddressable-column failure this fix closes.
pub(super) fn resolve_insert_columns(
    columns: Vec<String>,
    info: &CollectionInfo,
    rows: &[Vec<ast::Expr>],
) -> Result<Vec<String>> {
    if !columns.is_empty() || info.columns.is_empty() {
        return Ok(columns);
    }

    let declared: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
    if let Some(row) = rows.iter().find(|row| row.len() > declared.len()) {
        return Err(SqlError::InsertColumnArityMismatch {
            collection: info.name.clone(),
            given: row.len(),
            declared: declared.len(),
        });
    }
    Ok(declared)
}

pub(super) fn expr_to_sql_value(expr: &ast::Expr) -> Result<SqlValue> {
    match expr {
        ast::Expr::Value(v) => convert_value(&v.value),
        // Array literals lower element-wise into `SqlValue::Array`; there is
        // no array-literal `SqlValue` the constant folder could produce.
        ast::Expr::Array(ast::Array { elem, .. }) => {
            let vals = elem.iter().map(expr_to_sql_value).collect::<Result<_>>()?;
            Ok(SqlValue::Array(vals))
        }
        // `ST_Point(...)` / `ST_GeomFromGeoJSON(...)` synthesise a GeoJSON
        // string in place rather than resolving as registered scalar
        // functions, so they keep their bespoke handling.
        ast::Expr::Function(func) => match super::spatial_ctor::try_spatial_constructor(func) {
            Some(result) => result,
            // Non-spatial functions (`now()`, `date_add(...)`, registered
            // scalars) fold through the shared pipeline below.
            None => fold_constant_value(expr),
        },
        // Everything else — `::TYPE` / `CAST(... AS TYPE)` casts, arithmetic,
        // string concatenation, parenthesised literals — goes through the
        // same resolver and constant folder the `SELECT` projection path
        // uses, so the two surfaces never drift. Only genuinely row- or
        // runtime-dependent expressions (column refs, subqueries, unknown
        // functions) fail here.
        _ => fold_constant_value(expr),
    }
}

fn fold_constant_value(expr: &ast::Expr) -> Result<SqlValue> {
    let sql_expr = crate::resolver::expr::convert_expr(expr)?;
    super::const_fold::fold_constant_default(&sql_expr).ok_or_else(|| SqlError::Unsupported {
        detail: format!("value expression: {expr}"),
    })
}

pub(super) fn extract_table_name_from_table_with_joins(
    table: &ast::TableWithJoins,
) -> Result<String> {
    match &table.relation {
        ast::TableFactor::Table { name, .. } => Ok(normalize_object_name_checked(name)?),
        _ => Err(SqlError::Unsupported {
            detail: "non-table target in DML".into(),
        }),
    }
}

/// Extract point-operation keys from WHERE clause (WHERE pk = literal OR pk IN (...)).
pub fn extract_point_keys(selection: Option<&ast::Expr>, info: &CollectionInfo) -> Vec<SqlValue> {
    let pk = match &info.primary_key {
        Some(pk) => pk.clone(),
        None => return Vec::new(),
    };

    let expr = match selection {
        Some(e) => e,
        None => return Vec::new(),
    };

    let mut keys = Vec::new();
    collect_pk_equalities(expr, &pk, &mut keys);
    keys
}

fn collect_pk_equalities(expr: &ast::Expr, pk: &str, keys: &mut Vec<SqlValue>) {
    match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if is_column(left, pk)
                && let Ok(v) = expr_to_sql_value(right)
            {
                keys.push(v);
            } else if is_column(right, pk)
                && let Ok(v) = expr_to_sql_value(left)
            {
                keys.push(v);
            }
        }
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Or,
            right,
        } => {
            collect_pk_equalities(left, pk, keys);
            collect_pk_equalities(right, pk, keys);
        }
        ast::Expr::InList {
            expr: inner,
            list,
            negated: false,
        } if is_column(inner, pk) => {
            for item in list {
                if let Ok(v) = expr_to_sql_value(item) {
                    keys.push(v);
                }
            }
        }
        _ => {}
    }
}

fn is_column(expr: &ast::Expr, name: &str) -> bool {
    match expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident) == name,
        // Three or more parts: schema.table.col — never matches a plain pk name.
        ast::Expr::CompoundIdentifier(parts) if parts.len() >= 3 => false,
        ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            normalize_ident(&parts[1]) == name
        }
        _ => false,
    }
}

/// Build a `SqlPlan::VectorPrimaryInsert` from parsed rows.
///
/// Extracts the vector-field column into `vector: Vec<f32>` and collects
/// all remaining columns into `payload_fields`. Rows missing the vector
/// column are rejected.
pub(super) fn build_vector_primary_insert_plan(
    collection: &str,
    vpc: &nodedb_types::VectorPrimaryConfig,
    _columns: &[String],
    rows: Vec<Vec<(String, SqlValue)>>,
) -> Result<Vec<SqlPlan>> {
    let mut result_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut vector: Option<Vec<f32>> = None;
        let mut payload_fields = std::collections::HashMap::new();

        for (col, val) in row {
            if col == vpc.vector_field {
                match val {
                    SqlValue::Array(items) => {
                        let floats: Result<Vec<f32>> = items
                            .iter()
                            .map(|v| match v {
                                SqlValue::Float(f) => Ok(*f as f32),
                                SqlValue::Int(i) => Ok(*i as f32),
                                SqlValue::Decimal(d) => {
                                    use rust_decimal::prelude::ToPrimitive;
                                    d.to_f32().ok_or_else(|| SqlError::Parse {
                                        detail: format!(
                                            "vector element decimal '{d}' is out of f32 range"
                                        ),
                                    })
                                }
                                other => Err(SqlError::Parse {
                                    detail: format!(
                                        "vector field must contain numbers, got {other:?}"
                                    ),
                                }),
                            })
                            .collect();
                        vector = Some(floats?);
                    }
                    other => {
                        return Err(SqlError::Parse {
                            detail: format!(
                                "vector field '{}' must be an array literal, got {other:?}",
                                vpc.vector_field
                            ),
                        });
                    }
                }
            } else {
                payload_fields.insert(col, val);
            }
        }

        let vector = vector.ok_or_else(|| SqlError::Parse {
            detail: format!(
                "vector-primary INSERT missing required vector field '{}'",
                vpc.vector_field
            ),
        })?;

        result_rows.push(VectorPrimaryRow {
            surrogate: nodedb_types::Surrogate::ZERO,
            vector,
            payload_fields,
        });
    }

    Ok(vec![SqlPlan::VectorPrimaryInsert {
        collection: collection.to_string(),
        field: vpc.vector_field.clone(),
        quantization: vpc.quantization,
        storage_dtype: vpc.storage_dtype,
        payload_indexes: vpc.payload_indexes.clone(),
        rows: result_rows,
    }])
}

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
pub(super) fn build_kv_insert_plan(
    table_name: String,
    columns: &[String],
    rows_ast: &[Vec<ast::Expr>],
    intent: KvInsertIntent,
    on_conflict_updates: Vec<(String, SqlExpr)>,
    pk_col: Option<&str>,
) -> Result<Vec<SqlPlan>> {
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
    let mut entries = Vec::with_capacity(rows_ast.len());
    let mut ttl_secs: u64 = 0;
    for row_exprs in rows_ast {
        let key_val = match key_idx {
            Some(idx) => expr_to_sql_value(&row_exprs[idx])?,
            None => SqlValue::String(String::new()),
        };
        if let Some(idx) = ttl_idx {
            match expr_to_sql_value(&row_exprs[idx]) {
                Ok(SqlValue::Int(n)) => ttl_secs = n.max(0) as u64,
                Ok(SqlValue::Float(f)) => ttl_secs = f.max(0.0) as u64,
                _ => {}
            }
        }
        let value_cols: Vec<(String, SqlValue)> = columns
            .iter()
            .enumerate()
            .filter(|(i, _)| !exclude_from_value.contains(i))
            .map(|(i, col)| {
                let val = expr_to_sql_value(&row_exprs[i])?;
                Ok((col.clone(), val))
            })
            .collect::<Result<Vec<_>>>()?;
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
