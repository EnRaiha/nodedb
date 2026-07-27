// SPDX-License-Identifier: Apache-2.0

//! Effective-column resolution for positional `VALUES`-clause inserts.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::types::*;

/// Resolve the effective column list for a `VALUES`-clause INSERT/UPSERT.
///
/// A *positional* insert — `INSERT INTO t VALUES (...)` with no explicit
/// column list — must still bind each value to the collection's declared
/// column names. Left alone, `convert_value_rows` falls back to synthetic
/// `col0`, `col1`, ... names for every value (see its `col{i}` fallback
/// below): the row stores fine, but named projections and WHERE predicates
/// can never find it again.
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
pub(crate) fn resolve_insert_columns(
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
