// SPDX-License-Identifier: BUSL-1.1

//! Write checks the INSERT and UPSERT converters run on the rows a statement
//! supplies.
//!
//! A row that supplies a generated column is refused with SQLSTATE `428C9`:
//! the engine computes the column on every write.

use nodedb_sql::types::SqlValue;

/// Refuse a statement whose rows supply a generated column.
pub(in crate::control::planner::sql_plan_convert::dml) fn refuse_generated_cells(
    collection: &str,
    generated: &[String],
    rows: &[Vec<(String, SqlValue)>],
) -> crate::Result<()> {
    for row in rows {
        crate::control::security::catalog::collection_generated::refuse_generated_write(
            collection,
            generated,
            row.iter().map(|(name, _)| name.as_str()),
        )?;
    }
    Ok(())
}
