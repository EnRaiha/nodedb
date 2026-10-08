// SPDX-License-Identifier: Apache-2.0

//! Row-oriented iteration and single-row lookup over the memtable.

use nodedb_types::columnar::ColumnDef;
use nodedb_types::value::Value;

use super::column_data::ColumnData;
use super::core::ColumnarMemtable;
use crate::error::ColumnarError;

impl ColumnarMemtable {
    /// Iterate rows as `Vec<Value>`. For scan/read operations.
    ///
    /// Every cell is typed by its column's declared type, so a time cell
    /// comes back as the instant variant the schema declares.
    pub fn iter_rows(&self) -> MemtableRowIter<'_> {
        MemtableRowIter {
            columns: &self.columns,
            column_defs: &self.schema.columns,
            row_count: self.row_count,
            current: 0,
        }
    }

    /// Get a single row by index as `Vec<Value>`.
    ///
    /// `Ok(None)` when `row_idx` is past the last row. `Err` when a cell of
    /// the row is corrupt.
    pub fn get_row(&self, row_idx: usize) -> Result<Option<Vec<Value>>, ColumnarError> {
        if row_idx >= self.row_count {
            return Ok(None);
        }
        read_row(&self.columns, &self.schema.columns, row_idx).map(Some)
    }
}

/// Read row `row_idx` of `columns`, typing each cell by its column def.
fn read_row(
    columns: &[ColumnData],
    column_defs: &[ColumnDef],
    row_idx: usize,
) -> Result<Vec<Value>, ColumnarError> {
    columns
        .iter()
        .zip(column_defs)
        .map(|(col, def)| col.get_value(row_idx, &def.column_type, &def.name))
        .collect()
}

/// Row iterator over a columnar memtable.
pub struct MemtableRowIter<'a> {
    columns: &'a [ColumnData],
    column_defs: &'a [ColumnDef],
    row_count: usize,
    current: usize,
}

/// Yields `Err` for a row with a corrupt cell. The iterator still advances
/// past that row.
impl Iterator for MemtableRowIter<'_> {
    type Item = Result<Vec<Value>, ColumnarError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.row_count {
            return None;
        }
        let row = read_row(self.columns, self.column_defs, self.current);
        self.current += 1;
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.row_count - self.current;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MemtableRowIter<'_> {}

#[cfg(test)]
mod tests {
    use nodedb_types::NdbDateTime;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::super::core::ColumnarMemtable;

    const MICROS: i64 = 1_583_402_400_000_000;

    /// A row read back from the memtable types each time cell by its declared
    /// column: `Timestamp` is a naive instant, `Timestamptz` a UTC instant,
    /// `SystemTimestamp` the integer stored.
    #[test]
    fn a_time_cell_reads_back_as_its_declared_type() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("at", ColumnType::Timestamp),
            ColumnDef::required("at_tz", ColumnType::Timestamptz),
            ColumnDef::required("sys", ColumnType::SystemTimestamp),
        ])
        .expect("valid schema");
        let mut mt = ColumnarMemtable::new(&schema);
        let dt = NdbDateTime::from_micros(MICROS);
        mt.append_row(&[
            Value::Integer(1),
            Value::NaiveDateTime(dt),
            Value::DateTime(dt),
            Value::Integer(7),
        ])
        .expect("append");

        let expected = vec![
            Value::Integer(1),
            Value::NaiveDateTime(dt),
            Value::DateTime(dt),
            Value::Integer(7),
        ];
        assert_eq!(mt.get_row(0).expect("read"), Some(expected.clone()));
        let rows: Vec<Vec<Value>> = mt.iter_rows().collect::<Result<_, _>>().expect("read");
        assert_eq!(rows, vec![expected]);
        assert_eq!(mt.get_row(1).expect("read"), None);
    }
}
