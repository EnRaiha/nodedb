// SPDX-License-Identifier: Apache-2.0

//! Row-oriented iteration and single-row lookup over the memtable.

use nodedb_types::value::Value;

use super::column_data::ColumnData;
use super::core::ColumnarMemtable;

impl ColumnarMemtable {
    /// Iterate rows as `Vec<Value>`. For scan/read operations.
    pub fn iter_rows(&self) -> MemtableRowIter<'_> {
        MemtableRowIter {
            columns: &self.columns,
            row_count: self.row_count,
            current: 0,
        }
    }

    /// Get a single row by index as `Vec<Value>`.
    pub fn get_row(&self, row_idx: usize) -> Option<Vec<Value>> {
        if row_idx >= self.row_count {
            return None;
        }
        let mut row = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            row.push(col.get_value(row_idx));
        }
        Some(row)
    }
}

/// Row iterator over a columnar memtable.
pub struct MemtableRowIter<'a> {
    columns: &'a [ColumnData],
    row_count: usize,
    current: usize,
}

impl Iterator for MemtableRowIter<'_> {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.row_count {
            return None;
        }
        let mut row = Vec::with_capacity(self.columns.len());
        for col in self.columns {
            row.push(col.get_value(self.current));
        }
        self.current += 1;
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.row_count - self.current;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MemtableRowIter<'_> {}
