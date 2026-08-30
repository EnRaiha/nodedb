//! Abstraction over columnar data sources (memtable or sealed partition).

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};
use nodedb_types::timeseries::SymbolDictionary;

/// Abstraction over columnar data sources (memtable or sealed partition).
/// Provides column lookup by name and symbol dictionary access.
pub(crate) trait ColumnarSource {
    fn resolve_column(&self, name: &str) -> Option<(usize, ColumnType, &ColumnData)>;
    fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary>;
}
