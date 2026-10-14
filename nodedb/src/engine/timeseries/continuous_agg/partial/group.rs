// SPDX-License-Identifier: BUSL-1.1

//! GROUP BY key cell of one aggregate bucket.
//!
//! A key holds the cell value, never a symbol ID. A memtable resets its
//! symbol dictionaries at every flush, and each partition carries its own,
//! so one ID names different tags in two drains.

use nodedb_types::Value;
use nodedb_types::timeseries::SymbolDictionary;

use crate::engine::timeseries::columnar_memtable::ColumnData;

/// One GROUP BY cell of a bucket key.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum GroupCell {
    /// No value: an absent column, a null symbol, or a null dictionary cell.
    Null,
    Int(i64),
    /// `f64::to_bits` of the cell. `-0.0` and `0.0` group apart.
    Float(u64),
    Text(String),
}

impl GroupCell {
    /// Key cell of `row` in `column`. `dict` is the column's symbol
    /// dictionary in the same drain or partition.
    pub fn of(column: Option<&ColumnData>, dict: Option<&SymbolDictionary>, row: usize) -> Self {
        match column {
            Some(ColumnData::Symbol(ids)) => ids
                .get(row)
                .and_then(|id| dict.and_then(|d| d.get(*id)))
                .map_or(Self::Null, |s| Self::Text(s.to_owned())),
            Some(ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            }) => {
                if valid.get(row).copied().unwrap_or(false) {
                    ids.get(row)
                        .and_then(|id| dictionary.get(*id as usize))
                        .map_or(Self::Null, |s| Self::Text(s.clone()))
                } else {
                    Self::Null
                }
            }
            Some(ColumnData::Int64(v)) | Some(ColumnData::Timestamp(v)) => {
                v.get(row).map_or(Self::Null, |x| Self::Int(*x))
            }
            Some(ColumnData::Float64(v)) => {
                v.get(row).map_or(Self::Null, |x| Self::Float(x.to_bits()))
            }
            None => Self::Null,
        }
    }

    /// The cell as a value.
    pub fn to_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Int(i) => Value::Integer(*i),
            Self::Float(bits) => Value::Float(f64::from_bits(*bits)),
            Self::Text(s) => Value::String(s.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_cells_resolve_through_their_own_dictionary() {
        let mut first = SymbolDictionary::new();
        first.resolve("a", 10);
        first.resolve("b", 10);
        let mut second = SymbolDictionary::new();
        second.resolve("b", 10);
        second.resolve("a", 10);
        let ids = ColumnData::Symbol(vec![0, 1]);
        assert_eq!(
            GroupCell::of(Some(&ids), Some(&first), 0),
            GroupCell::Text("a".into())
        );
        assert_eq!(
            GroupCell::of(Some(&ids), Some(&second), 0),
            GroupCell::Text("b".into())
        );
    }

    #[test]
    fn null_symbol_and_absent_column_are_null() {
        let dict = SymbolDictionary::new();
        let ids = ColumnData::Symbol(vec![u32::MAX]);
        assert_eq!(GroupCell::of(Some(&ids), Some(&dict), 0), GroupCell::Null);
        assert_eq!(GroupCell::of(None, None, 0), GroupCell::Null);
    }

    #[test]
    fn integer_and_float_cells_key_by_value() {
        let ints = ColumnData::Int64(vec![7, 9]);
        assert_eq!(GroupCell::of(Some(&ints), None, 1), GroupCell::Int(9));
        let floats = ColumnData::Float64(vec![1.5]);
        assert_eq!(
            GroupCell::of(Some(&floats), None, 0).to_value(),
            Value::Float(1.5)
        );
    }
}
