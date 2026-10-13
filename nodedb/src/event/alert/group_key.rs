// SPDX-License-Identifier: BUSL-1.1

//! The typed identity of one alert group.
//!
//! The Data Plane groups by typed cells: integer `1`, text `'1'`, and NULL
//! are three groups. An alert group keeps that identity, so each group has
//! its own hysteresis state. The rendered text form names each cell as a SQL
//! literal, so two distinct groups never render alike.

use std::fmt;

/// One GROUP BY cell of an alert group.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlertGroupCell {
    Null,
    Bool(bool),
    Int(i128),
    /// `f64` bits, the identity the Data Plane groups a float cell by.
    Float(u64),
    Text(String),
    /// A typed MessagePack extension cell (an instant), by its encoding.
    Ext {
        bytes: Vec<u8>,
        rendered: String,
    },
}

/// The typed key of one alert group: one cell per GROUP BY column, in
/// column order. An alert without GROUP BY has one group, the empty key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct AlertGroupKey {
    cells: Vec<AlertGroupCell>,
}

impl AlertGroupKey {
    /// The single group of an alert without GROUP BY.
    pub fn whole_window() -> Self {
        Self::default()
    }

    /// The key from cells in GROUP BY column order.
    pub fn from_cells(cells: Vec<AlertGroupCell>) -> Self {
        Self { cells }
    }

    pub fn cells(&self) -> &[AlertGroupCell] {
        &self.cells
    }
}

impl AlertGroupCell {
    /// The cell of GROUP BY column `column` from a Data Plane response cell.
    /// A cell the Data Plane never groups by (binary, array, map, a
    /// non-UTF-8 string) is an error naming the column.
    pub fn from_rmpv(column: &str, value: &rmpv::Value) -> crate::Result<Self> {
        let refuse = |what: &str| crate::Error::Internal {
            detail: format!("alert group column '{column}' holds {what}, which is no group key"),
        };
        Ok(match value {
            rmpv::Value::Nil => Self::Null,
            rmpv::Value::Boolean(b) => Self::Bool(*b),
            rmpv::Value::Integer(n) => match (n.as_i64(), n.as_u64()) {
                (Some(i), _) => Self::Int(i128::from(i)),
                (None, Some(u)) => Self::Int(i128::from(u)),
                (None, None) => return Err(refuse("an integer out of range")),
            },
            rmpv::Value::F32(f) => Self::Float(f64::from(*f).to_bits()),
            rmpv::Value::F64(f) => Self::Float(f.to_bits()),
            rmpv::Value::String(s) => match s.as_str() {
                Some(text) => Self::Text(text.to_owned()),
                None => return Err(refuse("a string that is not UTF-8")),
            },
            rmpv::Value::Ext(_, _) => {
                let mut bytes = Vec::new();
                rmpv::encode::write_value(&mut bytes, value).map_err(|e| crate::Error::Codec {
                    detail: format!("alert group column '{column}': {e}"),
                })?;
                let decoded =
                    nodedb_types::value_from_msgpack(&bytes).map_err(|e| crate::Error::Codec {
                        detail: format!("alert group column '{column}': {e}"),
                    })?;
                Self::Ext {
                    rendered: decoded.to_string(),
                    bytes,
                }
            }
            rmpv::Value::Binary(_) => return Err(refuse("binary")),
            rmpv::Value::Array(_) => return Err(refuse("an array")),
            rmpv::Value::Map(_) => return Err(refuse("a map")),
        })
    }
}

impl fmt::Display for AlertGroupCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(bits) => write!(f, "{:?}", f64::from_bits(*bits)),
            Self::Text(text) => write!(f, "'{}'", text.replace('\'', "''")),
            Self::Ext { rendered, .. } => write!(f, "'{}'", rendered.replace('\'', "''")),
        }
    }
}

/// `__all__` for the whole-window group, else the cells as SQL literals
/// joined by `, `.
impl fmt::Display for AlertGroupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.cells.is_empty() {
            return f.write_str("__all__");
        }
        for (i, cell) in self.cells.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{cell}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(value: rmpv::Value) -> AlertGroupCell {
        AlertGroupCell::from_rmpv("g", &value).expect("group cell")
    }

    #[test]
    fn integer_text_and_null_are_three_groups() {
        let int = cell(rmpv::Value::Integer(1.into()));
        let text = cell(rmpv::Value::String("1".into()));
        let null = cell(rmpv::Value::Nil);
        assert_ne!(int, text);
        assert_ne!(int, null);
        assert_ne!(text, null);
        assert_eq!(int.to_string(), "1");
        assert_eq!(text.to_string(), "'1'");
        assert_eq!(null.to_string(), "NULL");
    }

    #[test]
    fn text_null_is_not_sql_null() {
        let text = cell(rmpv::Value::String("NULL".into()));
        assert_ne!(text, AlertGroupCell::Null);
        assert_ne!(text.to_string(), AlertGroupCell::Null.to_string());
    }

    #[test]
    fn a_container_cell_is_refused_by_column() {
        match AlertGroupCell::from_rmpv("zone", &rmpv::Value::Array(Vec::new())) {
            Err(crate::Error::Internal { detail }) => {
                assert!(detail.contains("'zone'"), "{detail}")
            }
            other => panic!("expected an error, got {other:?}"),
        }
    }

    #[test]
    fn keys_render_in_column_order() {
        let key = AlertGroupKey::from_cells(vec![
            cell(rmpv::Value::String("pump-1".into())),
            cell(rmpv::Value::Integer(7.into())),
        ]);
        assert_eq!(key.to_string(), "'pump-1', 7");
        assert_eq!(AlertGroupKey::whole_window().to_string(), "__all__");
    }
}
