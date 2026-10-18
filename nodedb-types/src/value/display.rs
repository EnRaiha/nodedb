// SPDX-License-Identifier: Apache-2.0

//! [`std::fmt::Display`] implementation for [`Value`].

use std::fmt;

use crate::value::core::Value;

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Vector(v) => {
                write!(f, "vector(")?;
                let show = v.len().min(8);
                for (i, elem) in v[..show].iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{elem}")?;
                }
                if v.len() > 8 {
                    write!(f, ", … ({} total)", v.len())?;
                }
                write!(f, ")")
            }
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Float(fl) => write!(f, "{fl}"),
            Value::String(s) | Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => {
                write!(f, "{s}")
            }
            Value::Bytes(b) => write!(f, "<bytes:{}>", b.len()),
            Value::Array(arr) | Value::Set(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Value::Object(map) => {
                write!(f, "{{")?;
                for (i, (k, v)) in map.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            Value::DateTime(dt) | Value::NaiveDateTime(dt) => write!(f, "{dt}"),
            Value::Duration(d) => write!(f, "{d}"),
            Value::Decimal(d) => write!(f, "{d}"),
            Value::Geometry(g) => write!(f, "{g}"),
            Value::Range {
                start,
                end,
                inclusive,
            } => {
                if let Some(s) = start {
                    write!(f, "{s}")?;
                }
                if *inclusive {
                    write!(f, "..=")?;
                } else {
                    write!(f, "..")?;
                }
                if let Some(e) = end {
                    write!(f, "{e}")?;
                }
                Ok(())
            }
            Value::Record { table, id } => write!(f, "{table}:{id}"),
            Value::ArrayCell(cell) => write!(f, "<array_cell coords={}>", cell.coords.len()),
        }
    }
}
