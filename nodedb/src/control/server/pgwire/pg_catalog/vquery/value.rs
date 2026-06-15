// SPDX-License-Identifier: BUSL-1.1

//! Typed value representation for virtual-table rows and expression results.

use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq)]
pub enum VValue {
    Null,
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Text(String),
    /// A SQL array (e.g. the `TEXT[]` returned by `current_schemas`). Used as
    /// the right-hand side of `ANY` / `ALL` and for array literals.
    Array(Vec<VValue>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VType {
    Bool,
    Int4,
    Int8,
    Text,
}

impl VValue {
    pub fn is_null(&self) -> bool {
        matches!(self, VValue::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            VValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            VValue::Int4(i) => Some(*i as i64),
            VValue::Int8(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            VValue::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[VValue]> {
        match self {
            VValue::Array(items) => Some(items),
            _ => None,
        }
    }

    /// Render the value as PostgreSQL output text (used when an array or other
    /// value is projected directly).
    pub fn to_pg_text(&self) -> Option<String> {
        match self {
            VValue::Null => None,
            VValue::Bool(b) => Some(if *b { "t".into() } else { "f".into() }),
            VValue::Int4(i) => Some(i.to_string()),
            VValue::Int8(i) => Some(i.to_string()),
            VValue::Text(s) => Some(s.clone()),
            VValue::Array(items) => {
                let mut out = String::from("{");
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    match item.to_pg_text() {
                        Some(s) => out.push_str(&s),
                        None => out.push_str("NULL"),
                    }
                }
                out.push('}');
                Some(out)
            }
        }
    }

    /// SQL three-valued comparison. Returns `None` if either side is NULL or
    /// the operands are not order-comparable.
    pub fn sql_cmp(&self, other: &VValue) -> Option<Ordering> {
        if self.is_null() || other.is_null() {
            return None;
        }
        match (self, other) {
            (VValue::Bool(a), VValue::Bool(b)) => Some(a.cmp(b)),
            (VValue::Text(a), VValue::Text(b)) => Some(a.cmp(b)),
            (a, b) => {
                let (ai, bi) = (a.as_i64(), b.as_i64());
                match (ai, bi) {
                    (Some(x), Some(y)) => Some(x.cmp(&y)),
                    _ => None,
                }
            }
        }
    }
}

/// A single virtual-table column. `name` is the static catalog column name;
/// `qualifier` is the relation alias (or table name) the column belongs to,
/// used to disambiguate `c.relname` vs `n.nspname` in a join.
#[derive(Debug, Clone)]
pub struct VColumn {
    pub name: &'static str,
    pub ty: VType,
    pub qualifier: Option<String>,
}

impl VColumn {
    pub const fn new(name: &'static str, ty: VType) -> Self {
        Self {
            name,
            ty,
            qualifier: None,
        }
    }

    /// Return a copy of this column tagged with a relation qualifier (alias).
    pub fn qualified(&self, qualifier: &str) -> Self {
        Self {
            name: self.name,
            ty: self.ty,
            qualifier: Some(qualifier.to_string()),
        }
    }
}
