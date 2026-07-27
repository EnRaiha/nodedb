// SPDX-License-Identifier: Apache-2.0

//! `ScanFilter` record, its wire codec, and per-row evaluation against a
//! `nodedb_types::Value` document.

use crate::expr::{EvalError, SqlExpr};

use super::like;
use super::op::FilterOp;

/// A single filter predicate for document scan evaluation.
///
/// Supports simple comparison operators (eq, ne, gt, gte, lt, lte, contains,
/// is_null, is_not_null), disjunctive groups via the `"or"` operator, and
/// full SqlExpr predicates via `FilterOp::Expr` for anything the planner
/// cannot reduce to a simple `(field, op, value)` — scalar functions in
/// WHERE, non-literal IN lists, column arithmetic, `NOT(...)`, etc.
///
/// OR representation: `{"op": "or", "clauses": [[filter1, filter2], [filter3]]}`
/// means `(filter1 AND filter2) OR filter3`. Each clause is an AND-group;
/// the document matches if ANY clause group fully matches.
#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ScanFilter {
    #[serde(default)]
    pub field: String,
    pub op: FilterOp,
    #[serde(default)]
    pub value: nodedb_types::Value,
    /// Disjunctive clause groups for OR predicates.
    /// Each inner Vec is an AND-group. The document matches if ANY group matches.
    #[serde(default)]
    pub clauses: Vec<Vec<ScanFilter>>,
    /// Expression predicate payload. Only meaningful when `op == FilterOp::Expr`;
    /// must be `None` for every other operator.
    #[serde(default)]
    pub expr: Option<SqlExpr>,
}

impl zerompk::ToMessagePack for ScanFilter {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(5)?;
        self.field.write(writer)?;
        writer.write_string(self.op.as_str())?;
        // Convert nodedb_types::Value → serde_json::Value for wire compat.
        let json_val: serde_json::Value = self.value.clone().into();
        nodedb_types::JsonValue(json_val).write(writer)?;
        self.clauses.write(writer)?;
        self.expr.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for ScanFilter {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(5)?;
        let field = String::read(reader)?;
        let op_str = String::read(reader)?;
        let jv = nodedb_types::JsonValue::read(reader)?;
        let clauses = Vec::<Vec<ScanFilter>>::read(reader)?;
        let expr = Option::<SqlExpr>::read(reader)?;
        Ok(Self {
            field,
            op: FilterOp::parse_op(&op_str),
            // Convert serde_json::Value → nodedb_types::Value at wire boundary.
            value: nodedb_types::Value::from(jv.0),
            clauses,
            expr,
        })
    }
}

impl ScanFilter {
    /// Evaluate an AND-group of filters, short-circuiting on the first
    /// `false` or the first evaluation error — same semantics as
    /// `group.iter().all(|f| f.matches_value(doc))` had before filter
    /// evaluation could fail. `pub` so the many call
    /// sites across the `nodedb` crate that used to write
    /// `filters.iter().all(|f| f.matches_value(doc))` have a drop-in
    /// replacement instead of each hand-rolling the same short-circuit loop.
    pub fn all_match_value(
        group: &[ScanFilter],
        doc: &nodedb_types::Value,
    ) -> Result<bool, EvalError> {
        for f in group {
            if !f.matches_value(doc)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Evaluate this filter against a `nodedb_types::Value` document.
    ///
    /// Same semantics as `matches()` but operates on the native Value type
    /// instead of serde_json::Value, avoiding lossy JSON roundtrips.
    ///
    /// Returns `Err(EvalError::DivisionByZero)` when the filter is (or
    /// contains, via an `OR` group) a `FilterOp::Expr` predicate whose
    /// expression divides or takes a modulus by zero.
    /// This is the deliberate WHERE-clause behavior flip: a predicate like
    /// `10 / denom > 1` used to evaluate to `Value::Null`/`false` (silently
    /// filtering the row out) when `denom` was `0`; it now fails the whole
    /// query with SQLSTATE `22012`, matching Postgres.
    pub fn matches_value(&self, doc: &nodedb_types::Value) -> Result<bool, EvalError> {
        match self.op {
            FilterOp::MatchAll | FilterOp::Exists | FilterOp::NotExists => return Ok(true),
            FilterOp::Or => {
                for clause in &self.clauses {
                    if Self::all_match_value(clause, doc)? {
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            FilterOp::Expr => {
                return match &self.expr {
                    Some(expr) => Ok(crate::value_ops::is_truthy(&expr.eval(doc)?)),
                    None => Ok(false),
                };
            }
            _ => {}
        }

        let field_val = match doc.get(&self.field) {
            Some(v) => v,
            None => return Ok(self.op == FilterOp::IsNull),
        };

        Ok(match self.op {
            FilterOp::Eq => self.value.eq_coerced(field_val),
            FilterOp::Ne => !self.value.eq_coerced(field_val),
            FilterOp::Gt => self.value.cmp_coerced(field_val) == std::cmp::Ordering::Less,
            FilterOp::Gte => {
                let cmp = self.value.cmp_coerced(field_val);
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
            FilterOp::Lt => self.value.cmp_coerced(field_val) == std::cmp::Ordering::Greater,
            FilterOp::Lte => {
                let cmp = self.value.cmp_coerced(field_val);
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            FilterOp::Contains => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    s.contains(pattern)
                } else {
                    false
                }
            }
            FilterOp::Like => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    like::sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            FilterOp::NotLike => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !like::sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            FilterOp::Ilike => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    like::sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            FilterOp::NotIlike => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !like::sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            FilterOp::In => {
                if let Some(mut iter) = self.value.as_array_iter() {
                    iter.any(|v| v.eq_coerced(field_val))
                } else {
                    false
                }
            }
            FilterOp::NotIn => {
                if let Some(mut iter) = self.value.as_array_iter() {
                    !iter.any(|v| v.eq_coerced(field_val))
                } else {
                    true
                }
            }
            FilterOp::IsNull => field_val.is_null(),
            FilterOp::IsNotNull => !field_val.is_null(),
            FilterOp::ArrayContains => {
                if let Some(arr) = field_val.as_array() {
                    arr.iter().any(|v| self.value.eq_coerced(v))
                } else {
                    false
                }
            }
            FilterOp::ArrayContainsAll => {
                if let (Some(field_arr), Some(mut needles)) =
                    (field_val.as_array(), self.value.as_array_iter())
                {
                    needles.all(|needle| field_arr.iter().any(|v| needle.eq_coerced(v)))
                } else {
                    false
                }
            }
            FilterOp::ArrayOverlap => {
                if let (Some(field_arr), Some(mut needles)) =
                    (field_val.as_array(), self.value.as_array_iter())
                {
                    needles.any(|needle| field_arr.iter().any(|v| needle.eq_coerced(v)))
                } else {
                    false
                }
            }
            FilterOp::GtColumn
            | FilterOp::GteColumn
            | FilterOp::LtColumn
            | FilterOp::LteColumn
            | FilterOp::EqColumn
            | FilterOp::NeColumn => {
                let other_col = match &self.value {
                    nodedb_types::Value::String(s) => s.as_str(),
                    _ => return Ok(false),
                };
                let other_val = match doc.get(other_col) {
                    Some(v) => v,
                    None => return Ok(false),
                };
                match self.op {
                    FilterOp::GtColumn => {
                        field_val.cmp_coerced(other_val) == std::cmp::Ordering::Greater
                    }
                    FilterOp::GteColumn => {
                        field_val.cmp_coerced(other_val) != std::cmp::Ordering::Less
                    }
                    FilterOp::LtColumn => {
                        field_val.cmp_coerced(other_val) == std::cmp::Ordering::Less
                    }
                    FilterOp::LteColumn => {
                        field_val.cmp_coerced(other_val) != std::cmp::Ordering::Greater
                    }
                    FilterOp::EqColumn => field_val.eq_coerced(other_val),
                    FilterOp::NeColumn => !field_val.eq_coerced(other_val),
                    _ => false,
                }
            }
            _ => false,
        })
    }
}
