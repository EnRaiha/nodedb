// SPDX-License-Identifier: Apache-2.0

//! Physical sort key: one ORDER BY term as dispatched to the Data Plane.

/// A single ORDER BY term.
///
/// The key is an expression, not a column name. `ORDER BY 100 / weight`,
/// `ORDER BY UPPER(name)`, and `ORDER BY qty * price` all sort by a value that
/// exists nowhere in the stored row, so a key that could only name a column
/// would have to drop them — answering a query that declared a sort with rows
/// in storage order, and reporting success while doing it.
///
/// A bare `ORDER BY col` is simply the `SqlExpr::Column` case and still
/// resolves by direct field lookup.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SortKeySpec {
    /// Expression evaluated against each row to produce its sort value.
    pub expr: nodedb_query::expr::SqlExpr,
    /// `true` = ascending, `false` = descending.
    pub ascending: bool,
    /// Where NULLs go: `true` places them before all non-NULL values,
    /// `false` after.
    ///
    /// This is independent of `ascending` — SQL's `NULLS FIRST` / `NULLS LAST`
    /// is absolute and is not flipped by the sort direction. When the query
    /// does not say, the planner fills in PostgreSQL's default (`ASC` → NULLS
    /// LAST, `DESC` → NULLS FIRST).
    pub nulls_first: bool,
}

impl SortKeySpec {
    /// Sort by a bare column name, with PostgreSQL's default NULL placement
    /// for the given direction.
    pub fn column(name: impl Into<String>, ascending: bool) -> Self {
        Self {
            expr: nodedb_query::expr::SqlExpr::Column(name.into()),
            ascending,
            nulls_first: !ascending,
        }
    }

    /// Order two rows that may be NULL on this key.
    ///
    /// `Some(ordering)` when at least one side is NULL — decided entirely by
    /// `nulls_first`, never reversed by the direction. `None` when both sides
    /// carry a value and the caller must compare them.
    pub fn order_nulls(&self, a_is_null: bool, b_is_null: bool) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        match (a_is_null, b_is_null) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(if self.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }),
            (false, true) => Some(if self.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }),
            (false, false) => None,
        }
    }

    /// Apply the sort direction to a comparison of two non-NULL values.
    pub fn direct(&self, ordering: std::cmp::Ordering) -> std::cmp::Ordering {
        if self.ascending {
            ordering
        } else {
            ordering.reverse()
        }
    }

    /// The column this key sorts by, when the key is a bare column reference.
    ///
    /// Storage layers that can only push a *stored column* down to an index
    /// use this to decide whether the key is pushable; a computed key returns
    /// `None` and must be sorted after the rows are materialized.
    pub fn as_column(&self) -> Option<&str> {
        match &self.expr {
            nodedb_query::expr::SqlExpr::Column(name) => Some(name.as_str()),
            _ => None,
        }
    }
}
