// SPDX-License-Identifier: Apache-2.0

//! The ORDER BY / LIMIT / OFFSET / FETCH clauses that hang off a `Query`
//! rather than its SELECT body, and their conversion into plan-level values.
//!
//! They are threaded down into `plan_select` because the engine rules need
//! them at `plan_scan` time: an engine that rewrites a scan into a
//! narrower access path (e.g. `SqlPlan::DocumentIndexLookup`) has to decline
//! the rewrite when the query asks for an order that path cannot produce.
//! Deciding that after the scan plan is already built is too late — the
//! rewrite has happened and the order has nowhere to live.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::types::SortKey;

/// The trailing clauses of the enclosing `Query`.
pub(in crate::planner::select) struct QueryTail<'a> {
    pub order_by: Option<&'a ast::OrderBy>,
    pub limit_clause: &'a Option<ast::LimitClause>,
    pub fetch: Option<&'a ast::Fetch>,
}

impl QueryTail<'_> {
    /// Sort keys for the ORDER BY clause, or empty when there is none.
    ///
    /// `ORDER BY ALL` carries no expressions to convert and yields an empty
    /// list, matching `apply_order_by`'s treatment of the same clause.
    ///
    /// This is the same conversion `apply_order_by` performs on the plan it
    /// receives, so a scan that already carries these keys is overwritten
    /// downstream with an identical list, never an appended one.
    pub(in crate::planner::select) fn sort_keys(&self) -> Result<Vec<SortKey>> {
        match self.order_by.map(|o| &o.kind) {
            Some(ast::OrderByKind::Expressions(exprs)) => {
                crate::planner::sort::convert_sort_keys(exprs)
            }
            Some(ast::OrderByKind::All(_)) | None => Ok(Vec::new()),
        }
    }

    /// `(limit, offset)` for the LIMIT / OFFSET / FETCH clauses.
    ///
    /// A missing clause is `(None, 0)`. A bound outside `[0, usize::MAX]`,
    /// or one the planner cannot read as a literal, fails the statement
    /// with SQLSTATE `2201W` rather than widening the scan.
    pub(in crate::planner::select) fn limit_offset(&self) -> Result<(Option<usize>, usize)> {
        let (limit_from_clause, offset) = match self.limit_clause {
            None => (None, 0),
            Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => {
                let lv = match limit {
                    Some(e) => crate::coerce::checked_row_bound("LIMIT", e)?.limit(),
                    None => None,
                };
                let ov = match offset {
                    Some(o) => crate::coerce::checked_row_bound("OFFSET", &o.value)?.offset(),
                    None => 0,
                };
                (lv, ov)
            }
            Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
                let lv = crate::coerce::checked_row_bound("LIMIT", limit)?.limit();
                let ov = crate::coerce::checked_row_bound("OFFSET", offset)?.offset();
                (lv, ov)
            }
        };

        let Some(fetch) = self.fetch else {
            return Ok((limit_from_clause, offset));
        };
        // `limit_from_clause` is `None` both when LIMIT is absent and when it
        // resolved to `RowBound::Unbounded` (`LIMIT NULL` / `LIMIT ALL`). A
        // `LIMIT NULL` alongside FETCH names no competing bound, so it does
        // not trip this rejection — only a LIMIT that resolved to a count does.
        if limit_from_clause.is_some() {
            return Err(SqlError::Unsupported {
                detail: "a query cannot combine LIMIT with FETCH FIRST/NEXT".into(),
            });
        }
        if fetch.with_ties {
            // `WITH TIES` returns the top N plus every row tying the Nth row
            // on the ORDER BY key. `SqlPlan` has no primitive for that —
            // dropping the modifier and applying a plain LIMIT is wrong.
            return Err(SqlError::Unsupported {
                detail: "FETCH ... WITH TIES is not supported".into(),
            });
        }
        if fetch.percent {
            // `PERCENT` needs the total row count before the bound resolves
            // to a number, which the planner does not have at plan time.
            return Err(SqlError::Unsupported {
                detail: "FETCH ... PERCENT is not supported".into(),
            });
        }
        let fetch_limit = match &fetch.quantity {
            Some(expr) => crate::coerce::checked_row_bound("FETCH FIRST", expr)?.limit(),
            // Standard SQL: `FETCH FIRST ROW ONLY` with no count means one row.
            None => Some(1),
        };
        Ok((fetch_limit, offset))
    }
}
