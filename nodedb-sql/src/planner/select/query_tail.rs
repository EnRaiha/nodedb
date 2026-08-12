// SPDX-License-Identifier: Apache-2.0

//! The ORDER BY / LIMIT / OFFSET clauses that hang off a `Query` rather than
//! its SELECT body, and their conversion into plan-level values.
//!
//! They are threaded down into `plan_select` because the engine rules need
//! them at `plan_scan` time: an engine that rewrites a scan into a
//! narrower access path (e.g. `SqlPlan::DocumentIndexLookup`) has to decline
//! the rewrite when the query asks for an order that path cannot produce.
//! Deciding that after the scan plan is already built is too late — the
//! rewrite has happened and the order has nowhere to live.

use sqlparser::ast;

use crate::error::Result;
use crate::types::SortKey;

/// The trailing clauses of the enclosing `Query`.
pub(in crate::planner::select) struct QueryTail<'a> {
    pub order_by: Option<&'a ast::OrderBy>,
    pub limit_clause: &'a Option<ast::LimitClause>,
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

    /// `(limit, offset)` for the LIMIT clause. A missing clause is
    /// `(None, 0)`; a non-literal bound is `None` (unbounded).
    pub(in crate::planner::select) fn limit_offset(&self) -> (Option<usize>, usize) {
        match self.limit_clause {
            None => (None, 0),
            Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => {
                let lv = limit
                    .as_ref()
                    .and_then(crate::coerce::expr_as_usize_literal);
                let ov = offset
                    .as_ref()
                    .and_then(|o| crate::coerce::expr_as_usize_literal(&o.value))
                    .unwrap_or(0);
                (lv, ov)
            }
            Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
                let lv = crate::coerce::expr_as_usize_literal(limit);
                let ov = crate::coerce::expr_as_usize_literal(offset).unwrap_or(0);
                (lv, ov)
            }
        }
    }
}
