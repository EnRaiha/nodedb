// SPDX-License-Identifier: Apache-2.0

//! ORDER BY planning and sort key extraction.
//!
//! Search-triggering sort detection (vector_distance, bm25_score, rrf_score)
//! is handled in select.rs as part of apply_order_by. This module provides
//! sort key extraction utilities.

use crate::error::Result;
use crate::resolver::expr::convert_expr;
use crate::types::SortKey;

/// Convert sqlparser OrderByExpr list to SortKey list.
pub fn convert_sort_keys(exprs: &[sqlparser::ast::OrderByExpr]) -> Result<Vec<SortKey>> {
    exprs
        .iter()
        .map(|o| {
            Ok(SortKey {
                expr: convert_expr(&o.expr)?,
                ascending: o.options.asc.unwrap_or(true),
                nulls_first: o
                    .options
                    .nulls_first
                    .unwrap_or(!o.options.asc.unwrap_or(true)),
            })
        })
        .collect()
}
