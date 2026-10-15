// SPDX-License-Identifier: Apache-2.0

//! ORDER BY planning and sort key extraction.
//!
//! Search-triggering sort detection (vector_distance, bm25_score, rrf_score)
//! is handled in select.rs as part of apply_order_by. This module provides
//! sort key extraction utilities.

use sqlparser::ast::{OrderByOptions, OrderBySort};

use crate::error::{Result, SqlError};
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::SortKey;

/// Convert sqlparser OrderByExpr list to SortKey list.
pub fn convert_sort_keys(
    exprs: &[sqlparser::ast::OrderByExpr],
    scope: &TableScope,
) -> Result<Vec<SortKey>> {
    let scope = ColumnScope::Relations(scope);
    exprs
        .iter()
        .map(|o| {
            Ok(SortKey {
                expr: convert_expr(&o.expr, &scope)?,
                ascending: sort_ascending(&o.options)?,
                nulls_first: sort_nulls_first(&o.options)?,
            })
        })
        .collect()
}

/// Whether an `ORDER BY` key sorts ascending. `USING <operator>` is refused:
/// no operator orders values here.
pub fn sort_ascending(options: &OrderByOptions) -> Result<bool> {
    match &options.sort {
        None | Some(OrderBySort::Asc) => Ok(true),
        Some(OrderBySort::Desc) => Ok(false),
        Some(OrderBySort::Using(operator)) => Err(SqlError::Unsupported {
            detail: format!("ORDER BY ... USING {operator} is not supported"),
        }),
    }
}

/// Whether an `ORDER BY` key puts NULLs first. Unstated, NULLs sort last
/// ascending and first descending, as in PostgreSQL.
pub fn sort_nulls_first(options: &OrderByOptions) -> Result<bool> {
    match options.nulls_first {
        Some(first) => Ok(first),
        None => Ok(!sort_ascending(options)?),
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{OrderByKind, Statement};

    use super::*;
    use crate::parser::statement::parse_sql;

    /// The options of the first `ORDER BY` key of `SELECT a FROM t ORDER BY <key>`.
    fn options(key: &str) -> OrderByOptions {
        let statements = parse_sql(&format!("SELECT a FROM t ORDER BY {key}")).expect("parses");
        let Statement::Query(query) = &statements[0] else {
            panic!("expected a query");
        };
        let Some(OrderByKind::Expressions(keys)) = query.order_by.as_ref().map(|o| &o.kind) else {
            panic!("expected ORDER BY expressions");
        };
        keys[0].options.clone()
    }

    #[test]
    fn nulls_follow_the_direction_unless_stated() {
        let asc = options("a");
        assert!(sort_ascending(&asc).expect("ascending"));
        assert!(!sort_nulls_first(&asc).expect("nulls last"));

        let desc = options("a DESC");
        assert!(!sort_ascending(&desc).expect("descending"));
        assert!(sort_nulls_first(&desc).expect("nulls first"));

        let stated = options("a DESC NULLS LAST");
        assert!(!sort_nulls_first(&stated).expect("nulls last"));
    }

    #[test]
    fn using_an_operator_is_refused() {
        let using = options("a USING <");
        let err = sort_ascending(&using).expect_err("USING is refused");
        assert!(matches!(err, SqlError::Unsupported { .. }), "{err}");
        assert!(sort_nulls_first(&using).is_err());
    }
}
