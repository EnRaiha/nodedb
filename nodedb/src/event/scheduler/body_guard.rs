// SPDX-License-Identifier: BUSL-1.1

//! Pre-execution SQL guard for scheduled job bodies.
//!
//! Scheduled jobs run non-interactively on a cron cadence — a careless
//! `SELECT * FROM huge_table` without a `LIMIT` or `WHERE` clause quietly
//! materialises the full table on every fire. The runtime byte ceiling
//! in `dispatch_to_data_plane` catches it eventually, but we prefer to
//! reject the body at registration/fire time with a specific, actionable
//! error rather than drain a gigabyte before aborting.
//!
//! This guard walks the body's SQL AST and rejects any top-level
//! `SELECT *` that has no `LIMIT`, no `WHERE`, and no `FETCH` clause.
//! That's the "forgot LIMIT" footgun — every realistic scheduled
//! analytic has at least one of the three.

use sqlparser::ast::{SelectItem, SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Inspect a scheduled job's SQL body and reject unbounded wildcard
/// SELECTs. Returns `Ok(())` if the body is safe or isn't SQL we can
/// parse (we defer to the runtime ceiling in that case).
pub fn validate_scheduled_body(body_sql: &str) -> crate::Result<()> {
    // Strip a leading `BEGIN ... END` procedural wrapper if present so
    // we can reach the bare statements inside.
    let stripped = strip_procedural_wrapper(body_sql);

    let dialect = PostgreSqlDialect {};
    let Ok(stmts) = Parser::parse_sql(&dialect, stripped) else {
        // Unparseable — let the planner surface its own error later.
        return Ok(());
    };

    for stmt in &stmts {
        if let Statement::Query(q) = stmt
            && is_unbounded_wildcard_select(q)
        {
            return Err(crate::Error::BadRequest {
                detail: "scheduled job body contains `SELECT *` with no WHERE, LIMIT, or FETCH clause — \
                         refusing to execute an unbounded scan on every fire. Add an explicit LIMIT \
                         or WHERE predicate."
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn strip_procedural_wrapper(sql: &str) -> &str {
    let trimmed = sql.trim();
    if let Some(rest) = nodedb_types::strip_prefix_ascii_case_insensitive(trimmed, "BEGIN")
        && let Some(end_pos) = nodedb_types::rfind_ascii_case_insensitive(rest, "END")
        && let Some(body) = rest.get(..end_pos)
    {
        return body.trim().trim_end_matches(';').trim();
    }
    trimmed
}

fn is_unbounded_wildcard_select(q: &sqlparser::ast::Query) -> bool {
    // Any LIMIT or FETCH clause makes it bounded.
    if q.limit_clause.is_some() || q.fetch.is_some() {
        return false;
    }
    let SetExpr::Select(select) = q.body.as_ref() else {
        // Subquery, VALUES, set operation — out of scope for this heuristic.
        return false;
    };
    // WHERE clause makes it bounded enough (user knows to narrow it).
    if select.selection.is_some() {
        return false;
    }
    // Any non-wildcard projection means the user was explicit about columns.
    let all_wildcard = !select.projection.is_empty()
        && select.projection.iter().all(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        });
    if !all_wildcard {
        return false;
    }
    // Must target at least one table — bare `SELECT *` has no target.
    !select.from.is_empty()
        // Aggregate SELECTs (GROUP BY / HAVING) are always bounded by
        // cardinality of the grouping keys, so allow them.
        && select.group_by == sqlparser::ast::GroupByExpr::Expressions(vec![], vec![])
        && select.having.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_bare_wildcard_select() {
        assert!(validate_scheduled_body("SELECT * FROM metrics").is_err());
        assert!(validate_scheduled_body("  select *   from orders  ").is_err());
    }

    #[test]
    fn accepts_wildcard_with_limit() {
        assert!(validate_scheduled_body("SELECT * FROM metrics LIMIT 1000").is_ok());
    }

    #[test]
    fn accepts_wildcard_with_where() {
        assert!(validate_scheduled_body("SELECT * FROM metrics WHERE ts > 0").is_ok());
    }

    #[test]
    fn accepts_projection() {
        assert!(validate_scheduled_body("SELECT id, name FROM metrics").is_ok());
    }

    #[test]
    fn accepts_aggregate() {
        assert!(validate_scheduled_body("SELECT count(*) FROM metrics").is_ok());
        assert!(
            validate_scheduled_body("SELECT * FROM metrics GROUP BY tenant_id").is_ok(),
            "GROUP BY bounds cardinality"
        );
    }

    #[test]
    fn accepts_delete_and_update() {
        // Not a SELECT — outside this guard's remit.
        assert!(validate_scheduled_body("DELETE FROM metrics WHERE ts < 0").is_ok());
        assert!(validate_scheduled_body("UPDATE metrics SET n = 0").is_ok());
    }

    #[test]
    fn accepts_procedural_wrapper() {
        assert!(
            validate_scheduled_body("BEGIN INSERT INTO tally SELECT * FROM metrics LIMIT 100; END")
                .is_ok()
        );
    }

    #[test]
    fn unparseable_defers_to_runtime() {
        // Not our job to reject unparseable SQL — the planner will surface it.
        assert!(validate_scheduled_body("this is not sql").is_ok());
    }
}
