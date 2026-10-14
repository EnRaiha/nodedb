// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER/SHOW SCHEDULE + SHOW SCHEDULE HISTORY.

use super::clause::{ClauseCursor, tokenize};
use super::helpers::if_exists_name;
use crate::ddl_ast::statement::{AutomationStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("SHOW SCHEDULES") {
        return Some(Ok(NodedbStatement::Automation(
            AutomationStmt::ShowSchedules,
        )));
    }
    let known = [
        "CREATE SCHEDULE ",
        "DROP SCHEDULE ",
        "ALTER SCHEDULE ",
        "SHOW SCHEDULE HISTORY ",
    ];
    if !known.iter().any(|p| upper.starts_with(p)) {
        return None;
    }
    Some(parse(trimmed).map(NodedbStatement::Automation))
}

fn parse(sql: &str) -> Result<AutomationStmt, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "SCHEDULE");
    if c.eat_keywords(&["CREATE", "SCHEDULE"]) {
        return parse_create_schedule(&mut c);
    }
    if c.eat_keywords(&["DROP", "SCHEDULE"]) {
        let (if_exists, name) = if_exists_name(&mut c, "schedule name")?;
        c.finish()?;
        return Ok(AutomationStmt::DropSchedule { name, if_exists });
    }
    if c.eat_keywords(&["ALTER", "SCHEDULE"]) {
        // ALTER SCHEDULE <name> ENABLE | DISABLE | SET CRON '<expr>'
        let name = c.name("schedule name")?;
        let action = c.one_of("action", &["ENABLE", "DISABLE", "SET"])?;
        let cron_expr = if action == "SET" {
            c.expect_keyword("CRON")?;
            Some(c.string("CRON")?)
        } else {
            None
        };
        c.finish()?;
        return Ok(AutomationStmt::AlterSchedule {
            name,
            action: action.to_string(),
            cron_expr,
        });
    }
    c.expect_keywords(&["SHOW", "SCHEDULE", "HISTORY"])?;
    let name = c.raw("schedule name")?.to_string();
    c.finish()?;
    Ok(AutomationStmt::ShowScheduleHistory { name })
}

/// Read `CREATE SCHEDULE <name> CRON <expr> [SCOPE LOCAL|NORMAL]
/// [WITH (ALLOW_OVERLAP = <bool>, MISSED = <policy>)] AS <body>`.
///
/// The cron expression is a quoted string or five unquoted fields. The body
/// is everything after the header's `AS`; an `AS` inside the body is part of
/// the body.
fn parse_create_schedule(c: &mut ClauseCursor<'_, '_>) -> Result<AutomationStmt, SqlError> {
    let name = c.name("schedule name")?;
    c.expect_keyword("CRON")?;
    let cron_expr = match c.string("CRON") {
        Ok(expr) => expr,
        Err(_) => {
            let mut fields = Vec::with_capacity(5);
            for _ in 0..5 {
                fields.push(c.raw_run("cron field")?);
            }
            fields.join(" ")
        }
    };

    let mut scope = "NORMAL";
    let mut allow_overlap = true;
    let mut missed_policy = "SKIP".to_string();
    loop {
        if c.eat_keyword("SCOPE") {
            c.clause("SCOPE")?;
            scope = c.one_of("SCOPE", &["LOCAL", "NORMAL"])?;
        } else if c.eat_keyword("WITH") {
            c.clause("WITH")?;
            let options = c.paren_list("WITH options", |c| {
                let key = c.word("option name")?.to_ascii_uppercase();
                c.expect_punct('=')?;
                Ok((key, c.raw_run("option value")?.to_string()))
            })?;
            for (key, value) in options {
                let value = value.trim_matches('\'').trim_matches('"');
                match key.as_str() {
                    "ALLOW_OVERLAP" => {
                        allow_overlap = if value.eq_ignore_ascii_case("true") {
                            true
                        } else if value.eq_ignore_ascii_case("false") {
                            false
                        } else {
                            return Err(c.error(format!(
                                "ALLOW_OVERLAP must be TRUE or FALSE — found '{value}'"
                            )));
                        };
                    }
                    "MISSED" => missed_policy = value.to_uppercase(),
                    other => {
                        return Err(c.error(format!(
                            "unknown option '{other}' (expected ALLOW_OVERLAP | MISSED)"
                        )));
                    }
                }
            }
        } else {
            break;
        }
    }

    c.expect_keyword("AS")?;
    let body_sql = c.rest().to_string();
    if body_sql.is_empty() {
        return Err(c.error("AS needs a schedule body"));
    }

    Ok(AutomationStmt::CreateSchedule {
        name,
        cron_expr,
        body_sql,
        scope: scope.to_string(),
        missed_policy,
        allow_overlap,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> AutomationStmt {
        parse(sql).expect("parses")
    }

    #[test]
    fn unicode_schedule_name_preserves_original_offsets() {
        let sql = "CREATE SCHEDULE cleanupﬀﬀ CRON '0 0 * * *' AS BEGIN DELETE FROM old; END";
        let AutomationStmt::CreateSchedule {
            name,
            cron_expr,
            body_sql,
            ..
        } = create(sql)
        else {
            panic!("expected CreateSchedule");
        };
        assert_eq!(name, "cleanupﬀﬀ");
        assert_eq!(cron_expr, "0 0 * * *");
        assert_eq!(body_sql, "BEGIN DELETE FROM old; END");
    }

    #[test]
    fn basic_schedule() {
        let sql = "CREATE SCHEDULE cleanup CRON '0 0 * * *' AS BEGIN DELETE FROM old; END";
        let AutomationStmt::CreateSchedule {
            name,
            cron_expr,
            body_sql,
            scope,
            allow_overlap,
            ..
        } = create(sql)
        else {
            panic!("expected CreateSchedule");
        };
        assert_eq!(name, "cleanup");
        assert_eq!(cron_expr, "0 0 * * *");
        assert!(body_sql.contains("DELETE FROM old"));
        assert_eq!(scope, "NORMAL");
        assert!(allow_overlap);
    }

    #[test]
    fn unquoted_cron_fields() {
        let sql = "CREATE SCHEDULE s CRON */5 * * * * AS BEGIN RETURN; END";
        let AutomationStmt::CreateSchedule { cron_expr, .. } = create(sql) else {
            panic!("expected CreateSchedule");
        };
        assert_eq!(cron_expr, "*/5 * * * *");
    }

    #[test]
    fn scope_local() {
        let sql = "CREATE SCHEDULE gc CRON '0 * * * *' SCOPE LOCAL AS BEGIN RETURN; END";
        let AutomationStmt::CreateSchedule { scope, .. } = create(sql) else {
            panic!("expected CreateSchedule");
        };
        assert_eq!(scope, "LOCAL");
    }

    #[test]
    fn scope_words_inside_the_body_set_nothing() {
        let sql = "CREATE SCHEDULE s CRON '0 * * * *' AS INSERT INTO log VALUES ('x SCOPE LOCAL')";
        let AutomationStmt::CreateSchedule {
            scope, body_sql, ..
        } = create(sql)
        else {
            panic!("expected CreateSchedule");
        };
        assert_eq!(scope, "NORMAL");
        assert_eq!(body_sql, "INSERT INTO log VALUES ('x SCOPE LOCAL')");
    }

    #[test]
    fn as_inside_the_body_stays_in_the_body() {
        let sql = "CREATE SCHEDULE s CRON '0 * * * *' AS INSERT INTO t SELECT a AS b FROM u";
        let AutomationStmt::CreateSchedule { body_sql, .. } = create(sql) else {
            panic!("expected CreateSchedule");
        };
        assert_eq!(body_sql, "INSERT INTO t SELECT a AS b FROM u");
    }

    #[test]
    fn with_options() {
        let sql = "CREATE SCHEDULE agg CRON '*/5 * * * *' \
                   WITH (ALLOW_OVERLAP = false, MISSED = CATCH_UP) \
                   AS BEGIN INSERT INTO stats SELECT 1; END";
        let AutomationStmt::CreateSchedule {
            allow_overlap,
            missed_policy,
            ..
        } = create(sql)
        else {
            panic!("expected CreateSchedule");
        };
        assert!(!allow_overlap);
        assert_eq!(missed_policy, "CATCH_UP");
    }

    #[test]
    fn unknown_option_and_non_boolean_overlap_are_refused() {
        assert!(
            parse("CREATE SCHEDULE s CRON '* * * * *' WITH (OVERLAP = false) AS SELECT 1").is_err()
        );
        assert!(
            parse("CREATE SCHEDULE s CRON '* * * * *' WITH (ALLOW_OVERLAP = no) AS SELECT 1")
                .is_err()
        );
    }

    #[test]
    fn alter_schedule_forms() {
        let AutomationStmt::AlterSchedule {
            action, cron_expr, ..
        } = parse("ALTER SCHEDULE cleanup SET CRON '0 0 * * *'").expect("parses")
        else {
            panic!("expected AlterSchedule");
        };
        assert_eq!(action, "SET");
        assert_eq!(cron_expr.as_deref(), Some("0 0 * * *"));
        assert!(parse("ALTER SCHEDULE cleanup ENABLE NOW").is_err());
    }
}
