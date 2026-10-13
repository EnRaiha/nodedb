// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER/SHOW ALERT + SHOW ALERT STATUS.

use super::clause::{ClauseCursor, Token, parse_err, tokenize};
use super::helpers::if_exists_name;
use crate::ddl_ast::statement::{AutomationStmt, NodedbStatement};
use crate::error::SqlError;

/// Clause keywords of `CREATE ALERT` after `ON <collection>`. An expression
/// clause ends at the first of these.
const ALERT_CLAUSES: &[&[&str]] = &[
    &["WHERE"],
    &["CONDITION"],
    &["GROUP", "BY"],
    &["WINDOW"],
    &["FOR"],
    &["RECOVER", "AFTER"],
    &["SEVERITY"],
    &["NOTIFY"],
];

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if !(upper.starts_with("CREATE ALERT ")
        || upper.starts_with("DROP ALERT ")
        || upper.starts_with("ALTER ALERT ")
        || upper.starts_with("SHOW ALERT"))
    {
        return None;
    }
    Some(parse(trimmed))
}

fn parse(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    if let Some(first) = ClauseCursor::new(sql, &toks, "ALERT").peek_word_upper() {
        let stmt = match first.as_str() {
            "CREATE" => parse_create_alert(sql, &toks)?,
            "DROP" => {
                let mut c = ClauseCursor::new(sql, &toks, "DROP ALERT");
                c.expect_keywords(&["DROP", "ALERT"])?;
                let (if_exists, name) = if_exists_name(&mut c, "alert name")?;
                c.finish()?;
                AutomationStmt::DropAlert { name, if_exists }
            }
            "ALTER" => {
                let mut c = ClauseCursor::new(sql, &toks, "ALTER ALERT");
                c.expect_keywords(&["ALTER", "ALERT"])?;
                let name = c.name("alert name")?;
                let action = c.one_of("action", &["ENABLE", "DISABLE"])?.to_string();
                c.finish()?;
                AutomationStmt::AlterAlert { name, action }
            }
            _ => {
                let mut c = ClauseCursor::new(sql, &toks, "SHOW ALERT");
                c.expect_keyword("SHOW")?;
                if c.eat_keywords(&["ALERT", "STATUS"]) {
                    let name = c.raw("alert name")?.to_string();
                    c.finish()?;
                    AutomationStmt::ShowAlertStatus { name }
                } else {
                    if !c.eat_keyword("ALERTS") {
                        c.expect_keyword("ALERT")?;
                    }
                    c.finish()?;
                    AutomationStmt::ShowAlerts
                }
            }
        };
        return Ok(NodedbStatement::Automation(stmt));
    }
    Err(parse_err("ALERT: empty statement"))
}

/// Read `CREATE ALERT <name> ON <collection>` and its clauses, in any order,
/// each at most once. `NOTIFY` takes the rest of the statement.
fn parse_create_alert<'a>(sql: &'a str, toks: &[Token<'a>]) -> Result<AutomationStmt, SqlError> {
    let mut c = ClauseCursor::new(sql, toks, "CREATE ALERT");
    c.expect_keywords(&["CREATE", "ALERT"])?;
    let name = c.name("alert name")?;
    c.expect_keyword("ON")?;
    let collection = c.name("collection")?;

    let mut where_filter = None;
    let mut condition_raw = String::new();
    let mut group_by = Vec::new();
    let mut window_raw = String::new();
    let mut fire_after = 1u32;
    let mut recover_after = 1u32;
    let mut severity = "warning".to_string();
    let mut notify_targets_raw = String::new();

    while !c.at_end() {
        if c.eat_keyword("WHERE") {
            c.clause("WHERE")?;
            where_filter = Some(c.raw_until("WHERE", ALERT_CLAUSES)?.to_string());
        } else if c.eat_keyword("CONDITION") {
            c.clause("CONDITION")?;
            condition_raw = c.raw_until("CONDITION", ALERT_CLAUSES)?.to_string();
        } else if c.eat_keywords(&["GROUP", "BY"]) {
            c.clause("GROUP BY")?;
            loop {
                group_by.push(c.name("GROUP BY column")?);
                if !c.eat_punct(',') {
                    break;
                }
            }
        } else if c.eat_keyword("WINDOW") {
            c.clause("WINDOW")?;
            window_raw = c.string("WINDOW")?;
        } else if c.eat_keyword("FOR") {
            c.clause("FOR")?;
            fire_after = consecutive_count(&mut c, "FOR")?;
        } else if c.eat_keywords(&["RECOVER", "AFTER"]) {
            c.clause("RECOVER AFTER")?;
            recover_after = consecutive_count(&mut c, "RECOVER AFTER")?;
        } else if c.eat_keyword("SEVERITY") {
            c.clause("SEVERITY")?;
            severity = c.string("SEVERITY")?;
        } else if c.eat_keyword("NOTIFY") {
            c.clause("NOTIFY")?;
            notify_targets_raw = c.rest().to_string();
            if notify_targets_raw.is_empty() {
                return Err(c.error("NOTIFY needs at least one target"));
            }
        } else {
            return Err(c.unknown_clause());
        }
    }

    Ok(AutomationStmt::CreateAlert {
        name,
        collection,
        where_filter,
        condition_raw,
        group_by,
        window_raw,
        fire_after,
        recover_after,
        severity,
        notify_targets_raw,
    })
}

/// Read `'N consecutive windows'` and return `N` (at least 1).
fn consecutive_count(c: &mut ClauseCursor<'_, '_>, clause: &str) -> Result<u32, SqlError> {
    let text = c.string(clause)?;
    let mut words = text.split_whitespace();
    let count = words.next().and_then(|n| n.parse::<u32>().ok());
    let unit_ok = words
        .next()
        .is_none_or(|w| w.eq_ignore_ascii_case("consecutive"));
    match count {
        Some(n) if unit_ok => Ok(n.max(1)),
        _ => Err(c.error(format!(
            "{clause} must be 'N consecutive windows' — found '{text}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> Result<AutomationStmt, SqlError> {
        let toks = tokenize(sql);
        parse_create_alert(sql, &toks)
    }

    #[test]
    fn parse_alert_unicode_name_preserves_original_offsets() {
        let sql = "CREATE ALERT highﬀﬀ ON sensors CONDITION AVG(temp) > 90 WINDOW '5 minutes' SEVERITY 'critical'";
        let AutomationStmt::CreateAlert {
            name,
            collection,
            condition_raw,
            window_raw,
            severity,
            ..
        } = create(sql).expect("parses")
        else {
            panic!("expected CreateAlert");
        };
        assert_eq!(name, "highﬀﬀ");
        assert_eq!(collection, "sensors");
        assert_eq!(condition_raw, "AVG(temp) > 90");
        assert_eq!(window_raw, "5 minutes");
        assert_eq!(severity, "critical");
    }

    #[test]
    fn parse_create_alert_full() {
        let sql = "CREATE ALERT high_temp ON sensors \
                   WHERE device_type = 'compressor' \
                   CONDITION AVG(temperature) > 90.0 \
                   GROUP BY device_id \
                   WINDOW '5 minutes' \
                   FOR '3 consecutive windows' \
                   RECOVER AFTER '2 consecutive windows' \
                   SEVERITY 'critical' \
                   NOTIFY TOPIC 'alerts', WEBHOOK 'https://ops.example.com/alerts'";
        let AutomationStmt::CreateAlert {
            name,
            collection,
            where_filter,
            condition_raw,
            group_by,
            window_raw,
            fire_after,
            recover_after,
            severity,
            notify_targets_raw,
        } = create(sql).expect("parses")
        else {
            panic!("expected CreateAlert");
        };
        assert_eq!(name, "high_temp");
        assert_eq!(collection, "sensors");
        assert_eq!(where_filter.as_deref(), Some("device_type = 'compressor'"));
        assert_eq!(condition_raw, "AVG(temperature) > 90.0");
        assert_eq!(group_by, vec!["device_id"]);
        assert_eq!(window_raw, "5 minutes");
        assert_eq!(fire_after, 3);
        assert_eq!(recover_after, 2);
        assert_eq!(severity, "critical");
        assert_eq!(
            notify_targets_raw,
            "TOPIC 'alerts', WEBHOOK 'https://ops.example.com/alerts'"
        );
    }

    #[test]
    fn parse_create_alert_minimal() {
        let sql = "CREATE ALERT simple ON metrics CONDITION MAX(cpu) > 95.0 WINDOW '1 minute' SEVERITY 'warning'";
        let AutomationStmt::CreateAlert {
            name,
            collection,
            where_filter,
            fire_after,
            recover_after,
            notify_targets_raw,
            ..
        } = create(sql).expect("parses")
        else {
            panic!("expected CreateAlert");
        };
        assert_eq!(name, "simple");
        assert_eq!(collection, "metrics");
        assert!(where_filter.is_none());
        assert_eq!(fire_after, 1);
        assert_eq!(recover_after, 1);
        assert!(notify_targets_raw.is_empty());
    }

    #[test]
    fn misspelled_severity_is_refused() {
        let sql = "CREATE ALERT a ON cpu CONDITION AVG(v) > 90 WINDOW '5m' SEVERTY 'critical'";
        assert!(create(sql).is_err());
    }

    #[test]
    fn notify_inside_a_where_literal_is_not_a_clause() {
        let sql = "CREATE ALERT a ON t WHERE msg = 'NOTIFY me' CONDITION COUNT(*) > 1 WINDOW '1m'";
        let AutomationStmt::CreateAlert {
            where_filter,
            notify_targets_raw,
            condition_raw,
            ..
        } = create(sql).expect("parses")
        else {
            panic!("expected CreateAlert");
        };
        assert_eq!(where_filter.as_deref(), Some("msg = 'NOTIFY me'"));
        assert_eq!(condition_raw, "COUNT(*) > 1");
        assert!(notify_targets_raw.is_empty());
    }

    #[test]
    fn non_numeric_consecutive_count_is_refused() {
        let sql = "CREATE ALERT a ON t CONDITION COUNT(*) > 1 FOR 'three consecutive windows'";
        assert!(create(sql).is_err());
    }

    #[test]
    fn alter_alert_with_trailing_tokens_is_refused() {
        assert!(parse("ALTER ALERT a ENABLE NOW").is_err());
        assert!(parse("ALTER ALERT a ENABLE").is_ok());
    }
}
