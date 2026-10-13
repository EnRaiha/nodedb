// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER/SHOW TRIGGER.

use super::clause::{ClauseCursor, Tok, Token, tokenize};
use super::helpers::if_exists_name;
use crate::ddl_ast::statement::{AutomationStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if !(upper.starts_with("CREATE ")
        || upper.starts_with("DROP TRIGGER ")
        || upper.starts_with("ALTER TRIGGER ")
        || upper.starts_with("SHOW TRIGGERS"))
    {
        return None;
    }
    let toks = tokenize(trimmed);
    if upper.starts_with("CREATE ") {
        // Only `CREATE [OR REPLACE] [SYNC|DEFERRED] TRIGGER` is a trigger.
        // The word TRIGGER anywhere else (a string, a body) is not.
        let mut probe = ClauseCursor::new(trimmed, &toks, "CREATE TRIGGER");
        probe.expect_keyword("CREATE").ok()?;
        probe.eat_keywords(&["OR", "REPLACE"]);
        if !probe.eat_keyword("SYNC") {
            probe.eat_keyword("DEFERRED");
        }
        if !probe.peek_keyword("TRIGGER") {
            return None;
        }
        return Some(parse_create_trigger(trimmed, &toks).map(NodedbStatement::Automation));
    }
    Some(parse_other(trimmed, &toks).map(NodedbStatement::Automation))
}

fn parse_other<'a>(sql: &'a str, toks: &[Token<'a>]) -> Result<AutomationStmt, SqlError> {
    let mut c = ClauseCursor::new(sql, toks, "TRIGGER");
    if c.eat_keywords(&["DROP", "TRIGGER"]) {
        let (if_exists, name) = if_exists_name(&mut c, "trigger name")?;
        let collection = if c.eat_keyword("ON") {
            c.raw("collection")?.to_string()
        } else {
            String::new()
        };
        c.finish()?;
        return Ok(AutomationStmt::DropTrigger {
            name,
            collection,
            if_exists,
        });
    }
    if c.eat_keywords(&["ALTER", "TRIGGER"]) {
        let name = c.name("trigger name")?;
        let action = c.one_of("action", &["ENABLE", "DISABLE", "OWNER"])?;
        let new_owner = if action == "OWNER" {
            c.expect_keyword("TO")?;
            Some(c.raw("new owner")?.to_string())
        } else {
            None
        };
        c.finish()?;
        return Ok(AutomationStmt::AlterTrigger {
            name,
            action: action.to_string(),
            new_owner,
        });
    }
    c.expect_keywords(&["SHOW", "TRIGGERS"])?;
    let collection = if c.eat_keyword("ON") {
        Some(c.raw("collection")?.to_string())
    } else {
        None
    };
    c.finish()?;
    Ok(AutomationStmt::ShowTriggers { collection })
}

/// Read `CREATE [OR REPLACE] [SYNC|DEFERRED] TRIGGER <name> [timing] <events>
/// ON <collection> [FOR EACH ROW|STATEMENT] [WHEN (<cond>)] [PRIORITY <n>]
/// [SECURITY <mode>]` followed by a `BEGIN ... END` or `$$ ... $$` body.
fn parse_create_trigger<'a>(sql: &'a str, toks: &[Token<'a>]) -> Result<AutomationStmt, SqlError> {
    let (header_end, body_sql) = split_body(sql, toks)?;
    let header_toks = &toks[..header_end];
    let mut c = ClauseCursor::new(sql, header_toks, "CREATE TRIGGER");
    c.expect_keyword("CREATE")?;
    let or_replace = c.eat_keywords(&["OR", "REPLACE"]);
    let execution_mode = if c.eat_keyword("SYNC") {
        "SYNC"
    } else if c.eat_keyword("DEFERRED") {
        "DEFERRED"
    } else {
        "ASYNC"
    };
    c.expect_keyword("TRIGGER")?;
    let name = c.name("trigger name")?;

    let timing = if c.eat_keyword("BEFORE") {
        "BEFORE"
    } else if c.eat_keywords(&["INSTEAD", "OF"]) || c.eat_keyword("INSTEAD") {
        "INSTEAD OF"
    } else {
        c.eat_keyword("AFTER");
        "AFTER"
    };

    let (mut events_insert, mut events_update, mut events_delete) = (false, false, false);
    loop {
        match c.one_of("trigger event", &["INSERT", "UPDATE", "DELETE"])? {
            "INSERT" => events_insert = true,
            "UPDATE" => events_update = true,
            _ => events_delete = true,
        }
        if !c.eat_keyword("OR") {
            break;
        }
    }

    c.expect_keyword("ON")?;
    let collection = c.name("collection")?;

    let granularity = if c.eat_keywords(&["FOR", "EACH"]) {
        c.one_of("FOR EACH", &["ROW", "STATEMENT"])?
    } else {
        "ROW"
    };

    let when_condition = if c.eat_keyword("WHEN") {
        Some(c.raw_paren("WHEN")?.to_string())
    } else {
        None
    };

    let priority = if c.eat_keyword("PRIORITY") {
        let value = c.i64("PRIORITY")?;
        i32::try_from(value).map_err(|_| c.error(format!("PRIORITY {value} is out of range")))?
    } else {
        0
    };

    // A SECURITY keyword with no mode yields `Some("")`: the clause was
    // written, so the DDL layer rejects it rather than the parser hiding it.
    let security = if c.eat_keyword("SECURITY") {
        match c.peek_word_upper() {
            Some(mode) => {
                c.word("SECURITY")?;
                Some(mode)
            }
            None => Some(String::new()),
        }
    } else {
        None
    };
    c.finish()?;

    Ok(AutomationStmt::CreateTrigger {
        or_replace,
        execution_mode: execution_mode.to_string(),
        name,
        timing: timing.to_string(),
        events_insert,
        events_update,
        events_delete,
        collection,
        granularity: granularity.to_string(),
        when_condition,
        priority,
        security,
        body_sql,
    })
}

/// Find where the trigger header ends and return `(header_token_count,
/// body_sql)`. The body starts at the first `$$` or `BEGIN` token at
/// parenthesis depth zero; a `$$` or `BEGIN` inside a string is not one.
fn split_body<'a>(sql: &'a str, toks: &[Token<'a>]) -> Result<(usize, String), SqlError> {
    let mut depth = 0i32;
    for (i, tok) in toks.iter().enumerate() {
        match &tok.tok {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => depth -= 1,
            Tok::Word(w) if depth == 0 && w.starts_with("$$") => {
                let inner_start = tok.span.start + 2;
                let close = sql[inner_start..]
                    .find("$$")
                    .ok_or_else(|| super::clause::parse_err("CREATE TRIGGER: unclosed $$ body"))?;
                let after = sql[inner_start + close + 2..].trim();
                let after = after.strip_suffix(';').unwrap_or(after).trim();
                if !after.is_empty() {
                    return Err(super::clause::parse_err(format!(
                        "CREATE TRIGGER: unexpected text after the $$ body: '{after}'"
                    )));
                }
                let body = sql[inner_start..inner_start + close].trim().to_string();
                return Ok((i, body));
            }
            Tok::Word(w) if depth == 0 && w.eq_ignore_ascii_case("BEGIN") => {
                return Ok((i, sql[tok.span.start..].trim().to_string()));
            }
            _ => {}
        }
    }
    Ok((toks.len(), String::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> Result<AutomationStmt, SqlError> {
        let toks = tokenize(sql);
        parse_create_trigger(sql, &toks)
    }

    #[test]
    fn unicode_trigger_name_before_when_preserves_original_offsets() {
        let sql = "CREATE TRIGGER tﬀﬀ AFTER INSERT ON orders FOR EACH ROW WHEN (NEW.status = 'active') BEGIN RETURN; END";
        let AutomationStmt::CreateTrigger {
            name,
            when_condition,
            body_sql,
            ..
        } = create(sql).expect("parses")
        else {
            panic!("expected CreateTrigger");
        };
        assert_eq!(name, "tﬀﬀ");
        assert_eq!(when_condition.as_deref(), Some("NEW.status = 'active'"));
        assert_eq!(body_sql, "BEGIN RETURN; END");
    }

    #[test]
    fn begin_inside_a_when_literal_is_not_the_body() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON orders WHEN (NEW.note = 'BEGIN $$') BEGIN RETURN; END";
        let AutomationStmt::CreateTrigger {
            when_condition,
            body_sql,
            ..
        } = create(sql).expect("parses")
        else {
            panic!("expected CreateTrigger");
        };
        assert_eq!(when_condition.as_deref(), Some("NEW.note = 'BEGIN $$'"));
        assert_eq!(body_sql, "BEGIN RETURN; END");
    }

    #[test]
    fn dollar_body() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON c $$ BEGIN RETURN; END $$";
        let AutomationStmt::CreateTrigger { body_sql, .. } = create(sql).expect("parses") else {
            panic!("expected CreateTrigger");
        };
        assert_eq!(body_sql, "BEGIN RETURN; END");
    }

    #[test]
    fn basic_after_insert() {
        let sql = "CREATE TRIGGER audit AFTER INSERT ON orders FOR EACH ROW BEGIN RETURN; END";
        let AutomationStmt::CreateTrigger {
            name,
            timing,
            events_insert,
            collection,
            granularity,
            ..
        } = create(sql).expect("parses")
        else {
            panic!("expected CreateTrigger");
        };
        assert_eq!(name, "audit");
        assert_eq!(timing, "AFTER");
        assert!(events_insert);
        assert_eq!(collection, "orders");
        assert_eq!(granularity, "ROW");
    }

    #[test]
    fn or_replace_sync() {
        let sql = "CREATE OR REPLACE SYNC TRIGGER t AFTER INSERT ON c FOR EACH ROW BEGIN END";
        let AutomationStmt::CreateTrigger {
            or_replace,
            execution_mode,
            ..
        } = create(sql).expect("parses")
        else {
            panic!("expected CreateTrigger");
        };
        assert!(or_replace);
        assert_eq!(execution_mode, "SYNC");
    }

    #[test]
    fn multi_event() {
        let sql = "CREATE TRIGGER t AFTER INSERT OR UPDATE OR DELETE ON c FOR EACH ROW BEGIN END";
        let AutomationStmt::CreateTrigger {
            events_insert,
            events_update,
            events_delete,
            ..
        } = create(sql).expect("parses")
        else {
            panic!("expected CreateTrigger");
        };
        assert!(events_insert && events_update && events_delete);
    }

    #[test]
    fn statement_granularity() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON c FOR EACH STATEMENT BEGIN END";
        let AutomationStmt::CreateTrigger { granularity, .. } = create(sql).expect("parses") else {
            panic!("expected CreateTrigger");
        };
        assert_eq!(granularity, "STATEMENT");
    }

    #[test]
    fn with_priority() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON c FOR EACH ROW PRIORITY 10 BEGIN END";
        let AutomationStmt::CreateTrigger { priority, .. } = create(sql).expect("parses") else {
            panic!("expected CreateTrigger");
        };
        assert_eq!(priority, 10);
    }

    #[test]
    fn security_without_mode_is_kept_as_written() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON c SECURITY BEGIN END";
        let AutomationStmt::CreateTrigger { security, .. } = create(sql).expect("parses") else {
            panic!("expected CreateTrigger");
        };
        assert_eq!(security.as_deref(), Some(""));
    }

    #[test]
    fn when_without_parentheses_is_refused() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON o FOR EACH ROW WHEN NEW.amount > 100 BEGIN RETURN; END";
        assert!(create(sql).is_err());
    }

    #[test]
    fn misspelled_timing_is_refused() {
        let sql = "CREATE TRIGGER t BEFOR INSERT ON o FOR EACH ROW BEGIN RETURN; END";
        assert!(create(sql).is_err());
    }

    #[test]
    fn non_numeric_priority_is_refused() {
        let sql = "CREATE TRIGGER t AFTER INSERT ON o PRIORITY high BEGIN RETURN; END";
        assert!(create(sql).is_err());
    }

    #[test]
    fn trigger_word_in_another_create_is_not_a_trigger() {
        let sql = "CREATE SCHEDULE s CRON '* * * * *' AS INSERT INTO log VALUES ('TRIGGER fired')";
        assert!(try_parse(&sql.to_uppercase(), &[], sql).is_none());
    }

    #[test]
    fn drop_trigger_with_trailing_tokens_is_refused() {
        let sql = "DROP TRIGGER t ON orders EXTRA";
        assert!(try_parse(&sql.to_uppercase(), &[], sql).is_some_and(|r| r.is_err()));
    }
}
