// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER/DESCRIBE/SHOW SEQUENCE.

use super::clause::{ClauseCursor, tokenize};
use super::helpers::{if_exists_name, if_not_exists_name};
use crate::ddl_ast::statement::{CollectionStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("SHOW SEQUENCES") {
        return Some(Ok(NodedbStatement::Collection(
            CollectionStmt::ShowSequences,
        )));
    }
    let known = [
        "CREATE SEQUENCE ",
        "DROP SEQUENCE ",
        "ALTER SEQUENCE ",
        "DESCRIBE SEQUENCE ",
    ];
    if !known.iter().any(|p| upper.starts_with(p)) {
        return None;
    }
    Some(parse(trimmed).map(NodedbStatement::Collection))
}

fn parse(sql: &str) -> Result<CollectionStmt, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "SEQUENCE");
    if c.eat_keywords(&["CREATE", "SEQUENCE"]) {
        let (if_not_exists, name) = if_not_exists_name(&mut c, "sequence name")?;
        let o = parse_sequence_options(&mut c)?;
        return Ok(CollectionStmt::CreateSequence {
            name,
            if_not_exists,
            start: o.start,
            increment: o.increment,
            min_value: o.min_value,
            max_value: o.max_value,
            cycle: o.cycle,
            cache: o.cache,
            format_template_raw: o.format_template_raw,
            reset_period_raw: o.reset_period_raw,
            gap_free: o.gap_free,
            scope: o.scope,
        });
    }
    if c.eat_keywords(&["DROP", "SEQUENCE"]) {
        let (if_exists, name) = if_exists_name(&mut c, "sequence name")?;
        c.finish()?;
        return Ok(CollectionStmt::DropSequence { name, if_exists });
    }
    if c.eat_keywords(&["ALTER", "SEQUENCE"]) {
        // ALTER SEQUENCE <name> RESTART [WITH <n>] | FORMAT '<template>'
        let name = c.name("sequence name")?;
        let action = c.one_of("action", &["RESTART", "FORMAT"])?;
        let with_value = if action == "RESTART" {
            if c.eat_keyword("WITH") {
                Some(c.i64("RESTART WITH")?.to_string())
            } else {
                None
            }
        } else {
            Some(c.string("FORMAT")?)
        };
        c.finish()?;
        return Ok(CollectionStmt::AlterSequence {
            name,
            action: action.to_string(),
            with_value,
        });
    }
    c.expect_keywords(&["DESCRIBE", "SEQUENCE"])?;
    let name = c.raw("sequence name")?.to_string();
    c.finish()?;
    Ok(CollectionStmt::DescribeSequence { name })
}

// ── Option parsing ────────────────────────────────────────────────────────────

#[derive(Default)]
struct SequenceOptions {
    start: Option<i64>,
    increment: Option<i64>,
    min_value: Option<i64>,
    max_value: Option<i64>,
    cycle: bool,
    cache: Option<i64>,
    format_template_raw: Option<String>,
    reset_period_raw: Option<String>,
    gap_free: bool,
    scope: Option<String>,
}

/// Read the sequence options after the name, in any order, each at most
/// once. A value that is not an integer, or a word that names no option, is a
/// parse error.
fn parse_sequence_options(c: &mut ClauseCursor<'_, '_>) -> Result<SequenceOptions, SqlError> {
    let mut o = SequenceOptions::default();
    while !c.at_end() {
        if c.eat_keyword("START") {
            c.clause("START")?;
            c.eat_keyword("WITH");
            o.start = Some(c.i64("START")?);
        } else if c.eat_keyword("INCREMENT") {
            c.clause("INCREMENT")?;
            c.eat_keyword("BY");
            o.increment = Some(c.i64("INCREMENT")?);
        } else if c.eat_keyword("MINVALUE") || c.eat_keyword("MIN") {
            c.clause("MINVALUE")?;
            o.min_value = Some(c.i64("MINVALUE")?);
        } else if c.eat_keyword("MAXVALUE") || c.eat_keyword("MAX") {
            c.clause("MAXVALUE")?;
            o.max_value = Some(c.i64("MAXVALUE")?);
        } else if c.eat_keyword("CYCLE") {
            c.clause("CYCLE")?;
            o.cycle = true;
        } else if c.eat_keyword("NO") {
            match c.one_of("NO", &["CYCLE", "MINVALUE", "MAXVALUE"])? {
                "CYCLE" => {
                    c.clause("CYCLE")?;
                    o.cycle = false;
                }
                "MINVALUE" => {
                    c.clause("MINVALUE")?;
                    o.min_value = None;
                }
                _ => {
                    c.clause("MAXVALUE")?;
                    o.max_value = None;
                }
            }
        } else if c.eat_keyword("CACHE") {
            c.clause("CACHE")?;
            o.cache = Some(c.i64("CACHE")?);
        } else if c.eat_keyword("FORMAT") {
            c.clause("FORMAT")?;
            o.format_template_raw = Some(c.string("FORMAT")?);
        } else if c.eat_keyword("RESET") {
            c.clause("RESET")?;
            o.reset_period_raw = Some(c.raw("RESET")?.to_string());
        } else if c.eat_keyword("GAP_FREE") {
            c.clause("GAP_FREE")?;
            o.gap_free = true;
        } else if c.eat_keyword("SCOPE") {
            c.clause("SCOPE")?;
            o.scope = Some(c.word("SCOPE")?.to_uppercase());
        } else {
            return Err(c.unknown_clause());
        }
    }
    Ok(o)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> Result<CollectionStmt, SqlError> {
        parse(sql)
    }

    #[test]
    fn full_option_set() {
        let CollectionStmt::CreateSequence {
            name,
            start,
            increment,
            min_value,
            max_value,
            cycle,
            cache,
            ..
        } = create("CREATE SEQUENCE s START WITH 5 INCREMENT BY -1 MINVALUE -10 MAXVALUE 99 CYCLE CACHE 10")
            .expect("parses")
        else {
            panic!("expected CreateSequence");
        };
        assert_eq!(name, "s");
        assert_eq!(start, Some(5));
        assert_eq!(increment, Some(-1));
        assert_eq!(min_value, Some(-10));
        assert_eq!(max_value, Some(99));
        assert!(cycle);
        assert_eq!(cache, Some(10));
    }

    #[test]
    fn non_numeric_start_is_refused_not_dropped() {
        assert!(create("CREATE SEQUENCE s START WITH abc").is_err());
    }

    #[test]
    fn unknown_option_is_refused() {
        assert!(create("CREATE SEQUENCE s STRAT 5").is_err());
    }

    #[test]
    fn format_template_keeps_spaces() {
        let CollectionStmt::CreateSequence {
            format_template_raw,
            ..
        } = create("CREATE SEQUENCE s FORMAT 'INV {YY} {SEQ:04}'").expect("parses")
        else {
            panic!("expected CreateSequence");
        };
        assert_eq!(format_template_raw.as_deref(), Some("INV {YY} {SEQ:04}"));
    }

    #[test]
    fn if_not_exists_inside_a_value_is_not_the_flag() {
        let CollectionStmt::CreateSequence { if_not_exists, .. } =
            create("CREATE SEQUENCE s FORMAT 'IF NOT EXISTS'").expect("parses")
        else {
            panic!("expected CreateSequence");
        };
        assert!(!if_not_exists);
    }

    #[test]
    fn alter_restart_requires_an_integer() {
        assert!(parse("ALTER SEQUENCE s RESTART WITH ten").is_err());
        let CollectionStmt::AlterSequence {
            action, with_value, ..
        } = parse("ALTER SEQUENCE s RESTART WITH 10").expect("parses")
        else {
            panic!("expected AlterSequence");
        };
        assert_eq!(action, "RESTART");
        assert_eq!(with_value.as_deref(), Some("10"));
    }
}
