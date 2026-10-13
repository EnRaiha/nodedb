// SPDX-License-Identifier: Apache-2.0

//! Parse `CREATE SYNONYM GROUP`, `DROP SYNONYM GROUP`, and `SHOW SYNONYM GROUPS`.
//!
//! Syntax:
//! - `CREATE SYNONYM GROUP <name> AS ('term1', 'term2', ...)`
//! - `DROP SYNONYM GROUP [IF EXISTS] <name>`
//! - `SHOW SYNONYM GROUPS`

use super::clause::{ClauseCursor, tokenize};
use crate::ddl_ast::statement::{NodedbStatement, PolicyStmt};
use crate::error::SqlError;

/// Try to parse synonym group DDL statements.
pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("CREATE SYNONYM GROUP ") {
        return Some(parse_create(trimmed));
    }
    if upper.starts_with("DROP SYNONYM GROUP ") {
        return Some(parse_drop(trimmed));
    }
    if upper == "SHOW SYNONYM GROUPS" {
        return Some(Ok(NodedbStatement::Policy(PolicyStmt::ShowSynonymGroups)));
    }
    None
}

/// Parse `CREATE SYNONYM GROUP <name> AS ('term1', 'term2', ...)`. Terms are
/// trimmed and case-folded; a duplicate term is refused.
fn parse_create(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "CREATE SYNONYM GROUP");
    c.expect_keywords(&["CREATE", "SYNONYM", "GROUP"])?;
    let name = c.name("group name")?;
    c.expect_keyword("AS")?;
    let terms = c.paren_list("term list", |c| {
        let term = c.string("term")?.trim().to_lowercase();
        if term.is_empty() {
            return Err(c.error("a term must not be empty"));
        }
        Ok(term)
    })?;
    c.finish()?;

    let mut seen = std::collections::HashSet::new();
    for term in &terms {
        if !seen.insert(term.as_str()) {
            return Err(c.error(format!("duplicate term '{term}'")));
        }
    }

    Ok(NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup {
        name,
        terms,
    }))
}

/// Parse `DROP SYNONYM GROUP [IF EXISTS] <name>`.
fn parse_drop(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "DROP SYNONYM GROUP");
    c.expect_keywords(&["DROP", "SYNONYM", "GROUP"])?;
    let if_exists = c.eat_keywords(&["IF", "EXISTS"]);
    let name = c.name("group name")?;
    c.finish()?;
    Ok(NodedbStatement::Policy(PolicyStmt::DropSynonymGroup {
        name,
        if_exists,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn err(sql: &str) -> SqlError {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
    }

    #[test]
    fn create_unicode_group_name_preserves_original_offsets() {
        let stmt = ok("CREATE SYNONYM GROUP termsﬀﬀ AS ('database', 'db')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { name, terms }) => {
                assert_eq!(name, "termsﬀﬀ");
                assert_eq!(terms, vec!["database", "db"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_basic() {
        let stmt = ok("CREATE SYNONYM GROUP db_terms AS ('database', 'db', 'datastore')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { name, terms }) => {
                assert_eq!(name, "db_terms");
                assert_eq!(terms, vec!["database", "db", "datastore"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_name_lowercased() {
        let stmt = ok("CREATE SYNONYM GROUP MyGroup AS ('foo', 'bar')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { name, .. }) => {
                assert_eq!(name, "mygroup");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_empty_list_rejected() {
        let e = err("CREATE SYNONYM GROUP foo AS ()");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn create_duplicate_term_rejected() {
        let e = err("CREATE SYNONYM GROUP foo AS ('db', 'db', 'database')");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("duplicate"));
    }

    #[test]
    fn unconsumed_clause_before_as_is_refused() {
        let e = err("CREATE SYNONYM GROUP cars FOR LANGUAGE 'de' AS ('auto', 'wagen')");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn trailing_tokens_after_term_list_are_refused() {
        let e = err("CREATE SYNONYM GROUP cars AS ('auto', 'wagen') EXTRA");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn drop_basic() {
        let stmt = ok("DROP SYNONYM GROUP db_terms");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::DropSynonymGroup {
                name: "db_terms".to_string(),
                if_exists: false,
            })
        );
    }

    #[test]
    fn drop_if_exists() {
        let stmt = ok("DROP SYNONYM GROUP IF EXISTS db_terms");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::DropSynonymGroup {
                name: "db_terms".to_string(),
                if_exists: true,
            })
        );
    }

    #[test]
    fn show_synonym_groups() {
        let stmt = ok("SHOW SYNONYM GROUPS");
        assert_eq!(stmt, NodedbStatement::Policy(PolicyStmt::ShowSynonymGroups));
    }

    #[test]
    fn terms_case_folded() {
        let stmt = ok("CREATE SYNONYM GROUP g AS ('DB', 'Database', 'DataStore')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { terms, .. }) => {
                assert_eq!(terms, vec!["db", "database", "datastore"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
