// SPDX-License-Identifier: Apache-2.0

//! `CLONE DATABASE <new> FROM <source> [AS OF SYSTEM TIME <ms> | AS OF
//! LATEST | LATEST]`.

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::ddl_ast::statement::{CloneAsOf, DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::dispatch::db_name;

/// Read `<new> FROM <source> [AS OF LATEST | AS OF SYSTEM TIME <ms> | AS OF
/// SYSTEM TIME LATEST]` after `CLONE DATABASE`. Keywords are matched in any
/// case; a misspelled `AS OF` clause is refused, never read as LATEST.
pub(super) fn parse_clone_database(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    if c.at_end() {
        return Err(c.error("CLONE DATABASE requires a target name"));
    }
    let new_name = db_name(c, "target name")?;
    if !c.eat_keyword("FROM") {
        return Err(c.error("CLONE DATABASE requires FROM <source>"));
    }
    if c.at_end() {
        return Err(c.error("CLONE DATABASE FROM requires a source name"));
    }
    let source_name = db_name(c, "source name")?;

    let as_of = if c.eat_keywords(&["AS", "OF"]) {
        if c.eat_keyword("LATEST") {
            CloneAsOf::Latest
        } else {
            c.expect_keywords(&["SYSTEM", "TIME"])?;
            if c.eat_keyword("LATEST") {
                CloneAsOf::Latest
            } else if c.at_end() {
                return Err(
                    c.error("CLONE DATABASE AS OF SYSTEM TIME requires a timestamp or LATEST")
                );
            } else {
                let next = c.describe_next();
                let ms = c.i64("AS OF SYSTEM TIME").map_err(|_| SqlError::Parse {
                    detail: format!(
                        "CLONE DATABASE AS OF SYSTEM TIME: expected integer milliseconds \
                         or LATEST, got {next}"
                    ),
                })?;
                CloneAsOf::SystemTimeMs(ms)
            }
        }
    } else {
        // A bare `LATEST` names the default explicitly.
        c.eat_keyword("LATEST");
        CloneAsOf::Latest
    };
    c.finish()?;

    Ok(NodedbStatement::Database(DatabaseStmt::CloneDatabase {
        new_name,
        source_name,
        as_of,
    }))
}

#[cfg(test)]
mod tests {
    use super::super::dispatch::try_parse;
    use super::*;

    fn ok(sql: &str) -> NodedbStatement {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql)
            .expect("expected Some")
            .expect("expected Ok")
    }

    #[test]
    fn parse_clone_database_as_of_system_time() {
        let stmt = ok("CLONE DATABASE newdb FROM srcdb AS OF SYSTEM TIME 1730000000000");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::CloneDatabase {
                new_name: "newdb".into(),
                source_name: "srcdb".into(),
                as_of: CloneAsOf::SystemTimeMs(1_730_000_000_000),
            })
        );
    }

    #[test]
    fn parse_clone_database_latest() {
        let stmt = ok("CLONE DATABASE newdb FROM srcdb AS OF LATEST");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::CloneDatabase {
                new_name: "newdb".into(),
                source_name: "srcdb".into(),
                as_of: CloneAsOf::Latest,
            })
        );
    }

    #[test]
    fn lowercase_as_of_system_time_is_read() {
        let stmt = ok("clone database newdb from srcdb as of system time 42");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::CloneDatabase {
                new_name: "newdb".into(),
                source_name: "srcdb".into(),
                as_of: CloneAsOf::SystemTimeMs(42),
            })
        );
    }

    #[test]
    fn misspelled_as_of_clause_is_refused() {
        let sql = "CLONE DATABASE newdb FROM srcdb AS OF SYSTEM TIMESTAMP 42";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        assert!(matches!(
            try_parse(&upper, &parts, sql),
            Some(Err(SqlError::Parse { .. }))
        ));
    }

    #[test]
    fn a_bare_latest_is_read() {
        let stmt = ok("CLONE DATABASE newdb FROM srcdb LATEST");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::CloneDatabase {
                new_name: "newdb".into(),
                source_name: "srcdb".into(),
                as_of: CloneAsOf::Latest,
            })
        );
    }

    #[test]
    fn parse_clone_database_no_as_of_defaults_to_latest() {
        let stmt = ok("CLONE DATABASE newdb FROM srcdb");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::CloneDatabase {
                new_name: "newdb".into(),
                source_name: "srcdb".into(),
                as_of: CloneAsOf::Latest,
            })
        );
    }
}
