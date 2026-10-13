// SPDX-License-Identifier: Apache-2.0

//! `DROP DATABASE [IF EXISTS] <name> [CASCADE | FORCE]`.

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::dispatch::db_name;

/// Read `[IF EXISTS] <name> [CASCADE | FORCE]` after `DROP DATABASE`.
pub(super) fn parse_drop_database(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    // FORCE is accepted as a synonym for CASCADE: both set `cascade = true`.
    // Any token after the name that is not CASCADE/FORCE is a parse error —
    // silently ignoring trailing garbage masks typos in user SQL.
    let if_exists = c.eat_keywords(&["IF", "EXISTS"]);
    if c.at_end() {
        return Err(c.error("DROP DATABASE requires a name"));
    }
    let name = db_name(c, "database name")?;

    let mut cascade = false;
    while !c.at_end() {
        if c.eat_keyword("CASCADE") || c.eat_keyword("FORCE") {
            cascade = true;
        } else {
            let word = c.peek_word_upper().unwrap_or_else(|| c.describe_next());
            return Err(c.error(format!("unexpected token '{word}'")));
        }
    }

    Ok(NodedbStatement::Database(DatabaseStmt::DropDatabase {
        name,
        if_exists,
        cascade,
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
    fn parse_drop_database_cascade() {
        let stmt = ok("DROP DATABASE mydb CASCADE");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::DropDatabase {
                name: "mydb".into(),
                if_exists: false,
                cascade: true,
            })
        );
    }

    #[test]
    fn parse_drop_database_force_is_cascade() {
        let stmt = ok("DROP DATABASE mydb FORCE");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::DropDatabase {
                name: "mydb".into(),
                if_exists: false,
                cascade: true,
            })
        );
    }

    #[test]
    fn parse_drop_database_if_exists() {
        let stmt = ok("DROP DATABASE IF EXISTS mydb");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::DropDatabase {
                name: "mydb".into(),
                if_exists: true,
                cascade: false,
            })
        );
    }

    #[test]
    fn parse_drop_database_rejects_unknown_trailing_token() {
        let sql = "DROP DATABASE mydb GARBAGE";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let err = try_parse(&upper, &parts, sql).unwrap().unwrap_err();
        match err {
            SqlError::Parse { detail } => {
                assert!(detail.contains("GARBAGE"), "unexpected: {detail}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
