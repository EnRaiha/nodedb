// SPDX-License-Identifier: Apache-2.0

//! `CREATE DATABASE [IF NOT EXISTS] <name> [WITH (...)]`.

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::dispatch::db_name;
use super::with_options::parse_with_options;

/// Read `[IF NOT EXISTS] <name> [WITH (...)]` after `CREATE DATABASE`.
pub(super) fn parse_create_database(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    let if_not_exists = c.eat_keywords(&["IF", "NOT", "EXISTS"]);
    if c.at_end() {
        return Err(c.error("CREATE DATABASE requires a name"));
    }
    let name = db_name(c, "database name")?;
    let options = parse_with_options(c)?;
    c.finish()?;

    Ok(NodedbStatement::Database(DatabaseStmt::CreateDatabase {
        name,
        if_not_exists,
        options,
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
    fn parse_create_database_simple() {
        let stmt = ok("CREATE DATABASE mydb");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::CreateDatabase {
                name: "mydb".into(),
                if_not_exists: false,
                options: vec![],
            })
        );
    }

    #[test]
    fn parse_create_database_if_not_exists() {
        let stmt = ok("CREATE DATABASE IF NOT EXISTS mydb");
        match stmt {
            NodedbStatement::Database(DatabaseStmt::CreateDatabase {
                name,
                if_not_exists,
                ..
            }) => {
                assert_eq!(name, "mydb");
                assert!(if_not_exists);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
