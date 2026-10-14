// SPDX-License-Identifier: Apache-2.0

//! `USE DATABASE <name>`.

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::dispatch::db_name;

/// Read `<name>` after `USE DATABASE`.
pub(super) fn parse_use_database(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    if c.at_end() {
        return Err(c.error("USE DATABASE requires a name"));
    }
    let name = db_name(c, "database name")?;
    c.finish()?;
    Ok(NodedbStatement::Database(DatabaseStmt::UseDatabase {
        name,
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
    fn parse_use_database() {
        let stmt = ok("USE DATABASE mydb");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::UseDatabase {
                name: "mydb".into()
            })
        );
    }
}
