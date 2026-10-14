// SPDX-License-Identifier: Apache-2.0

//! `MOVE TENANT <tenant> FROM <db_a> TO <db_b>`.

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::dispatch::db_name;

/// Read `<tenant> FROM <db_a> TO <db_b>` after `MOVE TENANT`. Each keyword is
/// read at its position, so a tenant or database named `from` or `to` is
/// never mistaken for the clause.
pub(super) fn parse_move_tenant(c: &mut ClauseCursor<'_, '_>) -> Result<NodedbStatement, SqlError> {
    if c.at_end() {
        return Err(c.error("MOVE TENANT requires a tenant name"));
    }
    let tenant_name = db_name(c, "tenant name")?;
    if !c.eat_keyword("FROM") {
        return Err(c.error("MOVE TENANT requires FROM <db>"));
    }
    if c.at_end() {
        return Err(c.error("MOVE TENANT FROM requires a source database name"));
    }
    let from_db = db_name(c, "source database")?;
    if !c.eat_keyword("TO") {
        return Err(c.error("MOVE TENANT requires TO <db>"));
    }
    if c.at_end() {
        return Err(c.error("MOVE TENANT TO requires a destination database name"));
    }
    let to_db = db_name(c, "destination database")?;
    c.finish()?;

    Ok(NodedbStatement::Database(DatabaseStmt::MoveTenant {
        tenant_name,
        from_db,
        to_db,
    }))
}

#[cfg(test)]
mod tests {
    use super::super::dispatch::try_parse;
    use super::*;

    fn try_sql(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&sql.to_uppercase(), &parts, sql)
    }

    #[test]
    fn move_tenant_reads_clauses_in_order() {
        assert_eq!(
            try_sql("MOVE TENANT acme FROM a TO b")
                .expect("claimed")
                .expect("parses"),
            NodedbStatement::Database(DatabaseStmt::MoveTenant {
                tenant_name: "acme".into(),
                from_db: "a".into(),
                to_db: "b".into(),
            })
        );
    }

    #[test]
    fn swapped_or_trailing_clauses_are_refused() {
        assert!(matches!(
            try_sql("MOVE TENANT acme TO b FROM a"),
            Some(Err(SqlError::Parse { .. }))
        ));
        assert!(matches!(
            try_sql("MOVE TENANT acme FROM a TO b NOW"),
            Some(Err(SqlError::Parse { .. }))
        ));
    }
}
