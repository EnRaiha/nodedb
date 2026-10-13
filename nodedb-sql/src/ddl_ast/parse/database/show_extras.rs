// SPDX-License-Identifier: Apache-2.0

//! `SHOW DATABASE QUOTA FOR <name>`, `SHOW DATABASE USAGE FOR <name>`,
//! and `SHOW DATABASE LINEAGE FOR <name>`.

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::dispatch::db_name;

/// Read `FOR <name>` and require the end of the statement.
fn for_name(c: &mut ClauseCursor<'_, '_>, statement: &str) -> Result<String, SqlError> {
    if !c.eat_keyword("FOR") {
        return Err(c.error(format!("{statement} requires FOR <name>")));
    }
    if c.at_end() {
        return Err(c.error(format!("{statement} FOR requires a database name")));
    }
    let name = db_name(c, "database name")?;
    c.finish()?;
    Ok(name)
}

/// Parse `FOR <name>` after `SHOW DATABASE QUOTA` or `SHOW DATABASE USAGE`.
/// `is_usage = false` → quota, `is_usage = true` → usage.
pub(super) fn parse_show_database_quota_or_usage(
    c: &mut ClauseCursor<'_, '_>,
    is_usage: bool,
) -> Result<NodedbStatement, SqlError> {
    let name = for_name(c, "SHOW DATABASE QUOTA / USAGE")?;

    if is_usage {
        Ok(NodedbStatement::Database(DatabaseStmt::ShowDatabaseUsage {
            name,
        }))
    } else {
        Ok(NodedbStatement::Database(DatabaseStmt::ShowDatabaseQuota {
            name,
        }))
    }
}

/// Parse `FOR <name>` after `SHOW DATABASE LINEAGE`.
pub(super) fn parse_show_database_lineage(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    let name = for_name(c, "SHOW DATABASE LINEAGE")?;
    Ok(NodedbStatement::Database(
        DatabaseStmt::ShowDatabaseLineage { name },
    ))
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
    fn parse_show_database_lineage() {
        let stmt = ok("SHOW DATABASE LINEAGE FOR mydb");
        assert_eq!(
            stmt,
            NodedbStatement::Database(DatabaseStmt::ShowDatabaseLineage {
                name: "mydb".into()
            })
        );
    }

    #[test]
    fn parse_show_database_lineage_missing_name_errors() {
        let sql = "SHOW DATABASE LINEAGE FOR";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let err = try_parse(&upper, &parts, sql).unwrap().unwrap_err();
        match err {
            SqlError::Parse { detail } => {
                assert!(detail.contains("requires a database name"), "{detail}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
