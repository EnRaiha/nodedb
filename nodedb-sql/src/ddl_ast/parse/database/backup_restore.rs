// SPDX-License-Identifier: Apache-2.0

//! `BACKUP DATABASE <name> TO '<uri>'` and
//! `RESTORE DATABASE <name> FROM '<uri>' [FORCE] [DRY RUN]`.

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::dispatch::db_name;

/// Read `<name> <keyword> '<uri>'`. The keyword is read right after the
/// name, so a name or path containing it is never mistaken for it.
fn name_and_uri(
    c: &mut ClauseCursor<'_, '_>,
    keyword: &str,
    statement: &str,
) -> Result<(String, String), SqlError> {
    if c.at_end() {
        return Err(c.error(format!("{statement} requires a database name")));
    }
    let name = db_name(c, "database name")?;
    if !c.eat_keyword(keyword) {
        return Err(c.error(format!("{statement} requires {keyword} '<uri>'")));
    }
    let uri = c
        .string(keyword)
        .ok()
        .filter(|uri| !uri.is_empty())
        .ok_or_else(|| c.error(format!("{statement} requires {keyword} '<uri>'")))?;
    Ok((name, uri))
}

pub(super) fn parse_backup_database(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    let statement = "BACKUP DATABASE";
    let (name, uri) = name_and_uri(c, "TO", statement)?;
    if !c.at_end() {
        return Err(c.error(format!("unexpected {} after the URI", c.describe_next())));
    }
    Ok(NodedbStatement::Database(DatabaseStmt::BackupDatabase {
        name,
        uri,
    }))
}

pub(super) fn parse_restore_database(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<NodedbStatement, SqlError> {
    let statement = "RESTORE DATABASE";
    let (name, uri) = name_and_uri(c, "FROM", statement)?;
    let (mut force, mut dry_run) = (false, false);
    while !c.at_end() {
        if !force && c.eat_keyword("FORCE") {
            force = true;
        } else if !dry_run && c.eat_keywords(&["DRY", "RUN"]) {
            dry_run = true;
        } else {
            return Err(c.error(format!(
                "unexpected {} after the URI; expected FORCE or DRY RUN",
                c.describe_next()
            )));
        }
    }
    Ok(NodedbStatement::Database(DatabaseStmt::RestoreDatabase {
        name,
        uri,
        force,
        dry_run,
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
    fn uri_with_spaces_and_keywords_is_one_value() {
        let Some(Ok(NodedbStatement::Database(DatabaseStmt::RestoreDatabase {
            uri,
            force,
            dry_run,
            ..
        }))) = try_sql("RESTORE DATABASE db FROM 's3://b/FORCE DRY RUN x' FORCE")
        else {
            panic!("expected RestoreDatabase");
        };
        assert_eq!(uri, "s3://b/FORCE DRY RUN x");
        assert!(force && !dry_run);
    }

    #[test]
    fn backup_trailing_tokens_are_refused() {
        assert!(matches!(
            try_sql("BACKUP DATABASE db TO 's3://b/x' NOW"),
            Some(Err(SqlError::Parse { .. }))
        ));
    }
}
