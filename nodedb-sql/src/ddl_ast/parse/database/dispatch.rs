// SPDX-License-Identifier: Apache-2.0

//! Database-DDL dispatch: matches the leading verb tokens and delegates to
//! the per-operation parser. Keep this file thin — the actual parsing logic
//! belongs in the sibling modules.

use crate::ddl_ast::parse::clause::{ClauseCursor, tokenize};
use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

use super::alter::parse_alter_database;
use super::backup_restore::{parse_backup_database, parse_restore_database};
use super::clone::parse_clone_database;
use super::create::parse_create_database;
use super::drop_db::parse_drop_database;
use super::mirror::{parse_mirror_database, parse_show_database_mirror_status};
use super::move_tenant::parse_move_tenant;
use super::show_extras::{parse_show_database_lineage, parse_show_database_quota_or_usage};
use super::use_db::parse_use_database;

/// Try to parse a database-level DDL statement.
///
/// Returns `None` for SQL that does not match any database-DDL prefix.
/// Returns `Some(Err(...))` for structurally valid database DDL that contains a
/// parse error (e.g. missing required name token). Each parser reads its
/// statement left to right and refuses any token it does not consume.
pub fn try_parse(
    _upper: &str,
    _parts: &[&str],
    original: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    let toks = tokenize(original);
    let mut c = ClauseCursor::new(original, &toks, "DATABASE");
    let result = if c.eat_keywords(&["CREATE", "DATABASE"]) {
        parse_create_database(&mut c)
    } else if c.eat_keywords(&["DROP", "DATABASE"]) {
        parse_drop_database(&mut c)
    } else if c.eat_keywords(&["ALTER", "DATABASE"]) {
        parse_alter_database(&mut c)
    } else if c.eat_keywords(&["USE", "DATABASE"]) {
        parse_use_database(&mut c)
    } else if c.eat_keywords(&["CLONE", "DATABASE"]) {
        parse_clone_database(&mut c)
    } else if c.eat_keywords(&["MIRROR", "DATABASE"]) {
        parse_mirror_database(&mut c)
    } else if c.eat_keywords(&["MOVE", "TENANT"]) {
        parse_move_tenant(&mut c)
    } else if c.eat_keywords(&["BACKUP", "DATABASE"]) {
        parse_backup_database(&mut c)
    } else if c.eat_keywords(&["RESTORE", "DATABASE"]) {
        parse_restore_database(&mut c)
    } else if c.peek_keywords(&["SHOW", "DATABASES"]) {
        c.eat_keywords(&["SHOW", "DATABASES"]);
        if !c.at_end() {
            return None;
        }
        Ok(NodedbStatement::Database(DatabaseStmt::ShowDatabases))
    } else if c.eat_keywords(&["SHOW", "DATABASE", "QUOTA"]) {
        parse_show_database_quota_or_usage(&mut c, false)
    } else if c.eat_keywords(&["SHOW", "DATABASE", "USAGE"]) {
        parse_show_database_quota_or_usage(&mut c, true)
    } else if c.eat_keywords(&["SHOW", "DATABASE", "LINEAGE"]) {
        parse_show_database_lineage(&mut c)
    } else if c.eat_keywords(&["SHOW", "DATABASE", "MIRROR", "STATUS"]) {
        parse_show_database_mirror_status(&mut c)
    } else {
        return None;
    };
    Some(result)
}

/// A database or tenant name: a bare word as written, or a quoted
/// identifier's content.
pub(super) fn db_name(c: &mut ClauseCursor<'_, '_>, what: &str) -> Result<String, SqlError> {
    let raw = c.raw(what)?;
    Ok(raw
        .strip_prefix('"')
        .and_then(|r| r.strip_suffix('"'))
        .map_or_else(|| raw.to_string(), |inner| inner.replace("\"\"", "\"")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_show_databases() {
        let sql = "SHOW DATABASES";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let stmt = try_parse(&upper, &parts, sql).unwrap().unwrap();
        assert_eq!(stmt, NodedbStatement::Database(DatabaseStmt::ShowDatabases));
    }
}
