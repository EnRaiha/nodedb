// SPDX-License-Identifier: Apache-2.0

//! `BACKUP DATABASE <name> TO '<uri>'` and
//! `RESTORE DATABASE <name> FROM '<uri>' [FORCE] [DRY RUN]`.

use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

fn parse_error(detail: impl Into<String>) -> SqlError {
    SqlError::Parse {
        detail: detail.into(),
    }
}

/// The database name at `parts[2]`.
fn database_name(parts: &[&str], statement: &str) -> Result<String, SqlError> {
    parts
        .get(2)
        .map(|name| name.trim_matches('"').to_string())
        .ok_or_else(|| parse_error(format!("{statement} requires a database name")))
}

/// The URI that follows the keyword `keyword`, with its quotes removed.
fn uri_after<'a>(
    parts: &'a [&'a str],
    keyword: &str,
    statement: &str,
) -> Result<(String, &'a [&'a str]), SqlError> {
    let at = parts
        .iter()
        .position(|w| w.eq_ignore_ascii_case(keyword))
        .ok_or_else(|| parse_error(format!("{statement} requires {keyword} '<uri>'")))?;
    let uri = parts
        .get(at + 1)
        .map(|uri| uri.trim_matches('\'').to_string())
        .filter(|uri| !uri.is_empty())
        .ok_or_else(|| parse_error(format!("{statement} requires {keyword} '<uri>'")))?;
    Ok((uri, parts.get(at + 2..).unwrap_or(&[])))
}

pub(super) fn parse_backup_database(parts: &[&str]) -> Result<NodedbStatement, SqlError> {
    let statement = "BACKUP DATABASE";
    let name = database_name(parts, statement)?;
    let (uri, rest) = uri_after(parts, "TO", statement)?;
    if let Some(extra) = rest.first() {
        return Err(parse_error(format!(
            "{statement}: unexpected '{extra}' after the URI"
        )));
    }
    Ok(NodedbStatement::Database(DatabaseStmt::BackupDatabase {
        name,
        uri,
    }))
}

pub(super) fn parse_restore_database(parts: &[&str]) -> Result<NodedbStatement, SqlError> {
    let statement = "RESTORE DATABASE";
    let name = database_name(parts, statement)?;
    let (uri, mut rest) = uri_after(parts, "FROM", statement)?;
    let (mut force, mut dry_run) = (false, false);
    while let Some(word) = rest.first() {
        if word.eq_ignore_ascii_case("FORCE") && !force {
            force = true;
            rest = &rest[1..];
        } else if word.eq_ignore_ascii_case("DRY")
            && rest.get(1).is_some_and(|w| w.eq_ignore_ascii_case("RUN"))
            && !dry_run
        {
            dry_run = true;
            rest = &rest[2..];
        } else {
            return Err(parse_error(format!(
                "{statement}: unexpected '{word}' after the URI; expected FORCE or DRY RUN"
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
