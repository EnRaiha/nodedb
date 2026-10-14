// SPDX-License-Identifier: Apache-2.0

//! Parse BACKUP TENANT / RESTORE TENANT.

use super::clause::{ClauseCursor, tokenize};
use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("BACKUP TENANT ") || upper.starts_with("RESTORE TENANT ") {
        return Some(parse(trimmed).map(NodedbStatement::Database));
    }
    None
}

/// `BACKUP TENANT <id> [TO '<path>']` or `RESTORE TENANT <id> [FROM '<path>'] [FORCE]
/// [DRY RUN]`. A modifier is read at its position, so a path literal that
/// contains FORCE or DRY RUN sets nothing.
fn parse(sql: &str) -> Result<DatabaseStmt, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "TENANT BACKUP");
    if c.eat_keywords(&["BACKUP", "TENANT"]) {
        let tenant_id = c.raw("tenant id")?.to_string();
        // The path form is read so the statement reaches its handler, which
        // refuses every bare form and names the streaming COPY form. No path
        // is ever opened.
        if c.eat_keyword("TO") {
            c.string("TO")?;
        }
        c.finish()?;
        return Ok(DatabaseStmt::BackupTenant { tenant_id });
    }
    c.expect_keywords(&["RESTORE", "TENANT"])?;
    let tenant_id = c.raw("tenant id")?.to_string();
    if c.eat_keyword("FROM") {
        c.string("FROM")?;
    }
    let (mut force, mut dry_run) = (false, false);
    while !c.at_end() {
        if c.eat_keyword("FORCE") {
            c.clause("FORCE")?;
            force = true;
        } else if c.eat_keywords(&["DRY", "RUN"]) || c.eat_keyword("DRYRUN") {
            c.clause("DRY RUN")?;
            dry_run = true;
        } else {
            return Err(c.unknown_clause());
        }
    }
    Ok(DatabaseStmt::RestoreTenant {
        dry_run,
        force,
        tenant_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modifiers_inside_the_path_set_nothing() {
        let DatabaseStmt::RestoreTenant { dry_run, force, .. } =
            parse("RESTORE TENANT 7 FROM '/backups/FORCE DRY RUN.bak'").expect("parses")
        else {
            panic!("expected RestoreTenant");
        };
        assert!(!dry_run && !force);
    }

    /// The path form parses, so the handler refuses it with the COPY form.
    #[test]
    fn backup_path_form_parses() {
        assert!(matches!(
            parse("BACKUP TENANT 1 TO '/proc/self/mem'"),
            Ok(DatabaseStmt::BackupTenant { .. })
        ));
        assert!(parse("BACKUP TENANT 1 TO").is_err());
    }

    #[test]
    fn modifiers_after_the_path_are_read() {
        let DatabaseStmt::RestoreTenant { dry_run, force, .. } =
            parse("RESTORE TENANT 7 FROM '/b.bak' FORCE DRY RUN").expect("parses")
        else {
            panic!("expected RestoreTenant");
        };
        assert!(dry_run && force);
        assert!(parse("RESTORE TENANT 7 FROM '/b.bak' ENFORCE").is_err());
    }
}
