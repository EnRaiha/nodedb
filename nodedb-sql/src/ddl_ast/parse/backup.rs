// SPDX-License-Identifier: Apache-2.0

//! Parse BACKUP TENANT / RESTORE TENANT.

use crate::ddl_ast::statement::{DatabaseStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    _trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("BACKUP TENANT ") {
        // BACKUP TENANT <tenant_id>
        let tenant_id = parts.get(2).map(|s| s.to_string()).unwrap_or_default();
        return Some(Ok(NodedbStatement::Database(DatabaseStmt::BackupTenant {
            tenant_id,
        })));
    }
    if upper.starts_with("RESTORE TENANT ") {
        // RESTORE TENANT <tenant_id> FROM '<path>' [FORCE] [DRY RUN]
        // Scan modifiers only in the tail after the closing path quote, so a
        // path literal that itself contains FORCE / DRY RUN is never mistaken
        // for a modifier. With no path quote present, fall back to the whole
        // statement (no literal exists to false-positive on).
        let modifier_tail = upper
            .rsplit_once('\'')
            .map(|(_, tail)| tail)
            .unwrap_or(upper);
        // Exact-token scan so a longer word (e.g. ENFORCE) is never read as a
        // modifier keyword.
        let modifiers: Vec<&str> = modifier_tail.split_whitespace().collect();
        let dry_run =
            modifiers.contains(&"DRYRUN") || modifiers.windows(2).any(|w| w == ["DRY", "RUN"]);
        let force = modifiers.contains(&"FORCE");
        let tenant_id = parts.get(2).map(|s| s.to_string()).unwrap_or_default();
        return Some(Ok(NodedbStatement::Database(DatabaseStmt::RestoreTenant {
            dry_run,
            force,
            tenant_id,
        })));
    }
    None
}
