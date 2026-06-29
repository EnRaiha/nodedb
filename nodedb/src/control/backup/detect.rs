// SPDX-License-Identifier: BUSL-1.1

//! Pre-sqlparser detection of the wire-streaming COPY shapes used by
//! BACKUP and RESTORE. These shapes don't fit the plain `COPY <table>`
//! grammar sqlparser supports, so we recognise them with a tight
//! tokenizer pass before the SQL ever reaches sqlparser.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyIntent {
    /// `COPY (BACKUP TENANT <id>) TO STDOUT`
    BackupTenant { tenant_id: u64 },
    /// `COPY tenant_restore(<id>) FROM STDIN [FORCE] [DRY RUN]`
    RestoreTenant {
        tenant_id: u64,
        dry_run: bool,
        force: bool,
    },
}

/// Returns Some(intent) if the SQL is one of the recognised wire-COPY shapes.
/// Tokenisation is whitespace-only and case-insensitive on keywords.
pub fn detect(sql: &str) -> Option<CopyIntent> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();

    // Quick reject: every recognised shape starts with COPY.
    if !upper.starts_with("COPY ") && !upper.starts_with("COPY\t") {
        return None;
    }

    if let Some(intent) = match_backup(trimmed, &upper) {
        return Some(intent);
    }
    if let Some(intent) = match_restore(trimmed, &upper) {
        return Some(intent);
    }
    None
}

fn match_backup(sql: &str, upper: &str) -> Option<CopyIntent> {
    // Pattern: COPY ( BACKUP TENANT <id> ) TO STDOUT
    let after_copy = upper.strip_prefix("COPY")?.trim_start();
    let after_paren = after_copy.strip_prefix('(')?.trim_start();
    let after_backup = after_paren.strip_prefix("BACKUP")?.trim_start();
    let after_tenant = after_backup.strip_prefix("TENANT")?.trim_start();

    // Find the matching ')'.
    let close_idx = after_tenant.find(')')?;
    let id_token = after_tenant[..close_idx].trim();
    let tenant_id: u64 = id_token.parse().ok()?;

    let after_close = after_tenant[close_idx + 1..].trim_start();
    let after_to = after_close.strip_prefix("TO")?.trim_start();
    if !after_to.starts_with("STDOUT") {
        return None;
    }
    // Tail must be empty (modulo whitespace).
    let tail = after_to.strip_prefix("STDOUT")?.trim();
    if !tail.is_empty() {
        return None;
    }
    let _ = sql; // sql kept in signature for symmetry / future tracing.
    Some(CopyIntent::BackupTenant { tenant_id })
}

fn match_restore(sql: &str, upper: &str) -> Option<CopyIntent> {
    // Pattern: COPY tenant_restore ( <id> ) FROM STDIN [ DRY RUN ]
    let after_copy = upper.strip_prefix("COPY")?.trim_start();
    let after_fn = after_copy.strip_prefix("TENANT_RESTORE")?.trim_start();
    let after_paren = after_fn.strip_prefix('(')?.trim_start();

    let close_idx = after_paren.find(')')?;
    let id_token = after_paren[..close_idx].trim();
    let tenant_id: u64 = id_token.parse().ok()?;

    let after_close = after_paren[close_idx + 1..].trim_start();
    let after_from = after_close.strip_prefix("FROM")?.trim_start();
    let after_stdin = after_from.strip_prefix("STDIN")?.trim_start();

    // Optional trailing modifiers in any order: FORCE and DRY RUN (or DRYRUN).
    // Collapse "DRY RUN" to a single token so a flat word scan can validate
    // that nothing unrecognised trails STDIN.
    let normalized = after_stdin.trim().replace("DRY RUN", "DRYRUN");
    let mut dry_run = false;
    let mut force = false;
    for token in normalized.split_whitespace() {
        match token {
            "DRYRUN" => dry_run = true,
            "FORCE" => force = true,
            _ => return None,
        }
    }

    let _ = sql;
    Some(CopyIntent::RestoreTenant {
        tenant_id,
        dry_run,
        force,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_backup() {
        assert_eq!(
            detect("COPY (BACKUP TENANT 7) TO STDOUT"),
            Some(CopyIntent::BackupTenant { tenant_id: 7 })
        );
    }

    #[test]
    fn detects_backup_with_trailing_semicolon_and_whitespace() {
        assert_eq!(
            detect("  copy ( backup tenant 42 ) to stdout ;  "),
            Some(CopyIntent::BackupTenant { tenant_id: 42 })
        );
    }

    #[test]
    fn detects_restore() {
        assert_eq!(
            detect("COPY tenant_restore(7) FROM STDIN"),
            Some(CopyIntent::RestoreTenant {
                tenant_id: 7,
                dry_run: false,
                force: false
            })
        );
    }

    #[test]
    fn detects_restore_dry_run() {
        assert_eq!(
            detect("COPY tenant_restore(7) FROM STDIN DRY RUN"),
            Some(CopyIntent::RestoreTenant {
                tenant_id: 7,
                dry_run: true,
                force: false
            })
        );
        assert_eq!(
            detect("COPY tenant_restore(7) FROM STDIN DRYRUN"),
            Some(CopyIntent::RestoreTenant {
                tenant_id: 7,
                dry_run: true,
                force: false
            })
        );
    }

    #[test]
    fn detects_restore_force() {
        assert_eq!(
            detect("COPY tenant_restore(7) FROM STDIN FORCE"),
            Some(CopyIntent::RestoreTenant {
                tenant_id: 7,
                dry_run: false,
                force: true
            })
        );
    }

    #[test]
    fn detects_restore_force_and_dry_run_any_order() {
        assert_eq!(
            detect("COPY tenant_restore(7) FROM STDIN FORCE DRY RUN"),
            Some(CopyIntent::RestoreTenant {
                tenant_id: 7,
                dry_run: true,
                force: true
            })
        );
        assert_eq!(
            detect("COPY tenant_restore(7) FROM STDIN DRY RUN FORCE"),
            Some(CopyIntent::RestoreTenant {
                tenant_id: 7,
                dry_run: true,
                force: true
            })
        );
    }

    #[test]
    fn rejects_unknown_restore_modifier() {
        assert_eq!(detect("COPY tenant_restore(7) FROM STDIN BOGUS"), None);
    }

    #[test]
    fn rejects_legacy_path_form() {
        assert_eq!(detect("BACKUP TENANT 7 TO '/tmp/x.bak'"), None);
        assert_eq!(detect("RESTORE TENANT 7 FROM '/tmp/x.bak'"), None);
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(detect("SELECT 1"), None);
        assert_eq!(detect("COPY tenant_restore"), None);
        assert_eq!(detect("COPY (BACKUP TENANT abc) TO STDOUT"), None);
        assert_eq!(detect("COPY (BACKUP TENANT 7) TO STDIN"), None);
    }
}
