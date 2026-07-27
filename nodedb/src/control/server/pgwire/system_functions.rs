// SPDX-License-Identifier: BUSL-1.1

//! Scalar-function shims that live in the pgwire layer.
//!
//! These accept a `SELECT <fn>(<args>)` call and rewrite it to
//! whatever the underlying DDL actually is, so scripts have a stable
//! function name they can pipe through any SQL client. They do not
//! introduce new privileges; the rewritten DDL goes through the
//! normal router and its existing authorization checks apply.

use nodedb_types::strip_prefix_ascii_case_insensitive;

/// Rewrite `SELECT _system.purge_collection('<name>')` (optionally
/// `SELECT _system.purge_collection('<tenant>', '<name>')` —
/// two-argument form supported so superusers can target a specific
/// tenant) into the equivalent `DROP COLLECTION ... PURGE` DDL.
///
/// Returns `None` if the SQL is not a call into this function so the
/// caller can fall through to the normal planner.
///
/// The caller passes both the original `sql` (so identifier casing is
/// preserved in the rewritten form) and the already-uppercased
/// `upper` (so pattern matching is cheap and case-insensitive).
pub fn rewrite_purge_collection(sql: &str, upper: &str) -> Option<String> {
    // Fast reject — avoid string work on every query.
    if !upper.contains("_SYSTEM.PURGE_COLLECTION(") {
        return None;
    }
    let trimmed = sql.trim().trim_end_matches(';').trim();
    strip_prefix_ascii_case_insensitive(trimmed, "SELECT ")?;

    let paren = trimmed.find('(')?;
    let close = trimmed.rfind(')')?;
    if close <= paren + 1 {
        return None;
    }
    let args = &trimmed[paren + 1..close];

    let parts: Vec<&str> = args.split(',').map(str::trim).collect();
    let name = match parts.as_slice() {
        [only] => strip_quotes(only)?,
        // Two-arg form: (tenant_id, name). Only the name is needed
        // in the rewritten DDL — tenant scoping is done by the
        // session identity in the DROP COLLECTION handler.
        [_tenant, name] => strip_quotes(name)?,
        _ => return None,
    };

    if name.is_empty() {
        return None;
    }
    // Preserve identifier casing with double-quotes; the DROP
    // COLLECTION handler normalizes.
    Some(format!(r#"DROP COLLECTION "{name}" PURGE"#))
}

/// Strip a single-quoted SQL string literal or a double-quoted
/// identifier into the raw inner value. Returns `None` if neither
/// quote style matches. Escapes are not honored — names with embedded
/// quotes will be rejected, which is the right answer for a collection
/// identifier (they aren't allowed anywhere else in the system
/// either).
fn strip_quotes(s: &str) -> Option<String> {
    let s = s.trim();
    if let Some(inner) = s
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
    {
        return (!inner.contains('\'')).then(|| inner.to_string());
    }
    if let Some(inner) = s
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    {
        return (!inner.contains('"')).then(|| inner.to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite(sql: &str) -> Option<String> {
        rewrite_purge_collection(sql, &sql.to_uppercase())
    }

    #[test]
    fn rewrites_single_arg_form() {
        assert_eq!(
            rewrite("SELECT _system.purge_collection('events')"),
            Some(r#"DROP COLLECTION "events" PURGE"#.to_string())
        );
    }

    #[test]
    fn rewrites_two_arg_form() {
        assert_eq!(
            rewrite("SELECT _system.purge_collection(1, 'events')"),
            Some(r#"DROP COLLECTION "events" PURGE"#.to_string())
        );
    }

    #[test]
    fn accepts_trailing_semicolon() {
        assert_eq!(
            rewrite("SELECT _system.purge_collection('events');"),
            Some(r#"DROP COLLECTION "events" PURGE"#.to_string())
        );
    }

    #[test]
    fn accepts_double_quoted_identifier() {
        assert_eq!(
            rewrite(r#"SELECT _system.purge_collection("MyCol")"#),
            Some(r#"DROP COLLECTION "MyCol" PURGE"#.to_string())
        );
    }

    #[test]
    fn unicode_collection_name_preserves_original_argument_boundaries() {
        assert_eq!(
            rewrite("SeLeCt _system.purge_collection('Straße')"),
            Some(r#"DROP COLLECTION "Straße" PURGE"#.to_string())
        );
    }

    #[test]
    fn rejects_unrelated_select() {
        assert_eq!(rewrite("SELECT 1"), None);
        assert_eq!(rewrite("SELECT _system.dropped_collections"), None);
    }

    #[test]
    fn rejects_empty_and_unquoted() {
        assert_eq!(rewrite("SELECT _system.purge_collection()"), None);
        assert_eq!(rewrite("SELECT _system.purge_collection(events)"), None);
    }
}
