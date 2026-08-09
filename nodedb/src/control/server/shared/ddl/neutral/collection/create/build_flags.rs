// SPDX-License-Identifier: BUSL-1.1

//! Name / flag validation for `build_and_persist`: collection-name shape,
//! the `WITH (crdt=...)` boolean, and `SIGNED_DELTAS` ⇒ CRDT + authenticated
//! WAL. Relocated verbatim from the pgwire
//! `pgwire::ddl::collection::create::build` module (now deleted).

use super::super::super::super::result::DdlError;

pub(super) fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Parse a `WITH (crdt=...)` option value as a boolean, accepting
/// `"true"`/`"false"` case-insensitively. Any other value is a
/// user error surfaced as a typed DDL error (SQLSTATE 42601).
fn parse_crdt_flag(value: &str) -> Result<bool, DdlError> {
    match value.trim() {
        v if v.eq_ignore_ascii_case("true") => Ok(true),
        v if v.eq_ignore_ascii_case("false") => Ok(false),
        other => Err(err(
            "42601",
            format!("invalid value for WITH (crdt=...): '{other}'; expected 'true' or 'false'"),
        )),
    }
}

/// Resolve the CRDT storage flag from the `WITH (...)` option list.
///
/// A missing `crdt` option defaults to `false`. CRDT (Loro) storage is a
/// document-engine capability, so `crdt=true` is rejected with SQLSTATE
/// 42601 on any non-document collection rather than persisting a flag no
/// engine would honor.
pub(super) fn resolve_crdt_flag(
    options: &[(String, String)],
    collection_type: &nodedb_types::CollectionType,
) -> Result<bool, DdlError> {
    let crdt = match options.iter().find(|(k, _)| k.eq_ignore_ascii_case("crdt")) {
        Some((_, v)) => parse_crdt_flag(v)?,
        None => false,
    };
    if crdt && !matches!(collection_type, nodedb_types::CollectionType::Document(_)) {
        return Err(err(
            "42601",
            "WITH (crdt=true) is only supported on document collections".to_string(),
        ));
    }
    Ok(crdt)
}

pub(super) fn validate_crdt_signing_storage(
    signing_required: bool,
    crdt: bool,
    wal_authenticated: bool,
) -> Result<(), DdlError> {
    if signing_required && !crdt {
        return Err(err(
            "42601",
            "SIGNED_DELTAS requires WITH (crdt=true)".to_string(),
        ));
    }
    if signing_required && !wal_authenticated {
        return Err(err(
            "55000",
            "SIGNED_DELTAS requires authenticated WAL encryption".to_string(),
        ));
    }
    Ok(())
}

/// Reject names that aren't `[A-Za-z0-9_-]+`. Both `collection` and
/// `table` share the rule; only the error label differs.
pub(super) fn validate_name(name: &str, label: &str) -> Result<(), DdlError> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(err(
            "42601",
            format!(
                "invalid {label} name '{name}': only letters, digits, '-', and '_' are allowed"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Collection name validation tests. Relocated verbatim from the pgwire
    //! `pgwire::ddl::collection::create::tests` module (now deleted).

    use super::{resolve_crdt_flag, validate_crdt_signing_storage};

    fn opts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn crdt_true_on_document_collection_resolves_true() {
        let options = opts(&[("crdt", "true")]);
        let flag = resolve_crdt_flag(&options, &nodedb_types::CollectionType::document())
            .expect("crdt=true on a document collection must resolve");
        assert!(flag);
    }

    #[test]
    fn crdt_true_on_non_document_collection_rejected() {
        let options = opts(&[("crdt", "true")]);
        let err = resolve_crdt_flag(&options, &nodedb_types::CollectionType::columnar())
            .expect_err("crdt=true on a non-document collection must be rejected");
        assert_eq!(err.sqlstate, "42601");
    }

    #[test]
    fn crdt_garbage_value_rejected() {
        let options = opts(&[("crdt", "maybe")]);
        let err = resolve_crdt_flag(&options, &nodedb_types::CollectionType::document())
            .expect_err("a non-boolean crdt value must be rejected");
        assert_eq!(err.sqlstate, "42601");
    }

    #[test]
    fn signed_deltas_require_crdt_and_authenticated_wal() {
        let no_crdt = validate_crdt_signing_storage(true, false, true)
            .expect_err("signed deltas without CRDT must be rejected");
        assert_eq!(no_crdt.sqlstate, "42601");

        let unauthenticated_wal = validate_crdt_signing_storage(true, true, false)
            .expect_err("signed deltas without authenticated WAL must be rejected");
        assert_eq!(unauthenticated_wal.sqlstate, "55000");

        validate_crdt_signing_storage(true, true, true)
            .expect("signed CRDT deltas with authenticated WAL must be accepted");
        validate_crdt_signing_storage(false, false, false)
            .expect("ordinary collections do not require WAL encryption");
    }

    #[test]
    fn crdt_absent_defaults_false() {
        let options = opts(&[("engine", "kv")]);
        let flag = resolve_crdt_flag(&options, &nodedb_types::CollectionType::document())
            .expect("absent crdt option must resolve to a default");
        assert!(!flag);
    }

    /// Collection name validation: allowed chars are `[a-zA-Z0-9_-]`.
    fn validate_name(name: &str) -> bool {
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    }

    #[test]
    fn valid_collection_names() {
        assert!(validate_name("docs"));
        assert!(validate_name("my_collection"));
        assert!(validate_name("my-collection"));
        assert!(validate_name("Collection123"));
        assert!(validate_name("a"));
    }

    #[test]
    fn invalid_collection_names_rejected() {
        // Semicolons are sent by psql in multi-statement queries —
        // must be rejected with a clear error, not stored silently.
        assert!(!validate_name("docs;"));
        assert!(!validate_name("bad;name"));
        assert!(!validate_name("bad name"));
        assert!(!validate_name("bad.name"));
        assert!(!validate_name("bad/name"));
        assert!(!validate_name(""));
        assert!(!validate_name("events;"));
        assert!(!validate_name("orders;"));
    }
}
