// SPDX-License-Identifier: BUSL-1.1

//! Shared parsing utilities for DDL handlers.

/// Strip a leading `IF NOT EXISTS` clause that sits immediately after the
/// `keyword_count` leading DDL keyword tokens (2 for `CREATE TENANT`, 3 for
/// `CREATE SERVICE ACCOUNT`). Returns whether the clause was present and the
/// token slice with the clause removed, so handlers can parse the object
/// name from a fixed position without the clause keywords shifting it.
pub(crate) fn strip_if_not_exists<'a>(
    parts: &[&'a str],
    keyword_count: usize,
) -> (bool, Vec<&'a str>) {
    if parts.len() >= keyword_count + 3
        && parts[keyword_count].eq_ignore_ascii_case("IF")
        && parts[keyword_count + 1].eq_ignore_ascii_case("NOT")
        && parts[keyword_count + 2].eq_ignore_ascii_case("EXISTS")
    {
        let mut remaining: Vec<&str> = parts[..keyword_count].to_vec();
        remaining.extend_from_slice(&parts[keyword_count + 3..]);
        (true, remaining)
    } else {
        (false, parts.to_vec())
    }
}

/// Strip a leading `IF EXISTS` clause that sits immediately after the
/// `keyword_count` leading DDL keyword tokens. Counterpart of
/// [`strip_if_not_exists`] for `DROP` statements.
pub(crate) fn strip_if_exists<'a>(parts: &[&'a str], keyword_count: usize) -> (bool, Vec<&'a str>) {
    if parts.len() >= keyword_count + 2
        && parts[keyword_count].eq_ignore_ascii_case("IF")
        && parts[keyword_count + 1].eq_ignore_ascii_case("EXISTS")
    {
        let mut remaining: Vec<&str> = parts[..keyword_count].to_vec();
        remaining.extend_from_slice(&parts[keyword_count + 2..]);
        (true, remaining)
    } else {
        (false, parts.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn if_not_exists_stripped_after_keywords() {
        let parts = ["CREATE", "TENANT", "IF", "NOT", "EXISTS", "acme"];
        let (present, rest) = strip_if_not_exists(&parts, 2);
        assert!(present);
        assert_eq!(rest, ["CREATE", "TENANT", "acme"]);
    }

    #[test]
    fn if_not_exists_absent_leaves_parts_intact() {
        let parts = ["CREATE", "TENANT", "acme", "ID", "5"];
        let (present, rest) = strip_if_not_exists(&parts, 2);
        assert!(!present);
        assert_eq!(rest, parts);
    }

    #[test]
    fn if_exists_stripped_after_keywords() {
        let parts = ["DROP", "ROLE", "IF", "EXISTS", "auditor"];
        let (present, rest) = strip_if_exists(&parts, 2);
        assert!(present);
        assert_eq!(rest, ["DROP", "ROLE", "auditor"]);
    }

    #[test]
    fn strip_helpers_handle_three_keyword_prefix() {
        let parts = ["CREATE", "SERVICE", "ACCOUNT", "IF", "NOT", "EXISTS", "svc"];
        let (present, rest) = strip_if_not_exists(&parts, 3);
        assert!(present);
        assert_eq!(rest, ["CREATE", "SERVICE", "ACCOUNT", "svc"]);
    }
}
