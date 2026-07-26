// SPDX-License-Identifier: Apache-2.0

//! Shared boundary-aware keyword matching for declared SQL type strings.
//!
//! [`IntWidth::from_declared_type`](super::IntWidth::from_declared_type),
//! [`FloatWidth::from_declared_type`](super::FloatWidth::from_declared_type),
//! and pgwire catalog introspection's declared-type → OID mapping all need the
//! same test: does a (trimmed, lowercased) declared type string name a given
//! keyword, allowing a trailing `(...)` parameter or whitespace modifier. One
//! shared function is what keeps the three matchers from drifting apart on
//! that boundary rule.

/// Whether `normalized` (already trimmed and lowercased) names the keyword
/// `name` — either exactly, or followed by `(` (a parameterised spelling like
/// `"int(11)"`) or whitespace (a trailing modifier like `"bigint not null"`).
pub fn declared_type_matches(normalized: &str, name: &str) -> bool {
    normalized == name
        || normalized.strip_prefix(name).is_some_and(|rest| {
            rest.starts_with('(') || rest.chars().next().is_some_and(char::is_whitespace)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_exact_keyword() {
        assert!(declared_type_matches("real", "real"));
    }

    #[test]
    fn matches_parameterised_and_whitespace_modifiers() {
        assert!(declared_type_matches("int(11)", "int"));
        assert!(declared_type_matches("bigint not null", "bigint"));
    }

    #[test]
    fn does_not_prefix_match_a_longer_keyword() {
        assert!(!declared_type_matches("int64", "int"));
    }
}
