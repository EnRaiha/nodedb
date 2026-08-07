// SPDX-License-Identifier: BUSL-1.1

//! The one claim resolver every claim-consuming path uses to turn a configured
//! claim name into a value from a verified token's payload.
//!
//! `JwtClaims::extra` captures the payload's non-standard claims flattened at
//! the top level, so a plain map lookup can only ever see the outermost keys.
//! Real providers nest what NodeDB needs — Keycloak puts roles under
//! `realm_access.roles` — which is why resolution is a path walk, not a lookup.
//!
//! Resolution order, and why:
//!
//! 1. **Exact top-level key first.** Claim names legally contain dots and
//!    colons (`cognito:groups`, `custom:namespace.field`). Trying the literal
//!    key before splitting keeps those working and removes the need for an
//!    escaping syntax an operator would have to learn (and get wrong).
//! 2. **Then dot-split traversal.** `a.b.c` walks nested JSON objects. Only
//!    objects are traversed; a non-object mid-path resolves to nothing.
//!
//! A literal key therefore always shadows a traversal that would reach a
//! different value — the operator wrote the exact name the provider emits.

use std::collections::HashMap;

/// Resolve `path` against a token's extended claims.
///
/// Returns the exact top-level claim named `path` when one exists, otherwise
/// walks `path` split on `.` through nested JSON objects.
pub fn resolve_claim<'a>(
    extra: &'a HashMap<String, serde_json::Value>,
    path: &str,
) -> Option<&'a serde_json::Value> {
    if let Some(value) = extra.get(path) {
        return Some(value);
    }

    let mut segments = path.split('.');
    let mut current = extra.get(segments.next()?)?;
    for segment in segments {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

/// Interpret a claim value as a list of strings: either a JSON array of
/// strings or a single string. Anything else yields `None`.
///
/// Providers are inconsistent about this shape for the same logical claim —
/// one emits `"groups": "admin"`, the next `"groups": ["admin", "ops"]` — so
/// every consumer of a name-valued claim reads it through here rather than
/// accepting only one of the two.
pub fn string_list(value: &serde_json::Value) -> Option<Vec<String>> {
    match value {
        serde_json::Value::String(single) => Some(vec![single.clone()]),
        serde_json::Value::Array(items) => Some(
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extra(pairs: Vec<(&str, serde_json::Value)>) -> HashMap<String, serde_json::Value> {
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect()
    }

    #[test]
    fn top_level_key_resolves() {
        let claims = extra(vec![("email", serde_json::json!("alice@example.com"))]);
        assert_eq!(
            resolve_claim(&claims, "email").and_then(|v| v.as_str()),
            Some("alice@example.com")
        );
    }

    /// Keycloak's roles live one level down; a flat lookup never sees them.
    #[test]
    fn nested_path_is_traversed() {
        let claims = extra(vec![(
            "realm_access",
            serde_json::json!({ "roles": ["admin", "ops"] }),
        )]);
        assert_eq!(
            resolve_claim(&claims, "realm_access.roles"),
            Some(&serde_json::json!(["admin", "ops"]))
        );
    }

    #[test]
    fn deeply_nested_path_is_traversed() {
        let claims = extra(vec![("a", serde_json::json!({ "b": { "c": "deep" } }))]);
        assert_eq!(
            resolve_claim(&claims, "a.b.c").and_then(|v| v.as_str()),
            Some("deep")
        );
    }

    /// A provider that legitimately names a claim with a dot in it must keep
    /// working without an escaping syntax.
    #[test]
    fn literal_dotted_key_resolves_by_exact_match() {
        let claims = extra(vec![("custom:namespace.field", serde_json::json!("value"))]);
        assert_eq!(
            resolve_claim(&claims, "custom:namespace.field").and_then(|v| v.as_str()),
            Some("value")
        );
    }

    /// When both readings exist, the literal key wins — the operator wrote the
    /// exact name the provider emits.
    #[test]
    fn literal_key_takes_precedence_over_traversal() {
        let claims = extra(vec![
            ("a.b", serde_json::json!("literal")),
            ("a", serde_json::json!({ "b": "traversed" })),
        ]);
        assert_eq!(
            resolve_claim(&claims, "a.b").and_then(|v| v.as_str()),
            Some("literal")
        );
    }

    #[test]
    fn missing_and_non_object_paths_resolve_to_nothing() {
        let claims = extra(vec![("scalar", serde_json::json!("x"))]);
        assert!(resolve_claim(&claims, "absent").is_none());
        assert!(resolve_claim(&claims, "scalar.deeper").is_none());
        assert!(resolve_claim(&claims, "absent.deeper").is_none());
    }

    #[test]
    fn string_list_accepts_both_provider_shapes() {
        assert_eq!(
            string_list(&serde_json::json!("admin")),
            Some(vec!["admin".to_owned()])
        );
        assert_eq!(
            string_list(&serde_json::json!(["admin", "ops"])),
            Some(vec!["admin".to_owned(), "ops".to_owned()])
        );
        assert_eq!(string_list(&serde_json::json!(42)), None);
    }
}
