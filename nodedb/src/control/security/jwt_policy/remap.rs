// SPDX-License-Identifier: BUSL-1.1

//! Claim remapping: `[auth.jwt.claims]` renames a provider's claims onto the
//! fields NodeDB actually reads out of a verified token.
//!
//! Applied by the JWKS registry immediately after signature, issuer, audience,
//! and time validation, so every bearer route — HTTP static providers and
//! native/OIDC catalog providers alike — sees the same remapped claim set.
//!
//! A mapping entry is `"<provider claim name>" = "<NodeDB field>"`. The source
//! claim is left in place after the copy, so catalog claim-mapping rules that
//! reference a provider's own claim names keep matching.
//!
//! Source names are resolved by [`resolve_claim`], so a nested provider claim
//! (`"realm_access.roles"`) is reachable and a claim name that literally
//! contains a dot still resolves by exact match.

use std::collections::HashMap;

use crate::control::security::jwt::JwtClaims;
use crate::control::security::jwt_policy::{resolve_claim, string_list};

/// The fields a remap may target: everything NodeDB reads out of a verified
/// token when building session context, plus the `roles` claim consumed when
/// binding an identity.
///
/// Closed on purpose — a target outside this set would name a field no reader
/// consults, making the mapping silently inert. Config validation rejects it.
pub const REMAPPABLE_FIELDS: [&str; 9] = [
    "email",
    "groups",
    "metadata",
    "org_id",
    "org_ids",
    "permissions",
    "roles",
    "scope_expires",
    "status",
];

/// Validate a `[auth.jwt.claims]` table. Called from `JwtAuthConfig::validate`
/// so a mapping that could never take effect fails startup instead of leaving
/// the operator with a knob that does nothing.
///
/// Rejects targets outside [`REMAPPABLE_FIELDS`] and two sources competing for
/// one target (whose winner would depend on hash iteration order).
pub fn validate_claim_remap(map: &HashMap<String, String>) -> crate::Result<()> {
    let mut seen_targets: Vec<&str> = Vec::with_capacity(map.len());
    let mut sources: Vec<&String> = map.keys().collect();
    sources.sort();

    for source in sources {
        let target = &map[source];
        if !REMAPPABLE_FIELDS.contains(&target.as_str()) {
            return Err(crate::Error::Config {
                detail: format!(
                    "auth.jwt.claims maps '{source}' to unknown field '{target}'; \
                     NodeDB reads only {}",
                    REMAPPABLE_FIELDS.join(", ")
                ),
            });
        }
        if source.is_empty() {
            return Err(crate::Error::Config {
                detail: "auth.jwt.claims has an empty source claim name".into(),
            });
        }
        if seen_targets.contains(&target.as_str()) {
            return Err(crate::Error::Config {
                detail: format!(
                    "auth.jwt.claims maps more than one claim onto '{target}'; \
                     each NodeDB field may be the target of at most one claim"
                ),
            });
        }
        seen_targets.push(target.as_str());
    }
    Ok(())
}

/// Copy each mapped source claim onto the NodeDB field it names.
///
/// Sources absent from the token are skipped. Sources are processed in sorted
/// order so the result never depends on hash iteration order; config validation
/// already forbids two sources sharing one target.
pub fn remap_claims(map: &HashMap<String, String>, claims: &mut JwtClaims) {
    if map.is_empty() {
        return;
    }
    let mut sources: Vec<&String> = map.keys().collect();
    sources.sort();

    for source in sources {
        let target = &map[source];
        if source == target {
            continue;
        }
        let Some(value) = resolve_claim(&claims.extra, source).cloned() else {
            continue;
        };
        if target == "roles" {
            if let Some(roles) = string_list(&value) {
                claims.roles = roles;
            }
            continue;
        }
        claims.extra.insert(target.clone(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims_with(extra: Vec<(&str, serde_json::Value)>) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: Vec::new(),
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1,
            iss: "https://idp.example.com".into(),
            aud: vec!["nodedb".into()],
            user_id: 7,
            is_superuser: false,
            extra: extra
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        }
    }

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(source, target)| ((*source).to_owned(), (*target).to_owned()))
            .collect()
    }

    /// The whole point of the knob: a provider that calls the address
    /// `upn` must land in the `email` field NodeDB reads.
    #[test]
    fn provider_named_claim_lands_in_the_field_nodedb_reads() {
        let mut claims = claims_with(vec![("upn", serde_json::json!("alice@example.com"))]);
        remap_claims(&map(&[("upn", "email")]), &mut claims);

        assert_eq!(
            claims.extra.get("email").and_then(|v| v.as_str()),
            Some("alice@example.com")
        );
        // The source claim survives so catalog claim-mapping rules written
        // against the provider's own names keep matching.
        assert!(claims.extra.contains_key("upn"));
    }

    #[test]
    fn remap_onto_roles_replaces_the_typed_role_list() {
        let mut claims = claims_with(vec![("groups", serde_json::json!(["readwrite", "reader"]))]);
        remap_claims(&map(&[("groups", "roles")]), &mut claims);

        assert_eq!(
            claims.roles,
            vec!["readwrite".to_owned(), "reader".to_owned()]
        );
    }

    #[test]
    fn absent_source_claim_changes_nothing() {
        let mut claims = claims_with(vec![("org", serde_json::json!("acme"))]);
        remap_claims(&map(&[("upn", "email")]), &mut claims);

        assert!(!claims.extra.contains_key("email"));
    }

    /// Keycloak nests roles under `realm_access`, so a dotted source path must
    /// reach them; a flat lookup would silently map nothing.
    #[test]
    fn dotted_source_path_resolves_nested_claim() {
        let mut claims = claims_with(vec![(
            "realm_access",
            serde_json::json!({ "roles": ["admin"] }),
        )]);
        remap_claims(&map(&[("realm_access.roles", "roles")]), &mut claims);

        assert_eq!(claims.roles, vec!["admin".to_owned()]);
    }

    /// A claim name that legally contains a dot resolves by exact match and
    /// shadows the traversal reading of the same string.
    #[test]
    fn literal_dotted_claim_name_wins_over_traversal() {
        let mut claims = claims_with(vec![
            ("realm_access.roles", serde_json::json!(["literal"])),
            (
                "realm_access",
                serde_json::json!({ "roles": ["traversed"] }),
            ),
        ]);
        remap_claims(&map(&[("realm_access.roles", "roles")]), &mut claims);

        assert_eq!(claims.roles, vec!["literal".to_owned()]);
    }

    #[test]
    fn unknown_target_field_is_rejected_by_validation() {
        let err = validate_claim_remap(&map(&[("upn", "e_mail")]))
            .expect_err("a target no reader consults must fail startup");
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn two_sources_for_one_target_are_rejected_by_validation() {
        let err = validate_claim_remap(&map(&[("upn", "email"), ("mail", "email")]))
            .expect_err("an order-dependent winner must fail startup");
        assert!(err.to_string().contains("more than one claim"));
    }

    #[test]
    fn valid_mapping_passes_validation() {
        assert!(validate_claim_remap(&map(&[("upn", "email"), ("grp", "groups")])).is_ok());
    }
}
