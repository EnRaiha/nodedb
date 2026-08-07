// SPDX-License-Identifier: BUSL-1.1

//! Pure claim-mapping logic: maps JWT claims to NodeDB databases and roles.
//!
//! No I/O, no state. Called by `verify_bearer_token` after the token has been
//! cryptographically verified.

use crate::control::security::catalog::oidc_providers::StoredClaimMappingRule;
use crate::control::security::jwt::JwtClaims;
use crate::control::security::jwt_policy::{resolve_claim, string_list};

/// Result of applying claim-mapping rules to a verified JWT.
#[derive(Debug, Clone, Default)]
pub struct ClaimMappingResult {
    /// Default database ID for the session. `None` = no claim-mapping override;
    /// the caller falls back to the user's stored default.
    pub default_database: Option<u64>,
    /// Database IDs the session may access, in addition to the default.
    pub accessible_databases: Vec<u64>,
    /// Role names granted by the matching rules.
    pub roles: Vec<String>,
}

/// Apply `rules` to `claims` and return the merged `ClaimMappingResult`.
///
/// Rules are evaluated in order. The **first** matching rule that sets a
/// `default_database` wins for that field. `add_databases` and `add_roles`
/// accumulate across all matching rules.
///
/// `claim_value = "*"` matches any non-empty claim value.
///
/// A claim may carry one string or an array of them (`cognito:groups`, `aud`),
/// and a rule matches when ANY element matches. Names are resolved through the
/// shared claim resolver, so nested paths (`realm_access.roles`) work here
/// exactly as they do in `[auth.jwt.claims]` remapping.
pub fn apply_claim_mapping(
    claims: &JwtClaims,
    rules: &[StoredClaimMappingRule],
) -> ClaimMappingResult {
    let mut result = ClaimMappingResult::default();

    for rule in rules {
        // Resolve the actual claim values from the JWT payload.
        let actual_values: Option<Vec<String>> = match rule.claim_name.as_str() {
            "sub" => Some(vec![claims.sub.clone()]),
            "iss" => Some(vec![claims.iss.clone()]),
            "aud" => Some(claims.aud.clone()),
            other => resolve_claim(&claims.extra, other).and_then(string_list),
        };

        let Some(values) = actual_values else {
            continue;
        };

        // Match: exact value or wildcard. Equality is per element and exact —
        // never a substring or prefix test, so a rule for one value cannot be
        // satisfied by an unrelated value that merely contains it.
        let matches = if rule.claim_value == "*" {
            values.iter().any(|value| !value.is_empty())
        } else {
            values.iter().any(|value| value == &rule.claim_value)
        };

        if !matches {
            continue;
        }

        // First matching rule that sets a default_database wins.
        if result.default_database.is_none()
            && let Some(db_id) = rule.default_database
        {
            result.default_database = Some(db_id);
        }

        // Accumulate accessible databases.
        for &db_id in &rule.add_databases {
            if !result.accessible_databases.contains(&db_id) {
                result.accessible_databases.push(db_id);
            }
        }

        // Accumulate roles.
        for role in &rule.add_roles {
            if !result.roles.contains(role) {
                result.roles.push(role.clone());
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::oidc_providers::StoredClaimMappingRule;
    use crate::control::security::jwt::JwtClaims;

    fn claims_with_org(org: &str) -> JwtClaims {
        claims_with_extra(&[("org_id", serde_json::Value::String(org.to_owned()))])
    }

    fn claims_with_extra(pairs: &[(&str, serde_json::Value)]) -> JwtClaims {
        let mut extra = std::collections::HashMap::new();
        for (key, value) in pairs {
            extra.insert((*key).to_owned(), value.clone());
        }
        JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec![],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 0,
            iss: "https://idp.example.com".into(),
            aud: vec!["nodedb".into()],
            user_id: 0,
            is_superuser: false,
            extra,
        }
    }

    #[test]
    fn exact_match_resolves_database_and_roles() {
        let rules = vec![StoredClaimMappingRule {
            claim_name: "org_id".into(),
            claim_value: "acme".into(),
            default_database: Some(42),
            add_databases: vec![43],
            add_roles: vec!["readwrite".into()],
        }];
        let res = apply_claim_mapping(&claims_with_org("acme"), &rules);
        assert_eq!(res.default_database, Some(42));
        assert_eq!(res.accessible_databases, vec![43]);
        assert_eq!(res.roles, vec!["readwrite"]);
    }

    #[test]
    fn unknown_value_no_match() {
        let rules = vec![StoredClaimMappingRule {
            claim_name: "org_id".into(),
            claim_value: "acme".into(),
            default_database: Some(42),
            add_databases: vec![],
            add_roles: vec![],
        }];
        let res = apply_claim_mapping(&claims_with_org("other"), &rules);
        assert!(res.default_database.is_none());
        assert!(res.roles.is_empty());
    }

    #[test]
    fn wildcard_matches_any_nonempty() {
        let rules = vec![StoredClaimMappingRule {
            claim_name: "org_id".into(),
            claim_value: "*".into(),
            default_database: Some(1),
            add_databases: vec![],
            add_roles: vec!["readonly".into()],
        }];
        let res = apply_claim_mapping(&claims_with_org("anything"), &rules);
        assert_eq!(res.default_database, Some(1));
        assert_eq!(res.roles, vec!["readonly"]);
    }

    #[test]
    fn wildcard_does_not_match_empty_value() {
        let rules = vec![StoredClaimMappingRule {
            claim_name: "org_id".into(),
            claim_value: "*".into(),
            default_database: Some(1),
            add_databases: vec![],
            add_roles: vec![],
        }];
        let res = apply_claim_mapping(&claims_with_org(""), &rules);
        assert!(res.default_database.is_none());
    }

    #[test]
    fn first_matching_rule_wins_default_db() {
        let rules = vec![
            StoredClaimMappingRule {
                claim_name: "org_id".into(),
                claim_value: "*".into(),
                default_database: Some(10),
                add_databases: vec![],
                add_roles: vec![],
            },
            StoredClaimMappingRule {
                claim_name: "org_id".into(),
                claim_value: "*".into(),
                default_database: Some(20),
                add_databases: vec![],
                add_roles: vec![],
            },
        ];
        let res = apply_claim_mapping(&claims_with_org("x"), &rules);
        // First rule wins for default_database.
        assert_eq!(res.default_database, Some(10));
    }

    /// Cognito emits group membership as an array. A rule naming one group
    /// must match when that group is anywhere in the list.
    #[test]
    fn array_valued_claim_matches_any_element() {
        let rules = vec![StoredClaimMappingRule {
            claim_name: "cognito:groups".into(),
            claim_value: "engineering".into(),
            default_database: Some(7),
            add_databases: vec![],
            add_roles: vec!["readwrite".into()],
        }];
        let claims = claims_with_extra(&[(
            "cognito:groups",
            serde_json::json!(["sales", "engineering"]),
        )]);

        let res = apply_claim_mapping(&claims, &rules);
        assert_eq!(res.default_database, Some(7));
        assert_eq!(res.roles, vec!["readwrite"]);
    }

    /// The match is exact per element: an unrelated value that merely contains
    /// the configured one must not satisfy the rule.
    #[test]
    fn array_valued_claim_does_not_match_on_substring() {
        let rules = vec![StoredClaimMappingRule {
            claim_name: "cognito:groups".into(),
            claim_value: "eng".into(),
            default_database: Some(7),
            add_databases: vec![],
            add_roles: vec![],
        }];
        let claims = claims_with_extra(&[("cognito:groups", serde_json::json!(["engineering"]))]);

        assert!(
            apply_claim_mapping(&claims, &rules)
                .default_database
                .is_none()
        );
    }

    /// Keycloak nests roles; the shared resolver makes them reachable from a
    /// catalog rule too, not only from `[auth.jwt.claims]` remapping.
    #[test]
    fn nested_claim_path_matches() {
        let rules = vec![StoredClaimMappingRule {
            claim_name: "realm_access.roles".into(),
            claim_value: "admin".into(),
            default_database: Some(3),
            add_databases: vec![],
            add_roles: vec![],
        }];
        let claims = claims_with_extra(&[(
            "realm_access",
            serde_json::json!({ "roles": ["admin", "ops"] }),
        )]);

        assert_eq!(
            apply_claim_mapping(&claims, &rules).default_database,
            Some(3)
        );
    }

    /// `aud` is a list; a rule on it matches any element.
    #[test]
    fn audience_rule_matches_any_element() {
        let rules = vec![StoredClaimMappingRule {
            claim_name: "aud".into(),
            claim_value: "nodedb".into(),
            default_database: Some(5),
            add_databases: vec![],
            add_roles: vec![],
        }];
        let mut claims = claims_with_org("acme");
        claims.aud = vec!["other".into(), "nodedb".into()];

        assert_eq!(
            apply_claim_mapping(&claims, &rules).default_database,
            Some(5)
        );
    }

    #[test]
    fn roles_accumulate_across_rules() {
        let rules = vec![
            StoredClaimMappingRule {
                claim_name: "org_id".into(),
                claim_value: "*".into(),
                default_database: Some(1),
                add_databases: vec![],
                add_roles: vec!["r1".into()],
            },
            StoredClaimMappingRule {
                claim_name: "sub".into(),
                claim_value: "alice".into(),
                default_database: None,
                add_databases: vec![],
                add_roles: vec!["r2".into()],
            },
        ];
        let res = apply_claim_mapping(&claims_with_org("y"), &rules);
        assert!(res.roles.contains(&"r1".to_owned()));
        assert!(res.roles.contains(&"r2".to_owned()));
    }
}
