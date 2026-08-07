// SPDX-License-Identifier: BUSL-1.1

//! Scope declaration enforcement for verified JWTs.
//!
//! Authority over what a scope *grants* belongs to the scope-grant machinery,
//! which derives every effective scope from `ScopeGrantStore` and discards
//! claim-supplied scope state during enrichment. What nothing else covers is
//! the token *carrying* a scope name this server has never defined: RLS
//! predicates referencing `$auth.permissions` read that array verbatim, so an
//! undefined name reaches predicate evaluation unchallenged.
//!
//! `[auth.jwt] enforce_scopes = true` closes that: a token whose `permissions`
//! claim names a scope absent from `ScopeStore` is refused at authentication.

use crate::control::security::jwt::JwtClaims;
use crate::control::security::scope::store::ScopeStore;
use crate::types::TenantId;

/// Reject a verified token carrying a scope this server has not defined.
///
/// Inert when `enabled` is false. A token with no `permissions` claim, or one
/// that is not an array of strings, declares no scopes and passes.
pub fn enforce_declared_scopes(
    enabled: bool,
    scope_defs: &ScopeStore,
    claims: &JwtClaims,
    tenant_id: TenantId,
) -> crate::Result<()> {
    if !enabled {
        return Ok(());
    }
    let Some(permissions) = claims.extra.get("permissions").and_then(|v| v.as_array()) else {
        return Ok(());
    };

    for permission in permissions {
        let Some(scope) = permission.as_str() else {
            return Err(crate::Error::RejectedAuthz {
                tenant_id,
                resource: "JWT permissions claim contains a non-string scope".into(),
            });
        };
        if scope_defs.get(scope).is_none() {
            return Err(crate::Error::RejectedAuthz {
                tenant_id,
                resource: format!("JWT declares undefined scope '{scope}'"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims_with_permissions(permissions: serde_json::Value) -> JwtClaims {
        let mut extra = std::collections::HashMap::new();
        extra.insert("permissions".to_owned(), permissions);
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
            extra,
        }
    }

    fn store_with(scope: &str) -> ScopeStore {
        let store = ScopeStore::new();
        store
            .define(
                scope,
                vec![("read".into(), "user_profiles".into())],
                Vec::new(),
                "admin",
            )
            .expect("defining a scope in an in-memory store cannot fail");
        store
    }

    #[test]
    fn undefined_scope_is_rejected_when_enforcement_is_on() {
        let claims = claims_with_permissions(serde_json::json!(["profile:read", "orders:write"]));
        let err =
            enforce_declared_scopes(true, &store_with("profile:read"), &claims, TenantId::new(1))
                .expect_err("a scope the server never defined must be refused");
        assert!(err.to_string().contains("orders:write"));
    }

    #[test]
    fn declared_scopes_pass_when_all_are_defined() {
        let claims = claims_with_permissions(serde_json::json!(["profile:read"]));
        assert!(
            enforce_declared_scopes(true, &store_with("profile:read"), &claims, TenantId::new(1))
                .is_ok()
        );
    }

    #[test]
    fn enforcement_off_accepts_undefined_scopes() {
        let claims = claims_with_permissions(serde_json::json!(["orders:write"]));
        assert!(
            enforce_declared_scopes(
                false,
                &store_with("profile:read"),
                &claims,
                TenantId::new(1)
            )
            .is_ok()
        );
    }

    #[test]
    fn non_string_scope_entry_is_rejected() {
        let claims = claims_with_permissions(serde_json::json!([42]));
        assert!(
            enforce_declared_scopes(true, &store_with("profile:read"), &claims, TenantId::new(1))
                .is_err()
        );
    }
}
