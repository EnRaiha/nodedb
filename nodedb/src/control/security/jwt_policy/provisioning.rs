// SPDX-License-Identifier: BUSL-1.1

//! JIT provisioning of externally authenticated users from verified JWTs.
//!
//! `[auth.jwt] jit_provisioning` turns the machinery on: on every verified
//! bearer token the subject gets an `_system.auth_users` record, created on
//! first sight and touched afterwards. `jit_sync_claims` decides whether each
//! subsequent request also re-syncs the claim-derived fields (email, status,
//! roles, groups) onto that record, or whether the record keeps whatever it was
//! provisioned with.
//!
//! With `jit_provisioning = false` nothing here writes: the JWT path leaves the
//! auth-user store alone, which is what a deployment with pre-provisioned users
//! depends on. `jit_sync_claims` therefore only has an effect while
//! provisioning is enabled — it is the sync half of the same feature.
//!
//! The provisioned record's status gates the request: a user whose record is
//! deactivated or carries a blocking status is refused even though the token
//! itself verified.

use crate::config::auth::JwtAuthConfig;
use crate::control::security::auth_context::AuthStatus;
use crate::control::security::jit::auth_user::AuthUserStore;
use crate::control::security::jit::provisioner::{JitConfig, provision_from_jwt};
use crate::control::security::jwt::JwtClaims;
use crate::control::security::org::store::OrgStore;
use crate::types::TenantId;

/// Provision (or refresh) the auth-user record for a verified token and refuse
/// the request when the resulting account status blocks access.
///
/// The record's `provider` field is the token's verified issuer, so both bearer
/// routes attribute a user to the same provider string.
pub fn provision_and_check_status(
    auth_users: &AuthUserStore,
    orgs: Option<&OrgStore>,
    config: &JwtAuthConfig,
    claims: &JwtClaims,
    tenant_id: TenantId,
) -> crate::Result<()> {
    if !config.jit_provisioning {
        return Ok(());
    }

    let jit = JitConfig {
        enabled: true,
        sync_claims: config.jit_sync_claims,
    };
    let status = provision_from_jwt(auth_users, claims, &claims.iss, tenant_id, &jit, orgs)?;

    match status {
        AuthStatus::Active | AuthStatus::Restricted | AuthStatus::ReadOnly => Ok(()),
        AuthStatus::Suspended | AuthStatus::Banned => Err(crate::Error::RejectedAuthz {
            tenant_id,
            resource: format!("account is {status}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims_with_email(email: &str) -> JwtClaims {
        let mut extra = std::collections::HashMap::new();
        extra.insert("email".to_owned(), serde_json::json!(email));
        JwtClaims {
            sub: "alice".into(),
            // Deliberately not the tenant passed in below: the record must
            // carry the provider-bound tenant, never the token's assertion.
            tenant_id: 999,
            roles: vec!["readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1,
            iss: "https://idp.example.com".into(),
            aud: vec!["nodedb".into()],
            user_id: 42,
            is_superuser: false,
            extra,
        }
    }

    fn config(jit_provisioning: bool, jit_sync_claims: bool) -> JwtAuthConfig {
        JwtAuthConfig {
            jit_provisioning,
            jit_sync_claims,
            ..JwtAuthConfig::default()
        }
    }

    #[test]
    fn provisioning_disabled_leaves_the_store_untouched() {
        let store = AuthUserStore::new();
        provision_and_check_status(
            &store,
            None,
            &config(false, true),
            &claims_with_email("alice@example.com"),
            TenantId::new(1),
        )
        .expect("a verified token must not be refused when JIT is off");

        assert!(store.get("42").is_none());
    }

    #[test]
    fn provisioning_enabled_creates_the_record() {
        let store = AuthUserStore::new();
        provision_and_check_status(
            &store,
            None,
            &config(true, true),
            &claims_with_email("alice@example.com"),
            TenantId::new(1),
        )
        .expect("first sight of a verified subject must provision it");

        let user = store.get("42").expect("record provisioned");
        assert_eq!(user.email, "alice@example.com");
        assert_eq!(user.provider, "https://idp.example.com");
        assert_eq!(user.tenant_id, 1);
    }

    /// `jit_sync_claims = true` re-applies changed claims to the stored record.
    #[test]
    fn claim_sync_updates_the_record_when_enabled() {
        let store = AuthUserStore::new();
        let cfg = config(true, true);
        provision_and_check_status(
            &store,
            None,
            &cfg,
            &claims_with_email("alice@example.com"),
            TenantId::new(1),
        )
        .expect("provision");
        provision_and_check_status(
            &store,
            None,
            &cfg,
            &claims_with_email("alice@new.example.com"),
            TenantId::new(1),
        )
        .expect("second request");

        assert_eq!(
            store.get("42").expect("record").email,
            "alice@new.example.com"
        );
    }

    /// `jit_sync_claims = false` freezes the record at what it was provisioned
    /// with. This is the assertion that fails if the knob is not read.
    #[test]
    fn claim_sync_is_skipped_when_disabled() {
        let store = AuthUserStore::new();
        let cfg = config(true, false);
        provision_and_check_status(
            &store,
            None,
            &cfg,
            &claims_with_email("alice@example.com"),
            TenantId::new(1),
        )
        .expect("provision");
        provision_and_check_status(
            &store,
            None,
            &cfg,
            &claims_with_email("alice@new.example.com"),
            TenantId::new(1),
        )
        .expect("second request");

        assert_eq!(store.get("42").expect("record").email, "alice@example.com");
    }

    #[test]
    fn deactivated_account_is_refused() {
        let store = AuthUserStore::new();
        let cfg = config(true, true);
        provision_and_check_status(
            &store,
            None,
            &cfg,
            &claims_with_email("alice@example.com"),
            TenantId::new(1),
        )
        .expect("provision");
        store.deactivate("42").expect("deactivate");

        assert!(
            provision_and_check_status(
                &store,
                None,
                &cfg,
                &claims_with_email("alice@example.com"),
                TenantId::new(1),
            )
            .is_err()
        );
    }
}
