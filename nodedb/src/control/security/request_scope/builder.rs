// SPDX-License-Identifier: BUSL-1.1

//! [`RequestAuthScopeBuilder`] — infallible construction of a
//! [`RequestAuthScope`](super::RequestAuthScope).

use nodedb_types::DatabaseId;

use crate::control::security::auth_context::{AuthContext, generate_session_id};
use crate::control::security::deny::DenyMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwks::registry::VerifiedJwtClaims;
use crate::control::security::scope::enrichment::enrich_auth_context_with_scopes;
use crate::control::security::scope::grant::ScopeGrantStore;

use super::resolved::RequestAuthScope;

/// Builder for [`RequestAuthScope`].
///
/// `scope_grants` is a required constructor argument, not an optional
/// builder method. Making enrichment opt-in would let a transport silently
/// skip [`enrich_auth_context_with_scopes`]: without it, `$auth.metadata`
/// never carries `scope_status.<name>`, so `$auth.scope_status()` resolves
/// to `None` in RLS predicates — which is indistinguishable from "this user
/// has no such scope" and fails closed (denies access) rather than erroring
/// loudly. Requiring the store at construction makes that skip impossible
/// to write by accident.
pub struct RequestAuthScopeBuilder<'a> {
    identity: &'a AuthenticatedIdentity,
    scope_grants: &'a ScopeGrantStore,
    session_database: Option<DatabaseId>,
    on_deny: Option<DenyMode>,
    verified_jwt: Option<&'a VerifiedJwtClaims>,
    session_id: Option<String>,
}

impl<'a> RequestAuthScopeBuilder<'a> {
    /// Only [`RequestAuthScope::builder`](super::RequestAuthScope::builder)
    /// constructs a builder — callers reach this type through that entry
    /// point so the required `scope_grants` argument can never be omitted.
    pub(super) fn new(
        identity: &'a AuthenticatedIdentity,
        scope_grants: &'a ScopeGrantStore,
    ) -> Self {
        Self {
            identity,
            scope_grants,
            session_database: None,
            on_deny: None,
            verified_jwt: None,
            session_id: None,
        }
    }

    /// The session's currently active database (from `USE DATABASE` or a
    /// prior session bind). Takes precedence over `identity.default_database`
    /// when resolving the scope's database.
    pub fn with_session_database(mut self, db: Option<DatabaseId>) -> Self {
        self.session_database = db;
        self
    }

    /// A denial-behavior override (e.g. from `SET LOCAL nodedb.on_deny` or a
    /// per-query `ON DENY` clause).
    pub fn with_on_deny(mut self, mode: Option<DenyMode>) -> Self {
        self.on_deny = mode;
        self
    }

    /// An already-verified JWT to build the `AuthContext` from. When absent,
    /// the context is built from `identity` alone via
    /// [`AuthContext::from_identity`].
    pub fn with_verified_jwt(mut self, claims: &'a VerifiedJwtClaims) -> Self {
        self.verified_jwt = Some(claims);
        self
    }

    /// Same as [`Self::with_verified_jwt`], but accepts the `Option` a caller
    /// typically has in hand (e.g. from
    /// [`resolve_auth_parts`](crate::control::server::http::auth::resolve_auth_parts))
    /// instead of requiring an `if let Some(..) = ..` reassignment at every
    /// call site.
    pub fn with_optional_verified_jwt(mut self, claims: Option<&'a VerifiedJwtClaims>) -> Self {
        self.verified_jwt = claims;
        self
    }

    /// The opaque session identifier to stamp on the resulting context.
    /// Defaults to a freshly generated one via `generate_session_id()`.
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// Resolve the scope. Infallible: database precedence resolution cannot
    /// fail, and JWT verification already happened upstream of this builder
    /// (see [`Self::with_verified_jwt`]).
    ///
    /// Order of operations:
    /// 1. Resolve the database once: session database -> identity's default
    ///    database -> [`DatabaseId::DEFAULT`].
    /// 2. Build the `AuthContext` from the verified JWT if supplied, else
    ///    from `identity` alone.
    /// 3. Stamp `auth.database_id` with the resolved database.
    /// 4. Apply the `on_deny` override, if any.
    /// 5. Enrich `auth` with scope-grant status via
    ///    [`enrich_auth_context_with_scopes`] — the entire reason
    ///    `scope_grants` is a required argument.
    pub fn build(self) -> RequestAuthScope<'a> {
        let resolved_db = self
            .session_database
            .or(self.identity.default_database)
            .unwrap_or(DatabaseId::DEFAULT);

        let session_id = self.session_id.unwrap_or_else(generate_session_id);

        let mut auth = match self.verified_jwt {
            Some(claims) => AuthContext::from_verified_jwt(claims, self.identity, session_id),
            None => AuthContext::from_identity(self.identity, session_id),
        };

        auth.database_id = Some(resolved_db);

        if let Some(mode) = self.on_deny {
            auth.on_deny_override = Some(mode);
        }

        // `enrich_auth_context_with_scopes` reads `org_ids` while also
        // taking `auth` mutably; clone the (small) org list up front rather
        // than restructuring the helper's signature.
        let org_ids = auth.org_ids.clone();
        enrich_auth_context_with_scopes(&mut auth, self.scope_grants, &org_ids);

        RequestAuthScope::new(self.identity, auth, resolved_db)
    }
}

#[cfg(test)]
mod tests {
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::control::security::jwt::JwtClaims;
    use crate::control::security::scope::grant::ScopeGrantParams;
    use crate::types::TenantId;
    use std::collections::HashMap;

    use super::*;

    fn test_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    fn test_claims(is_superuser: bool) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec!["superuser".into(), "readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 0,
            iss: "nodedb-auth".into(),
            aud: "nodedb".into(),
            user_id: 42,
            is_superuser,
            extra: HashMap::new(),
        }
    }

    #[test]
    fn session_database_wins_over_identity_default() {
        let mut identity = test_identity();
        identity.default_database = Some(DatabaseId::new(7));
        let grants = ScopeGrantStore::new();

        let scope = RequestAuthScope::builder(&identity, &grants)
            .with_session_database(Some(DatabaseId::new(99)))
            .build();

        assert_eq!(scope.database_id(), DatabaseId::new(99));
    }

    #[test]
    fn identity_default_used_when_no_session_database() {
        let mut identity = test_identity();
        identity.default_database = Some(DatabaseId::new(7));
        let grants = ScopeGrantStore::new();

        let scope = RequestAuthScope::builder(&identity, &grants).build();

        assert_eq!(scope.database_id(), DatabaseId::new(7));
    }

    #[test]
    fn falls_back_to_database_default_when_neither_present() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();

        let scope = RequestAuthScope::builder(&identity, &grants).build();

        assert_eq!(scope.database_id(), DatabaseId::DEFAULT);
    }

    #[test]
    fn auth_and_scope_database_id_always_agree_after_build() {
        let mut identity = test_identity();
        identity.default_database = Some(DatabaseId::new(3));
        let grants = ScopeGrantStore::new();

        let scope = RequestAuthScope::builder(&identity, &grants).build();

        assert_eq!(scope.auth().database_id, Some(scope.database_id()));
    }

    #[test]
    fn rebind_database_restamps_both_fields_together() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();

        let scope = RequestAuthScope::builder(&identity, &grants)
            .build()
            .rebind_database(DatabaseId::new(55));

        assert_eq!(scope.database_id(), DatabaseId::new(55));
        assert_eq!(scope.auth().database_id, Some(DatabaseId::new(55)));
    }

    #[test]
    fn scope_enrichment_runs_during_build() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        grants
            .grant(ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "user",
                grantee_id: "42",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
            })
            .unwrap();

        let scope = RequestAuthScope::builder(&identity, &grants).build();

        assert_eq!(
            scope.auth().metadata.get("scope_status.pro:all"),
            Some(&"active".to_string())
        );
    }

    #[test]
    fn superuser_authority_is_not_forgeable_via_verified_jwt() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let claims = test_claims(true);
        let verified = VerifiedJwtClaims::new_for_test(claims);

        let scope = RequestAuthScope::builder(&identity, &grants)
            .with_verified_jwt(&verified)
            .build();

        assert!(!scope.auth().is_superuser());
        assert_eq!(scope.auth().roles, vec!["readwrite"]);
    }
}
