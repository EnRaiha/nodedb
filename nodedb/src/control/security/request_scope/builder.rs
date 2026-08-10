// SPDX-License-Identifier: BUSL-1.1

//! [`RequestAuthScopeBuilder`] — infallible construction of a
//! [`RequestAuthScope`](super::RequestAuthScope).

use nodedb_types::DatabaseId;

use crate::control::security::auth_context::{AuthContext, generate_session_id};
use crate::control::security::deny::DenyMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwks::registry::VerifiedJwtClaims;
use crate::control::security::risk::client_ip_from_peer;
use crate::control::security::scope::enrichment::enrich_auth_context_with_scopes;

use super::client_scope::ClientRequestScope;
use super::resolved::RequestAuthScope;
use super::stores::AuthStores;

/// Builder for [`RequestAuthScope`].
///
/// `stores` is a required constructor argument, not an optional builder
/// method. Making enrichment opt-in would let a transport silently skip
/// [`enrich_auth_context_with_scopes`]: without it, `$auth.metadata` never
/// carries `scope_status.<name>` / `quota_remaining.<name>` /
/// `quota_pct.<name>`, so `$auth.scope_status()` / `$auth.quota_remaining()`
/// / `$auth.quota_pct()` resolve to `None` in RLS predicates — which is
/// indistinguishable from "this user has no such scope/quota" and fails
/// closed (denies access) rather than erroring loudly. Requiring the stores
/// at construction makes that skip impossible to write by accident.
pub struct RequestAuthScopeBuilder<'a> {
    identity: &'a AuthenticatedIdentity,
    stores: AuthStores<'a>,
    session_database: Option<DatabaseId>,
    on_deny: Option<DenyMode>,
    verified_jwt: Option<&'a VerifiedJwtClaims>,
    session_id: Option<String>,
    adopted_auth_context: Option<AuthContext>,
    client_ip: Option<String>,
}

impl<'a> RequestAuthScopeBuilder<'a> {
    /// Only [`RequestAuthScope::builder`](super::RequestAuthScope::builder)
    /// constructs a builder — callers reach this type through that entry
    /// point so the required `stores` argument can never be omitted.
    pub(super) fn new(identity: &'a AuthenticatedIdentity, stores: AuthStores<'a>) -> Self {
        Self {
            identity,
            stores,
            session_database: None,
            on_deny: None,
            verified_jwt: None,
            session_id: None,
            adopted_auth_context: None,
            client_ip: None,
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

    /// Adopt an already-built `AuthContext` instead of constructing one from
    /// `identity` (or a verified JWT) inside [`Self::build`].
    ///
    /// The one sanctioned caller is pgwire's pooled opaque session-handle
    /// path (`SET LOCAL nodedb.auth_session = '<handle>'`):
    /// `SessionHandleStore::resolve` hands back a whole cached `AuthContext`
    /// from a prior authentication, not the raw identity/JWT ingredients
    /// [`Self::build`] normally starts from. Discarding that cached context
    /// and rebuilding one from `identity` would silently drop whatever the
    /// pooled request's context carried (email, org membership, groups,
    /// metadata, ...) and answer `$auth.*` for the wrong principal.
    ///
    /// Note the scope's `identity` remains the *connection's* authenticated
    /// identity while the adopted context describes the *pooled request's*
    /// principal. That split is deliberate: the connection identity governs
    /// RBAC and tenant scoping, the adopted context governs `$auth.*`
    /// substitution and audit attribution.
    ///
    /// Adopting it keeps that identity intact while still routing it through
    /// the exact same [`Self::build`] tail every other construction path
    /// gets: database stamping (so `database_id` and `auth.database_id`
    /// still cannot drift apart, even on this path) and scope-grant
    /// enrichment (so a pooled session's `$auth.scope_status(...)` resolves
    /// instead of fail-closed-denying, which it did before this existed —
    /// a cached context is enriched only at the moment it is created, never
    /// again while pooled).
    pub fn with_adopted_auth_context(mut self, ctx: AuthContext) -> Self {
        self.adopted_auth_context = Some(ctx);
        self
    }

    /// Resolve the scope. Infallible: database precedence resolution cannot
    /// fail, and JWT verification already happened upstream of this builder
    /// (see [`Self::with_verified_jwt`]).
    ///
    /// Order of operations:
    /// 1. Resolve the database once: session database -> identity's default
    ///    database -> [`DatabaseId::DEFAULT`].
    /// 2. Take the adopted `AuthContext` if [`Self::with_adopted_auth_context`]
    ///    supplied one; else build it from the verified JWT if supplied, else
    ///    from `identity` alone.
    /// 3. Stamp `auth.database_id` with the resolved database.
    /// 4. Apply the `on_deny` override, if any.
    /// 5. Enrich `auth` with scope-grant and quota status via
    ///    [`enrich_auth_context_with_scopes`] — the entire reason `stores` is
    ///    a required argument. This step runs even for an adopted context,
    ///    since a pooled `AuthContext` was enriched only once, at
    ///    handle-creation time, and may be stale. It is also where a
    ///    conditional grant's `WHEN` / `REQUIRE` clauses are evaluated: this
    ///    builder is the single place a grant meets the request it might
    ///    apply to, holding both the `AuthContext` and the client address
    ///    those conditions need, so a grant whose conditions fail is dropped
    ///    here and contributes no scope.
    /// 6. Stamp `auth.risk_score` when risk scoring is enabled, the identity
    ///    is not an internal service, and [`Self::build_for_client`] supplied
    ///    a usable client address. Enforcement of the resulting decision lives
    ///    at the request-admission gate, not here — `build` stays infallible.
    pub fn build(self) -> RequestAuthScope<'a> {
        let resolved_db = self
            .session_database
            .or(self.identity.default_database)
            .unwrap_or(DatabaseId::DEFAULT);

        let mut auth = match self.adopted_auth_context {
            Some(adopted) => adopted,
            None => {
                let session_id = self.session_id.unwrap_or_else(generate_session_id);
                match self.verified_jwt {
                    Some(claims) => {
                        AuthContext::from_verified_jwt(claims, self.identity, session_id)
                    }
                    None => AuthContext::from_identity(self.identity, session_id),
                }
            }
        };

        auth.database_id = Some(resolved_db);

        if let Some(mode) = self.on_deny {
            auth.on_deny_override = Some(mode);
        }

        // `enrich_auth_context_with_scopes` reads `org_ids` while also
        // taking `auth` mutably; clone the (small) org list up front rather
        // than restructuring the helper's signature.
        let org_ids = auth.org_ids.clone();
        enrich_auth_context_with_scopes(
            &mut auth,
            self.stores.scope_grants,
            self.stores.quota_manager,
            &org_ids,
            self.client_ip.as_deref(),
            crate::control::security::time::now_secs(),
        );

        // Risk scoring. Internal-service identities (triggers, Raft apply,
        // CRDT sync, scheduler, replay) are exempt here for the same reason
        // the request-admission gate exempts them from the blacklist and
        // rate-limit guards: server-owned work must never be scored or
        // refused, and scoring it would also pollute the known-IP cache with
        // loopback traffic.
        if self.stores.risk_scorer.is_enabled()
            && !self.identity.is_internal_service()
            && let Some(client_ip) = self.client_ip.as_deref()
        {
            let (score, _decision, _signals) =
                self.stores.risk_scorer.score(&auth.id, client_ip, &auth);
            auth.risk_score = Some(score);
        }

        RequestAuthScope::new(self.identity, auth, resolved_db)
    }

    /// Resolve the scope against the transport's real peer address and keep
    /// the two bound together as a [`ClientRequestScope`].
    ///
    /// This is the only way a client address enters a scope, and the only way
    /// to produce the value the request-admission gate accepts. Both facts are
    /// deliberate: before this existed, the address was an optional builder
    /// method *and* a separate argument to the gate, so a transport could pass
    /// the gate a real address while its scope carried none — leaving
    /// `$auth.risk_score` unstamped (which the gate refuses as unassessed once
    /// risk scoring is enabled) and every `REQUIRE IP` grant silently
    /// withheld, with nothing at the call site to show for it.
    ///
    /// `peer_addr` must be the genuine remote address in whatever shape its
    /// socket layer produced (`10.1.2.3:5432`, `[::1]:5432`, or a bare
    /// address) — never a placeholder and never a transport label. Anything
    /// that does not parse as an address is discarded by
    /// [`client_ip_from_peer`], leaving the scope unassessed rather than
    /// mis-scoring every request behind that transport as if the placeholder
    /// were a real client.
    ///
    /// A scope that is never presented to an admission door — row-level
    /// security and redaction resolution inside the SQL execution tree — uses
    /// [`Self::build`] instead.
    pub fn build_for_client<'p>(mut self, peer_addr: &'p str) -> ClientRequestScope<'a, 'p> {
        self.client_ip = client_ip_from_peer(peer_addr);
        ClientRequestScope::new(self.build(), peer_addr)
    }
}

#[cfg(test)]
mod tests {
    use crate::control::security::conditional::GrantCondition;
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::control::security::jwt::JwtClaims;
    use crate::control::security::metering::quota::QuotaManager;
    use crate::control::security::risk::{RiskConfig, RiskScorer, STEP_UP_REQUIRED};
    use crate::control::security::scope::grant::{ScopeGrantParams, ScopeGrantStore};
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
            aud: vec!["nodedb".into()],
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
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores)
            .with_session_database(Some(DatabaseId::new(99)))
            .build();

        assert_eq!(scope.database_id(), DatabaseId::new(99));
    }

    #[test]
    fn identity_default_used_when_no_session_database() {
        let mut identity = test_identity();
        identity.default_database = Some(DatabaseId::new(7));
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores).build();

        assert_eq!(scope.database_id(), DatabaseId::new(7));
    }

    #[test]
    fn falls_back_to_database_default_when_neither_present() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores).build();

        assert_eq!(scope.database_id(), DatabaseId::DEFAULT);
    }

    #[test]
    fn auth_and_scope_database_id_always_agree_after_build() {
        let mut identity = test_identity();
        identity.default_database = Some(DatabaseId::new(3));
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores).build();

        assert_eq!(scope.auth().database_id, Some(scope.database_id()));
    }

    #[test]
    fn rebind_database_restamps_both_fields_together() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores)
            .build()
            .rebind_database(DatabaseId::new(55));

        assert_eq!(scope.database_id(), DatabaseId::new(55));
        assert_eq!(scope.auth().database_id, Some(DatabaseId::new(55)));
    }

    #[test]
    fn scope_enrichment_runs_during_build() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);
        grants
            .grant(ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "user",
                grantee_id: "42",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions: Vec::new(),
            })
            .unwrap();

        let scope = RequestAuthScope::builder(&identity, stores).build();

        assert_eq!(
            scope.auth().metadata.get("scope_status.pro:all"),
            Some(&nodedb_types::Value::String("active".into()))
        );
    }

    #[test]
    fn quota_metadata_present_after_build_for_held_scope_with_quota() {
        use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};

        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        grants
            .grant(ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "user",
                grantee_id: "42",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions: Vec::new(),
            })
            .unwrap();
        quotas
            .define_quota(QuotaDefinition {
                scope_name: "pro:all".into(),
                max_tokens: 100,
                period_secs: 86400,
                enforcement: QuotaEnforcement::Hard,
                warning_threshold: 0.8,
            })
            .expect("define quota in test");
        // `build()` reads quota state on the wall clock, and a quota period
        // rolls over lazily on access — so seed the usage on that same clock,
        // or the read lands past the period end and reports a fresh allowance.
        quotas.record_usage(
            "pro:all",
            "42",
            10,
            crate::control::security::time::now_secs(),
        );
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores).build();

        assert_eq!(
            scope.auth().metadata.get("quota_remaining.pro:all"),
            Some(&nodedb_types::Value::Integer(90))
        );
        assert_eq!(
            scope.auth().metadata.get("quota_pct.pro:all"),
            Some(&nodedb_types::Value::Float(0.1))
        );
    }

    #[test]
    fn superuser_authority_is_not_forgeable_via_verified_jwt() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);
        let claims = test_claims(true);
        let verified = VerifiedJwtClaims::new_for_test(claims);

        let scope = RequestAuthScope::builder(&identity, stores)
            .with_verified_jwt(&verified)
            .build();

        assert!(!scope.auth().is_superuser());
        assert_eq!(scope.auth().roles, vec!["readwrite"]);
    }

    #[test]
    fn adopted_auth_context_is_restamped_with_scope_database() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);
        // Simulate a pooled session's cached `AuthContext`, resolved for a
        // different database than the one this request is scoped to.
        let mut pooled = AuthContext::from_identity(&identity, "s_pooled".into());
        pooled.database_id = Some(DatabaseId::new(1));
        pooled.email = Some("pooled@example.com".into());

        let scope = RequestAuthScope::builder(&identity, stores)
            .with_session_database(Some(DatabaseId::new(2)))
            .with_adopted_auth_context(pooled)
            .build();

        // The adopted identity detail survives...
        assert_eq!(scope.auth().email, Some("pooled@example.com".into()));
        // ...but database_id is re-stamped to this request's scope, in
        // lockstep on both fields, not the value baked into the pooled
        // context at handle-creation time.
        assert_eq!(scope.database_id(), DatabaseId::new(2));
        assert_eq!(scope.auth().database_id, Some(DatabaseId::new(2)));
    }

    #[test]
    fn adopted_auth_context_still_runs_scope_enrichment() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);
        grants
            .grant(ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "user",
                grantee_id: "42",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions: Vec::new(),
            })
            .unwrap();
        let pooled = AuthContext::from_identity(&identity, "s_pooled".into());

        let scope = RequestAuthScope::builder(&identity, stores)
            .with_adopted_auth_context(pooled)
            .build();

        assert_eq!(
            scope.auth().metadata.get("scope_status.pro:all"),
            Some(&nodedb_types::Value::String("active".into()))
        );
    }

    // ── Conditional grants ──────────────────────────────────────────────

    fn conditional_grant(grants: &ScopeGrantStore, conditions: Vec<GrantCondition>) {
        grants
            .grant(ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "user",
                grantee_id: "42",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions,
            })
            .expect("grant");
    }

    /// The hook: an IP-restricted grant reaches `build` through the same
    /// peer address risk scoring uses, and applies only from that network.
    #[test]
    fn ip_restricted_grant_applies_only_from_its_network() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        conditional_grant(
            &grants,
            vec![GrantCondition::RequireIp {
                allowed_cidrs: vec!["10.0.0.0/8".into()],
            }],
        );
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let inside = RequestAuthScope::builder(&identity, stores)
            .build_for_client("10.0.0.1:5432")
            .into_scope();
        assert_eq!(
            inside.auth().metadata.get("scope_status.pro:all"),
            Some(&nodedb_types::Value::String("active".into()))
        );

        let outside = RequestAuthScope::builder(&identity, stores)
            .build_for_client("203.0.113.9:5432")
            .into_scope();
        assert!(
            !outside.auth().metadata.contains_key("scope_status.pro:all"),
            "a request from outside the permitted network must not get the scope"
        );
    }

    /// Fail closed: a transport that supplied no usable peer address cannot
    /// satisfy `REQUIRE IP`, so the grant is withheld rather than applied.
    #[test]
    fn ip_restricted_grant_is_withheld_without_a_peer_address() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        conditional_grant(
            &grants,
            vec![GrantCondition::RequireIp {
                allowed_cidrs: vec!["10.0.0.0/8".into()],
            }],
        );
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores).build();

        assert!(!scope.auth().metadata.contains_key("scope_status.pro:all"));
        assert_eq!(
            scope.auth().metadata.get("scope_denied.pro:all"),
            Some(&nodedb_types::Value::String(
                "client address unavailable for an IP-restricted grant".into()
            ))
        );
    }

    /// An MFA-conditioned grant is withheld with the same reason string the
    /// risk gate uses for its step-up band.
    #[test]
    fn mfa_conditioned_grant_reports_the_risk_step_up_reason() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        conditional_grant(&grants, vec![GrantCondition::RequireMfa]);
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores).build();

        assert_eq!(
            scope.auth().metadata.get("scope_denied.pro:all"),
            Some(&nodedb_types::Value::String(STEP_UP_REQUIRED.to_string()))
        );
    }

    // ── Risk scoring ────────────────────────────────────────────────────

    fn enabled_scorer(allow: f64, deny: f64) -> RiskScorer {
        RiskScorer::new(RiskConfig {
            enabled: true,
            allow_threshold: allow,
            deny_threshold: deny,
            ..Default::default()
        })
    }

    #[test]
    fn risk_score_is_not_stamped_when_scoring_is_disabled() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = RiskScorer::default();
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores)
            .build_for_client("10.0.0.1:5432")
            .into_scope();

        assert_eq!(scope.auth().risk_score, None);
    }

    #[test]
    fn risk_score_is_stamped_when_enabled_with_a_real_peer_address() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = enabled_scorer(0.3, 0.7);
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores)
            .build_for_client("10.0.0.1:5432")
            .into_scope();

        let score = scope
            .auth()
            .risk_score
            .expect("an enabled scorer with a real peer address must stamp a score");
        assert!(score >= 0.0);
        assert_eq!(
            scope.auth().resolve_variable("risk_score"),
            Some(serde_json::json!(score)),
            "$auth.risk_score must resolve to the stamped score for RLS substitution"
        );
    }

    /// The configured thresholds must reach the scorer — a `RiskConfig`
    /// that is constructed but never read would leave every score in the
    /// allow band no matter what the operator set.
    #[test]
    fn configured_thresholds_reach_the_scorer() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        // Everything is denied: an allow band that ends below zero and a
        // deny band that starts at zero.
        let scorer = enabled_scorer(-1.0, 0.0);
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores)
            .build_for_client("10.0.0.1:5432")
            .into_scope();

        let refusal = scorer
            .refusal_for(scope.auth())
            .expect("configured deny threshold must refuse");
        assert_eq!(refusal.resource, "denied by risk policy");
    }

    /// A peer address that is not an address at all (the literal `"http"`
    /// the HTTP query routes pass today) must not be scored as if it were
    /// one — the scope stays unassessed and the gate fails closed.
    #[test]
    fn placeholder_peer_address_leaves_the_scope_unassessed() {
        let identity = test_identity();
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = enabled_scorer(0.3, 0.7);
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores)
            .build_for_client("http")
            .into_scope();

        assert_eq!(scope.auth().risk_score, None);
        assert!(scorer.refusal_for(scope.auth()).is_some());
    }

    #[test]
    fn internal_service_identity_is_never_scored() {
        let identity = AuthenticatedIdentity::new_internal_service(
            7,
            "internal-service",
            TenantId::new(1),
            vec![],
            false,
            None,
            DatabaseSet::All,
        );
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        let scorer = enabled_scorer(-1.0, 0.0);
        let stores = AuthStores::new(&grants, &quotas, &scorer);

        let scope = RequestAuthScope::builder(&identity, stores)
            .build_for_client("10.0.0.1:5432")
            .into_scope();

        assert_eq!(scope.auth().risk_score, None);
    }
}
