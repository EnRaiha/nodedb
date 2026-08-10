// SPDX-License-Identifier: BUSL-1.1

//! Session-scoped authentication context derived from JWT claims or DB identity.
//!
//! `AuthContext` is the rich session context built after authentication. It
//! carries all claims needed for RLS predicate substitution, scope checks,
//! rate-limit tier resolution, and audit attribution.
//!
//! **Lifecycle**: Built once per session/request in the Control Plane. Passed to
//! the planner for `$auth.*` substitution. Never crosses the SPSC bridge — the
//! Data Plane receives only concrete, substituted `ScanFilter` values.

use std::collections::HashMap;

use nodedb_types::conversion::json_to_value;
use nodedb_types::{DatabaseId, Value};

use crate::types::TenantId;

use super::identity::{AuthMethod, AuthenticatedIdentity};
use super::jwks::registry::VerifiedJwtClaims;
use super::jwt::JwtClaims;

/// Account status for access-control decisions.
///
/// Determines what actions a user can perform. Checked after authentication
/// (valid JWT/password) but before authorization (RLS, scopes).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AuthStatus {
    /// Normal access — all granted permissions apply.
    #[default]
    Active,
    /// Temporarily blocked — all requests denied except status queries.
    Suspended,
    /// Permanently blocked — all requests denied. Requires admin intervention.
    Banned,
    /// Limited access — only permissions in the user's restriction set apply.
    Restricted,
    /// Can read but not write. Used for billing holds / grace periods.
    ReadOnly,
}

impl std::fmt::Display for AuthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Suspended => write!(f, "suspended"),
            Self::Banned => write!(f, "banned"),
            Self::Restricted => write!(f, "restricted"),
            Self::ReadOnly => write!(f, "read_only"),
        }
    }
}

impl std::str::FromStr for AuthStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "suspended" => Ok(Self::Suspended),
            "banned" => Ok(Self::Banned),
            "restricted" => Ok(Self::Restricted),
            "read_only" | "readonly" => Ok(Self::ReadOnly),
            other => Err(format!("unknown auth status: '{other}'")),
        }
    }
}

/// Rich session context built from JWT claims or DB user record.
///
/// All `$auth.*` references in RLS predicates resolve against this struct.
/// The planner substitutes `$auth.id` -> `"user_7291"` at plan time so the
/// Data Plane never parses JWT or resolves session variables.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// Unique user identifier (JWT `sub` or DB user_id).
    pub id: String,
    /// Display username.
    pub username: String,
    /// Email address (if available from claims or DB).
    pub email: Option<String>,
    /// Tenant this session belongs to.
    pub tenant_id: TenantId,
    /// Current organization context (from JWT `org_id` or session SET).
    pub org_id: Option<String>,
    /// All organization memberships (from JWT `org_ids` array claim).
    pub org_ids: Vec<String>,
    /// Server-issued superuser authority. This is not derived from role-name
    /// strings and cannot be mutated by claim enrichment.
    is_superuser: bool,
    /// Role names as strings (for RLS predicate matching).
    pub roles: Vec<String>,
    /// Group memberships (from JWT `groups` claim).
    pub groups: Vec<String>,
    /// Granted permissions/scopes (from JWT `permissions` or `scope` claim).
    pub permissions: Vec<String>,
    /// Account status.
    pub status: AuthStatus,
    /// Arbitrary key-value metadata from JWT or DB (plan tier, region, etc.).
    ///
    /// Typed rather than stringly: a provider that issues `"seats": 5` or
    /// `"beta": true` must have that survive as `Value::Integer` /
    /// `Value::Bool`, not get silently dropped (string-only) or forced back
    /// to text at read time, which would make numeric/boolean custom claims
    /// unusable in RLS predicates.
    pub metadata: HashMap<String, Value>,
    /// How the user authenticated.
    pub auth_method: AuthMethod,
    /// When the user last authenticated (Unix epoch seconds).
    /// Used for step-up auth: `$auth.auth_time > (now() - 15min)`.
    pub auth_time: Option<u64>,
    /// Opaque session identifier for audit correlation.
    pub session_id: String,
    /// Per-request ON DENY override: `None` = use policy default.
    /// Set via `SET LOCAL nodedb.on_deny = 'error'` (pgwire) or `X-On-Deny: error` (HTTP).
    pub on_deny_override: Option<super::deny::DenyMode>,
    /// The database this session is connected to.
    ///
    /// Populated from the session's active database at request time. Used by
    /// `$auth.database_id` RLS predicates for per-database row isolation.
    /// `None` when the session has no explicit database (cluster-level queries).
    pub database_id: Option<DatabaseId>,
    /// Adaptive-auth risk score for this request, as `$auth.risk_score`.
    ///
    /// Stamped by `RequestAuthScopeBuilder::build` when risk scoring is
    /// enabled and the transport supplied a usable client address. `None`
    /// means "not assessed" — never "zero risk"; predicates referencing
    /// `$auth.risk_score` then resolve to `None` and deny, and the
    /// request-admission gate refuses the request outright.
    pub risk_score: Option<f64>,
}

impl AuthContext {
    /// Build claim-enriched context after JWT verification and provider binding.
    fn from_verified_claims(
        claims: &JwtClaims,
        identity: &AuthenticatedIdentity,
        session_id: String,
    ) -> Self {
        // Extract extended claims from the `extra` map if present.
        let email = claims
            .extra
            .get("email")
            .and_then(|v| v.as_str())
            .map(String::from);
        let org_id = claims
            .extra
            .get("org_id")
            .and_then(|v| v.as_str())
            .map(String::from);
        let org_ids = extract_string_array(&claims.extra, "org_ids");
        let groups = extract_string_array(&claims.extra, "groups");
        let permissions = extract_string_array(&claims.extra, "permissions");

        let status = claims
            .extra
            .get("status")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<AuthStatus>().ok())
            .unwrap_or(AuthStatus::Active);

        // Every claim value is kept, whatever its JSON type. A claim that
        // silently vanished here (the old behavior for non-string values)
        // is indistinguishable from one the provider never sent, so an RLS
        // policy keyed on it would fail open with no diagnostic — a numeric
        // `seats` or boolean `beta` claim must reach `$auth.metadata.*`
        // typed, not be dropped or coerced to text.
        let mut metadata: HashMap<String, Value> = claims
            .extra
            .get("metadata")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), json_to_value(v.clone())))
                    .collect()
            })
            .unwrap_or_default();

        // Parse scope_expires claim: { "pro:all": 1735689600, "basic": 0 }
        if let Some(scope_expires) = claims
            .extra
            .get("scope_expires")
            .and_then(|v| v.as_object())
        {
            for (scope_name, ts) in scope_expires {
                if let Some(ts_val) = ts.as_u64() {
                    metadata.insert(
                        format!("scope_expires_at.{scope_name}"),
                        Value::Integer(ts_val as i64),
                    );
                }
            }
        }

        Self {
            id: if identity.user_id == 0 {
                claims.sub.clone()
            } else {
                identity.user_id.to_string()
            },
            username: identity.username.clone(),
            email,
            tenant_id: identity.tenant_id,
            org_id,
            org_ids,
            is_superuser: identity.is_superuser(),
            roles: identity.roles.iter().map(ToString::to_string).collect(),
            groups,
            permissions,
            status,
            metadata,
            auth_method: identity.auth_method.clone(),
            auth_time: if claims.iat > 0 {
                Some(claims.iat)
            } else {
                None
            },
            session_id,
            on_deny_override: None,
            database_id: None,
            risk_score: None,
        }
    }

    /// Build `AuthContext` from an already-verified JWT and its bound identity.
    ///
    /// JWT claims retain non-authoritative session detail such as organization,
    /// group, permission, and metadata fields. Identity fields that control
    /// authorization are taken from the verified identity, whose tenant and
    /// roles may be provider-bound rather than token-claim-derived.
    pub(crate) fn from_verified_jwt(
        verified_claims: &VerifiedJwtClaims,
        identity: &AuthenticatedIdentity,
        session_id: String,
    ) -> Self {
        Self::from_verified_claims(verified_claims.claims(), identity, session_id)
    }

    /// Build `AuthContext` from a DB-authenticated identity (SCRAM, password, API key).
    ///
    /// This is the fallback path when no JWT is presented. The context is
    /// populated from the `AuthenticatedIdentity` and credential store data.
    /// Extended fields (email, groups, org) are empty — they require JWT or
    /// JIT-provisioned `_system.auth_users` records.
    pub fn from_identity(identity: &AuthenticatedIdentity, session_id: String) -> Self {
        Self {
            id: identity.user_id.to_string(),
            username: identity.username.clone(),
            email: None,
            tenant_id: identity.tenant_id,
            org_id: None,
            org_ids: Vec::new(),
            is_superuser: identity.is_superuser(),
            roles: identity.roles.iter().map(|r| r.to_string()).collect(),
            groups: Vec::new(),
            permissions: Vec::new(),
            status: AuthStatus::Active,
            metadata: HashMap::new(),
            auth_method: identity.auth_method.clone(),
            auth_time: None,
            session_id,
            on_deny_override: None,
            database_id: None,
            risk_score: None,
        }
    }

    /// Check if the account status allows the request to proceed.
    ///
    /// Returns `Ok(())` for active accounts, `Err` with reason for blocked.
    pub fn check_status(&self) -> crate::Result<()> {
        match self.status {
            AuthStatus::Active | AuthStatus::Restricted | AuthStatus::ReadOnly => Ok(()),
            AuthStatus::Suspended => Err(crate::Error::RejectedAuthz {
                tenant_id: self.tenant_id,
                resource: "account suspended".into(),
            }),
            AuthStatus::Banned => Err(crate::Error::RejectedAuthz {
                tenant_id: self.tenant_id,
                resource: "account banned".into(),
            }),
        }
    }

    /// Check if the account allows write operations.
    pub fn allows_write(&self) -> bool {
        matches!(self.status, AuthStatus::Active | AuthStatus::Restricted)
    }

    /// Resolve a `$auth.<field>` reference to its concrete value.
    ///
    /// Returns `None` if the field doesn't exist or is empty (deny by default).
    pub fn resolve_variable(&self, field: &str) -> Option<serde_json::Value> {
        match field {
            "id" => Some(serde_json::Value::String(self.id.clone())),
            "username" => Some(serde_json::Value::String(self.username.clone())),
            "email" => self
                .email
                .as_ref()
                .map(|e| serde_json::Value::String(e.clone())),
            "tenant_id" => Some(serde_json::json!(self.tenant_id.as_u64())),
            "org_id" => self
                .org_id
                .as_ref()
                .map(|o| serde_json::Value::String(o.clone())),
            "org_ids" => Some(serde_json::Value::Array(
                self.org_ids
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
            "roles" => Some(serde_json::Value::Array(
                self.roles
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
            "groups" => Some(serde_json::Value::Array(
                self.groups
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
            "permissions" => Some(serde_json::Value::Array(
                self.permissions
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
            "status" => Some(serde_json::Value::String(self.status.to_string())),
            "auth_method" => Some(serde_json::Value::String(format!("{:?}", self.auth_method))),
            "auth_time" => self.auth_time.map(|t| serde_json::json!(t)),
            "session_id" => Some(serde_json::Value::String(self.session_id.clone())),
            "database_id" => self.database_id.map(|d| serde_json::json!(d.as_u64())),
            // An unassessed request resolves to `None`, exactly like
            // `database_id` above: predicates that gate on risk deny rather
            // than reading a fabricated zero score as "no risk".
            "risk_score" => self.risk_score.map(|s| serde_json::json!(s)),
            // Metadata sub-fields: $auth.metadata.<key>
            //
            // Converts via `Value`'s own `From` impl rather than a hand-rolled
            // match: `Value` is `#[non_exhaustive]`, so a local match would
            // need a wildcard arm that silently drops future variants. Using
            // the shared impl also keeps a numeric/boolean claim numeric or
            // boolean here instead of re-flattening it to a JSON string,
            // which would re-introduce the exact typing loss this type fixes.
            other if other.starts_with("metadata.") => {
                let key = &other["metadata.".len()..];
                self.metadata
                    .get(key)
                    .map(|v| serde_json::Value::from(v.clone()))
            }
            _ => None,
        }
    }

    /// Whether this context represents a superuser.
    pub fn is_superuser(&self) -> bool {
        self.is_superuser
    }

    /// Whether `metadata[key]` is an affirmative boolean flag.
    ///
    /// Accepts both `Value::Bool(true)` — what a provider issuing a proper
    /// JSON boolean claim now produces — and the legacy `Value::String("true")`
    /// that every existing deployment still sends, since the claim parser
    /// used to coerce every metadata value to a string. Accepting only one
    /// of the two forms would break either new correctly-typed providers or
    /// every deployment already in the field. An absent key, or any other
    /// value (including `Value::Bool(false)` / `Value::String("false")`), is
    /// not affirmative — flags fail closed rather than defaulting to set.
    pub fn metadata_flag(&self, key: &str) -> bool {
        match self.metadata.get(key) {
            Some(Value::Bool(b)) => *b,
            Some(Value::String(s)) => s == "true",
            _ => false,
        }
    }
}

/// Extract a string array from a JSON object by key.
fn extract_string_array(obj: &HashMap<String, serde_json::Value>, key: &str) -> Vec<String> {
    obj.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Generate a cryptographically random session ID (`s_<128-bit hex>`).
///
/// Used as `$auth.session_id` in RLS predicates and as the audit
/// correlation key — must be unguessable.
pub fn generate_session_id() -> String {
    super::random::generate_tagged_random_hex("s_")
}

#[cfg(test)]
mod tests {
    use super::super::identity::Role;
    use super::*;

    fn test_identity() -> AuthenticatedIdentity {
        use crate::control::security::identity::DatabaseSet;
        AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT,]),
        )
    }

    #[test]
    fn from_identity_populates_core_fields() {
        let identity = test_identity();
        let ctx = AuthContext::from_identity(&identity, "s_test_001".into());

        assert_eq!(ctx.id, "42");
        assert_eq!(ctx.username, "alice");
        assert_eq!(ctx.tenant_id, TenantId::new(1));
        assert_eq!(ctx.roles, vec!["readwrite"]);
        assert_eq!(ctx.status, AuthStatus::Active);
        assert!(ctx.email.is_none());
        assert!(ctx.org_id.is_none());
        assert!(ctx.groups.is_empty());
    }

    #[test]
    fn resolve_variable_core_fields() {
        let ctx = AuthContext::from_identity(&test_identity(), "s_test_002".into());

        assert_eq!(ctx.resolve_variable("id"), Some(serde_json::json!("42")));
        assert_eq!(
            ctx.resolve_variable("username"),
            Some(serde_json::json!("alice"))
        );
        assert_eq!(
            ctx.resolve_variable("tenant_id"),
            Some(serde_json::json!(1))
        );
        assert_eq!(
            ctx.resolve_variable("roles"),
            Some(serde_json::json!(["readwrite"]))
        );
        assert_eq!(
            ctx.resolve_variable("status"),
            Some(serde_json::json!("active"))
        );
    }

    #[test]
    fn resolve_variable_metadata() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_003".into());
        ctx.metadata.insert("plan".into(), "pro".into());

        assert_eq!(
            ctx.resolve_variable("metadata.plan"),
            Some(serde_json::json!("pro"))
        );
        assert_eq!(ctx.resolve_variable("metadata.missing"), None);
    }

    /// A JWT `metadata` claim carrying a JSON number survives claim parsing
    /// as `Value::Integer`, not as a string and not dropped.
    #[test]
    fn jwt_metadata_claim_carrying_a_number_survives_as_value_integer() {
        let mut extra = HashMap::new();
        extra.insert("metadata".into(), serde_json::json!({"seats": 5}));
        let claims = JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec!["readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 0,
            iss: "nodedb-auth".into(),
            aud: vec!["nodedb".into()],
            user_id: 42,
            is_superuser: false,
            extra,
        };

        let ctx = AuthContext::from_verified_claims(&claims, &test_identity(), "s_num".into());

        assert_eq!(ctx.metadata.get("seats"), Some(&Value::Integer(5)));
    }

    /// A JWT `metadata` claim carrying a JSON boolean survives claim parsing
    /// as `Value::Bool`, not stringified.
    #[test]
    fn jwt_metadata_claim_carrying_a_boolean_survives_as_value_bool() {
        let mut extra = HashMap::new();
        extra.insert("metadata".into(), serde_json::json!({"beta": true}));
        let claims = JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec!["readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 0,
            iss: "nodedb-auth".into(),
            aud: vec!["nodedb".into()],
            user_id: 42,
            is_superuser: false,
            extra,
        };

        let ctx = AuthContext::from_verified_claims(&claims, &test_identity(), "s_bool".into());

        assert_eq!(ctx.metadata.get("beta"), Some(&Value::Bool(true)));
    }

    /// A JWT `metadata` claim carrying a JSON string still survives unchanged
    /// — no regression from typing the map.
    #[test]
    fn jwt_metadata_claim_carrying_a_string_survives_unchanged() {
        let mut extra = HashMap::new();
        extra.insert("metadata".into(), serde_json::json!({"plan": "pro"}));
        let claims = JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec!["readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 0,
            iss: "nodedb-auth".into(),
            aud: vec!["nodedb".into()],
            user_id: 42,
            is_superuser: false,
            extra,
        };

        let ctx = AuthContext::from_verified_claims(&claims, &test_identity(), "s_str".into());

        assert_eq!(ctx.metadata.get("plan"), Some(&Value::String("pro".into())));
    }

    /// A JWT `metadata` claim carrying a nested object or array survives
    /// rather than being dropped by the claim parser.
    #[test]
    fn jwt_metadata_claim_carrying_a_nested_object_or_array_survives() {
        let mut extra = HashMap::new();
        extra.insert(
            "metadata".into(),
            serde_json::json!({
                "limits": {"max_rows": 100},
                "tags": ["a", "b"],
            }),
        );
        let claims = JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec!["readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 0,
            iss: "nodedb-auth".into(),
            aud: vec!["nodedb".into()],
            user_id: 42,
            is_superuser: false,
            extra,
        };

        let ctx = AuthContext::from_verified_claims(&claims, &test_identity(), "s_nested".into());

        match ctx.metadata.get("limits") {
            Some(Value::Object(obj)) => {
                assert_eq!(obj.get("max_rows"), Some(&Value::Integer(100)));
            }
            other => panic!("expected Value::Object for nested claim, got {other:?}"),
        }
        match ctx.metadata.get("tags") {
            Some(Value::Array(arr)) => {
                assert_eq!(
                    arr,
                    &vec![Value::String("a".into()), Value::String("b".into())]
                );
            }
            other => panic!("expected Value::Array for array claim, got {other:?}"),
        }
    }

    /// `resolve_variable("metadata.<key>")` returns a typed `serde_json::Value`
    /// — a claim number stays a JSON number rather than being flattened to a
    /// string, since that would re-drop the typing the parser now preserves.
    #[test]
    fn resolve_variable_metadata_returns_a_typed_value_not_a_stringified_one() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_typed".into());
        ctx.metadata.insert("seats".into(), Value::Integer(5));

        let resolved = ctx
            .resolve_variable("metadata.seats")
            .expect("metadata.seats must resolve");
        assert!(
            resolved.is_number(),
            "expected a JSON number, got {resolved:?}"
        );
        assert_eq!(resolved, serde_json::json!(5));
    }

    #[test]
    fn resolve_variable_unknown() {
        let ctx = AuthContext::from_identity(&test_identity(), "s_test_004".into());
        assert_eq!(ctx.resolve_variable("nonexistent"), None);
    }

    #[test]
    fn resolve_variable_database_id_none() {
        // When no database context is stamped, $auth.database_id resolves to
        // None (fail-closed: predicates that require a database_id deny access).
        let ctx = AuthContext::from_identity(&test_identity(), "s_test_db_none".into());
        assert_eq!(ctx.resolve_variable("database_id"), None);
    }

    #[test]
    fn resolve_variable_database_id_some() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_db_some".into());
        ctx.database_id = Some(nodedb_types::id::DatabaseId::new(42));
        assert_eq!(
            ctx.resolve_variable("database_id"),
            Some(serde_json::json!(42u64))
        );
    }

    /// Fail-closed: an unassessed request must resolve `$auth.risk_score` to
    /// `None` (deny), never to `0` — a fabricated zero would read as "no
    /// risk" and open every `$auth.risk_score < threshold` predicate.
    #[test]
    fn resolve_variable_risk_score_none_is_fail_closed() {
        let ctx = AuthContext::from_identity(&test_identity(), "s_test_risk_none".into());
        assert_eq!(ctx.risk_score, None);
        assert_eq!(ctx.resolve_variable("risk_score"), None);
    }

    #[test]
    fn resolve_variable_risk_score_some() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_risk_some".into());
        ctx.risk_score = Some(0.35);
        assert_eq!(
            ctx.resolve_variable("risk_score"),
            Some(serde_json::json!(0.35))
        );
    }

    #[test]
    fn check_status_active_ok() {
        let ctx = AuthContext::from_identity(&test_identity(), "s_test_005".into());
        assert!(ctx.check_status().is_ok());
    }

    #[test]
    fn check_status_suspended_err() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_006".into());
        ctx.status = AuthStatus::Suspended;
        assert!(ctx.check_status().is_err());
    }

    #[test]
    fn check_status_banned_err() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_007".into());
        ctx.status = AuthStatus::Banned;
        assert!(ctx.check_status().is_err());
    }

    #[test]
    fn allows_write_by_status() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_008".into());
        assert!(ctx.allows_write()); // Active

        ctx.status = AuthStatus::ReadOnly;
        assert!(!ctx.allows_write());

        ctx.status = AuthStatus::Restricted;
        assert!(ctx.allows_write());
    }

    #[test]
    fn auth_status_display_roundtrip() {
        for status in [
            AuthStatus::Active,
            AuthStatus::Suspended,
            AuthStatus::Banned,
            AuthStatus::Restricted,
            AuthStatus::ReadOnly,
        ] {
            let s = status.to_string();
            let parsed: AuthStatus = s.parse().unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn session_id_generation_unique() {
        let id1 = generate_session_id();
        let id2 = generate_session_id();
        assert_ne!(id1, id2);
        assert!(id1.starts_with("s_"));
    }

    /// Sanity check that `generate_session_id` uses the CSPRNG helper and
    /// carries the `s_` tag. Entropy / leak / enumerability guarantees are
    /// tested on the shared helper in `super::random`.
    #[test]
    fn session_id_uses_shared_csprng_helper_with_s_prefix() {
        let id = generate_session_id();
        assert!(id.starts_with("s_"));
        let rest = id.strip_prefix("s_").unwrap();
        assert_eq!(rest.len(), 32, "expected 128-bit (32 hex char) payload");
        assert!(rest.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn from_verified_jwt_uses_identity_for_authorization_fields() {
        let claims = JwtClaims {
            sub: "claim-user".into(),
            tenant_id: 999,
            roles: vec!["readonly".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1_700_000_000,
            iss: "provider".into(),
            aud: vec!["nodedb".into()],
            user_id: 7,
            is_superuser: false,
            extra: HashMap::from([
                ("groups".into(), serde_json::json!(["engineering"])),
                ("metadata".into(), serde_json::json!({"region": "us-west"})),
            ]),
        };
        let mut identity = test_identity();
        identity.user_id = 42;
        identity.username = "provider-user".into();
        identity.tenant_id = TenantId::new(1);
        identity.auth_method = AuthMethod::OidcBearer;
        identity.roles = vec![Role::TenantAdmin];

        let ctx = AuthContext::from_verified_claims(&claims, &identity, "s_jwt_bound".into());

        assert_eq!(ctx.id, "42");
        assert_eq!(ctx.username, "provider-user");
        assert_eq!(ctx.tenant_id, TenantId::new(1));
        assert_eq!(ctx.roles, vec!["tenant_admin"]);
        assert_eq!(ctx.auth_method, AuthMethod::OidcBearer);
        assert_eq!(ctx.groups, vec!["engineering"]);
        assert_eq!(ctx.metadata.get("region"), Some(&"us-west".into()));
    }

    #[test]
    fn from_verified_jwt_uses_distinct_subject_ids_when_identity_id_is_zero() {
        let first_claims = JwtClaims {
            sub: "oidc-subject-alice".into(),
            tenant_id: 999,
            roles: vec!["readonly".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1_700_000_000,
            iss: "https://issuer.example".into(),
            aud: vec!["nodedb".into()],
            user_id: 0,
            is_superuser: false,
            extra: HashMap::new(),
        };
        let mut second_claims = first_claims.clone();
        second_claims.sub = "oidc-subject-bob".into();

        let mut identity = test_identity();
        identity.user_id = 0;
        identity.auth_method = AuthMethod::OidcBearer;

        let first = AuthContext::from_verified_claims(&first_claims, &identity, "s_oidc_1".into());
        let second =
            AuthContext::from_verified_claims(&second_claims, &identity, "s_oidc_2".into());

        assert_eq!(first.id, first_claims.sub);
        assert_eq!(second.id, second_claims.sub);
        assert_ne!(first.id, second.id);
        assert_ne!(first.id, "0");
        assert_eq!(first.tenant_id, identity.tenant_id);
        assert_eq!(first.username, identity.username);
        assert_eq!(first.roles, vec!["readwrite"]);
        assert_eq!(first.auth_method, AuthMethod::OidcBearer);
    }

    #[test]
    fn from_jwt_removes_externally_asserted_superuser_authority() {
        let claims = JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec!["superuser".into(), "readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 0,
            iss: "nodedb-auth".into(),
            aud: vec!["nodedb".into()],
            user_id: 42,
            is_superuser: true,
            extra: HashMap::new(),
        };

        let context =
            AuthContext::from_verified_claims(&claims, &test_identity(), "s_jwt_roles".into());

        assert!(!context.is_superuser());
        assert_eq!(context.roles, vec!["readwrite"]);
    }

    #[test]
    fn from_jwt_populates_extended_fields() {
        let mut extra = HashMap::new();
        extra.insert("email".into(), serde_json::json!("alice@example.com"));
        extra.insert("org_id".into(), serde_json::json!("org_acme"));
        extra.insert(
            "org_ids".into(),
            serde_json::json!(["org_acme", "org_beta"]),
        );
        extra.insert("groups".into(), serde_json::json!(["engineering", "leads"]));
        extra.insert(
            "permissions".into(),
            serde_json::json!(["profile:read", "data:write"]),
        );
        extra.insert("status".into(), serde_json::json!("active"));
        extra.insert(
            "metadata".into(),
            serde_json::json!({"plan": "enterprise", "region": "us-west"}),
        );

        let claims = JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec!["readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1_700_000_000,
            iss: "nodedb-auth".into(),
            aud: vec!["nodedb".into()],
            user_id: 42,
            is_superuser: false,
            extra,
        };

        let ctx = AuthContext::from_verified_claims(&claims, &test_identity(), "s_jwt_001".into());

        assert_eq!(ctx.id, "42");
        assert_eq!(ctx.username, "alice");
        assert_eq!(ctx.email, Some("alice@example.com".into()));
        assert_eq!(ctx.org_id, Some("org_acme".into()));
        assert_eq!(ctx.org_ids, vec!["org_acme", "org_beta"]);
        assert_eq!(ctx.groups, vec!["engineering", "leads"]);
        assert_eq!(ctx.permissions, vec!["profile:read", "data:write"]);
        assert_eq!(ctx.auth_time, Some(1_700_000_000));
        assert_eq!(ctx.metadata.get("plan"), Some(&"enterprise".into()));
        assert_eq!(ctx.metadata.get("region"), Some(&"us-west".into()));
    }
}
