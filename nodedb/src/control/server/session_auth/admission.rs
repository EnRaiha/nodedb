// SPDX-License-Identifier: BUSL-1.1

//! [`check_request_admission`] — the single composed entry point for the
//! blacklist, account-status, and rate-limit guards.
//!
//! `guards.rs` defines the individual primitives; this module composes them
//! in the order every transport needs, so no call site can apply them out of
//! order or forget one.

use nodedb_types::DatabaseId;

use crate::control::security::ratelimit::limiter::RateLimitResult;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::state::SharedState;

use super::guards::{check_blacklist, check_rate_limit};

/// Run the full request-admission gate: internal-service exemption,
/// blacklist, account status, then rate limit.
///
/// Returns `Ok(None)` when the request was exempt (server-owned work) and
/// nothing further was checked, or `Ok(Some(result))` with the rate-limit
/// outcome once every guard passed. HTTP uses the `Some` case to emit
/// `X-RateLimit-*` / `Retry-After` headers; other transports may discard it.
///
/// Order matters:
/// 1. `scope.identity().is_internal_service()` — server-owned work (triggers,
///    Raft apply, CRDT sync, scheduler, replay) must never be blacklisted or
///    rate-limited; doing so could stall replay. This is the cheapest check
///    and short-circuits everything else.
/// 2. [`check_blacklist`] — cheap, identity-shaped rejection before any
///    heavier work.
/// 3. [`AuthContext::check_status`](crate::control::security::auth_context::AuthContext::check_status)
///    — account status (`Suspended` / `Banned`). This is *not* redundant with
///    the blacklist's auth-user status check: the blacklist reads the
///    persistent `state.auth_users` store, whereas `AuthContext.status` is
///    built `Active` and is only mutated in-session by the escalation engine
///    onto a possibly-pooled context. Both must be checked.
/// 4. [`check_rate_limit`] — runs last, and before any planning/catalog work,
///    so load is shed before it is spent.
pub fn check_request_admission(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    peer_addr: &str,
    operation: &str,
) -> crate::Result<Option<RateLimitResult>> {
    if scope.identity().is_internal_service() {
        return Ok(None);
    }

    check_blacklist(state, scope.identity(), peer_addr)?;
    scope.auth().check_status()?;

    let database_id: DatabaseId = scope.database_id();
    let result = check_rate_limit(
        state,
        scope.identity(),
        scope.auth(),
        operation,
        database_id,
    )?;

    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::auth_context::AuthStatus;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::TenantId;
    use crate::wal::WalManager;

    use super::*;

    /// Returns the state plus the backing `TempDir` guard — the caller must
    /// keep the guard alive for as long as `state` is in use, or the WAL's
    /// backing file is removed out from under it.
    async fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (state, dir)
    }

    fn regular_identity(user_id: u64, auth_method: AuthMethod) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            "regular-user",
            TenantId::new(1),
            auth_method,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    /// `new_internal_service` is crate-private; tests reach it exactly the
    /// way `authenticated.rs`'s own test module does.
    fn internal_service_identity(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            user_id,
            "internal-service",
            TenantId::new(1),
            vec![],
            false,
            None,
            AuthenticatedIdentity::default_database_set(false),
        )
    }

    #[tokio::test]
    async fn internal_service_identity_short_circuits_even_when_blacklisted() {
        let (state, _dir) = test_state().await;
        let identity = internal_service_identity(9001);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let scope =
            RequestAuthScope::for_database(&identity, &state.scope_grants, DatabaseId::DEFAULT);

        let result = check_request_admission(&state, &scope, "127.0.0.1", "point_get")
            .expect("internal-service identity must never be blocked");
        assert!(
            result.is_none(),
            "internal-service identity must short-circuit with Ok(None)"
        );
    }

    #[tokio::test]
    async fn regular_identity_is_blocked_when_blacklisted() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9002, AuthMethod::ScramSha256);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let scope =
            RequestAuthScope::for_database(&identity, &state.scope_grants, DatabaseId::DEFAULT);

        let result = check_request_admission(&state, &scope, "127.0.0.1", "point_get");
        assert!(
            result.is_err(),
            "blacklisted regular identity must be rejected"
        );
    }

    /// Security-critical: `AuthMethod::Trust` alone must never confer
    /// exemption. `trust_identity` / `configured_trust_identity` build real
    /// external identities with `AuthMethod::Trust` for servers running in
    /// trust-auth mode — if the wrapper exempted on `auth_method ==
    /// AuthMethod::Trust` instead of the dedicated `is_internal_service`
    /// flag, every trust-mode external client would silently bypass
    /// blacklist and rate-limit enforcement. A `new_regular` identity
    /// carrying `AuthMethod::Trust` is exactly that shape, built through the
    /// normal external-identity constructor (not `new_internal_service`), so
    /// `is_internal_service()` is `false` and the guards must still apply.
    #[tokio::test]
    async fn trust_auth_method_alone_does_not_exempt_from_guards() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9003, AuthMethod::Trust);
        assert!(!identity.is_internal_service());
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let scope =
            RequestAuthScope::for_database(&identity, &state.scope_grants, DatabaseId::DEFAULT);

        let result = check_request_admission(&state, &scope, "127.0.0.1", "point_get");
        assert!(
            result.is_err(),
            "a trust-mode identity built via the normal external path must not be exempt"
        );
    }

    #[tokio::test]
    async fn suspended_account_is_rejected_at_status_check() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9004, AuthMethod::ScramSha256);

        // `RequestAuthScope` has no public `auth_mut()`, so a pre-suspended
        // `AuthContext` must be built directly and adopted through the
        // builder rather than mutated after the fact.
        let mut ctx = crate::control::security::auth_context::AuthContext::from_identity(
            &identity,
            "s_test_suspended".into(),
        );
        ctx.status = AuthStatus::Suspended;
        let scope = RequestAuthScope::builder(&identity, &state.scope_grants)
            .with_session_database(Some(DatabaseId::DEFAULT))
            .with_adopted_auth_context(ctx)
            .build();

        let result = check_request_admission(&state, &scope, "127.0.0.1", "point_get");
        assert!(result.is_err(), "suspended account must be rejected");
    }

    #[tokio::test]
    async fn happy_path_returns_rate_limit_result() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9005, AuthMethod::ScramSha256);
        let scope =
            RequestAuthScope::for_database(&identity, &state.scope_grants, DatabaseId::DEFAULT);

        let result = check_request_admission(&state, &scope, "127.0.0.1", "point_get")
            .expect("non-blacklisted, active, unthrottled request must be admitted");
        assert!(
            result.is_some(),
            "checked path must report Some(rate limit result)"
        );
    }
}
