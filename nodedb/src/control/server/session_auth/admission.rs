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

/// Run the blacklist + account-status guards without rate limiting.
///
/// For admission doors whose traffic is not the per-query traffic the
/// rate-limiter's cost table models — ILP/OTLP ingest, CRDT delta sync,
/// shape subscription/resync, and admin-scoped COPY backup/restore — but
/// which must still refuse a blacklisted or suspended/banned account.
/// Composes the same internal-service exemption, [`check_blacklist`], and
/// [`AuthContext::check_status`](crate::control::security::auth_context::AuthContext::check_status)
/// steps [`check_request_admission`] runs, minus [`check_rate_limit`] —
/// see that function's doc for why the order (exemption, then blacklist,
/// then status) matters.
pub fn check_blacklist_and_status(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    peer_addr: &str,
) -> crate::Result<()> {
    if scope.identity().is_internal_service() {
        return Ok(());
    }

    check_blacklist(state, scope.identity(), peer_addr)?;
    scope.auth().check_status()
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
            RequestAuthScope::for_database(&identity, state.auth_stores(), DatabaseId::DEFAULT);

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
            RequestAuthScope::for_database(&identity, state.auth_stores(), DatabaseId::DEFAULT);

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
            RequestAuthScope::for_database(&identity, state.auth_stores(), DatabaseId::DEFAULT);

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
        let scope = RequestAuthScope::builder(&identity, state.auth_stores())
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
            RequestAuthScope::for_database(&identity, state.auth_stores(), DatabaseId::DEFAULT);

        let result = check_request_admission(&state, &scope, "127.0.0.1", "point_get")
            .expect("non-blacklisted, active, unthrottled request must be admitted");
        assert!(
            result.is_some(),
            "checked path must report Some(rate limit result)"
        );
    }

    // ── `check_blacklist_and_status` — the blacklist-only-plus-status door
    //    shared by ILP/OTLP ingest, CRDT delta sync, shape subscribe/resync,
    //    and pgwire COPY backup/restore. ──────────────────────────────────

    #[tokio::test]
    async fn blacklist_and_status_rejects_user_blacklisted_identity() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9101, AuthMethod::ApiKey);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let scope =
            RequestAuthScope::for_database(&identity, state.auth_stores(), DatabaseId::DEFAULT);

        let result = check_blacklist_and_status(&state, &scope, "127.0.0.1:5432");
        assert!(
            result.is_err(),
            "a user-blacklisted identity must be rejected"
        );
    }

    /// The IP half of the gate — this is what "peer address threading" is
    /// for: a client whose IP was never blacklisted by user id must still be
    /// rejected once its real peer address matches a `BLACKLIST IP` entry.
    #[tokio::test]
    async fn blacklist_and_status_rejects_blacklisted_peer_ip() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9102, AuthMethod::ApiKey);
        state
            .blacklist
            .blacklist_ip("10.0.0.0/8", "test ip ban", "admin", 0)
            .expect("blacklist CIDR range");

        let scope =
            RequestAuthScope::for_database(&identity, state.auth_stores(), DatabaseId::DEFAULT);

        let allowed = check_blacklist_and_status(&state, &scope, "203.0.113.5:5432");
        assert!(
            allowed.is_ok(),
            "an address outside the blacklisted range must be admitted"
        );

        let denied = check_blacklist_and_status(&state, &scope, "10.1.2.3:5432");
        assert!(
            denied.is_err(),
            "an address inside the blacklisted CIDR range must be rejected, proving the real \
             peer address (not an empty placeholder) reaches the IP-blacklist check"
        );
    }

    #[tokio::test]
    async fn blacklist_and_status_rejects_suspended_account() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9103, AuthMethod::ApiKey);
        let mut ctx = crate::control::security::auth_context::AuthContext::from_identity(
            &identity,
            "s_test_suspended_no_ratelimit".into(),
        );
        ctx.status = AuthStatus::Suspended;
        let scope = RequestAuthScope::builder(&identity, state.auth_stores())
            .with_session_database(Some(DatabaseId::DEFAULT))
            .with_adopted_auth_context(ctx)
            .build();

        let result = check_blacklist_and_status(&state, &scope, "127.0.0.1:5432");
        assert!(
            result.is_err(),
            "a suspended account must be rejected even though no rate limit runs on this door"
        );
    }

    #[tokio::test]
    async fn blacklist_and_status_exempts_internal_service_identity() {
        let (state, _dir) = test_state().await;
        let identity = internal_service_identity(9104);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let scope =
            RequestAuthScope::for_database(&identity, state.auth_stores(), DatabaseId::DEFAULT);

        let result = check_blacklist_and_status(&state, &scope, "127.0.0.1:5432");
        assert!(
            result.is_ok(),
            "internal-service identities must never be blocked, even when blacklisted"
        );
    }

    #[tokio::test]
    async fn blacklist_and_status_allows_active_unblocked_identity() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9105, AuthMethod::ApiKey);
        let scope =
            RequestAuthScope::for_database(&identity, state.auth_stores(), DatabaseId::DEFAULT);

        let result = check_blacklist_and_status(&state, &scope, "127.0.0.1:5432");
        assert!(
            result.is_ok(),
            "a non-blacklisted, active identity must be admitted with no rate limit involved"
        );
    }
}
