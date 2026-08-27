// SPDX-License-Identifier: BUSL-1.1

//! Concurrent correct-credential logins must all succeed, and the login
//! rate limiter must reject only *failed*-attempt bursts — never a pool of
//! legitimate reconnects.
//!
//! Regression for the bug where the pre-authentication login rate limiter
//! consumed its per-IP budget on every attempt BEFORE verifying credentials,
//! so a burst of correct-credential reconnects from one source (a connection
//! pool warming up) exhausted the shared bucket and was rejected — and, worse,
//! the transient rejection was collapsed into the same "authentication failed"
//! error as a genuine wrong password.
//!
//! The fix drives the brute-force budget from FAILED verifies only and surfaces
//! a rate-limit rejection as a distinct, retryable `RateExceeded` error.

use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::security::credential::store::CredentialStore;
use nodedb::control::security::identity::Role;
use nodedb::control::server::session_auth::authenticate;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb::wal::WalManager;

/// Build a `SharedState` with a real credential store and the given per-IP /
/// per-user login-failure caps. The credential lockout policy is set
/// deliberately high so the *rate limiter* — not the lockout counter — is the
/// component under test.
fn state_with_login_caps(ip_cap: u64, user_cap: u64) -> (Arc<SharedState>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());

    let mut store = CredentialStore::open(&dir.path().join("system.redb")).unwrap();
    store.set_lockout_policy(1000, 300, 0);
    let credentials = Arc::new(store);

    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let state = SharedState::new_with_credentials(dispatcher, wal, credentials).unwrap();
    state.rate_limiter.set_login_capacities(ip_cap, user_cap);
    (state, dir)
}

fn password_body(username: &str, password: &str) -> serde_json::Value {
    serde_json::json!({
        "method": "password",
        "username": username,
        "password": password,
    })
}

/// A burst of N concurrent CORRECT-credential logins from effectively one
/// client must ALL authenticate. On the pre-fix tree the shared per-IP bucket
/// (cap 5 here) admits only ~5 of them and rejects the rest — this assertion
/// fails there and passes only once correct credentials stop consuming the
/// brute-force budget.
#[tokio::test]
async fn concurrent_correct_credential_burst_all_succeed() {
    // Small caps make the pre-fix failure deterministic: pre-fix, only `ip_cap`
    // of the 20 attempts would be admitted.
    let (state, _dir) = state_with_login_caps(5, 5);
    state
        .credentials
        .create_user(
            "pool",
            "correct-pw",
            TenantId::new(1),
            vec![Role::Superuser],
        )
        .unwrap();

    const N: usize = 20;
    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let state = Arc::clone(&state);
        handles.push(tokio::spawn(async move {
            // All connections share one source IP — the pool case.
            authenticate(
                &state,
                &AuthMode::Password,
                &password_body("pool", "correct-pw"),
                "10.0.0.1:6000",
            )
            .await
            .is_ok()
        }));
    }

    let mut successes = 0usize;
    for h in handles {
        if h.await.unwrap() {
            successes += 1;
        }
    }

    assert_eq!(
        successes, N,
        "all {N} concurrent correct-credential logins from one IP must succeed; \
         got {successes}"
    );
}

/// A burst of WRONG-credential attempts from one IP must still be rate-limited
/// once the failure budget is spent, and the transient rejection must surface
/// as the distinct retryable `RateExceeded` error — NOT the generic
/// "authentication failed" that a genuine wrong password returns.
#[tokio::test]
async fn wrong_credential_burst_is_rate_limited_with_distinct_error() {
    let (state, _dir) = state_with_login_caps(5, 100);
    state
        .credentials
        .create_user(
            "victim",
            "correct-pw",
            TenantId::new(1),
            vec![Role::Superuser],
        )
        .unwrap();

    let peer = "10.0.0.2:7000";

    // The first `ip_cap` wrong attempts are genuine credential failures: each
    // returns the generic authorization rejection and drains the per-IP
    // failure budget.
    for i in 0..5 {
        let err = authenticate(
            &state,
            &AuthMode::Password,
            &password_body("victim", "wrong-pw"),
            peer,
        )
        .await
        .expect_err("wrong password must fail");
        assert!(
            matches!(err, nodedb::Error::RejectedAuthz { .. }),
            "wrong-password attempt {i} must be a credential rejection, got: {err:?}"
        );
    }

    // Budget exhausted: the next attempt is rejected pre-verify as a distinct,
    // retryable rate-limit error.
    let err = authenticate(
        &state,
        &AuthMode::Password,
        &password_body("victim", "wrong-pw"),
        peer,
    )
    .await
    .expect_err("attempt beyond the failure budget must be rejected");
    assert!(
        matches!(err, nodedb::Error::RateExceeded { .. }),
        "rate-limited attempt must surface as a distinct retryable RateExceeded \
         error, not a credential failure; got: {err:?}"
    );
}

/// Correct credentials must never trip the brute-force window, even
/// interleaved right up against the failure cap: draining is driven only by
/// failed verifies.
#[tokio::test]
async fn correct_credentials_do_not_consume_brute_force_budget() {
    let (state, _dir) = state_with_login_caps(5, 5);
    state
        .credentials
        .create_user("svc", "correct-pw", TenantId::new(1), vec![Role::Superuser])
        .unwrap();

    let peer = "10.0.0.3:8000";
    // Far more correct logins than the failure cap — none may be rejected.
    for i in 0..30 {
        let res = authenticate(
            &state,
            &AuthMode::Password,
            &password_body("svc", "correct-pw"),
            peer,
        )
        .await;
        assert!(
            res.is_ok(),
            "correct-credential login {i} must succeed (brute-force budget untouched), got: {:?}",
            res.err()
        );
    }
}
