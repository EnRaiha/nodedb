// SPDX-License-Identifier: BUSL-1.1

//! Credential-lockout counting: what is and is not a credential failure.
//!
//! The account-lockout counter is a brute-force defense. It must be
//! incremented only by *credential* failures — a wrong password, an
//! unknown user, a failed SCRAM exchange. Authentication attempts that
//! are rejected for a *non-credential* reason while the supplied
//! password is verified-correct (password marked `must_change`, password
//! past its expiry) are policy rejections, not brute-force attempts, and
//! must not move the lockout counter.
//!
//! These tests drive the real protocol auth entry points (native JSON
//! `authenticate`, RESP `AUTH`, pgwire SCRAM) and assert the counter's
//! spec end to end. The pgwire tests additionally assert that a locked
//! account is not advertised as such on the wire.

mod common;

use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::security::credential::store::CredentialStore;
use nodedb::control::security::identity::Role;
use nodedb::control::server::resp::command::RespCommand;
use nodedb::control::server::resp::handler::execute as resp_execute;
use nodedb::control::server::resp::session::RespSession;
use nodedb::control::server::session_auth::authenticate;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb::wal::WalManager;

/// Lockout policy used by every test: 5 failures within the window locks
/// the account for 300 seconds — the production default the bug report
/// reproduced against.
const MAX_FAILED: u32 = 5;
const LOCKOUT_SECS: u64 = 300;

/// Build a `SharedState` whose `CredentialStore` has the lockout policy
/// configured. The policy must be set before the store is wrapped in an
/// `Arc`, so this cannot reuse the shared `make_state` helper.
fn state_with_lockout() -> (Arc<SharedState>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());

    let mut store = CredentialStore::open(&dir.path().join("system.redb")).unwrap();
    // password_expiry_days = 0 → accounts never expire unless a test
    // explicitly stamps `password_expires_at`.
    store.set_lockout_policy(MAX_FAILED, LOCKOUT_SECS, 0);
    let credentials = Arc::new(store);

    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let state = SharedState::new_with_credentials(dispatcher, wal, credentials).unwrap();
    // Generous login rate-limit so the per-user bucket never trips before
    // the lockout counter is exercised — the lockout counter, not the
    // rate limiter, is the subject under test.
    state.rate_limiter.set_login_capacities(10_000, 10_000);
    (state, dir)
}

/// Issue one native-protocol password login.
async fn native_password_login(state: &SharedState, username: &str, password: &str) {
    let body = serde_json::json!({
        "method": "password",
        "username": username,
        "password": password,
    });
    let _ = authenticate(state, &AuthMode::Password, &body, "127.0.0.1:5000").await;
}

/// Issue one RESP `AUTH <user> <password>` command.
async fn resp_auth(state: &SharedState, username: &str, password: &str) {
    let cmd = RespCommand {
        name: "AUTH".to_string(),
        args: vec![username.as_bytes().to_vec(), password.as_bytes().to_vec()],
    };
    let mut session = RespSession::default();
    let _ = resp_execute(&cmd, &mut session, state).await;
}

// ── Non-credential rejections must not lock the account ─────────────────

#[tokio::test]
async fn correct_password_with_pending_change_does_not_count_as_credential_failure() {
    let (state, _dir) = state_with_lockout();
    state
        .credentials
        .create_user(
            "admin",
            "correct-pw",
            TenantId::new(1),
            vec![Role::Superuser],
        )
        .unwrap();
    state
        .credentials
        .set_must_change_password("admin", true)
        .unwrap();

    // Five logins, every one with the *correct* password. The account is
    // rejected only because a password change is pending — a policy
    // rejection, not a brute-force attempt.
    for _ in 0..MAX_FAILED {
        native_password_login(&state, "admin", "correct-pw").await;
    }

    assert!(
        state.credentials.check_lockout("admin").is_ok(),
        "a correct password rejected for a pending password change must \
         not count toward credential lockout"
    );
}

#[tokio::test]
async fn correct_password_past_expiry_does_not_count_as_credential_failure() {
    let (state, _dir) = state_with_lockout();
    state
        .credentials
        .create_user("ops", "correct-pw", TenantId::new(1), vec![Role::Superuser])
        .unwrap();
    // Stamp an expiry one second after the Unix epoch — unconditionally
    // in the past, with no grace window configured.
    state.credentials.set_password_expires_at("ops", 1).unwrap();

    for _ in 0..MAX_FAILED {
        native_password_login(&state, "ops", "correct-pw").await;
    }

    assert!(
        state.credentials.check_lockout("ops").is_ok(),
        "a correct password rejected for password expiry must not count \
         toward credential lockout"
    );
}

#[tokio::test]
async fn resp_auth_correct_password_with_pending_change_does_not_count() {
    let (state, _dir) = state_with_lockout();
    state
        .credentials
        .create_user(
            "admin",
            "correct-pw",
            TenantId::new(1),
            vec![Role::Superuser],
        )
        .unwrap();
    state
        .credentials
        .set_must_change_password("admin", true)
        .unwrap();

    // The RESP `AUTH` entry point shares the same credential store and
    // the same lockout counter as the native and pgwire paths.
    for _ in 0..MAX_FAILED {
        resp_auth(&state, "admin", "correct-pw").await;
    }

    assert!(
        state.credentials.check_lockout("admin").is_ok(),
        "RESP AUTH with a correct password rejected for a pending password \
         change must not count toward credential lockout"
    );
}

// ── Genuine credential failures must still lock the account ─────────────
// Positive controls: the fix must narrow what counts as a credential
// failure, not disable lockout. These pass today and must keep passing.

#[tokio::test]
async fn wrong_password_counts_toward_lockout() {
    let (state, _dir) = state_with_lockout();
    state
        .credentials
        .create_user(
            "admin",
            "correct-pw",
            TenantId::new(1),
            vec![Role::Superuser],
        )
        .unwrap();

    for _ in 0..MAX_FAILED {
        native_password_login(&state, "admin", "wrong-pw").await;
    }

    assert!(
        state.credentials.check_lockout("admin").is_err(),
        "five wrong-password attempts must lock the account"
    );
}

#[tokio::test]
async fn unknown_user_counts_toward_lockout() {
    let (state, _dir) = state_with_lockout();

    for _ in 0..MAX_FAILED {
        native_password_login(&state, "ghost", "any-pw").await;
    }

    assert!(
        state.credentials.check_lockout("ghost").is_err(),
        "five attempts against an unknown user must lock the account"
    );
}

// ── pgwire SCRAM path ───────────────────────────────────────────────────
// The SCRAM connection factory shares the same lockout counter. Its
// credential lookup (`get_scram_credentials`) returns the same opaque
// `None` for a wrong password and for a policy rejection, so it has the
// identical flaw as the native/RESP paths.

#[tokio::test]
async fn pgwire_scram_correct_password_with_pending_change_does_not_count() {
    let server = common::pgwire_harness::TestServer::start_password().await;
    server
        .shared
        .rate_limiter
        .set_login_capacities(10_000, 10_000);
    // Mark the harness superuser as needing a password change. SCRAM
    // credential lookup now rejects every login, but the password
    // `nodedb` is still correct — a policy rejection, not a credential
    // failure.
    server
        .shared
        .credentials
        .set_must_change_password("nodedb", true)
        .unwrap();

    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb password=nodedb dbname=nodedb",
        server.pg_port
    );
    for _ in 0..MAX_FAILED {
        let attempt = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await;
        assert!(
            attempt.is_err(),
            "a pending password change must reject the login"
        );
    }

    assert!(
        server.shared.credentials.check_lockout("nodedb").is_ok(),
        "a correct password rejected over SCRAM for a pending password \
         change must not count toward credential lockout"
    );
}

#[tokio::test]
async fn pgwire_locked_account_rejection_does_not_advertise_lockout() {
    let server = common::pgwire_harness::TestServer::start_password().await;
    server
        .shared
        .rate_limiter
        .set_login_capacities(10_000, 10_000);

    // Lock the account with five genuine wrong-password SCRAM attempts.
    let wrong = format!(
        "host=127.0.0.1 port={} user=nodedb password=wrong-pw dbname=nodedb",
        server.pg_port
    );
    for _ in 0..MAX_FAILED {
        let _ = tokio_postgres::connect(&wrong, tokio_postgres::NoTls).await;
    }
    assert!(
        server.shared.credentials.check_lockout("nodedb").is_err(),
        "five wrong-password SCRAM attempts must lock the account"
    );

    // A subsequent attempt is rejected. The wire error must look like an
    // ordinary authentication failure: it must not announce that the
    // account is locked, which would confirm the username to an
    // unauthenticated probe and leak the account's lockout state.
    let err = match tokio_postgres::connect(&wrong, tokio_postgres::NoTls).await {
        Ok(_) => panic!("a locked account must reject the connection"),
        Err(e) => e,
    };
    // The server-supplied error message is what reaches the client (and
    // is printed verbatim by `psql`). It must read like an ordinary
    // authentication failure — not announce the lockout state.
    let server_msg = err
        .as_db_error()
        .map(|d| d.message().to_lowercase())
        .unwrap_or_default();
    assert!(
        !server_msg.contains("locked") && !server_msg.contains("lockout"),
        "the wire rejection must not advertise the account's lockout \
         state to an unauthenticated client: {server_msg}"
    );
}
