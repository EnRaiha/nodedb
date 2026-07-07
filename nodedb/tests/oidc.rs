// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for OIDC provider DDL + claim mapping.

mod common;

use common::pgwire_auth_helpers::{
    ddl_err, ddl_ok, make_state_with_catalog, readonly_user, superuser,
};
use nodedb::control::security::oidc::claim_mapping::apply_claim_mapping;

// ── CREATE OIDC PROVIDER ────────────────────────────────────────────────────

#[tokio::test]
async fn create_oidc_provider_persists_in_catalog() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER okta \
         ISSUER 'https://acme.okta.com' \
         JWKS_URI 'https://acme.okta.com/.well-known/jwks.json' \
         AUDIENCE 'nodedb'",
    )
    .await;

    let cat = state.credentials.catalog();
    let stored = cat
        .get_oidc_provider("okta")
        .expect("catalog read must succeed")
        .expect("provider must exist after CREATE");
    assert_eq!(stored.issuer, "https://acme.okta.com");
    assert_eq!(stored.audience.as_deref(), Some("nodedb"));
}

#[tokio::test]
async fn create_oidc_provider_requires_superuser() {
    let state = make_state_with_catalog();
    let viewer = readonly_user();
    let err = ddl_err(
        &state,
        &viewer,
        "CREATE OIDC PROVIDER bad \
         ISSUER 'https://x.example/' \
         JWKS_URI 'https://x.example/jwks'",
    )
    .await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected permission denied, got: {err}"
    );
}

// ── DROP OIDC PROVIDER ──────────────────────────────────────────────────────

#[tokio::test]
async fn drop_oidc_provider_removes_record() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER auth0 \
         ISSUER 'https://x.auth0.com' \
         JWKS_URI 'https://x.auth0.com/.well-known/jwks.json'",
    )
    .await;
    ddl_ok(&state, &su, "DROP OIDC PROVIDER auth0").await;

    let cat = state.credentials.catalog();
    let stored = cat
        .get_oidc_provider("auth0")
        .expect("catalog read must succeed");
    assert!(stored.is_none(), "provider must be absent after DROP");
}

#[tokio::test]
async fn drop_oidc_provider_unknown_returns_not_found() {
    let state = make_state_with_catalog();
    let su = superuser();
    let err = ddl_err(&state, &su, "DROP OIDC PROVIDER does_not_exist").await;
    assert!(
        err.contains("42704") || err.contains("does not exist"),
        "expected not-found error, got: {err}"
    );
}

#[tokio::test]
async fn drop_oidc_provider_if_exists_unknown_succeeds() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "DROP OIDC PROVIDER IF EXISTS does_not_exist").await;
}

// ── SHOW OIDC PROVIDERS ─────────────────────────────────────────────────────

#[tokio::test]
async fn show_oidc_providers_lists_registered() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER p1 \
         ISSUER 'https://p1.example/' \
         JWKS_URI 'https://p1.example/jwks'",
    )
    .await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER p2 \
         ISSUER 'https://p2.example/' \
         JWKS_URI 'https://p2.example/jwks'",
    )
    .await;
    ddl_ok(&state, &su, "SHOW OIDC PROVIDERS").await;
}

// ── Claim-mapping public surface smoke check ────────────────────────────────

#[test]
fn claim_mapping_apply_function_is_public() {
    // Verifies that `apply_claim_mapping` is accessible from integration tests.
    let _ = apply_claim_mapping;
}
