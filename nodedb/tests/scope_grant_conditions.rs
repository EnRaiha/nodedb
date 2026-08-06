// SPDX-License-Identifier: BUSL-1.1

//! End-to-end coverage for conditional scope grants.
//!
//! `GrantCondition` existed with no grammar to create one, no field to store
//! one, and no caller to evaluate one. These tests are the proof the feature
//! reaches a user: the `WHEN` / `REQUIRE` clauses parse, the conditions land
//! in the catalog, they are still there after a restart, and `SHOW SCOPE
//! GRANTS` shows an operator exactly what is attached to a grant.
//!
//! Whether a conditioned grant *applies* to a given request is decided in
//! `RequestAuthScopeBuilder::build` (via `enrich_auth_context_with_scopes`),
//! which is unit-tested at that choke point: no protocol surface exposes one
//! request's resolved scope set, so an end-to-end assertion of "applies now,
//! not at 20:00" would have to move the server clock.

mod common;

use common::pgwire_harness::TestServer;

/// The grant used throughout: a business-hours, office-network entitlement.
const CONDITIONAL_GRANT: &str = "GRANT SCOPE 'ops:all' TO USER 'user_42' \
     WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS REQUIRE MFA \
     REQUIRE IP IN ('10.0.0.0/8')";

/// The rendered form `SHOW SCOPE GRANTS` must display for that grant.
const RENDERED: [&str; 3] = [
    "WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS",
    "REQUIRE MFA",
    "REQUIRE IP IN ('10.0.0.0/8')",
];

async fn show_scope_grants(server: &TestServer) -> Vec<String> {
    server
        .query_text_joined("SHOW SCOPE GRANTS")
        .await
        .expect("SHOW SCOPE GRANTS")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_grant_is_shown_and_survives_a_restart() {
    let server = TestServer::start().await;

    server
        .exec("DEFINE SCOPE 'ops:all' AS READ ON widgets")
        .await
        .expect("DEFINE SCOPE");
    server.exec(CONDITIONAL_GRANT).await.expect("GRANT SCOPE");

    let rows = show_scope_grants(&server).await;
    for clause in RENDERED {
        assert!(
            rows.iter().any(|row| row.contains(clause)),
            "SHOW SCOPE GRANTS must display {clause}: {rows:?}"
        );
    }

    // The conditions live in the system catalog, so a reopened server must
    // still know them — a grant that came back unconditional would be a
    // silent privilege widening at every restart.
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let rows = show_scope_grants(&server).await;
    for clause in RENDERED {
        assert!(
            rows.iter().any(|row| row.contains(clause)),
            "the grant's conditions must survive a restart, missing {clause}: {rows:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unconditional_grant_is_shown_as_such() {
    let server = TestServer::start().await;

    server
        .exec("GRANT SCOPE 'basic:all' TO USER 'user_7'")
        .await
        .expect("GRANT SCOPE");

    let rows = show_scope_grants(&server).await;
    let row = rows
        .iter()
        .find(|row| row.contains("basic:all"))
        .expect("the grant must be listed");
    assert!(
        !row.contains("REQUIRE") && !row.contains("WHEN BETWEEN"),
        "an unconditional grant must not display conditions: {row}"
    );
}

/// A malformed condition is a syntax error, never a grant that quietly
/// applies unconditionally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_conditions_are_rejected() {
    let server = TestServer::start().await;

    server
        .expect_error(
            "GRANT SCOPE 'ops:all' TO USER 'user_42' REQUIRE IP IN ('10.0.0.0/64')",
            "not a valid IP address or CIDR range",
        )
        .await;
    server
        .expect_error(
            "GRANT SCOPE 'ops:all' TO USER 'user_42' WHEN BETWEEN '09:00' AND '09:00'",
            "never open",
        )
        .await;
    server
        .expect_error(
            "GRANT SCOPE 'ops:all' TO USER 'user_42' REQUIRE TELEPATHY",
            "expected MFA, IP, STEP_UP, or DEVICE_TRUST",
        )
        .await;

    let rows = show_scope_grants(&server).await;
    assert!(
        !rows.iter().any(|row| row.contains("ops:all")),
        "a rejected statement must not leave a grant behind: {rows:?}"
    );
}
