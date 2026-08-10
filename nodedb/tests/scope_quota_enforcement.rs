// SPDX-License-Identifier: BUSL-1.1

//! Per-scope token quotas must be definable, durable, and enforcing.
//!
//! `QuotaDefinition`, `QuotaManager::check_quota`, and the
//! `quota_remaining.*` / `quota_pct.*` scope enrichment all existed with no
//! way for an operator to create a definition: `define_quota` had no
//! non-test caller, so `get_status` always answered `None`, the enrichment
//! never appeared, and `QuotaEnforcement::Hard` could never refuse anything.
//!
//! Contracts asserted here:
//! - `DEFINE QUOTA` creates a definition `SHOW QUOTAS` displays
//! - the definition survives a restart (it is a catalog object, not a
//!   process-local map — a rolling deploy must not lift every cap)
//! - `DROP QUOTA` removes it, and dropping a quota that was never defined is
//!   an error rather than a silent success
//! - a malformed definition is refused rather than stored in a form that
//!   never enforces

mod common;

use common::pgwire_harness::TestServer;

const SCOPE: &str = "ops:all";

async fn show_quotas(server: &TestServer) -> Vec<String> {
    server
        .query_text_joined("SHOW QUOTAS")
        .await
        .expect("SHOW QUOTAS")
}

/// Define a quota on [`SCOPE`] with the given enforcement mode.
async fn define(server: &TestServer, enforcement: &str) {
    server
        .exec(&format!(
            "DEFINE QUOTA ON SCOPE '{SCOPE}' MAX 1000 TOKENS PER 3600 SECONDS \
             ENFORCEMENT {enforcement} WARN AT 0.5"
        ))
        .await
        .expect("DEFINE QUOTA");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn defined_quota_is_shown_and_survives_a_restart() {
    let server = TestServer::start().await;
    define(&server, "HARD").await;

    let rows = show_quotas(&server).await;
    assert!(
        rows.iter()
            .any(|row| row.contains(SCOPE) && row.contains("1000")),
        "SHOW QUOTAS must display the defined quota: {rows:?}"
    );

    // The definition lives in the system catalog, so a reopened server must
    // still have it — a cap that came back absent would be a silent lifting
    // of every ceiling at each restart.
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let rows = show_quotas(&server).await;
    let row = rows
        .iter()
        .find(|row| row.contains(SCOPE))
        .expect("the quota must survive a restart");
    assert!(
        row.contains("1000") && row.contains("3600") && row.contains("hard"),
        "every field of the definition must survive, not just the scope: {row}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_quota_removes_it() {
    let server = TestServer::start().await;
    define(&server, "SOFT").await;

    server
        .exec(&format!("DROP QUOTA ON SCOPE '{SCOPE}'"))
        .await
        .expect("DROP QUOTA");

    let rows = show_quotas(&server).await;
    assert!(
        !rows.iter().any(|row| row.contains(SCOPE)),
        "the dropped quota must be gone: {rows:?}"
    );
}

/// Dropping a quota that was never defined must be an error. Reporting
/// success would tell an operator a cap is gone when nothing changed —
/// which, for a typo'd scope name, leaves the real cap in force.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_an_undefined_quota_is_an_error() {
    let server = TestServer::start().await;
    server
        .expect_error(
            "DROP QUOTA ON SCOPE 'never:defined'",
            "no quota defined on scope",
        )
        .await;
}

/// A malformed definition is refused outright. The dangerous failure mode is
/// the opposite: storing something that parses but never enforces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_definitions_are_rejected() {
    let server = TestServer::start().await;

    server
        .expect_error(
            "DEFINE QUOTA ON SCOPE 'bad:mode' MAX 10 TOKENS PER 60 SECONDS ENFORCEMENT HRAD",
            "unknown quota enforcement",
        )
        .await;
    server
        .expect_error(
            "DEFINE QUOTA ON SCOPE 'bad:period' MAX 10 TOKENS PER 0 SECONDS ENFORCEMENT HARD",
            "at least one second",
        )
        .await;
    server
        .expect_error(
            "DEFINE QUOTA ON SCOPE 'bad:max' MAX plenty TOKENS PER 60 SECONDS ENFORCEMENT HARD",
            "whole number",
        )
        .await;
    server
        .expect_error(
            "DEFINE QUOTA ON SCOPE 'bad:warn' MAX 10 TOKENS PER 60 SECONDS \
             ENFORCEMENT HARD WARN AT 4.2",
            "between 0.0 and 1.0",
        )
        .await;

    let rows = show_quotas(&server).await;
    assert!(
        !rows.iter().any(|row| row.contains("bad:")),
        "a rejected statement must not leave a definition behind: {rows:?}"
    );
}
