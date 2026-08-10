// SPDX-License-Identifier: BUSL-1.1

//! A `HARD` scope quota must actually refuse once its cap is spent.
//!
//! The definition path is covered by `scope_quota_enforcement.rs`. This is
//! the other half: that a defined cap changes what the server does.
//!
//! Enforcement cannot live where the charge does. `meter_dispatch` runs only
//! after a task has succeeded — a denied or errored request performed no
//! billable work — so by the time it sees the overspend, the work it would
//! have refused is already done. The refusal therefore runs pre-dispatch, in
//! `quota_admission::admit_quota_for_dispatch`, against the same coverage
//! rule the charge uses.
//!
//! Metering is disabled by default, and both accounting and enforcement are
//! no-ops while it is, so these tests start a server with it turned on.

mod common;

use common::pgwire_harness::TestServer;
use nodedb::control::security::metering::config::MeteringConfig;

const SCOPE: &str = "quota:reads";
const COLLECTION: &str = "quota_widgets";

fn metering_on() -> MeteringConfig {
    MeteringConfig {
        enabled: true,
        ..Default::default()
    }
}

/// The numeric user id the harness client authenticates as.
///
/// A scope grant is keyed by grantee id, and quota consumption is tracked per
/// `"{scope}:{grantee}"` — so the grant has to name the id this session's
/// requests actually resolve to. Read from the server rather than assumed,
/// since a wrong id yields a grant that silently never applies and a test
/// that passes for the wrong reason.
async fn harness_user_id(server: &TestServer) -> String {
    let rows = server.query_rows("SHOW USERS").await.expect("SHOW USERS");
    let row = rows
        .iter()
        .find(|row| row.iter().any(|field| field == "nodedb"))
        .unwrap_or_else(|| panic!("the harness user must be listed: {rows:?}"));
    row.iter()
        .find(|field| field.parse::<u64>().is_ok())
        .unwrap_or_else(|| panic!("SHOW USERS row must carry a numeric id: {row:?}"))
        .clone()
}

/// Start a metered server holding a read scope on [`COLLECTION`], with one
/// row to read, and return it alongside the grantee id.
async fn metered_server_with_scope() -> TestServer {
    let server = TestServer::start_with_metering(metering_on()).await;
    server
        .exec(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("create collection");
    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (id, name) VALUES ('a', 'first')"
        ))
        .await
        .expect("seed a row");

    let user_id = harness_user_id(&server).await;
    server
        .exec(&format!("DEFINE SCOPE '{SCOPE}' AS READ ON {COLLECTION}"))
        .await
        .expect("DEFINE SCOPE");
    server
        .exec(&format!("GRANT SCOPE '{SCOPE}' TO USER '{user_id}'"))
        .await
        .expect("GRANT SCOPE");

    server
}

/// Read until the quota refuses, returning the number of reads that were
/// admitted. Bounded so a cap that never bites fails the test rather than
/// looping forever.
async fn reads_until_refused(server: &TestServer) -> Option<usize> {
    for attempt in 0..20 {
        match server
            .query_text_joined(&format!("SELECT name FROM {COLLECTION}"))
            .await
        {
            Ok(_) => continue,
            Err(e) => {
                assert!(
                    e.contains("quota exceeded"),
                    "the refusal must come from the quota gate, not some other guard: {e}"
                );
                return Some(attempt);
            }
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hard_quota_refuses_once_the_cap_is_spent() {
    let server = metered_server_with_scope().await;
    // One token: any single read charges more than this (`document_scan`
    // costs 5), so the cap is spent after the first query and the next one
    // must be refused.
    server
        .exec(&format!(
            "DEFINE QUOTA ON SCOPE '{SCOPE}' MAX 1 TOKENS PER 3600 SECONDS ENFORCEMENT HARD"
        ))
        .await
        .expect("DEFINE QUOTA");

    let admitted = reads_until_refused(&server).await;
    assert!(
        admitted.is_some(),
        "a HARD quota of 1 token must refuse a read once consumption exceeds it; \
         20 reads all succeeded, so the cap never bit"
    );
}

/// The control: the same scope, the same reads, a cap large enough to cover
/// them. Without this, the test above would also pass if every read were
/// refused for some unrelated reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_within_the_cap_are_admitted() {
    let server = metered_server_with_scope().await;
    server
        .exec(&format!(
            "DEFINE QUOTA ON SCOPE '{SCOPE}' MAX 1000000 TOKENS PER 3600 SECONDS ENFORCEMENT HARD"
        ))
        .await
        .expect("DEFINE QUOTA");

    assert!(
        reads_until_refused(&server).await.is_none(),
        "reads well inside the cap must never be refused"
    );
}

/// `SOFT` accounts but never blocks. A soft cap that refused would turn a
/// warning-only mode into an outage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soft_quota_never_refuses() {
    let server = metered_server_with_scope().await;
    server
        .exec(&format!(
            "DEFINE QUOTA ON SCOPE '{SCOPE}' MAX 1 TOKENS PER 3600 SECONDS ENFORCEMENT SOFT"
        ))
        .await
        .expect("DEFINE QUOTA");

    assert!(
        reads_until_refused(&server).await.is_none(),
        "SOFT enforcement must warn and allow, never refuse"
    );
}
