// SPDX-License-Identifier: BUSL-1.1

//! An acknowledged write must survive a crash on every protocol, not just the
//! one protocol that happens to be covered.
//!
//! RESP `SET` and HTTP `POST /v1/query` route their local writes through the
//! gateway (`resp/gateway_dispatch.rs`, `http/routes/query/materialized.rs`),
//! a different path from the pgwire writes covered by `crash_recovery.rs`.
//! KV state is an in-memory hash table whose only durability is WAL replay or
//! the periodic checkpoint, so it is the sharpest probe available: a write
//! that never reached the WAL is simply gone after `kill -9`. These tests
//! pin that both protocols' acknowledged writes are durable, so a future
//! change to the gateway's dispatch or durability wiring cannot silently
//! regress the guarantee.
//!
//! Reading a value back MUST use the same protocol that wrote it. A value
//! written as a raw RESP blob does not project through a typed SQL column —
//! a pgwire `SELECT` returns an empty string for a RESP-written key with no
//! crash involved at all — so a cross-protocol read would report a
//! projection mismatch as data loss.
//!
//! The checkpoint interval is pushed far beyond the test's runtime so a
//! passing result can only mean the write was WAL-durable, never that a
//! checkpoint happened to rescue it.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::CrashHarness;
use crash_harness::resp_client::{self, Reply};

const PASSWORD: &str = "crash-resp-kv-secret-1";

/// Checkpoints happen on a periodic timer, and a checkpoint that happens to
/// land between the write and the kill would flush the in-memory KV state to
/// disk independent of the WAL — a false pass that proves nothing about the
/// durability bug under test. Pushing the interval out to an hour makes it
/// physically impossible for a checkpoint to fire during a test that
/// completes in seconds, so recovery can only come from WAL replay.
fn no_incidental_checkpoint() -> CrashHarness {
    CrashHarness::new().with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "3600")
}

/// The checkpoint interval override makes a mid-test checkpoint physically
/// impossible; this bound is a second, cheap guard against the harness
/// itself running slower than expected and accidentally crossing into
/// checkpoint territory anyway.
const MAX_TEST_WALL_CLOCK: Duration = Duration::from_secs(60);

// This test is RESP-write / RESP-read only: a value written as a raw RESP
// blob does not read back through a typed pgwire SQL column as the same
// string even with no crash involved (see `query_col` on `resp_kv_survive`
// pre-crash — it returns `[""]`). A cross-protocol read would conflate that
// projection mismatch with an actual durability failure, so RESP is used for
// both the write and the sole read-back assertion here.
#[tokio::test(flavor = "multi_thread")]
async fn resp_kv_set_survives_kill_9() {
    let mut h = no_incidental_checkpoint();
    let spawned_at = Instant::now();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION resp_kv_survive (id TEXT PRIMARY KEY, val TEXT) \
         WITH (engine='kv')",
    )
    .await;
    h.exec("CREATE USER resp_kv_user PASSWORD 'crash-resp-kv-secret-1'")
        .await;
    h.exec("GRANT ROLE readwrite TO resp_kv_user").await;

    let mut client = resp_client::session(
        resp_addr(h.resp_port),
        "resp_kv_user",
        PASSWORD,
        "resp_kv_survive",
    )
    .await;

    let set = client.cmd(&["SET", "k1", "durable-via-resp"]).await;
    assert_eq!(
        set,
        Reply::Simple("OK".to_string()),
        "RESP SET must ack with +OK: {set:?}"
    );

    // Pre-crash read-back over the SAME connection and protocol: rules out a
    // false positive where SET silently no-ops and there was never anything
    // to lose in the first place.
    let pre_crash = client.cmd(&["GET", "k1"]).await;
    assert_eq!(
        pre_crash,
        Reply::Bulk(Some("durable-via-resp".to_string())),
        "RESP GET must read back the value SET just wrote, before any crash: {pre_crash:?}"
    );

    assert!(
        spawned_at.elapsed() < MAX_TEST_WALL_CLOCK,
        "test ran long enough that an incidental checkpoint cycle becomes possible \
         even with the interval pushed out; tighten the test or the bound"
    );
    h.kill_9();
    h.reopen();

    // Primary durability check: read back over RESP, the same protocol that
    // wrote the value, on a fresh connection (the pre-crash socket died with
    // the killed process). This is the read path symmetric to the write
    // path, so a match here is direct evidence the write survived kill -9 +
    // WAL replay, with no cross-protocol projection in the way.
    let mut post_crash_client = resp_client::session(
        resp_addr(h.resp_port),
        "resp_kv_user",
        PASSWORD,
        "resp_kv_survive",
    )
    .await;
    let post_crash_resp = post_crash_client.cmd(&["GET", "k1"]).await;
    assert_eq!(
        post_crash_resp,
        Reply::Bulk(Some("durable-via-resp".to_string())),
        "a RESP SET acknowledged with +OK did not survive kill -9 + WAL replay, read back \
         over RESP (got {post_crash_resp:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn http_query_kv_write_survives_kill_9() {
    let mut h = no_incidental_checkpoint();
    let spawned_at = Instant::now();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION http_kv_survive (id TEXT PRIMARY KEY, val TEXT) \
         WITH (engine='kv')",
    )
    .await;

    // The HTTP query endpoint requires a bearer token even for the
    // superuser (default AuthMode::Password); mint one via SQL rather than
    // reaching into server internals, so the token comes from exactly the
    // path a real client would use.
    let token = h
        .query_col("CREATE API KEY FOR nodedb", "api_key")
        .await
        .into_iter()
        .next()
        .expect("CREATE API KEY must return the token row");

    let insert_status = post_query(
        h.http_port,
        &token,
        "INSERT INTO http_kv_survive (id, val) VALUES ('hk1', 'durable-via-http')",
    )
    .await
    .0;
    assert!(
        insert_status.is_success(),
        "HTTP INSERT must succeed: {insert_status}"
    );

    // Pre-crash read-back over the SAME protocol: rules out a false positive
    // where the INSERT silently no-ops.
    let (pre_status, pre_rows) = post_query(
        h.http_port,
        &token,
        "SELECT val FROM http_kv_survive WHERE id = 'hk1'",
    )
    .await;
    assert!(
        pre_status.is_success(),
        "pre-crash HTTP read-back must succeed: {pre_status}"
    );
    assert_eq!(
        pre_rows.first().and_then(|r| r["val"].as_str()),
        Some("durable-via-http"),
        "HTTP SELECT must read back the value INSERT just wrote, before any crash: {pre_rows:?}"
    );

    assert!(
        spawned_at.elapsed() < MAX_TEST_WALL_CLOCK,
        "test ran long enough that an incidental checkpoint cycle becomes possible \
         even with the interval pushed out; tighten the test or the bound"
    );
    h.kill_9();
    h.reopen();

    // Read back over pgwire — a different protocol than the one that wrote
    // the value — so recovery is proven independent of any HTTP-side read
    // quirk and is really coming from the server's own durable state.
    let recovered = h
        .query_col("SELECT val FROM http_kv_survive WHERE id = 'hk1'", "val")
        .await;
    assert_eq!(
        recovered,
        vec!["durable-via-http".to_string()],
        "an HTTP INSERT that returned success did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

/// Loopback socket address for the harness's RESP port.
fn resp_addr(port: u16) -> std::net::SocketAddr {
    format!("127.0.0.1:{port}")
        .parse()
        .expect("loopback RESP address must parse")
}

/// POST `sql` to `/v1/query` with bearer auth and return the status plus the
/// parsed `rows` array (empty if the body carries none).
async fn post_query(
    http_port: u16,
    token: &str,
    sql: &str,
) -> (reqwest::StatusCode, Vec<serde_json::Value>) {
    let url = format!("http://127.0.0.1:{http_port}/v1/query");
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .expect("POST /v1/query");
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    let rows = body
        .get("rows")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();
    (status, rows)
}
