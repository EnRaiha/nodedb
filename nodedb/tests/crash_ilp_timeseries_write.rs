// SPDX-License-Identifier: BUSL-1.1

//! Does a write routed through the Calvin scheduler survive `kill -9` on an
//! ordinary single-node boot?
//!
//! `single_node_calvin` defaults to true (`config/server/section.rs`), so
//! Calvin schedulers are live on every single-node server, not just
//! clustered deployments. ILP ingest routes to Calvin unconditionally
//! (`control/server/ilp_batch.rs`), and lands in the in-RAM
//! `TimeseriesMemtable` — WAL-only until a flush. `wal.wait_durable`, the
//! only fsync barrier in the codebase, has exactly one caller
//! (`dispatch_utils/submit_write.rs`) and zero callers anywhere under
//! `control/cluster/calvin/`. This test exercises exactly that path: it does
//! not assert which way durability goes, only that a write visible to a
//! reader is (or is not) still there after a hard crash and WAL replay.
//!
//! ILP is ingest-only and exposes no query surface of its own, so reading
//! the write back is unavoidably cross-protocol (pgwire). The pre-crash
//! pgwire read in this test exists specifically to rule out the trap that
//! bit an earlier version of this kind of test: without it, a pgwire-side
//! projection or visibility quirk (e.g. the row existing but not yet
//! reflected in a `COUNT(*)`) could be mistaken for data loss after the
//! crash. Proving the read path works BEFORE any crash is involved isolates
//! the post-crash assertion to durability alone.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::CrashHarness;
use crash_harness::Session;
use crash_harness::ilp_client;

const ILP_PASSWORD: &str = "crash-ilp-ts-secret-1";
const COLLECTION: &str = "crash_ilp_ts";

/// Same rationale as the RESP/HTTP KV crash tests: an incidental checkpoint
/// landing between the ILP send and the kill would flush the in-memory
/// timeseries memtable to disk independent of the WAL, producing a false
/// pass that proves nothing about the durability path under test. Pushing
/// the interval out to an hour makes that physically impossible for a test
/// that completes in seconds.
fn no_incidental_checkpoint() -> CrashHarness {
    CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "3600")
        // `with_env` is the only way `RUST_LOG` reaches the spawned child — a
        // shell-level `RUST_LOG` does not propagate (see `CrashHarness::spawn`,
        // which reads `extra_env` and otherwise defaults to `error`). At the
        // default level, ILP connection accept/auth/flush activity is
        // completely silent: a failure of this test's poll leaves no trace of
        // whether the line was ever read, batched, or flushed. Raise just the
        // ILP modules so a future failure is diagnosable from the server log
        // instead of reproducing today's "zero mentions of ILP" mystery.
        .with_env(
            "RUST_LOG",
            "warn,nodedb::control::server::ilp_listener=debug,nodedb::control::server::ilp_batch=debug",
        )
}

/// Cheap second guard against the harness itself running slower than
/// expected and accidentally crossing into checkpoint territory anyway.
const MAX_TEST_WALL_CLOCK: Duration = Duration::from_secs(60);

/// Poll `SELECT COUNT(*) FROM <collection>` until it reads back `expected`,
/// or panic with the last observed value once `timeout` elapses.
///
/// Takes an already-open `Session` rather than the `CrashHarness` itself: a
/// poll loop that opened a fresh pgwire connection per attempt (as
/// `CrashHarness::query_col_idx` does for one-shot callers) would flood the
/// server with logins and trip its login rate limiter
/// (`E53300: too many login attempts`) well before the row ever became
/// visible. Reusing one connection removes the per-attempt login cost
/// entirely, so the caller is expected to open the session once, before the
/// loop starts, and pass it in.
async fn wait_for_count(
    session: &Session<'_>,
    collection: &str,
    expected: &str,
    timeout: Duration,
) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let rows = session
            .query_col_idx(&format!("SELECT COUNT(*) FROM {collection}"), 0)
            .await;
        if rows.first().map(|v| v.as_str()) == Some(expected) {
            return rows;
        }
        if Instant::now() >= deadline {
            panic!(
                "SELECT COUNT(*) FROM {collection} never reached {expected} within {timeout:?}; \
                 last observed: {rows:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A write visible to a pgwire reader before any crash is involved is what
/// an ILP client's caller would treat as "the write happened" — there is no
/// per-line ack on the wire to tell them otherwise. This test asks whether
/// that same write is still there after `kill -9` + WAL replay.
#[tokio::test(flavor = "multi_thread")]
async fn ilp_write_visible_to_readers_survives_kill_9() {
    let mut h = no_incidental_checkpoint();
    let spawned_at = Instant::now();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION crash_ilp_ts \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await;
    h.exec(&format!(
        "CREATE USER crash_ilp_user PASSWORD '{ILP_PASSWORD}'"
    ))
    .await;
    h.exec("GRANT ROLE readwrite TO crash_ilp_user").await;

    let ilp_addr: std::net::SocketAddr = format!("127.0.0.1:{}", h.ilp_port)
        .parse()
        .expect("loopback ILP address must parse");
    let mut ilp_stream =
        ilp_client::connect_and_auth(ilp_addr, "crash_ilp_user", ILP_PASSWORD).await;

    let ts_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    ilp_client::send_line(
        &mut ilp_stream,
        &format!("crash_ilp_ts,metric=cpu value=42.5 {ts_ns}"),
    )
    .await;

    // ILP acks nothing per line; the only signal that the write happened is
    // another reader observing it. This also doubles as the pre-crash
    // sanity check described at the top of the file: it must succeed BEFORE
    // any crash, so a post-crash absence can only mean the write was lost,
    // never that the read path itself never worked.
    //
    // One session, opened before the poll starts and reused for every
    // attempt — see `wait_for_count` for why a connect-per-attempt loop is
    // unsafe here.
    let pre_crash_session = h.connect().await;
    wait_for_count(&pre_crash_session, COLLECTION, "1", Duration::from_secs(20)).await;
    // The server this session is connected to is about to be SIGKILLed;
    // drop the session now so nothing later mistakes it for a live
    // connection to the reopened process.
    drop(pre_crash_session);
    // The ILP connection must stay open until the write it sent is actually
    // observed above: dropping it earlier races the server's own batch flush
    // (size threshold, adaptive line-count target, or the coalescing timer —
    // see `handle_ilp_connection` in `ilp_listener.rs`) against this poll,
    // which would make the test's pass/fail depend on client-side timing
    // instead of the server's durability behavior under test. Only close it
    // now that the row has been confirmed visible.
    drop(ilp_stream);

    assert!(
        spawned_at.elapsed() < MAX_TEST_WALL_CLOCK,
        "test ran long enough that an incidental checkpoint cycle becomes possible \
         even with the interval pushed out; tighten the test or the bound"
    );

    // Do NOT insert any other write here. The write under test just became
    // visible to a reader; issuing any other write on this shared WAL before
    // the kill risks an incidental fsync (group commit, WAL rollover, etc.)
    // that would durably rescue this record for a reason unrelated to the
    // question this test asks. The kill must follow the read with nothing
    // else in between.
    h.kill_9();
    h.reopen();

    // `kill_9` destroyed the process the pre-crash session was connected
    // to, so this MUST be a fresh connection against the reopened process,
    // never the dropped session above.
    let post_crash_session = h.connect().await;
    let recovered = wait_for_count(
        &post_crash_session,
        COLLECTION,
        "1",
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(
        recovered,
        vec!["1".to_string()],
        "an ILP write that was visible to a pgwire reader before the crash did not survive \
         kill -9 + WAL replay (got {recovered:?})"
    );
}
