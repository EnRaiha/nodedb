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
//!
//! ILP itself has no retry for a transient `no sequencer leader elected
//! yet` error on the Calvin submit path — `handle_ilp_connection`
//! (`control/server/ilp_listener.rs`) just logs it and drops the
//! connection, unlike the pgwire path, which retries the same condition
//! (`crash_harness::pgwire::simple_query_ready`). That is a real
//! server-side gap, not something this test file should paper over
//! silently: both tests below call `CrashHarness::wait_for_calvin_ready`
//! after `wait_ready` and before their first ILP line specifically to work
//! around it, so a write into that startup window is never mistaken for
//! the durability bug this file exists to probe.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::CrashHarness;
use crash_harness::Session;
use nodedb_test_support::ilp_client;

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
    // Assigned by both arms of the match below before the deadline check reads
    // it, so no placeholder initial value is needed.
    let mut last: Result<Vec<String>, &str>;
    loop {
        // A descriptor-lease drain is exactly the "not settled yet" condition
        // this loop exists to wait out, and it is common right after `reopen()`
        // while boot replay re-establishes descriptors. Panicking on it inside
        // the loop would abandon the remaining budget over a condition the
        // server explicitly asks clients to retry, so absorb it here and let
        // this deadline — not the session helper's much shorter one — decide
        // when the wait has genuinely failed.
        match session
            .try_query_col_idx(&format!("SELECT COUNT(*) FROM {collection}"), 0)
            .await
        {
            Ok(rows) => {
                if rows.first().map(|v| v.as_str()) == Some(expected) {
                    return rows;
                }
                last = Ok(rows);
            }
            Err(crash_harness::RetryableSchemaChange) => {
                last = Err("retryable schema change still unresolved");
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "SELECT COUNT(*) FROM {collection} never reached {expected} within {timeout:?}; \
                 last observed: {last:?}"
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
    // `/healthz` (just polled above) reports ready before the Calvin
    // sequencer has necessarily elected a leader, and the ILP write below
    // has no retry for that race — see the file-level doc comment. Wait for
    // an actual Calvin-routed write to succeed before sending any ILP line.
    h.wait_for_calvin_ready(Duration::from_secs(20)).await;

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

const BULK_COLLECTION: &str = "crash_ilp_ts_bulk";
const BULK_ILP_PASSWORD: &str = "crash-ilp-ts-bulk-secret-1";

/// Number of ILP lines sent by [`many_calvin_writes_survive_immediate_kill_9`].
///
/// `handle_ilp_connection` (`control/server/ilp_listener.rs`) flushes a batch
/// once it hits an adaptive line-count target: 100 lines at low observed
/// rate, 1,000 at medium rate, up to a hard-coded maximum of 10,000 lines at
/// the highest rate tier (`IlpRateEstimator::suggest_batch_params`,
/// `control/server/ilp_batch.rs`). That maximum is a compile-time constant,
/// not something the client can influence, so any line count above it forces
/// at least two size-triggered flushes no matter how fast or slow this test's
/// single-line `send_line` loop happens to run — unlike relying on the
/// 50/100ms timer windows, which depend on send timing and would make the
/// "spans multiple flushes" property a race instead of a guarantee. 12,000 is
/// comfortably above that 10,000-line ceiling.
const BULK_LINE_COUNT: u64 = 12_000;

/// Generous but bounded: sending 12,000 individual ILP lines, each its own
/// `write` + `flush` syscall pair over loopback, plus waiting for all of them
/// to become visible through pgwire, takes far longer than the single-write
/// `ilp_write_visible_to_readers_survives_kill_9` above. The checkpoint
/// interval is still pushed out to an hour (`no_incidental_checkpoint`), so
/// this bound exists only to catch a hung test, not to protect against an
/// incidental checkpoint.
const MAX_BULK_TEST_WALL_CLOCK: Duration = Duration::from_secs(180);

/// Sharper measurement of the same question as
/// `ilp_write_visible_to_readers_survives_kill_9`, but built to close that
/// test's two weaknesses: a single write, and an uncontrolled window between
/// "write visible" and "kill". Here, thousands of Calvin-routed writes are
/// sent — guaranteed (see [`BULK_LINE_COUNT`]) to span multiple independent
/// ILP batch flushes, each its own `TimeseriesIngest` Calvin submission — and
/// the kill happens on the exact poll that first observes every one of them
/// visible, with nothing else run in between.
///
/// If claim A2 is right (Calvin-routed writes are acknowledged/visible before
/// their WAL record is fsynced), this test FAILS: some prefix or scatter of
/// the rows that were visible pre-crash will be missing after reopen, because
/// only `wal.wait_durable` fsyncs and nothing on the `RouteToCalvin` path
/// calls it. If A2 is wrong — durability is established by some other means
/// before a row becomes visible to a reader — this test PASSES. Both
/// outcomes are informative; this test does not assume which one is true.
#[tokio::test(flavor = "multi_thread")]
async fn many_calvin_writes_survive_immediate_kill_9() {
    let mut h = no_incidental_checkpoint();
    let spawned_at = Instant::now();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));
    // Same rationale as the single-write test above: `/healthz` does not
    // imply Calvin has a leader yet, and ILP has no retry for that race.
    h.wait_for_calvin_ready(Duration::from_secs(20)).await;

    h.exec(&format!(
        "CREATE COLLECTION {BULK_COLLECTION} \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
         WITH (engine='timeseries')"
    ))
    .await;
    h.exec(&format!(
        "CREATE USER crash_ilp_bulk_user PASSWORD '{BULK_ILP_PASSWORD}'"
    ))
    .await;
    h.exec("GRANT ROLE readwrite TO crash_ilp_bulk_user").await;

    let ilp_addr: std::net::SocketAddr = format!("127.0.0.1:{}", h.ilp_port)
        .parse()
        .expect("loopback ILP address must parse");
    let mut ilp_stream =
        ilp_client::connect_and_auth(ilp_addr, "crash_ilp_bulk_user", BULK_ILP_PASSWORD).await;

    // Distinct nanosecond timestamps per line so every one of the
    // `BULK_LINE_COUNT` writes is its own row rather than colliding on the
    // engine's (partition, ts) identity.
    let base_ts_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    for i in 0..BULK_LINE_COUNT {
        let ts_ns = base_ts_ns + u128::from(i);
        ilp_client::send_line(
            &mut ilp_stream,
            &format!("{BULK_COLLECTION},metric=cpu value=42.5 {ts_ns}"),
        )
        .await;
    }

    // Visibility is the only completion signal ILP gives a caller — see
    // `wait_for_count`'s doc comment. This poll observing `BULK_LINE_COUNT`
    // is the exact event the kill below must follow with nothing else in
    // between.
    let pre_crash_session = h.connect().await;
    wait_for_count(
        &pre_crash_session,
        BULK_COLLECTION,
        &BULK_LINE_COUNT.to_string(),
        Duration::from_secs(60),
    )
    .await;
    drop(pre_crash_session);
    // Same rationale as the single-write test above: hold the ILP connection
    // open until every line it sent is confirmed visible, so closing it
    // cannot race the server's own batch flush.
    drop(ilp_stream);

    assert!(
        spawned_at.elapsed() < MAX_BULK_TEST_WALL_CLOCK,
        "test ran long enough that an incidental checkpoint cycle becomes possible \
         even with the interval pushed out; tighten the test or the bound"
    );

    // Do NOT insert any other write or query here. The poll above just
    // observed every one of the BULK_LINE_COUNT writes visible to a reader;
    // issuing any other write on this shared WAL before the kill risks an
    // incidental fsync (group commit, WAL rollover, etc.) that would durably
    // rescue these records for a reason unrelated to the question this test
    // asks. The kill must follow the poll with nothing else in between.
    h.kill_9();
    h.reopen();

    // Check for a wedged applier BEFORE asking whether the rows survived, so
    // a wedge (a hung/degraded apply loop) and plain data loss never look
    // like the same failure. `nodedb.metadata_apply_wedged` fires from the
    // metadata Raft applier's own apply loop during boot replay — independent
    // of any query this test issues — and `nodedb.calvin_completion_timeout`
    // fires when a Calvin write's completion ack never arrives. Either one
    // present here means the row-count assertion below (if it also fails)
    // is not evidence about claim A2's fsync-before-ack ordering; it would
    // be evidence of a different, unrelated bug.
    let reports = crash_harness::diagnostics::faultbox_reports(h.data_dir());
    let wedge_indicators: Vec<String> = reports
        .iter()
        .filter(|g| {
            matches!(
                g.first.domain_kind.as_deref(),
                Some("nodedb.metadata_apply_wedged") | Some("nodedb.calvin_completion_timeout")
            )
        })
        .map(faultbox::reader::Group::summary)
        .collect();
    assert!(
        wedge_indicators.is_empty(),
        "the server filed a wedged-applier / Calvin-completion-timeout report after reopen — \
         a stalled apply loop or a lost completion ack, not claim A2's fsync-before-ack \
         ordering, would explain any missing rows below: {wedge_indicators:?} \
         (all faultbox reports: {:?})",
        reports
            .iter()
            .map(faultbox::reader::Group::summary)
            .collect::<Vec<_>>(),
    );

    let post_crash_session = h.connect().await;
    let recovered = wait_for_count(
        &post_crash_session,
        BULK_COLLECTION,
        &BULK_LINE_COUNT.to_string(),
        Duration::from_secs(60),
    )
    .await;
    assert_eq!(
        recovered,
        vec![BULK_LINE_COUNT.to_string()],
        "{BULK_LINE_COUNT} Calvin-routed ILP writes that were visible to a pgwire reader before \
         the crash did not all survive kill -9 + WAL replay (got {recovered:?} of \
         {BULK_LINE_COUNT}); this means at least one write completed/became visible before its \
         WAL record was fsynced"
    );
}
