// SPDX-License-Identifier: BUSL-1.1

//! Does a write routed through the Calvin scheduler survive `kill -9` on an
//! ordinary single-node boot? ILP routes to Calvin unconditionally and lands
//! in the WAL-only `TimeseriesMemtable`; `wal.wait_durable` has zero callers
//! under `control/cluster/calvin/`. The pre-crash pgwire read rules out a
//! visibility quirk being mistaken for post-crash data loss.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::CrashHarness;
use crash_harness::Session;
use nodedb_test_support::ilp_client;

const ILP_PASSWORD: &str = "crash-ilp-ts-secret-1";
const COLLECTION: &str = "crash_ilp_ts";

/// An incidental checkpoint between the ILP send and the kill would flush
/// the memtable independent of the WAL, producing a false pass. Pushing the
/// interval out to an hour makes that impossible within test runtime.
fn no_incidental_checkpoint() -> CrashHarness {
    CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "3600")
        // `with_env` is the only way `RUST_LOG` reaches the spawned child — a
        // shell-level `RUST_LOG` does not propagate. Raise just the ILP
        // modules so a poll failure is diagnosable from the server log.
        .with_env(
            "RUST_LOG",
            "warn,nodedb::control::server::ilp_listener=debug,nodedb::control::server::ilp_batch=debug",
        )
}

/// Cheap second guard against the harness itself running slower than
/// expected and accidentally crossing into checkpoint territory anyway.
const MAX_TEST_WALL_CLOCK: Duration = Duration::from_secs(60);

/// Poll `SELECT COUNT(*) FROM <collection>` until it reads back `expected`,
/// or panic with the last observed value once `timeout` elapses. Takes an
/// already-open `Session`, not `CrashHarness` — a connection per attempt
/// would trip the server's login rate limiter before the row is visible.
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
        // A descriptor-lease drain is common right after `reopen()`; absorb it
        // here and let this deadline, not the session helper's, decide failure.
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

/// A write visible to a pgwire reader is what an ILP caller treats as "the
/// write happened" — ILP has no per-line ack. Asks whether it survives
/// `kill -9` + WAL replay.
#[tokio::test(flavor = "multi_thread")]
async fn ilp_write_visible_to_readers_survives_kill_9() {
    let mut h = no_incidental_checkpoint();
    let spawned_at = Instant::now();
    h.spawn();
    h.wait_ready_extended();
    // `/healthz` doesn't imply Calvin has elected a leader, and ILP has no
    // retry for that race, so wait for an actual Calvin write to succeed first.
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

    // ILP acks nothing per line; the read confirms the write pre-crash so a
    // post-crash absence can only mean the write was lost.
    let pre_crash_session = h.connect().await;
    wait_for_count(&pre_crash_session, COLLECTION, "1", Duration::from_secs(20)).await;
    // This session's server is about to be SIGKILLed; drop it now.
    drop(pre_crash_session);
    // Keep the ILP connection open until the write is confirmed visible above,
    // or dropping it earlier races the server's own batch flush.
    drop(ilp_stream);

    assert!(
        spawned_at.elapsed() < MAX_TEST_WALL_CLOCK,
        "test ran long enough that an incidental checkpoint cycle becomes possible \
         even with the interval pushed out; tighten the test or the bound"
    );

    // Do not insert any other write here — it risks an incidental fsync that
    // would durably rescue this record for an unrelated reason.
    h.kill_9();
    h.reopen();

    // `kill_9` destroyed the pre-crash process; this must be a fresh connection.
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
/// The adaptive batch flush caps at 10,000 lines at the highest rate tier;
/// 12,000 guarantees at least two size-triggered flushes regardless of send
/// timing, unlike relying on the timer windows.
const BULK_LINE_COUNT: u64 = 12_000;

/// Generous but bounded: 12,000 individual ILP lines take far longer than
/// the single-write test above. Exists only to catch a hung test — the
/// checkpoint interval is still pushed out an hour.
const MAX_BULK_TEST_WALL_CLOCK: Duration = Duration::from_secs(180);

/// Sharper version of `ilp_write_visible_to_readers_survives_kill_9`: sends
/// thousands of writes spanning multiple independent ILP batch flushes (see
/// [`BULK_LINE_COUNT`]), killing on the exact poll that observes all of them
/// visible. If writes are acked before their WAL record fsyncs, some rows
/// go missing after reopen; either outcome is informative.
#[tokio::test(flavor = "multi_thread")]
async fn many_calvin_writes_survive_immediate_kill_9() {
    let mut h = no_incidental_checkpoint();
    let spawned_at = Instant::now();
    h.spawn();
    h.wait_ready_extended();
    // `/healthz` doesn't imply Calvin has a leader yet, and ILP has no retry.
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

    // Visibility is the only completion signal ILP gives; the kill below must
    // follow this poll with nothing else in between.
    let pre_crash_session = h.connect().await;
    wait_for_count(
        &pre_crash_session,
        BULK_COLLECTION,
        &BULK_LINE_COUNT.to_string(),
        Duration::from_secs(60),
    )
    .await;
    drop(pre_crash_session);
    // Hold the ILP connection open until every line is confirmed visible.
    drop(ilp_stream);

    assert!(
        spawned_at.elapsed() < MAX_BULK_TEST_WALL_CLOCK,
        "test ran long enough that an incidental checkpoint cycle becomes possible \
         even with the interval pushed out; tighten the test or the bound"
    );

    // Do not insert any other write or query here — it risks an incidental
    // fsync that would durably rescue these records for an unrelated reason.
    h.kill_9();
    h.reopen();

    // Check for a wedged applier before the row-count assertion, so a hung
    // apply loop and plain data loss never look like the same failure.
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
