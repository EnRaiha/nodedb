// SPDX-License-Identifier: BUSL-1.1

//! Replay idempotency when recovery itself is interrupted.
//!
//! Every other crash test crashes the server during normal operation and then
//! lets recovery run once, to completion. That proves replay restores an
//! acknowledged write; it says nothing about replay being *re-runnable*. A
//! recovering server is exactly as crashable as a serving one — a power cut
//! during boot lands mid-replay — and the next boot then replays the same WAL
//! on top of whatever the interrupted attempt already made durable.
//!
//! That second replay has two ways to be wrong, and they point at opposite
//! bugs:
//!
//! * **Rows missing** — replay treated some record as already applied (a
//!   watermark advanced, a marker written) when the interrupted pass had not
//!   actually made it durable. Acknowledged data is gone.
//! * **Rows duplicated** — replay re-applied a record whose effect the
//!   interrupted pass *had* made durable, and the effect is not an idempotent
//!   overwrite. Timeseries ingest is an append: nothing upserts a second copy
//!   away, so a double-apply is permanent.
//!
//! Presence checks cannot tell those apart, so every assertion here is on an
//! exact count.
//!
//! The interruption point is unreachable from outside the process — the whole
//! replay sequence finishes before the server ever opens a listener — so the
//! crash is injected from within, via a `NODEDB_FAILPOINTS` `abort` armed for
//! ONE boot only. Two points are covered, chosen to bracket the failure modes:
//!
//! * `replay::kv_mid_pass` — part way through a single engine's records, with
//!   earlier engines' passes already complete.
//! * `replay::between_standalone_and_redo` — every standalone engine pass done,
//!   the redo-only document / graph arms not yet run.
//!
//! Writes span more than one engine (KV, document_strict, timeseries) because
//! replay walks them in one globally LSN-ordered pass: an interruption part way
//! through leaves *some* engines applied and others not, which is the state a
//! single-engine test can never produce.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use crash_harness::{CrashHarness, diagnostics};
use std::collections::BTreeMap;
use std::time::Duration;

/// Rows written to each engine before the first crash. Small enough to keep
/// the test quick, more than one so a partial replay has somewhere to stop.
const ROWS: u32 = 6;

/// Bounded wait for the injected abort during replay. A timeout means the
/// fail point never fired, so the test would prove nothing — `await_self_crash`
/// panics rather than continuing.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

/// `SELECT COUNT(*)` as a number.
///
/// Counts, never presence: a presence check passes just as happily when replay
/// applied the row twice, which is half of what this test exists to catch.
async fn count_rows(h: &CrashHarness, collection: &str) -> u64 {
    let rows = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {collection}"), 0)
        .await;
    assert_eq!(rows.len(), 1, "expected one COUNT(*) row, got {rows:?}");
    rows[0]
        .parse()
        .unwrap_or_else(|e| panic!("COUNT(*) for {collection} was not a number: {rows:?}: {e}"))
}

/// Assert a recovered row count is EXACTLY the acknowledged count, naming which
/// fail point was armed and which direction the divergence went.
///
/// The two directions are different bugs — under-count means replay skipped
/// work the interrupted pass had not durably done, over-count means replay
/// redid work it already had — so they must never be reported with one
/// undifferentiated message.
fn assert_exact_count(actual: u64, expected: u64, collection: &str, fail_point: &str) {
    if actual < expected {
        panic!(
            "ROWS MISSING after re-replay: {collection} has {actual} of {expected} acknowledged \
             rows after a crash at fail point `{fail_point}` during WAL replay, followed by a \
             clean replay of the same WAL. The second replay skipped {} record(s) the \
             interrupted pass had NOT durably applied — acknowledged data is lost.",
            expected - actual
        );
    }
    if actual > expected {
        panic!(
            "ROWS DUPLICATED after re-replay: {collection} has {actual} rows but only {expected} \
             were ever acknowledged, after a crash at fail point `{fail_point}` during WAL replay \
             followed by a clean replay of the same WAL. The second replay re-applied {} \
             record(s) whose effect the interrupted pass HAD already made durable — replay is \
             not idempotent.",
            actual - expected
        );
    }
}

/// Faultbox groups keyed by fingerprint with their occurrence counts, so a
/// later snapshot can tell a report filed during the successful replay from one
/// that was already on disk.
fn report_counts(data_dir: &std::path::Path) -> BTreeMap<String, u64> {
    diagnostics::faultbox_reports(data_dir)
        .into_iter()
        .map(|g| (g.first.fingerprint.clone(), g.occurrences()))
        .collect()
}

/// Fail the test if the successful replay filed a new `InvariantViolation` or
/// `Corruption` report.
///
/// Row counts alone can be right while the server knew something was wrong and
/// papered over it — a dropped batch, a stalled watermark, a record it could
/// not decode. Those are recorded as structured reports precisely because they
/// are silent otherwise, so a "clean" recovery that filed one is not clean.
/// Only reports that appeared (or recurred) since `before` count: the run
/// leading up to this point deliberately crashes the server, and any report
/// from that half is not what is being judged.
fn assert_no_new_integrity_reports(
    data_dir: &std::path::Path,
    before: &BTreeMap<String, u64>,
    fail_point: &str,
) {
    let mut offenders = Vec::new();
    for group in diagnostics::faultbox_reports(data_dir) {
        let slug = group.first.kind.slug();
        if slug != "invariant_violation" && slug != "corruption" {
            continue;
        }
        let prior = before
            .get(&group.first.fingerprint)
            .copied()
            .unwrap_or_default();
        if group.occurrences() > prior {
            offenders.push(group.summary());
        }
    }
    assert!(
        offenders.is_empty(),
        "the replay that ran to completion after a crash at `{fail_point}` filed \
         invariant-violation / corruption report(s) — recovery detected a broken invariant even \
         though the row counts came out right:\n{}",
        offenders.join("\n")
    );
}

/// Write a known set of rows across three engines, crash, then crash AGAIN
/// inside the replay at `fail_point`, then let replay finish — and require the
/// final state to be exactly the acknowledged set.
async fn replay_is_idempotent_when_interrupted_at(fail_point: &str) {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec("CREATE COLLECTION replay_kv (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await;
    h.exec(
        "CREATE COLLECTION replay_doc (id TEXT PRIMARY KEY, v INT) \
         WITH (engine='document_strict')",
    )
    .await;
    h.exec(
        "CREATE COLLECTION replay_ts \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await;

    // Each statement returns before the next is sent, so every one of these
    // rows is acknowledged — the server promises them durable.
    for i in 0..ROWS {
        h.exec(&format!(
            "INSERT INTO replay_kv (k, v) VALUES ('k{i}', 'v{i}')"
        ))
        .await;
        h.exec(&format!(
            "INSERT INTO replay_doc (id, v) VALUES ('d{i}', {i})"
        ))
        .await;
        h.exec(&format!(
            "INSERT INTO replay_ts (id, ts, value) VALUES ('s{i}', {}, {i}.0)",
            1_700_000_000_000u64 + i as u64
        ))
        .await;
    }

    // Live sanity BEFORE any crash: a later failure is then attributable to
    // recovery rather than to test setup.
    for collection in ["replay_kv", "replay_doc", "replay_ts"] {
        let live = count_rows(&h, collection).await;
        assert_eq!(
            live, ROWS as u64,
            "{collection} must hold {ROWS} rows before the crash (test-setup sanity), got {live}"
        );
    }

    // First crash: hard kill, no graceful shutdown, so the next boot must
    // genuinely replay the WAL rather than reading a clean checkpoint.
    h.kill_9();

    // Second crash: armed for THIS boot only. The server dies part way through
    // the replay it started, leaving some engines applied and others not.
    h.set_env("NODEDB_FAILPOINTS", &format!("{fail_point}=abort"));
    h.spawn();
    h.await_self_crash(CRASH_TIMEOUT);

    // `await_self_crash` only proves the process exited — it would also be
    // satisfied by a boot that failed for an unrelated reason and never reached
    // replay at all. The abort path prints its own line before calling
    // `abort()`, so require it: without this, a test whose fail point silently
    // stopped existing would keep passing while injecting nothing.
    let abort_marker = format!("fail_point aborting process: {fail_point}");
    let log = h.server_log();
    assert!(
        log.contains(&abort_marker),
        "the server exited during the replay boot, but not via the armed fail point \
         `{fail_point}` — nothing was injected, so this test proves NOTHING about crashing \
         mid-replay.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&log)
    );

    // Snapshot what the server had already recorded about itself, so the check
    // below judges only the replay that is about to run to completion.
    let reports_before = report_counts(h.data_dir());

    // Third boot: fail point cleared, so replay runs over the SAME WAL, on top
    // of whatever the aborted attempt already made durable.
    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();

    for collection in ["replay_kv", "replay_doc", "replay_ts"] {
        let recovered = count_rows(&h, collection).await;
        assert_exact_count(recovered, ROWS as u64, collection, fail_point);
    }

    // Counts alone cannot catch a row that survived with the wrong value, or a
    // key replaced by a duplicate of another. Pin the exact contents too.
    let mut kv = h.query_col("SELECT v FROM replay_kv", "v").await;
    kv.sort();
    let expected_kv: Vec<String> = (0..ROWS).map(|i| format!("v{i}")).collect();
    assert_eq!(
        kv, expected_kv,
        "KV contents diverged after a crash at `{fail_point}` during replay followed by a clean \
         replay of the same WAL"
    );

    let mut doc = h.query_col("SELECT id FROM replay_doc", "id").await;
    doc.sort();
    let expected_doc: Vec<String> = (0..ROWS).map(|i| format!("d{i}")).collect();
    assert_eq!(
        doc, expected_doc,
        "document_strict contents diverged after a crash at `{fail_point}` during replay followed \
         by a clean replay of the same WAL"
    );

    let mut ts = h.query_col("SELECT id FROM replay_ts", "id").await;
    ts.sort();
    let expected_ts: Vec<String> = (0..ROWS).map(|i| format!("s{i}")).collect();
    assert_eq!(
        ts, expected_ts,
        "timeseries contents diverged after a crash at `{fail_point}` during replay followed by a \
         clean replay of the same WAL — timeseries ingest is an append, so a repeated id here is a \
         permanently double-applied record"
    );

    assert_no_new_integrity_reports(h.data_dir(), &reports_before, fail_point);
}

/// Crash part way through ONE engine's records, with earlier engines' passes
/// already complete. The interrupted engine's own state and the engines
/// replayed before it are at different points in the WAL when the next boot
/// starts, which is the case a whole-replay-or-nothing model gets wrong.
#[tokio::test(flavor = "multi_thread")]
async fn replay_is_idempotent_after_a_crash_mid_kv_pass() {
    replay_is_idempotent_when_interrupted_at("replay::kv_mid_pass").await;
}

/// Crash after every standalone engine pass but before the redo-only document /
/// graph arms. Committed-transaction redo is an absolute overwrite applied on
/// top of state the standalone passes just rebuilt, so re-running the whole
/// sequence must land on the same result rather than compounding.
#[tokio::test(flavor = "multi_thread")]
async fn replay_is_idempotent_after_a_crash_between_standalone_and_redo() {
    replay_is_idempotent_when_interrupted_at("replay::between_standalone_and_redo").await;
}
