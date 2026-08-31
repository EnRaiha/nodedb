// SPDX-License-Identifier: BUSL-1.1

//! Replay idempotency when recovery itself is interrupted: a power cut
//! mid-replay means the next boot replays the same WAL on top of whatever
//! the interrupted attempt already made durable. Two failure modes — rows
//! missing or duplicated — so every assertion is an exact count. Crash
//! injected via `NODEDB_FAILPOINTS` at `replay::kv_mid_pass` and
//! `replay::between_standalone_and_redo`. Requires `--features failpoints`.

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

/// `SELECT COUNT(*)` as a number. Counts, never presence — a presence check
/// passes just as happily when replay applied the row twice.
async fn count_rows(h: &CrashHarness, collection: &str) -> u64 {
    let rows = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {collection}"), 0)
        .await;
    assert_eq!(rows.len(), 1, "expected one COUNT(*) row, got {rows:?}");
    rows[0]
        .parse()
        .unwrap_or_else(|e| panic!("COUNT(*) for {collection} was not a number: {rows:?}: {e}"))
}

/// Assert a recovered row count is exactly the acknowledged count. The two
/// directions are different bugs, so never report them with one message.
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
/// `Corruption` report — row counts can be right while the server papered
/// over a dropped batch or stalled watermark. Only reports since `before`
/// count; the deliberate crash half is not what's being judged.
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
    h.wait_ready();
    // `/healthz` reports ready before the Calvin sequencer elects a leader.
    // A write issued in that window can be re-proposed after a leader-change
    // no-op and applied twice, which only the PK-less timeseries shows.
    h.wait_for_calvin_ready(std::time::Duration::from_secs(30))
        .await;

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
            live,
            ROWS as u64,
            "{collection} must hold {ROWS} rows before the crash (test-setup sanity), got \
             {live}. An over-count means one acknowledged INSERT applied twice — only the \
             PK-less timeseries can gain a row that way.\nRows: {:?}\nServer log:\n{}",
            h.query_col(&format!("SELECT id FROM {collection}"), "id")
                .await,
            diagnostics::log_tail_section(&h.server_log())
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

    // `await_self_crash` only proves the process exited, not that replay was
    // reached — require the abort path's own log line too.
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

/// Crash part way through one engine's records, with earlier engines' passes
/// already complete — the case a whole-replay-or-nothing model gets wrong.
#[tokio::test(flavor = "multi_thread")]
async fn replay_is_idempotent_after_a_crash_mid_kv_pass() {
    replay_is_idempotent_when_interrupted_at("replay::kv_mid_pass").await;
}

/// Crash after every standalone engine pass but before the redo-only
/// document/graph arms — redo is an absolute overwrite, so re-running the
/// whole sequence must land on the same result, not compound.
#[tokio::test(flavor = "multi_thread")]
async fn replay_is_idempotent_after_a_crash_between_standalone_and_redo() {
    replay_is_idempotent_when_interrupted_at("replay::between_standalone_and_redo").await;
}
