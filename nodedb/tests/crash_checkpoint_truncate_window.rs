// SPDX-License-Identifier: BUSL-1.1

//! Crash injection in the checkpoint's marker→truncate window.
//!
//! A checkpoint does two durable things in sequence: it writes a checkpoint
//! marker to the WAL, then it deletes the sealed segments below the checkpoint
//! LSN. Between those two points the log claims to be checkpointed while every
//! byte it covers is still on disk and still needed.
//!
//! A crash in that window must not lose an acknowledged write. The danger is
//! recovery reading the marker as proof that truncation already happened and
//! skipping replay of the records the marker covers — the records that, by
//! definition, are still sitting in the WAL precisely because the crash
//! interrupted their deletion.
//!
//! The window is a few instructions wide and unreachable from outside the
//! process, so the crash is injected from within: the server is spawned with
//! the fail point armed to `abort`, which kills it at the exact instruction
//! after the marker is durable. Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::Duration;

/// Short enough that a checkpoint fires within the test's lifetime — the
/// default interval dwarfs it and the window would never open.
const CHECKPOINT_INTERVAL_SECS: &str = "2";

/// Bounded wait for the injected abort. A timeout means no checkpoint ran, so
/// the crash never happened and the test proved nothing.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_rows_survive_a_crash_between_checkpoint_marker_and_truncate() {
    let mut h = CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", CHECKPOINT_INTERVAL_SECS)
        // The harness captures server stdout/stderr and prints it when
        // `await_self_crash` times out. Without checkpoint-manager logs at
        // debug, a timeout is an unexplained hang; with them, the captured
        // lines show whether a checkpoint even started and where it stopped.
        .with_env(
            "RUST_LOG",
            "warn,nodedb::control::checkpoint_manager=debug,nodedb::bootstrap=info",
        )
        .with_env(
            "NODEDB_FAILPOINTS",
            "checkpoint::after_marker_before_truncate=abort",
        );
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec("CREATE COLLECTION ckpt_window (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await;
    for i in 0..5 {
        h.exec(&format!(
            "INSERT INTO ckpt_window (k, v) VALUES ('row{i}', 'value{i}')"
        ))
        .await;
    }

    // Sanity before the crash: a later failure is then attributable to
    // recovery rather than to test setup.
    let live = h
        .query_col(
            "SELECT v FROM ckpt_window WHERE k LIKE 'row%' ORDER BY k",
            "v",
        )
        .await;
    assert_eq!(
        live.len(),
        5,
        "rows must read back before the crash: {live:?}"
    );

    // The next checkpoint cycle writes its marker and dies on the spot.
    h.await_self_crash(CRASH_TIMEOUT);

    h.reopen();

    let recovered = h
        .query_col(
            "SELECT v FROM ckpt_window WHERE k LIKE 'row%' ORDER BY k",
            "v",
        )
        .await;
    assert_eq!(
        recovered,
        (0..5).map(|i| format!("value{i}")).collect::<Vec<_>>(),
        "acknowledged rows were lost after a crash between the checkpoint marker and truncation \
         — recovery treated the marker as proof of a truncation that never ran (got {recovered:?})"
    );

    // A freshly-replayed core reports its checkpoint LSN as 0 until it takes a
    // new write of its own — replay restores rows but does not re-derive a
    // durable-LSN watermark from them. Without a write here, every checkpoint
    // cycle on the restarted server logs "global checkpoint LSN is 0 — skipping"
    // and returns before writing a marker, so the window under test never
    // reopens and the second `await_self_crash` below times out. The key is
    // deliberately outside `row%` so it cannot be mistaken for a canary.
    h.exec("INSERT INTO ckpt_window (k, v) VALUES ('trigger', 'post-restart')")
        .await;

    // The restarted server must be able to checkpoint and keep serving: the
    // interrupted cycle left no state that blocks the next one. The fail point
    // is still armed, so it aborts again — proving the second cycle actually
    // reached the same window rather than silently never running.
    h.await_self_crash(CRASH_TIMEOUT);
    h.reopen();

    let after = h
        .query_col(
            "SELECT v FROM ckpt_window WHERE k LIKE 'row%' ORDER BY k",
            "v",
        )
        .await;
    assert_eq!(
        after.len(),
        5,
        "rows lost across a second crash between the checkpoint marker and truncation, this time \
         on a server that had already replayed the WAL once: {after:?}"
    );
}
