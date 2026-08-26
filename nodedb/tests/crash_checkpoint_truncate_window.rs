// SPDX-License-Identifier: BUSL-1.1

//! Crash injection in the checkpoint's marker→truncate window: a checkpoint
//! writes its marker, then deletes sealed segments below the checkpoint LSN.
//! A crash between them must not let recovery read the marker as proof
//! truncation already ran and skip replay of records still on disk. Crash
//! is injected via `NODEDB_FAILPOINTS` abort right after the marker is
//! durable. Requires `--features failpoints`.

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
        // Checkpoint-manager logs at debug so a timeout shows whether a
        // checkpoint even started and where it stopped.
        .with_env(
            "RUST_LOG",
            "warn,nodedb::control::checkpoint_manager=debug,nodedb::bootstrap=info",
        )
        .with_env(
            "NODEDB_FAILPOINTS",
            "checkpoint::after_marker_before_truncate=abort",
        );
    h.spawn();
    h.wait_ready();

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

    // A freshly-replayed core reports checkpoint LSN 0 until its own new
    // write, or every checkpoint cycle logs "skipping" and the window under
    // test never reopens. Key is outside `row%` so it's not a canary.
    h.exec("INSERT INTO ckpt_window (k, v) VALUES ('trigger', 'post-restart')")
        .await;

    // The fail point is still armed, so it aborts again — proving the second
    // cycle actually reached the same window.
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
