// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill WAL-durability regressions for the columnar-storage
//! family (Columnar, Timeseries) and the Array (ND sparse) engine.
//!
//! Same contract as `crash_recovery.rs`: a write acknowledged by the
//! server must survive `kill -9`, because the write path acks the
//! client only after the WAL append is persisted. Reopening the same
//! data directory on a fresh process replays the WAL and must restore
//! the row.

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn columnar_write_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION crash_columnar \
         COLUMNS (id TEXT, region TEXT, revenue FLOAT) \
         WITH (engine='columnar')",
    )
    .await;
    h.exec("INSERT INTO crash_columnar (id, region, revenue) VALUES ('r1', 'us', 100.0)")
        .await;

    // Live sanity BEFORE the crash: the row reads back pre-restart, so any
    // post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col(
            "SELECT region FROM crash_columnar WHERE id = 'r1'",
            "region",
        )
        .await;
    assert_eq!(
        live,
        vec!["us".to_string()],
        "columnar row must read back BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "SELECT region FROM crash_columnar WHERE id = 'r1'",
            "region",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["us".to_string()],
        "committed columnar write did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn timeseries_write_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION crash_timeseries \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await;
    h.exec("INSERT INTO crash_timeseries (id, ts, metric, value) VALUES ('p1', 1000, 'cpu', 42.0)")
        .await;

    // Live sanity BEFORE the crash: the point reads back pre-restart, so any
    // post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col(
            "SELECT metric FROM crash_timeseries WHERE id = 'p1'",
            "metric",
        )
        .await;
    assert_eq!(
        live,
        vec!["cpu".to_string()],
        "timeseries point must read back BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "SELECT metric FROM crash_timeseries WHERE id = 'p1'",
            "metric",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["cpu".to_string()],
        "committed timeseries ingest did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn array_cell_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE ARRAY crash_arr \
         DIMS (k INT64 [0..15]) \
         ATTRS (v FLOAT64) \
         TILE_EXTENTS (16) \
         CELL_ORDER ROW_MAJOR",
    )
    .await;
    h.exec("INSERT INTO ARRAY crash_arr COORDS (3) VALUES (42.0)")
        .await;

    // Live sanity BEFORE the crash: the cell aggregates pre-restart, so any
    // post-restart failure is attributable to recovery, not test setup. The
    // scalar reducer path always emits a single `result` column (see
    // `data/executor/dispatch/array/aggregate.rs`), so this reads back
    // without depending on the ARRAY_SLICE/ARRAY_PROJECT column-naming
    // shape (which varies by query form).
    let live = h
        .query_col("SELECT * FROM ARRAY_AGG('crash_arr', 'v', 'sum')", "result")
        .await;
    assert_eq!(live.len(), 1, "expected one scalar agg row: {live:?}");
    let live_val: f64 = live[0]
        .parse()
        .unwrap_or_else(|e| panic!("result not a float: {live:?}: {e}"));
    assert!(
        (live_val - 42.0).abs() < 0.01,
        "array cell must aggregate to 42.0 BEFORE the crash (test-setup sanity): got {live_val}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col("SELECT * FROM ARRAY_AGG('crash_arr', 'v', 'sum')", "result")
        .await;
    assert_eq!(
        recovered.len(),
        1,
        "expected one scalar agg row after reopen: {recovered:?}"
    );
    let recovered_val: f64 = recovered[0]
        .parse()
        .unwrap_or_else(|e| panic!("result not a float after reopen: {recovered:?}: {e}"));
    assert!(
        (recovered_val - 42.0).abs() < 0.01,
        "array cell did not survive kill -9 + WAL replay (got {recovered_val})"
    );
}
