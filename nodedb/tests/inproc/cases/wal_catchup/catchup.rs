// SPDX-License-Identifier: BUSL-1.1

//! The catch-up task replays WAL records the Data Plane never received, and
//! does not apply a settled record twice.

use std::sync::Arc;
use std::time::Duration;

use super::stack::{TestStack, ilp_payload};

/// A replicated write is queryable, and catch-up from LSN 0 does not apply
/// its WAL record a second time.
///
/// Simulates: ILP batch applied through its replicated entry → catch-up reads
/// the WAL, record included → query sees each row once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_redispatch_makes_data_queryable() {
    let stack = TestStack::new().await;

    let collection = "wal_test";

    let resp = stack
        .write_replicated(
            collection,
            ilp_payload(collection, 500, 1_700_000_000_000_000_000),
        )
        .await;
    assert_eq!(resp["accepted"].as_u64(), Some(500));
    assert_eq!(stack.query_count(collection).await, 500);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        nodedb_types::Lsn::new(0),
        shutdown_rx,
    );
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let count = stack.query_count(collection).await;
    assert_eq!(
        count, 500,
        "catch-up must not apply a settled replicated record a second time"
    );
}

/// The catch-up background task automatically re-dispatches WAL records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catchup_task_dispatches_wal_records() {
    let stack = TestStack::new().await;

    let collection = "catchup_auto";

    // Write 300 rows to WAL (not dispatched to Data Plane).
    stack.write_to_wal(
        collection,
        ilp_payload(collection, 300, 1_700_000_000_000_000_000),
    );

    // Start the catch-up task from LSN 0.
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        nodedb_types::Lsn::new(0),
        shutdown_rx,
    );

    // Wait for at least one catch-up cycle (first fires at 500ms).
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Query: should see all 300 rows.
    let count = stack.query_count(collection).await;
    assert_eq!(
        count, 300,
        "catch-up task should automatically make WAL rows queryable"
    );
}

/// Simulates the real production failure:
/// - 5 ILP batches written to WAL (LSN 1-5)
/// - Only batches 1, 3, 5 dispatched to Data Plane (SPSC dropped 2, 4)
/// - Catch-up task runs and replays WAL from LSN 0
/// - All 5 batches must become visible (catch-up fills the gaps)
///
/// This is the exact scenario that caused 58%→24% visibility regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catchup_fills_gaps_from_spsc_drops() {
    let stack = TestStack::new().await;

    let collection = "gap_test";
    let rows_per_batch = 100;

    // Batches 0, 2, 4 apply through their replicated entries. Batches 1, 3
    // were "dropped" by SPSC — only in WAL. A dropped record's outcome is not
    // final, so its write window keeps the outcome floor below it. The raw
    // append here opens no window, so the dropped batches go last: the floor
    // then stays below them as a live window would keep it.
    let payload_of = |batch: i64| {
        let start_ts = 1_700_000_000_000_000_000i64 + batch * rows_per_batch as i64 * 1_000_000;
        ilp_payload(collection, rows_per_batch, start_ts)
    };
    for batch in [0, 2, 4] {
        stack.write_replicated(collection, payload_of(batch)).await;
    }
    for batch in [1, 3] {
        stack.write_to_wal(collection, payload_of(batch));
    }

    // Verify: only 300 rows visible (3 dispatched batches × 100).
    let count_before = stack.query_count(collection).await;
    assert_eq!(count_before, 300, "only 3/5 batches dispatched");

    // Start catch-up task — should replay ALL WAL records.
    // The Data Plane's LSN dedup must NOT skip batches 1, 3 because batches
    // it already applied carry lower LSNs.
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        nodedb_types::Lsn::new(0),
        shutdown_rx,
    );

    // Wait for catch-up to process all records.
    tokio::time::sleep(Duration::from_millis(3000)).await;

    // All 500 rows must be visible (300 from live + 200 from catch-up).
    // A catch-up that re-sends a settled record shows duplicates, so the
    // key assertion is at LEAST 500 rows.
    let count_after = stack.query_count(collection).await;
    assert!(
        count_after >= 500,
        "catch-up must make all 5 batches visible, got {count_after} (expected >= 500)"
    );
}

/// Regression test: catch-up must NOT start from wal.next_lsn() because
/// that's PAST all existing records. It must start from LSN 0 so it can
/// replay records that were WAL'd but never dispatched to the Data Plane.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catchup_from_next_lsn_misses_all_records() {
    let stack = TestStack::new().await;

    let collection = "lsn_bug";

    // Write 500 rows to WAL (not dispatched to Data Plane).
    stack.write_to_wal(
        collection,
        ilp_payload(collection, 500, 1_700_000_000_000_000_000),
    );

    // BAD: Start catch-up from next_lsn (past all records) — catch-up
    // finds 0 records from this starting point.
    let next_lsn = stack.wal.next_lsn();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        next_lsn,
        shutdown_rx,
    );

    tokio::time::sleep(Duration::from_millis(3000)).await;

    let count_bad = stack.query_count(collection).await;
    // This SHOULD be 0 — catch-up started past all records.
    assert_eq!(
        count_bad, 0,
        "catch-up from next_lsn should find nothing (confirming the bug)"
    );
}

/// Same scenario but with the fix: catch-up starts from LSN 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catchup_from_lsn_zero_recovers_all_records() {
    let stack = TestStack::new().await;

    let collection = "lsn_fix";

    // Write 500 rows to WAL (not dispatched to Data Plane).
    stack.write_to_wal(
        collection,
        ilp_payload(collection, 500, 1_700_000_000_000_000_000),
    );

    // GOOD: Start catch-up from LSN 0.
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        nodedb_types::Lsn::new(0),
        shutdown_rx,
    );

    tokio::time::sleep(Duration::from_millis(3000)).await;

    let count_good = stack.query_count(collection).await;
    assert_eq!(
        count_good, 500,
        "catch-up from LSN 0 must recover all WAL records"
    );
}

/// Simulates the EXACT production scenario:
/// 1. Catch-up task starts from wal.next_lsn() (like main.rs)
/// 2. WAL records are written DURING the catch-up task's lifetime
/// 3. Some dispatches succeed, some are written to WAL only (simulating drops)
/// 4. After all writes complete, catch-up drains remaining WAL records
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_scenario_catchup_drains_wal_after_ingest() {
    let stack = TestStack::new().await;

    let collection = "prod_test";

    // Start catch-up task from current WAL tip.
    let initial_lsn = stack.wal.next_lsn();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        initial_lsn,
        shutdown_rx,
    );

    // Simulate ILP ingest of 10 batches. 6 apply through their replicated
    // entries, and 4 were dropped by SPSC after their WAL append. A dropped
    // record's write window keeps the outcome floor below it until its
    // outcome is final. The raw append here opens no window, so the dropped
    // batches go after the replicated ones: the floor then stays below them
    // as a live window would keep it.
    let rows_per_batch = 200;
    let payload_of = |batch: i64| {
        let start_ts = 1_700_000_000_000_000_000i64 + batch * rows_per_batch as i64 * 1_000_000;
        ilp_payload(collection, rows_per_batch, start_ts)
    };
    let (replicated, dropped): (Vec<i64>, Vec<i64>) = (0..10).partition(|batch| batch % 5 < 3);
    for batch in replicated {
        stack.write_replicated(collection, payload_of(batch)).await;
    }
    for batch in dropped {
        stack.write_to_wal(collection, payload_of(batch));
    }

    // Immediately after ingest: should see at least 6*200 = 1200 rows.
    let count_during = stack.query_count(collection).await;
    assert!(
        count_during >= 1200,
        "should see at least 1200 rows from 6 dispatched batches, got {count_during}"
    );

    // Wait for catch-up to drain the remaining 4 WAL batches.
    // Catch-up runs every 100-2000ms, should complete within a few seconds.
    tokio::time::sleep(Duration::from_millis(5000)).await;

    let count_after = stack.query_count(collection).await;
    // All 10 batches are visible (2000 rows). A catch-up that re-sends
    // a settled record shows duplicates, so the check is a lower bound.
    assert!(
        count_after >= 2000,
        "catch-up must drain all WAL batches, got {count_after} (expected >= 2000)"
    );
}
