// SPDX-License-Identifier: BUSL-1.1

//! End-to-end Calvin pgwire test: verifies that a multi-shard transaction
//! submitted via `simple_query` as an interactive `BEGIN ... COMMIT` block
//! under `cross_shard_txn = 'strict'` is rejected — cross-shard atomicity
//! currently requires auto-commit (single-statement) writes.
//!
//! The Calvin multi-shard path is exercised by BEGIN + two point INSERTs into
//! collections on different vShards + COMMIT.  On COMMIT, `handle_commit`
//! calls `classify_dispatch` on the buffered task set, detects MultiShard, and
//! — because the writes were buffered inside an explicit transaction block
//! rather than sent auto-commit — rejects with
//! `Error::CrossShardInExplicitTransaction` instead of submitting the batch
//! to the Calvin sequencer inbox.
//!
//! Interactive cross-shard COMMIT inside an explicit block is not yet
//! supported; when that capability lands, this test should be flipped back
//! to assert the COMMIT succeeds and `admitted_total` advances. The
//! auto-commit (single-statement) cross-shard write path is covered
//! separately by `single_node_calvin_two_phase::cross_shard_calvin_write_flushes_and_is_visible`.
//!
//! Foldability of individual tasks is also spot-checked: for each buffered task
//! the Calvin response path in `transaction_cmds.rs` does NOT synthesise
//! per-task tags (COMMIT returns a single COMMIT tag), so that path is
//! verified separately by asserting that `is_calvin_foldable` on PointInsert
//! plans returns `true` at the unit-test level in `plan.rs`.
//!
//! File name contains "cluster" so nextest applies the cluster test group:
//! `binary(/cluster/)` → max-threads=1, threads-required=num-test-threads.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};

/// Find two collection names whose vShard ids differ.
fn two_distinct_vshard_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("calvin_e2e_{i}");
        let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
        if let Some((ref fname, fv)) = first {
            if fv != vshard {
                return (fname.clone(), name);
            }
        } else {
            first = Some((name, vshard));
        }
    }
    panic!("could not find two distinct-vshard collections in 512 tries");
}

/// Calvin multi-shard batch via pgwire `simple_query` is rejected when sent
/// as an interactive `BEGIN ... COMMIT` block.
///
/// Steps:
/// 1. Spin up a single-node cluster (Raft + Calvin sequencer wired by `start_raft`).
/// 2. Wait until `sequencer_metrics` is set (sequencer ready).
/// 3. Create two collections on different vShards.
/// 4. Enable strict cross-shard mode.
/// 5. Via one `simple_query` call, send BEGIN + two point INSERTs + COMMIT.
///    The server splits at semicolons, buffers the INSERTs during the
///    transaction, and on COMMIT the buffered write set spans two vShards.
///    Cross-shard atomicity currently requires auto-commit (single
///    statement), so `handle_commit` rejects the COMMIT with
///    `Error::CrossShardInExplicitTransaction` instead of submitting the
///    batch to the Calvin sequencer inbox.
/// 6. Assert the rejection's error text, and that `admitted_total` never
///    advanced (nothing was ever submitted to the sequencer).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_multishard_write_in_explicit_block_is_rejected() {
    let node = common::cluster_harness::TestClusterNode::spawn(1, vec![])
        .await
        .expect("single-node cluster spawn");

    // Allow Raft to elect and the sequencer to start ticking.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Gate on sequencer_metrics being set (wired by start_raft via SequencerService).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if node.shared.sequencer_metrics.get().is_some() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("sequencer_metrics not set within 10s — start_raft may not have wired it");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let (col_a, col_b) = two_distinct_vshard_collections();

    // Create both collections on this (single) node.
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {col_a} (id STRING PRIMARY KEY, v STRING)"
        ))
        .await
        .expect("CREATE COLLECTION col_a");

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {col_b} (id STRING PRIMARY KEY, v STRING)"
        ))
        .await
        .expect("CREATE COLLECTION col_b");

    // Enable strict Calvin mode so COMMIT's multi-shard path runs.
    node.client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    // Baseline before the (rejected) Calvin batch: nothing should ever be
    // admitted to the sequencer, since the reject fires before submission.
    let metrics = node
        .shared
        .sequencer_metrics
        .get()
        .expect("sequencer_metrics must be set");
    let admitted_before = metrics.admitted_total.load(Ordering::Relaxed);

    // Send the full transaction in one `simple_query` call.
    // tokio-postgres sends this as a single wire message; the server's
    // `execute_sql` splits at top-level semicolons and dispatches each
    // statement in order.  The two INSERTs are buffered during the
    // BEGIN block; on COMMIT the buffer spans two vShards → MultiShard.
    //
    // NOTE: interactive cross-shard COMMIT inside an explicit `BEGIN` block
    // is not yet supported — Calvin cross-shard atomicity currently requires
    // auto-commit (single-statement) writes. When that capability lands,
    // this test should be flipped back to assert the COMMIT succeeds and
    // `admitted_total` advances past `admitted_before`.
    let txn_sql = format!(
        "BEGIN; \
         INSERT INTO {col_a} (id, v) VALUES ('k1', 'hello'); \
         INSERT INTO {col_b} (id, v) VALUES ('k2', 'world'); \
         COMMIT"
    );
    let err = node
        .client
        .simple_query(&txn_sql)
        .await
        .expect_err("multi-shard write inside an explicit transaction block must be rejected");
    assert!(
        err.as_db_error()
            .map(|db| db.message())
            .unwrap_or_default()
            .contains("cross-shard write inside explicit transaction block is not supported"),
        "expected CrossShardInExplicitTransaction error text, got: {err:?}"
    );

    // Nothing was ever submitted to the Calvin sequencer inbox — the reject
    // fires at COMMIT-time classification, before dispatch.
    let admitted_after = metrics.admitted_total.load(Ordering::Relaxed);
    assert_eq!(
        admitted_after, admitted_before,
        "admitted_total must not advance for a rejected transaction: \
         before={admitted_before} after={admitted_after}"
    );

    node.shutdown().await;
}
