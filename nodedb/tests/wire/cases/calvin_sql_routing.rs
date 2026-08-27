// SPDX-License-Identifier: BUSL-1.1

//! Calvin SQL surface: SET/SHOW cross_shard_txn and EXPLAIN preamble.
//!
//! End-to-end multi-shard-commit tests are out of scope here — they require
//! a real or mocked sequencer with full Raft.

use crate::harness::TestServer;
use nodedb_types::id::VShardId;

/// Find two collection names whose vShards differ. Deterministic within a process.
fn find_two_distinct_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("col_{i}");
        let vshard =
            VShardId::from_collection_in_database(nodedb_types::id::DatabaseId::DEFAULT, &name)
                .as_u32();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_cross_shard_txn_strict_then_get() {
    let srv = TestServer::start().await;

    srv.exec("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn strict should succeed");

    let rows = srv
        .query_text("SHOW cross_shard_txn")
        .await
        .expect("SHOW cross_shard_txn should succeed");
    assert_eq!(rows.len(), 1, "expected 1 row from SHOW");
    assert_eq!(rows[0], "strict", "expected 'strict'");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_cross_shard_txn_best_effort_non_atomic_then_get() {
    let srv = TestServer::start().await;

    srv.exec("SET cross_shard_txn = 'best_effort_non_atomic'")
        .await
        .expect("SET cross_shard_txn best_effort_non_atomic should succeed");

    let rows = srv
        .query_text("SHOW cross_shard_txn")
        .await
        .expect("SHOW cross_shard_txn should succeed");
    assert_eq!(rows.len(), 1, "expected 1 row from SHOW");
    assert_eq!(
        rows[0], "best_effort_non_atomic",
        "expected 'best_effort_non_atomic'"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_cross_shard_txn_invalid_value_returns_22023() {
    let srv = TestServer::start().await;

    let err = srv
        .exec("SET cross_shard_txn = 'invalid_value'")
        .await
        .expect_err("should fail with SQLSTATE 22023");
    assert!(err.contains("22023"), "expected SQLSTATE 22023, got: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_cross_shard_txn_rejects_bare_best_effort() {
    let srv = TestServer::start().await;

    let err = srv
        .exec("SET cross_shard_txn = 'best_effort'")
        .await
        .expect_err("bare 'best_effort' should be rejected with SQLSTATE 22023");
    assert!(
        err.contains("22023"),
        "expected SQLSTATE 22023 for bare best_effort, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explain_multi_vshard_insert_includes_calvin_preamble() {
    let srv = TestServer::start().await;

    let (col_a, col_b) = find_two_distinct_collections();

    // Create two collections on different vShards.
    srv.exec(&format!(
        "CREATE COLLECTION {col_a} (id STRING PRIMARY KEY, val STRING)"
    ))
    .await
    .expect("CREATE COLLECTION col_a");

    srv.exec(&format!(
        "CREATE COLLECTION {col_b} (id STRING PRIMARY KEY, val STRING)"
    ))
    .await
    .expect("CREATE COLLECTION col_b");

    // EXPLAIN an insert into col_a (single-shard — no preamble expected).
    let rows = srv
        .query_text(&format!(
            "EXPLAIN INSERT INTO {col_a} (id, val) VALUES ('x', 'y')"
        ))
        .await
        .expect("EXPLAIN INSERT single-shard should succeed");

    // A single-shard insert should NOT have a Calvin preamble.
    let has_calvin = rows.iter().any(|r| r.contains("Calvin"));
    assert!(
        !has_calvin,
        "single-shard insert should not have Calvin preamble, got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explain_best_effort_includes_non_atomic_marker() {
    let srv = TestServer::start().await;

    srv.exec("SET cross_shard_txn = 'best_effort_non_atomic'")
        .await
        .expect("SET cross_shard_txn best_effort_non_atomic");

    let (col_a, _col_b) = find_two_distinct_collections();

    // Create one collection so EXPLAIN doesn't fail on unknown collection.
    srv.exec(&format!(
        "CREATE COLLECTION {col_a} (id STRING PRIMARY KEY, val STRING)"
    ))
    .await
    .expect("CREATE COLLECTION col_a");

    // EXPLAIN a single-shard insert under best_effort mode — no preamble since single-shard.
    let rows = srv
        .query_text(&format!(
            "EXPLAIN INSERT INTO {col_a} (id, val) VALUES ('x', 'y')"
        ))
        .await
        .expect("EXPLAIN INSERT should succeed");

    // Single-shard → no Calvin preamble even under best_effort_non_atomic.
    let has_non_atomic = rows.iter().any(|r| r.contains("NON-ATOMIC"));
    assert!(
        !has_non_atomic,
        "single-shard insert should not have NON-ATOMIC preamble, got: {rows:?}"
    );
}
