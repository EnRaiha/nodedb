// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill WAL-durability regressions.
//!
//! A write acknowledged by the server (an `INSERT` that returned) must
//! survive a `kill -9`, because the write path acks the client only after
//! the WAL append is persisted. Reopening the same data directory on a
//! fresh process replays the WAL and must restore the row. Covers
//! document_strict, KV, vector-index (HNSW rebuild from WAL), and spatial
//! R-tree recovery.

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn committed_write_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION crash_kv (id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')",
    )
    .await;
    h.exec("INSERT INTO crash_kv (id, v) VALUES ('a', 42)")
        .await;

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col("SELECT v FROM crash_kv WHERE id = 'a'", "v")
        .await;
    assert_eq!(
        recovered,
        vec!["42".to_string()],
        "committed document_strict write did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_write_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec("CREATE COLLECTION crash_kv_engine (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await;
    h.exec("INSERT INTO crash_kv_engine (k, v) VALUES ('key1', 'val1')")
        .await;

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col("SELECT v FROM crash_kv_engine WHERE k = 'key1'", "v")
        .await;
    assert_eq!(
        recovered,
        vec!["val1".to_string()],
        "committed KV write did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn vector_index_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec("CREATE COLLECTION crash_vec TYPE document").await;
    h.exec("CREATE VECTOR INDEX idx_crash_vec ON crash_vec (embedding) METRIC cosine DIM 4")
        .await;

    // Two rows on distinct axes — after recovery each must be the nearest
    // neighbour of its own embedding, proving the HNSW was rebuilt from WAL.
    h.exec("INSERT INTO crash_vec (id, embedding) VALUES ('r1', ARRAY[1.0,0.0,0.0,0.0])")
        .await;
    h.exec("INSERT INTO crash_vec (id, embedding) VALUES ('r2', ARRAY[0.0,1.0,0.0,0.0])")
        .await;

    // Live sanity BEFORE the crash: the vector index works pre-restart, so any
    // post-restart failure is attributable to recovery, not to test setup.
    let live_nn = h
        .query_col(
            "SELECT id FROM crash_vec ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) LIMIT 1",
            "id",
        )
        .await;
    assert_eq!(
        live_nn,
        vec!["r1".to_string()],
        "vector search must work BEFORE the crash (test-setup sanity): {live_nn:?}"
    );

    h.kill_9();
    h.reopen();

    // The document rows themselves survived.
    let ids = h
        .query_col("SELECT id FROM crash_vec ORDER BY id", "id")
        .await;
    assert_eq!(
        ids,
        vec!["r1".to_string(), "r2".to_string()],
        "vector-collection rows did not survive kill -9 (got {ids:?})"
    );

    // The HNSW index was rebuilt from WAL: each row's own embedding returns
    // that row as its nearest neighbour.
    let nn_r1 = h
        .query_col(
            "SELECT id FROM crash_vec ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) LIMIT 1",
            "id",
        )
        .await;
    assert_eq!(
        nn_r1,
        vec!["r1".to_string()],
        "vector index not rebuilt after kill -9: nearest neighbour of r1's embedding was not r1 (got {nn_r1:?})"
    );
    let nn_r2 = h
        .query_col(
            "SELECT id FROM crash_vec ORDER BY vector_distance(embedding, ARRAY[0.0,1.0,0.0,0.0]) LIMIT 1",
            "id",
        )
        .await;
    assert_eq!(
        nn_r2,
        vec!["r2".to_string()],
        "vector index not rebuilt after kill -9: nearest neighbour of r2's embedding was not r2 (got {nn_r2:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn spatial_index_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION geo_places (id TEXT, location GEOMETRY SPATIAL_INDEX, name TEXT) \
         WITH (engine='spatial')",
    )
    .await;
    h.exec(
        "INSERT INTO geo_places (id, location, name) \
         VALUES ('p1', ST_Point(-73.9857, 40.7580), 'Times Square')",
    )
    .await;
    h.exec(
        "INSERT INTO geo_places (id, location, name) \
         VALUES ('p2', ST_Point(2.3522, 48.8566), 'Paris')",
    )
    .await;

    // Live sanity BEFORE the crash: the R-tree query works, so any post-restart
    // failure is attributable to recovery, not test setup.
    let q = "SELECT name FROM geo_places WHERE \
             ST_DWithin(location, '{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}', 5000)";
    let live = h.query_col(q, "name").await;
    assert_eq!(
        live,
        vec!["Times Square".to_string()],
        "spatial query must work BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    // The in-memory R-tree must be rebuilt from the durable document store on
    // boot: the geometry documents survive in redb, so a kill -9 that emptied
    // the WAL must not leave the spatial index empty.
    let recovered = h.query_col(q, "name").await;
    assert_eq!(
        recovered,
        vec!["Times Square".to_string()],
        "spatial R-tree not rebuilt after kill -9 (got {recovered:?})"
    );
}
