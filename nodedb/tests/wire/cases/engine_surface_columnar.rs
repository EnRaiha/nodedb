// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Columnar engine.
//!
//! Covers: ingest, aggregations (SUM/AVG/MIN/MAX), predicate pushdown,
//! count, and WAL durability.

use crate::harness::TestServer;

#[tokio::test]
async fn ingest_and_select() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION col_basic \
         COLUMNS (id TEXT, region TEXT, revenue FLOAT, ts BIGINT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO col_basic (id, region, revenue, ts) VALUES ('r1', 'us', 100.0, 1)")
        .await
        .unwrap();
    srv.exec("INSERT INTO col_basic (id, region, revenue, ts) VALUES ('r2', 'eu', 200.0, 2)")
        .await
        .unwrap();
    srv.exec("INSERT INTO col_basic (id, region, revenue, ts) VALUES ('r3', 'us', 150.0, 3)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM col_basic ORDER BY ts")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "r1");
}

#[tokio::test]
async fn sum_aggregation() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION col_sum \
         COLUMNS (id TEXT, amount FLOAT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap();

    for (i, v) in [(1, 10.0_f64), (2, 20.0), (3, 30.0)] {
        srv.exec(&format!(
            "INSERT INTO col_sum (id, amount) VALUES ('r{i}', {v})"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows("SELECT SUM(amount) FROM col_sum")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let total: f64 = rows[0][0].parse().unwrap();
    assert!((total - 60.0).abs() < 0.01);
}

#[tokio::test]
async fn min_max_aggregation() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION col_minmax \
         COLUMNS (id TEXT, score INT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap();

    for (i, v) in [(1, 5i64), (2, 15), (3, 10)] {
        srv.exec(&format!(
            "INSERT INTO col_minmax (id, score) VALUES ('r{i}', {v})"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows("SELECT MIN(score), MAX(score) FROM col_minmax")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].parse::<i64>().unwrap(), 5);
    assert_eq!(rows[0][1].parse::<i64>().unwrap(), 15);
}

#[tokio::test]
async fn predicate_pushdown_filter() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION col_filter \
         COLUMNS (id TEXT, region TEXT, amount FLOAT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO col_filter (id, region, amount) VALUES ('f1', 'us', 50.0)")
        .await
        .unwrap();
    srv.exec("INSERT INTO col_filter (id, region, amount) VALUES ('f2', 'eu', 70.0)")
        .await
        .unwrap();
    srv.exec("INSERT INTO col_filter (id, region, amount) VALUES ('f3', 'us', 80.0)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM col_filter WHERE region = 'us' ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "f1");
    assert_eq!(rows[1][0], "f3");
}

#[tokio::test]
async fn count_aggregation() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION col_cnt \
         COLUMNS (id TEXT, n INT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap();

    for i in 0..7u32 {
        srv.exec(&format!("INSERT INTO col_cnt (id, n) VALUES ('c{i}', {i})"))
            .await
            .unwrap();
    }

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM col_cnt")
        .await
        .unwrap();
    assert_eq!(rows[0][0].parse::<u32>().unwrap(), 7);
}

#[tokio::test]
async fn wal_restart_durability() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION col_wal \
         COLUMNS (id TEXT, val FLOAT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO col_wal (id, val) VALUES ('w1', 42.0)")
        .await
        .unwrap();

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;

    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    let rows = srv2
        .query_rows("SELECT val FROM col_wal WHERE id = 'w1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let v: f64 = rows[0][0].parse().unwrap();
    assert!((v - 42.0).abs() < 0.01);
}
