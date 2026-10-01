// SPDX-License-Identifier: BUSL-1.1

//! A continuous aggregate stays active across a restart of a Raft-backed node.
//!
//! The per-core aggregate registry is in-memory. Boot re-registers every
//! stored aggregate from the catalog, so rows flushed after the restart
//! still reach the aggregate.

use std::time::Duration;

use crate::harness::TestServer;

/// A memtable budget of one byte flushes every ingest to a partition, and a
/// flush is what feeds an `OnFlush` aggregate.
const FLUSH_EVERY_INGEST: usize = 1;

/// `rows_aggregated` of `name` in `SHOW CONTINUOUS AGGREGATES`.
async fn rows_aggregated(srv: &TestServer, name: &str) -> u64 {
    let rows = srv
        .query_rows("SHOW CONTINUOUS AGGREGATES")
        .await
        .unwrap_or_else(|e| panic!("show continuous aggregates: {e}"));
    let row = rows
        .iter()
        .find(|r| r[0] == name)
        .unwrap_or_else(|| panic!("aggregate {name} missing from {rows:?}"));
    row[5]
        .parse()
        .unwrap_or_else(|e| panic!("rows_aggregated '{}': {e}", row[5]))
}

/// Poll until `name` aggregated at least `min` rows. The flush that feeds the
/// aggregate runs on the core after the insert answers.
async fn await_rows_aggregated(srv: &TestServer, name: &str, min: u64) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = rows_aggregated(srv, name).await;
        if seen >= min || tokio::time::Instant::now() >= deadline {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_aggregate_is_active_after_restart() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    srv.exec(
        "CREATE COLLECTION cagg_restart_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create source: {e}"));
    srv.exec(
        "CREATE CONTINUOUS AGGREGATE cagg_restart_view \
         ON cagg_restart_src BUCKET '5m' \
         AGGREGATE SUM(value) AS total_value",
    )
    .await
    .unwrap_or_else(|e| panic!("create aggregate: {e}"));
    srv.exec("INSERT INTO cagg_restart_src (id, ts, value) VALUES ('a', 1000, 1.5)")
        .await
        .unwrap_or_else(|e| panic!("insert before restart: {e}"));
    assert!(
        await_rows_aggregated(&srv, "cagg_restart_view", 1).await >= 1,
        "the aggregate must be active before the restart, or the check below proves nothing"
    );

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) =
        TestServer::open_on_path_with_timeseries_memtable_budget(dir, FLUSH_EVERY_INGEST).await;

    srv.exec("INSERT INTO cagg_restart_src (id, ts, value) VALUES ('b', 2000, 2.5)")
        .await
        .unwrap_or_else(|e| panic!("insert after restart: {e}"));
    let seen = await_rows_aggregated(&srv, "cagg_restart_view", 1).await;
    assert!(
        seen >= 1,
        "a row flushed after the restart must reach the aggregate; rows_aggregated = {seen}. \
         Zero means boot never re-registered the aggregate on the Data Plane."
    );
}
