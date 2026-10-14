// SPDX-License-Identifier: BUSL-1.1

//! A continuous aggregate keeps its rows across a restart of a Raft-backed
//! node, and folds rows that existed before it was created.
//!
//! The per-core aggregate registry is in-memory. Boot re-registers every
//! stored aggregate from the catalog. The maintenance tick then reads the
//! aggregate's image and folds every partition the image does not name, so
//! rows flushed before the restart and rows flushed after it both count
//! once.

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
    let seen = await_rows_aggregated(&srv, "cagg_restart_view", 2).await;
    assert_eq!(
        seen, 2,
        "the row flushed before the restart and the row flushed after it must each count \
         once; rows_aggregated = {seen}. One means the pre-restart partition was never \
         refolded; three means it was counted twice."
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_aggregate_backfills_rows_present_at_create() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    srv.exec(
        "CREATE COLLECTION cagg_backfill_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create source: {e}"));
    for (id, ts, v) in [("a", 1000, 1.5), ("b", 2000, 2.5), ("c", 3000, 3.5)] {
        srv.exec(&format!(
            "INSERT INTO cagg_backfill_src (id, ts, value) VALUES ('{id}', {ts}, {v})"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    srv.exec(
        "CREATE CONTINUOUS AGGREGATE cagg_backfill_view \
         ON cagg_backfill_src BUCKET '5m' \
         AGGREGATE SUM(value) AS total_value",
    )
    .await
    .unwrap_or_else(|e| panic!("create aggregate: {e}"));
    let seen = await_rows_aggregated(&srv, "cagg_backfill_view", 3).await;
    assert_eq!(
        seen, 3,
        "an aggregate created over existing rows folds every flushed partition once; \
         rows_aggregated = {seen}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_continuous_aggregate_folds_on_refresh() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    srv.exec(
        "CREATE COLLECTION cagg_manual_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create source: {e}"));
    srv.exec(
        "CREATE CONTINUOUS AGGREGATE cagg_manual_view \
         ON cagg_manual_src BUCKET '5m' \
         AGGREGATE SUM(value) AS total_value \
         WITH (refresh_policy = 'manual')",
    )
    .await
    .unwrap_or_else(|e| panic!("create aggregate: {e}"));
    for (id, ts, v) in [("a", 1000, 1.5), ("b", 2000, 2.5)] {
        srv.exec(&format!(
            "INSERT INTO cagg_manual_src (id, ts, value) VALUES ('{id}', {ts}, {v})"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    assert_eq!(
        rows_aggregated(&srv, "cagg_manual_view").await,
        0,
        "a manual aggregate takes no fold before REFRESH"
    );
    srv.exec("REFRESH CONTINUOUS AGGREGATE cagg_manual_view")
        .await
        .unwrap_or_else(|e| panic!("refresh: {e}"));
    assert_eq!(
        rows_aggregated(&srv, "cagg_manual_view").await,
        2,
        "REFRESH answers once its cut is folded"
    );

    srv.exec("INSERT INTO cagg_manual_src (id, ts, value) VALUES ('c', 3000, 3.5)")
        .await
        .unwrap_or_else(|e| panic!("insert after refresh: {e}"));
    assert_eq!(
        rows_aggregated(&srv, "cagg_manual_view").await,
        2,
        "a row after the cut waits for the next REFRESH"
    );
    srv.exec("REFRESH CONTINUOUS AGGREGATE cagg_manual_view")
        .await
        .unwrap_or_else(|e| panic!("second refresh: {e}"));
    assert_eq!(rows_aggregated(&srv, "cagg_manual_view").await, 3);

    let err = srv
        .exec("REFRESH CONTINUOUS AGGREGATE cagg_missing_view")
        .await
        .expect_err("refresh of a missing aggregate");
    assert!(
        err.to_string().contains("does not exist"),
        "unexpected error: {err}"
    );
}

/// A retention policy's downsample tier is a continuous aggregate. Boot
/// registers it again from the stored policy, so it keeps folding after a
/// restart and counts the row flushed before it once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_tier_is_active_after_restart() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    srv.exec(
        "CREATE COLLECTION cagg_tier_src (ts BIGINT TIME_KEY, v FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create source: {e}"));
    srv.exec(
        "CREATE RETENTION POLICY cagg_tier_policy ON cagg_tier_src (\
         RAW RETAIN '3650 days', \
         DOWNSAMPLE TO '1 minute' AGGREGATE (SUM(v), COUNT(*)) RETAIN 'forever')",
    )
    .await
    .unwrap_or_else(|e| panic!("create policy: {e}"));
    let tier = "_policy_cagg_tier_policy_tier1";
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_else(|e| panic!("clock before the epoch: {e}"));
    srv.exec(&format!(
        "INSERT INTO cagg_tier_src (ts, v) VALUES ({now_ms}, 1.5)"
    ))
    .await
    .unwrap_or_else(|e| panic!("insert before restart: {e}"));
    assert!(
        await_rows_aggregated(&srv, tier, 1).await >= 1,
        "the tier must be active before the restart, or the check below proves nothing"
    );

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) =
        TestServer::open_on_path_with_timeseries_memtable_budget(dir, FLUSH_EVERY_INGEST).await;

    srv.exec(&format!(
        "INSERT INTO cagg_tier_src (ts, v) VALUES ({}, 2.5)",
        now_ms + 1
    ))
    .await
    .unwrap_or_else(|e| panic!("insert after restart: {e}"));
    let seen = await_rows_aggregated(&srv, tier, 2).await;
    assert_eq!(
        seen, 2,
        "the tier must fold the row after the restart and keep the row before it once; \
         rows_aggregated = {seen}"
    );
}

/// A dropped aggregate's state does not survive in its root's image across a
/// restart. The state of the aggregates that exist stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_drops_state_of_dropped_aggregates() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    srv.exec(
        "CREATE COLLECTION cagg_prune_src \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create source: {e}"));
    for name in ["cagg_prune_kept", "cagg_prune_dropped"] {
        srv.exec(&format!(
            "CREATE CONTINUOUS AGGREGATE {name} \
             ON cagg_prune_src BUCKET '5m' \
             AGGREGATE SUM(value) AS total_value"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    srv.exec("INSERT INTO cagg_prune_src (id, ts, value) VALUES ('a', 1000, 1.5)")
        .await
        .unwrap_or_else(|e| panic!("insert: {e}"));
    assert!(await_rows_aggregated(&srv, "cagg_prune_kept", 1).await >= 1);
    srv.exec("DROP CONTINUOUS AGGREGATE cagg_prune_dropped")
        .await
        .unwrap_or_else(|e| panic!("drop: {e}"));

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, dir) =
        TestServer::open_on_path_with_timeseries_memtable_budget(dir, FLUSH_EVERY_INGEST).await;
    assert_eq!(await_rows_aggregated(&srv, "cagg_prune_kept", 1).await, 1);
    let image = std::fs::read(cagg_image_of(dir.path(), "cagg_prune_src"))
        .unwrap_or_else(|e| panic!("read the image: {e}"));
    let holds = |name: &str| image.windows(name.len()).any(|w| w == name.as_bytes());
    assert!(holds("cagg_prune_kept"), "the kept aggregate's state stays");
    assert!(
        !holds("cagg_prune_dropped"),
        "the dropped aggregate's state is gone from the image"
    );
}

/// The `cagg.state` image of `collection` under `data_dir`.
fn cagg_image_of(data_dir: &std::path::Path, collection: &str) -> std::path::PathBuf {
    let mut found = Vec::new();
    let mut stack = vec![data_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|n| n == "cagg.state")
                && path
                    .parent()
                    .and_then(|p| p.file_name())
                    .is_some_and(|n| n.to_string_lossy().contains(collection))
            {
                found.push(path);
            }
        }
    }
    assert_eq!(found.len(), 1, "one image of {collection}: {found:?}");
    found.remove(0)
}
