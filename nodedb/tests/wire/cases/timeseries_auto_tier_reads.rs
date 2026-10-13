// SPDX-License-Identifier: BUSL-1.1

//! AUTO_TIER reads of a timeseries collection.
//!
//! Rows older than the raw retention window are read from the policy's
//! first downsample tier. COUNT / SUM / MIN / MAX / AVG merge exactly from
//! its buckets. A query the buckets cannot answer exactly is refused: raw
//! row reads, GROUP BY columns, predicates on other columns, and time bounds
//! inside a bucket.

use crate::harness::TestServer;

/// A collection whose rows all sit at the start of the epoch, far older
/// than the one-day raw window, under an AUTO_TIER policy with a one-minute
/// tier. Values 1, 2 and 6 land in the minute buckets [0, 60000) and
/// [60000, 120000).
async fn seed(server: &TestServer, c: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {c} (ts BIGINT TIME_KEY, host TEXT, v FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    server
        .exec(&format!(
            "CREATE RETENTION POLICY {c}_policy ON {c} (\
             RAW RETAIN '1 day', \
             DOWNSAMPLE TO '1 minute' AGGREGATE (SUM(v), MIN(v), MAX(v), COUNT(*)) \
             RETAIN 'forever')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create policy on {c}: {e}"));
    server
        .exec(&format!(
            "ALTER RETENTION POLICY {c}_policy ON {c} SET AUTO_TIER = TRUE"
        ))
        .await
        .unwrap_or_else(|e| panic!("enable AUTO_TIER on {c}: {e}"));
    for (ts, host, v) in [(1_000, "a", 1.0), (2_000, "b", 2.0), (61_000, "a", 6.0)] {
        server
            .exec(&format!(
                "INSERT INTO {c} (ts, host, v) VALUES ({ts}, '{host}', {v})"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {c} row {ts}: {e}"));
    }
}

fn num(cell: &str) -> f64 {
    cell.parse()
        .unwrap_or_else(|e| panic!("cell '{cell}' is not a number: {e}"))
}

/// Scalar aggregates over the tiered range merge the tier's buckets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_aggregates_read_the_tier() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "tier_scalar").await;
    let rows = server
        .query_rows("SELECT COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM tier_scalar")
        .await
        .unwrap_or_else(|e| panic!("tiered aggregate: {e}"));
    assert_eq!(rows.len(), 1, "one row: {rows:?}");
    let row = &rows[0];
    assert_eq!(row[0], "3");
    assert_eq!(num(&row[1]), 9.0);
    assert_eq!(num(&row[2]), 1.0);
    assert_eq!(num(&row[3]), 6.0);
    assert_eq!(num(&row[4]), 3.0);
}

/// A time bucket that is a multiple of the tier resolution groups the
/// tier's buckets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucketed_aggregate_reads_the_tier() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "tier_bucket").await;
    let mut rows: Vec<String> = server
        .query_rows("SELECT time_bucket('1 minute', ts) AS b, COUNT(*) FROM tier_bucket GROUP BY b")
        .await
        .unwrap_or_else(|e| panic!("tiered bucket aggregate: {e}"))
        .into_iter()
        .map(|r| r[1].clone())
        .collect();
    rows.sort();
    assert_eq!(rows, vec!["1", "2"], "two minute buckets");
}

/// Reads the tier cannot answer exactly are refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inexact_tier_reads_are_refused() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "tier_refused").await;
    for (sql, why) in [
        ("SELECT * FROM tier_refused", "raw rows"),
        (
            "SELECT host, COUNT(*) FROM tier_refused GROUP BY host",
            "GROUP BY",
        ),
        (
            "SELECT COUNT(*) FROM tier_refused WHERE host = 'a'",
            "predicate",
        ),
        (
            "SELECT COUNT(*) FROM tier_refused WHERE ts >= 1500",
            "time bounds",
        ),
    ] {
        let result = server.query_rows(sql).await;
        assert!(
            result.is_err(),
            "{why}: {sql} must be refused, got {result:?}"
        );
    }
}

/// Bounds on bucket edges read exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aligned_bounds_read_the_tier() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "tier_aligned").await;
    let rows = server
        .query_rows("SELECT COUNT(*), SUM(v) FROM tier_aligned WHERE ts >= 60000")
        .await
        .unwrap_or_else(|e| panic!("aligned tier aggregate: {e}"));
    assert_eq!(rows[0][0], "1");
    assert_eq!(num(&rows[0][1]), 6.0);
}

/// EXPLAIN TIERS names the tier the old range reads from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explain_tiers_shows_the_tier() {
    let server = TestServer::start().await;
    seed(&server, "tier_explain").await;
    let rows = server
        .query_text_joined("EXPLAIN TIERS ON tier_explain RANGE 0 120000")
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN TIERS: {e}"));
    assert!(
        rows.iter().any(|r| r.contains("tier_explain_policy")),
        "the plan names the tier aggregate: {rows:?}"
    );
}
