// SPDX-License-Identifier: BUSL-1.1

//! AUTO_TIER reads across a two-tier policy.
//!
//! Raw rows are kept for one day, one-minute buckets for two more days, and
//! one-hour buckets forever. An unbounded aggregate reads each part of the
//! range from its own source: recent rows raw, the next two days from tier
//! 1, and everything older from tier 2.

use crate::harness::TestServer;

const MINUTE: i64 = 60_000;
const HOUR: i64 = 3_600_000;

fn now_ms() -> i64 {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch");
    i64::try_from(since_epoch.as_millis()).expect("millis fit i64")
}

/// Seed one row per source: three epoch-start rows in tier 2's range, one
/// row 36 hours old in tier 1's range, and one current row read raw.
/// `v` totals 1 + 2 + 6 + 10 + 100 = 119.
async fn seed(server: &TestServer, c: &str) -> i64 {
    server
        .exec(&format!(
            "CREATE COLLECTION {c} (ts BIGINT TIME_KEY, v FLOAT) WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    server
        .exec(&format!(
            "CREATE RETENTION POLICY {c}_policy ON {c} (\
             RAW RETAIN '1 day', \
             DOWNSAMPLE TO '1 minute' AGGREGATE (SUM(v), MIN(v), MAX(v), COUNT(*)) \
             RETAIN '2 days', \
             DOWNSAMPLE TO '1 hour' AGGREGATE (SUM(v), MIN(v), MAX(v), COUNT(*)) \
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
    let now = now_ms();
    let tier1_row = (now - 36 * HOUR).div_euclid(MINUTE) * MINUTE;
    for (ts, v) in [
        (1_000, 1.0),
        (2_000, 2.0),
        (61_000, 6.0),
        (tier1_row, 10.0),
        (now, 100.0),
    ] {
        server
            .exec(&format!("INSERT INTO {c} (ts, v) VALUES ({ts}, {v})"))
            .await
            .unwrap_or_else(|e| panic!("seed {c} row {ts}: {e}"));
    }
    now
}

fn num(cell: &str) -> f64 {
    cell.parse()
        .unwrap_or_else(|e| panic!("cell '{cell}' is not a number: {e}"))
}

/// An unbounded aggregate merges raw rows, tier 1, and tier 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unbounded_aggregate_reads_both_tiers() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "two_tier_all").await;
    let rows = server
        .query_rows("SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM two_tier_all")
        .await
        .unwrap_or_else(|e| panic!("two-tier aggregate: {e}"));
    assert_eq!(rows[0][0], "5", "every row counted once: {rows:?}");
    assert_eq!(num(&rows[0][1]), 119.0);
    assert_eq!(num(&rows[0][2]), 1.0);
    assert_eq!(num(&rows[0][3]), 100.0);
}

/// A range wholly inside tier 2's span reads only tier 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_inside_tier_two_reads_tier_two() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "two_tier_old").await;
    let rows = server
        .query_rows(&format!(
            "SELECT COUNT(*), SUM(v) FROM two_tier_old WHERE ts < {HOUR}"
        ))
        .await
        .unwrap_or_else(|e| panic!("tier-2 aggregate: {e}"));
    assert_eq!(rows[0][0], "3");
    assert_eq!(num(&rows[0][1]), 9.0);
}

/// An hourly bucket merges tier 2's buckets; a one-minute bucket cannot be
/// cut from them and is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_width_must_divide_into_tier_two() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "two_tier_bucket").await;
    let rows = server
        .query_rows(&format!(
            "SELECT time_bucket('1 hour', ts) AS b, COUNT(*) FROM two_tier_bucket \
             WHERE ts < {HOUR} GROUP BY b"
        ))
        .await
        .unwrap_or_else(|e| panic!("hourly tier-2 buckets: {e}"));
    assert_eq!(rows.len(), 1, "one hour bucket: {rows:?}");
    assert_eq!(rows[0][1], "3");

    let refused = server
        .query_rows(
            "SELECT time_bucket('1 minute', ts) AS b, COUNT(*) FROM two_tier_bucket GROUP BY b",
        )
        .await;
    assert!(refused.is_err(), "minute buckets over tier 2: {refused:?}");
}
