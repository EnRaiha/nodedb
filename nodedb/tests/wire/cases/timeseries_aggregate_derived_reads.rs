// SPDX-License-Identifier: BUSL-1.1

//! Reading an aggregate through a derived table, and bucketing a timestamp
//! column other than the time key.
//!
//! A derived table, a filter over it, and an outer aggregate over it read
//! each aggregate and each aliased GROUP BY key under its SELECT-list name,
//! on a timeseries and on a columnar collection. A `time_bucket` over a
//! timestamp column that is not the time key buckets by that column.

use std::time::{Duration, SystemTime};

use crate::harness::TestServer;

/// `2026-10-01T00:00:00Z` in seconds since the Unix epoch.
const MIDNIGHT_SECS: u64 = 1_790_812_800;
const HOUR_SECS: u64 = 3_600;

/// Two `TMR` rows in the first hour (`h_nt` mean 40001.0) and one `KAK` row
/// in the second hour.
async fn seed(server: &TestServer, c: &str, engine: &str) {
    let ddl = match engine {
        "timeseries" => format!(
            "CREATE COLLECTION {c} (ts TIMESTAMP TIME_KEY, station VARCHAR, h_nt FLOAT) \
             WITH (engine='timeseries')"
        ),
        _ => format!(
            "CREATE COLLECTION {c} (ts TIMESTAMP, station VARCHAR, h_nt FLOAT) \
             WITH (engine='{engine}')"
        ),
    };
    let insert = format!(
        "INSERT INTO {c} (ts, station, h_nt) VALUES \
         ('2026-10-01T00:00:00Z', 'TMR', 40000.5), \
         ('2026-10-01T00:01:00Z', 'TMR', 40001.5), \
         ('2026-10-01T01:00:00Z', 'KAK', 39990.0)"
    );
    for sql in [ddl, insert] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{engine}: {sql}: {e}"));
    }
}

/// The rows of `sql` over the simple protocol.
async fn rows(server: &TestServer, sql: &str) -> Vec<Vec<String>> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn num(cell: &str) -> f64 {
    cell.parse()
        .unwrap_or_else(|e| panic!("cell '{cell}' is not a number: {e}"))
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

async fn check_derived_reads(server: &TestServer, c: &str, engine: &str) {
    let mean_of_all = (40000.5 + 40001.5 + 39990.0) / 3.0;

    let sql = format!("SELECT * FROM (SELECT AVG(h_nt) AS m FROM {c}) AS t WHERE m > 0");
    let got = rows(server, &sql).await;
    assert_eq!(got.len(), 1, "{engine}: {sql}: {got:?}");
    assert!(
        close(num(&got[0][0]), mean_of_all),
        "{engine}: {sql}: {got:?}"
    );

    let sql = format!(
        "SELECT s, m FROM (SELECT station AS s, AVG(h_nt) AS m FROM {c} GROUP BY station) AS t \
         WHERE m > 0 ORDER BY s"
    );
    let got = rows(server, &sql).await;
    assert_eq!(got.len(), 2, "{engine}: {sql}: {got:?}");
    assert_eq!(got[0][0], "KAK", "{engine}: {sql}");
    assert!(close(num(&got[0][1]), 39990.0), "{engine}: {sql}: {got:?}");
    assert_eq!(got[1][0], "TMR", "{engine}: {sql}");
    assert!(close(num(&got[1][1]), 40001.0), "{engine}: {sql}: {got:?}");

    let sql = format!(
        "SELECT MAX(m) AS top FROM \
         (SELECT station AS s, AVG(h_nt) AS m FROM {c} GROUP BY station) AS t"
    );
    let got = rows(server, &sql).await;
    assert_eq!(got.len(), 1, "{engine}: {sql}: {got:?}");
    assert!(close(num(&got[0][0]), 40001.0), "{engine}: {sql}: {got:?}");

    let sql = format!(
        "SELECT b, n FROM \
         (SELECT time_bucket('1 hour', ts) AS b, COUNT(*) AS n FROM {c} GROUP BY b) AS t \
         WHERE n > 1"
    );
    let got = rows(server, &sql).await;
    assert_eq!(got.len(), 1, "{engine}: {sql}: {got:?}");
    assert_eq!(
        got[0][1], "2",
        "{engine}: {sql}: the first hour holds two rows"
    );
    assert!(!got[0][0].is_empty(), "{engine}: {sql}: b reads its bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_aggregates_read_through_a_derived_table() {
    let server = TestServer::start().await;
    seed(&server, "ts_derived_reads", "timeseries").await;
    check_derived_reads(&server, "ts_derived_reads", "timeseries").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_aggregates_read_through_a_derived_table() {
    let server = TestServer::start().await;
    seed(&server, "col_derived_reads", "columnar").await;
    check_derived_reads(&server, "col_derived_reads", "columnar").await;
}

/// Every row's `ts` falls in the first hour. Its `recorded_at` falls in the
/// first hour once and in the second hour twice.
async fn seed_two_timestamps(server: &TestServer, c: &str) {
    for sql in [
        format!(
            "CREATE COLLECTION {c} (ts TIMESTAMP TIME_KEY, recorded_at TIMESTAMP, v FLOAT) \
             WITH (engine='timeseries')"
        ),
        format!(
            "INSERT INTO {c} (ts, recorded_at, v) VALUES \
             ('2026-10-01T00:00:00Z', '2026-10-01T00:10:00Z', 1.0), \
             ('2026-10-01T00:01:00Z', '2026-10-01T01:10:00Z', 2.0), \
             ('2026-10-01T00:02:00Z', '2026-10-01T01:20:00Z', 3.0)"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// `(bucket, count)` of each row `sql` returns, in returned order.
async fn bucket_counts(server: &TestServer, sql: &str) -> Vec<(SystemTime, i64)> {
    server
        .client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| (row.get::<_, SystemTime>("b"), row.get::<_, i64>("n")))
        .collect()
}

async fn check_bucket_column(server: &TestServer, c: &str) {
    let hour = |n: u64| SystemTime::UNIX_EPOCH + Duration::from_secs(MIDNIGHT_SECS + n * HOUR_SECS);

    let sql = format!(
        "SELECT time_bucket('1 hour', recorded_at) AS b, COUNT(*) AS n FROM {c} \
         GROUP BY b ORDER BY b"
    );
    assert_eq!(
        bucket_counts(server, &sql).await,
        vec![(hour(0), 1), (hour(1), 2)],
        "{sql}: buckets follow recorded_at, not the time key"
    );

    let sql = format!(
        "SELECT time_bucket('1 hour', ts) AS b, COUNT(*) AS n FROM {c} GROUP BY b ORDER BY b"
    );
    assert_eq!(
        bucket_counts(server, &sql).await,
        vec![(hour(0), 3)],
        "{sql}: every time key falls in the first hour"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_bucket_over_a_non_time_key_column_buckets_by_that_column() {
    let server = TestServer::start().await;
    seed_two_timestamps(&server, "ts_bucket_column").await;
    check_bucket_column(&server, "ts_bucket_column").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_bucket_over_a_non_time_key_column_in_flushed_partitions() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed_two_timestamps(&server, "ts_bucket_column_flushed").await;
    check_bucket_column(&server, "ts_bucket_column_flushed").await;
}
