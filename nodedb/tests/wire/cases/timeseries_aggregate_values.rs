// SPDX-License-Identifier: BUSL-1.1

//! Aggregate values over a timeseries collection: DISTINCT and computed
//! arguments, MIN / MAX over text, NULL floats, `ts_stddev`, and an
//! aggregate over a join whose timeseries side sits in flushed partitions.

use crate::harness::TestServer;

/// Six rows over three hosts. One row has no `v`. `v` holds 1, 2, 2, 4, 5.
async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             (ts BIGINT TIME_KEY, host TEXT, v FLOAT) WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (ts, host, v) in [
        (1_000, "a", 1.0),
        (2_000, "a", 2.0),
        (3_000, "b", 2.0),
        (4_000, "b", 4.0),
        (5_000, "c", 5.0),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (ts, host, v) VALUES ({ts}, '{host}', {v})"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection} row {ts}: {e}"));
    }
    server
        .exec(&format!(
            "INSERT INTO {collection} (ts, host) VALUES (6000, 'c')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {collection} NULL row: {e}"));
}

/// The single row of `sql`.
async fn one_row(server: &TestServer, sql: &str) -> Vec<String> {
    let mut rows = server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: one row: {rows:?}");
    rows.remove(0)
}

fn num(cell: &str) -> f64 {
    cell.parse()
        .unwrap_or_else(|e| panic!("cell '{cell}' is not a number: {e}"))
}

async fn check_values(server: &TestServer, c: &str) {
    let row = one_row(
        server,
        &format!("SELECT COUNT(DISTINCT host), SUM(DISTINCT v), COUNT(DISTINCT v) FROM {c}"),
    )
    .await;
    assert_eq!(row[0], "3", "{c}: three distinct hosts");
    assert_eq!(num(&row[1]), 12.0, "{c}: distinct values 1 + 2 + 4 + 5");
    assert_eq!(row[2], "4", "{c}: four distinct non-NULL values");

    let row = one_row(server, &format!("SELECT SUM(v * 2), MAX(v + 1) FROM {c}")).await;
    assert_eq!(
        num(&row[0]),
        28.0,
        "{c}: computed argument doubles each value"
    );
    assert_eq!(
        num(&row[1]),
        6.0,
        "{c}: computed argument shifts the maximum"
    );

    let row = one_row(server, &format!("SELECT MIN(host), MAX(host) FROM {c}")).await;
    assert_eq!(row, vec!["a", "c"], "{c}: MIN / MAX compare text");

    let row = one_row(
        server,
        &format!("SELECT COUNT(*), COUNT(v), AVG(v), MIN(v) FROM {c}"),
    )
    .await;
    assert_eq!(row[0], "6", "{c}: COUNT(*) counts the NULL row");
    assert_eq!(row[1], "5", "{c}: COUNT(v) skips the NULL value");
    assert_eq!(num(&row[2]), 2.8, "{c}: AVG skips the NULL value");
    assert_eq!(num(&row[3]), 1.0, "{c}: MIN skips the NULL value");
}

/// Values over rows in the live memtable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_values_over_memtable_rows() {
    let server = TestServer::start().await;
    seed(&server, "ts_values_live").await;
    check_values(&server, "ts_values_live").await;
}

/// Values over rows that sit only in flushed partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_values_over_flushed_partitions() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "ts_values_flushed").await;
    check_values(&server, "ts_values_flushed").await;
}

/// `ts_stddev` is the population standard deviation, on the native grouped
/// scan and on the generic aggregate alike. The HAVING clause on a time
/// bucket sends the second query to the generic aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ts_stddev_matches_on_both_paths() {
    let server = TestServer::start().await;
    seed(&server, "ts_values_stddev").await;
    // Values 1, 2, 2, 4, 5: mean 2.8, population variance 2.16.
    let want = 2.16f64.sqrt();
    let native = one_row(&server, "SELECT ts_stddev(v) FROM ts_values_stddev").await;
    assert!(
        (num(&native[0]) - want).abs() < 1e-9,
        "native ts_stddev: {native:?}"
    );
    let generic = one_row(
        &server,
        "SELECT time_bucket('1h', ts) AS b, ts_stddev(v) AS s FROM ts_values_stddev \
         GROUP BY b HAVING COUNT(*) > 0",
    )
    .await;
    assert!(
        (num(&generic[1]) - want).abs() < 1e-9,
        "generic ts_stddev: {generic:?}"
    );
}

/// An aggregate with no generic accumulator is refused when the query shape
/// leaves the native grouped scan, never answered as a count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn function_without_generic_accumulator_is_refused() {
    let server = TestServer::start().await;
    seed(&server, "ts_values_refused").await;
    server
        .expect_error(
            "SELECT host, ts_percentile(v, 0.5) FROM ts_values_refused \
             GROUP BY ROLLUP (host)",
            "not supported",
        )
        .await;
}

/// An aggregate over a join reads the timeseries side from its flushed
/// partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_aggregate_reads_flushed_partitions() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "ts_values_join").await;
    server
        .exec("CREATE COLLECTION ts_join_hosts TYPE DOCUMENT SCHEMALESS")
        .await
        .unwrap_or_else(|e| panic!("create hosts: {e}"));
    server
        .exec(
            "INSERT INTO ts_join_hosts (id, name, region) VALUES \
             ('h1', 'a', 'east'), ('h2', 'b', 'east'), ('h3', 'c', 'west')",
        )
        .await
        .unwrap_or_else(|e| panic!("seed hosts: {e}"));
    let mut rows: Vec<String> = server
        .query_rows(
            "SELECT h.region, COUNT(*) FROM ts_values_join m \
             JOIN ts_join_hosts h ON m.host = h.name GROUP BY h.region",
        )
        .await
        .unwrap_or_else(|e| panic!("join aggregate: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec!["east|4", "west|2"],
        "join groups every flushed row"
    );
}
