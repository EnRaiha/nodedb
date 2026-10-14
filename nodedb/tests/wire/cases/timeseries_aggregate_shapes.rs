// SPDX-License-Identifier: BUSL-1.1

//! Aggregate shapes over a timeseries collection.
//!
//! Every shape answers the same whether the rows sit in the live memtable or
//! in flushed partitions: ROLLUP, CUBE with GROUPING(), HAVING (applied
//! before LIMIT), and GROUP BY on an expression, an output alias, or a
//! select-list position.

use crate::harness::TestServer;

/// Five rows over three hosts. `v` sums to 15.
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
        (3_000, "b", 3.0),
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
}

/// Every row's cells joined by `|`, sorted so assertions ignore row order.
async fn sorted_rows(server: &TestServer, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|cells| cells.join("|"))
        .collect();
    rows.sort();
    rows
}

/// A numeric cell as `f64`.
fn num(cell: &str) -> f64 {
    cell.parse()
        .unwrap_or_else(|e| panic!("cell '{cell}' is not a number: {e}"))
}

async fn rollup_counts_every_level(server: &TestServer, c: &str) {
    let rows = sorted_rows(
        server,
        &format!("SELECT host, COUNT(*) FROM {c} GROUP BY ROLLUP (host)"),
    )
    .await;
    assert_eq!(rows, vec!["a|2", "b|2", "c|1", "|5"], "{c}: ROLLUP rows");
}

async fn cube_marks_the_grand_total(server: &TestServer, c: &str) {
    let rows = server
        .query_rows(&format!(
            "SELECT host, GROUPING(host) AS g, SUM(v) AS s FROM {c} GROUP BY CUBE (host)"
        ))
        .await
        .unwrap_or_else(|e| panic!("{c}: CUBE: {e}"));
    assert_eq!(
        rows.len(),
        4,
        "{c}: three hosts plus the grand total: {rows:?}"
    );
    let total: Vec<&Vec<String>> = rows.iter().filter(|r| r[1] == "1").collect();
    assert_eq!(total.len(), 1, "{c}: one grand-total row: {rows:?}");
    assert!(total[0][0].is_empty(), "{c}: grand total has NULL host");
    assert_eq!(num(&total[0][2]), 15.0, "{c}: grand total sums every row");
    let a = rows
        .iter()
        .find(|r| r[0] == "a")
        .unwrap_or_else(|| panic!("{c}: no row for host a: {rows:?}"));
    assert_eq!(a[1], "0", "{c}: host a is a grouped value");
    assert_eq!(num(&a[2]), 3.0, "{c}: host a sums 1 + 2");
}

async fn having_keeps_matching_groups(server: &TestServer, c: &str) {
    let rows = sorted_rows(
        server,
        &format!("SELECT host, COUNT(*) AS n FROM {c} GROUP BY host HAVING COUNT(*) > 1"),
    )
    .await;
    assert_eq!(rows, vec!["a|2", "b|2"], "{c}: HAVING keeps two groups");
}

async fn having_applies_before_limit(server: &TestServer, c: &str) {
    let rows = server
        .query_rows(&format!(
            "SELECT host, COUNT(*) FROM {c} GROUP BY host \
             HAVING COUNT(*) = 1 ORDER BY host LIMIT 1"
        ))
        .await
        .unwrap_or_else(|e| panic!("{c}: HAVING + LIMIT: {e}"));
    assert_eq!(
        rows,
        vec![vec!["c".to_string(), "1".to_string()]],
        "{c}: LIMIT takes the first group HAVING keeps"
    );
}

async fn expression_alias_and_ordinal_keys_agree(server: &TestServer, c: &str) {
    let want = vec!["A|2", "B|2", "C|1"];
    for sql in [
        format!("SELECT UPPER(host), COUNT(*) FROM {c} GROUP BY UPPER(host)"),
        format!("SELECT UPPER(host) AS h, COUNT(*) FROM {c} GROUP BY h"),
        format!("SELECT UPPER(host) AS h, COUNT(*) FROM {c} GROUP BY 1"),
    ] {
        assert_eq!(sorted_rows(server, &sql).await, want, "{c}: {sql}");
    }
}

async fn run_every_shape(server: &TestServer, c: &str) {
    seed(server, c).await;
    rollup_counts_every_level(server, c).await;
    cube_marks_the_grand_total(server, c).await;
    having_keeps_matching_groups(server, c).await;
    having_applies_before_limit(server, c).await;
    expression_alias_and_ordinal_keys_agree(server, c).await;
}

/// Every shape over rows in the live memtable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_shapes_over_memtable_rows() {
    let server = TestServer::start().await;
    run_every_shape(&server, "ts_shapes_live").await;
}

/// Every shape over rows that sit only in flushed partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_shapes_over_flushed_partitions() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    run_every_shape(&server, "ts_shapes_flushed").await;
}

/// A GROUP BY position past the select list is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_position_out_of_range_is_refused() {
    let server = TestServer::start().await;
    seed(&server, "ts_shapes_ordinal").await;
    server
        .expect_error(
            "SELECT host, COUNT(*) FROM ts_shapes_ordinal GROUP BY 3",
            "GROUP BY position 3 is not in select list",
        )
        .await;
}

/// A HAVING over a time bucket filters the bucket rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn having_filters_time_buckets() {
    let server = TestServer::start().await;
    seed(&server, "ts_shapes_bucket").await;
    let rows = server
        .query_rows(
            "SELECT time_bucket('2s', ts) AS b, COUNT(*) AS n FROM ts_shapes_bucket \
             GROUP BY b HAVING COUNT(*) > 1",
        )
        .await
        .unwrap_or_else(|e| panic!("bucket HAVING: {e}"));
    // Buckets [0,2000) = 1 row, [2000,4000) = 2 rows, [4000,6000) = 2 rows.
    assert_eq!(
        rows.len(),
        2,
        "two buckets hold more than one row: {rows:?}"
    );
    assert!(
        rows.iter().all(|r| r[1] == "2"),
        "each kept bucket counts 2"
    );
}

/// An empty-string symbol group renders as `''`, not NULL, on the native
/// grouped scan and on the generic aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_string_group_key_is_not_null() {
    let server = TestServer::start().await;
    seed(&server, "ts_shapes_empty_key").await;
    server
        .exec("INSERT INTO ts_shapes_empty_key (ts, host, v) VALUES (6000, '', 6)")
        .await
        .unwrap_or_else(|e| panic!("seed empty host: {e}"));
    for sql in [
        "SELECT host, COUNT(*) FROM ts_shapes_empty_key WHERE host = '' GROUP BY host",
        "SELECT host, COUNT(*) FILTER (WHERE v > 0) FROM ts_shapes_empty_key \
         WHERE host = '' GROUP BY host",
    ] {
        let messages = server
            .client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let hosts: Vec<Option<String>> = messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(0).map(str::to_string))
                }
                _ => None,
            })
            .collect();
        assert_eq!(hosts, vec![Some(String::new())], "{sql}: '' is a value");
    }
}
