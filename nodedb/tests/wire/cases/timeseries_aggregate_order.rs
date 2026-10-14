// SPDX-License-Identifier: BUSL-1.1

//! ORDER BY over a grouped timeseries aggregate, before LIMIT.
//!
//! The native grouped scan sorts its group rows, then cuts them to LIMIT.
//! A sort on a group column, on an aggregate, on an aggregate's alias, and
//! on a time bucket each order the result, ascending and descending. An
//! aggregate named only in ORDER BY takes the generic aggregate and sorts
//! the same way.

use crate::harness::TestServer;

/// Hosts a (v 1, 2), b (3, 4), c (5): counts 2, 2, 1 and sums 3, 7, 5.
async fn seed(server: &TestServer, c: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {c} \
             (ts BIGINT TIME_KEY, host TEXT, v BIGINT) WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {c} (ts, host, v) VALUES \
             (1000, 'a', 1), (2000, 'a', 2), (3000, 'b', 3), (4000, 'b', 4), (5000, 'c', 5)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {c}: {e}"));
}

/// The rows of `sql` in returned order, cells joined by `|`.
async fn ordered(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect()
}

async fn check_order(server: &TestServer, c: &str) {
    let cases: [(String, Vec<&str>); 8] = [
        (
            format!("SELECT host, SUM(v) FROM {c} GROUP BY host ORDER BY host ASC LIMIT 2"),
            vec!["a|3", "b|7"],
        ),
        (
            format!("SELECT host, SUM(v) FROM {c} GROUP BY host ORDER BY host DESC LIMIT 2"),
            vec!["c|5", "b|7"],
        ),
        (
            format!("SELECT host, SUM(v) FROM {c} GROUP BY host ORDER BY SUM(v) ASC LIMIT 2"),
            vec!["a|3", "c|5"],
        ),
        (
            format!("SELECT host, SUM(v) FROM {c} GROUP BY host ORDER BY SUM(v) DESC LIMIT 2"),
            vec!["b|7", "c|5"],
        ),
        (
            format!("SELECT host, SUM(v) AS s FROM {c} GROUP BY host ORDER BY s DESC LIMIT 1"),
            vec!["b|7"],
        ),
        (
            format!(
                "SELECT host, COUNT(*) AS n FROM {c} GROUP BY host \
                 ORDER BY n ASC, host DESC LIMIT 2"
            ),
            vec!["c|1", "b|2"],
        ),
        (
            format!(
                "SELECT time_bucket('2s', ts) AS b, COUNT(*) FROM {c} GROUP BY b \
                 ORDER BY b DESC LIMIT 1"
            ),
            vec!["4000|2"],
        ),
        // Sorted by an aggregate the SELECT list does not compute.
        (
            format!("SELECT host FROM {c} GROUP BY host ORDER BY SUM(v) DESC LIMIT 2"),
            vec!["b", "c"],
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(ordered(server, &sql).await, want, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_over_memtable_groups() {
    let server = TestServer::start().await;
    seed(&server, "ts_order_live").await;
    check_order(&server, "ts_order_live").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_over_flushed_groups() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed(&server, "ts_order_flushed").await;
    check_order(&server, "ts_order_flushed").await;
}
