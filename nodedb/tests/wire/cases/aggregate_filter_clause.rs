// SPDX-License-Identifier: BUSL-1.1

//! `FILTER (WHERE ...)` on aggregates.
//!
//! A filtered aggregate folds only the rows its predicate admits. It lands
//! under its own output key, so it never collides with the same aggregate
//! unfiltered or with another filter. HAVING and ORDER BY can name it.

use crate::harness::TestServer;

async fn seed_document(server: &TestServer, c: &str) {
    server
        .exec(&format!("CREATE COLLECTION {c} TYPE DOCUMENT SCHEMALESS"))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {c} (id, host, v) VALUES \
             ('1', 'a', 1), ('2', 'a', 2), ('3', 'b', 3), ('4', 'b', 4), ('5', 'c', 5)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {c}: {e}"));
}

async fn seed_timeseries(server: &TestServer, c: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {c} \
             (ts BIGINT TIME_KEY, host TEXT, v BIGINT) WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    for (ts, host, v) in [
        (1_000, "a", 1),
        (2_000, "a", 2),
        (3_000, "b", 3),
        (4_000, "b", 4),
        (5_000, "c", 5),
    ] {
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

async fn scalar_filters(server: &TestServer, c: &str) {
    let rows = server
        .query_rows(&format!(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE v > 2), \
             SUM(v) FILTER (WHERE host = 'a'), SUM(v) FILTER (WHERE host = 'b') FROM {c}"
        ))
        .await
        .unwrap_or_else(|e| panic!("{c}: scalar FILTER: {e}"));
    assert_eq!(rows.len(), 1, "{c}: one row: {rows:?}");
    let row = &rows[0];
    assert_eq!(row[0], "5", "{c}: unfiltered COUNT(*)");
    assert_eq!(row[1], "3", "{c}: COUNT(*) FILTER keeps v > 2");
    assert_eq!(num(&row[2]), 3.0, "{c}: SUM FILTER host a");
    assert_eq!(num(&row[3]), 7.0, "{c}: SUM FILTER host b, a separate key");
}

async fn grouped_filter_with_having(server: &TestServer, c: &str) {
    let mut rows: Vec<String> = server
        .query_rows(&format!(
            "SELECT host, COUNT(*) FILTER (WHERE v >= 2) AS big FROM {c} \
             GROUP BY host HAVING COUNT(*) FILTER (WHERE v >= 2) > 0"
        ))
        .await
        .unwrap_or_else(|e| panic!("{c}: grouped FILTER: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec!["a|1", "b|2", "c|1"],
        "{c}: HAVING binds to the filtered aggregate"
    );
}

/// FILTER over a schemaless document collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_over_documents() {
    let server = TestServer::start().await;
    seed_document(&server, "agg_filter_doc").await;
    scalar_filters(&server, "agg_filter_doc").await;
    grouped_filter_with_having(&server, "agg_filter_doc").await;
}

/// FILTER over a timeseries collection, which leaves the native grouped
/// scan for the generic aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_over_timeseries() {
    let server = TestServer::start().await;
    seed_timeseries(&server, "agg_filter_ts").await;
    scalar_filters(&server, "agg_filter_ts").await;
    grouped_filter_with_having(&server, "agg_filter_ts").await;
}

/// FILTER over flushed timeseries partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_over_flushed_timeseries() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    seed_timeseries(&server, "agg_filter_ts_flushed").await;
    scalar_filters(&server, "agg_filter_ts_flushed").await;
}

/// The same aggregate with and without a filter, in one grouping-set query,
/// keeps two separate values per group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_in_rollup_keeps_separate_keys() {
    let server = TestServer::start().await;
    seed_document(&server, "agg_filter_rollup").await;
    let rows = server
        .query_rows(
            "SELECT host, SUM(v), SUM(v) FILTER (WHERE v > 1) FROM agg_filter_rollup \
             GROUP BY ROLLUP (host)",
        )
        .await
        .unwrap_or_else(|e| panic!("FILTER in ROLLUP: {e}"));
    let total = rows
        .iter()
        .find(|r| r[0].is_empty())
        .unwrap_or_else(|| panic!("no grand total: {rows:?}"));
    assert_eq!(num(&total[1]), 15.0, "grand total, unfiltered");
    assert_eq!(num(&total[2]), 14.0, "grand total, filtered");
}
