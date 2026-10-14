// SPDX-License-Identifier: BUSL-1.1

//! A continuous aggregate reads as a relation: `SELECT ... FROM <aggregate>`
//! returns its finalized buckets, one row per bucket and group, with the
//! `bucket` column, the GROUP BY columns and the output columns. WHERE,
//! ORDER BY, LIMIT, an outer aggregate and a join all run over those rows.

use std::time::Duration;

use crate::harness::TestServer;

/// A memtable budget of one byte flushes every ingest to a partition, and a
/// flush is what an `OnFlush` aggregate folds.
const FLUSH_EVERY_INGEST: usize = 1;

/// Two hosts in the first minute: `a` sums to 4 over 2 rows, `b` to 30.
const ROWS: [(i64, &str, f64); 4] = [
    (1_000, "a", 1.5),
    (2_000, "a", 2.5),
    (3_000, "b", 10.0),
    (4_000, "b", 20.0),
];

async fn seed(srv: &TestServer, src: &str, view: &str, with: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {src} (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
         WITH (engine='timeseries')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {src}: {e}"));
    srv.exec(&format!(
        "CREATE CONTINUOUS AGGREGATE {view} ON {src} BUCKET '1m' \
         AGGREGATE SUM(value) AS s, AVG(value) AS a, COUNT(*) AS c GROUP BY host{with}"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {view}: {e}"));
    for (ts, host, value) in ROWS {
        srv.exec(&format!(
            "INSERT INTO {src} (ts, host, value) VALUES ({ts}, '{host}', {value})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert row at {ts}: {e}"));
    }
}

fn num(cell: &str) -> f64 {
    cell.parse()
        .unwrap_or_else(|e| panic!("cell '{cell}' is not a number: {e}"))
}

/// Rows of `sql`, polled until there are `want` of them: the flush that
/// feeds an `OnFlush` aggregate runs on the core after the insert answers.
async fn rows_when(srv: &TestServer, sql: &str, want: usize) -> Vec<Vec<String>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rows = srv
            .query_rows(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        if rows.len() >= want || tokio::time::Instant::now() >= deadline {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_from_continuous_aggregate_returns_finalized_buckets() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    seed(&srv, "cagg_serve_src", "cagg_serve_view", "").await;

    let rows = rows_when(
        &srv,
        "SELECT bucket, host, s, a, c FROM cagg_serve_view ORDER BY host",
        2,
    )
    .await;
    assert_eq!(rows.len(), 2, "one row per host: {rows:?}");
    for (row, (host, sum, avg)) in rows.iter().zip([("a", 4.0, 2.0), ("b", 30.0, 15.0)]) {
        assert_eq!(num(&row[0]), 0.0, "the first minute's bucket: {row:?}");
        assert_eq!(row[1], host);
        assert_eq!(num(&row[2]), sum);
        assert_eq!(num(&row[3]), avg);
        assert_eq!(row[4], "2");
    }

    let ad_hoc = srv
        .query_rows(
            "SELECT host, SUM(value), COUNT(*) FROM cagg_serve_src GROUP BY host ORDER BY host",
        )
        .await
        .unwrap_or_else(|e| panic!("ad-hoc aggregate: {e}"));
    let served: Vec<(String, f64, String)> = rows
        .iter()
        .map(|r| (r[1].clone(), num(&r[2]), r[4].clone()))
        .collect();
    let expected: Vec<(String, f64, String)> = ad_hoc
        .iter()
        .map(|r| (r[0].clone(), num(&r[1]), r[2].clone()))
        .collect();
    assert_eq!(served, expected, "the aggregate equals the ad-hoc query");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_aggregate_where_order_limit_and_outer_aggregate() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    seed(&srv, "cagg_shape_src", "cagg_shape_view", "").await;
    rows_when(&srv, "SELECT host FROM cagg_shape_view", 2).await;

    let rows = srv
        .query_rows(
            "SELECT host, MAX(s) FROM cagg_shape_view WHERE host <> 'b' \
             GROUP BY host ORDER BY host LIMIT 1",
        )
        .await
        .unwrap_or_else(|e| panic!("outer aggregate: {e}"));
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0][0], "a");
    assert_eq!(num(&rows[0][1]), 4.0);

    let rows = srv
        .query_rows("SELECT host, c FROM cagg_shape_view ORDER BY s DESC LIMIT 1")
        .await
        .unwrap_or_else(|e| panic!("order and limit: {e}"));
    assert_eq!(rows, vec![vec!["b".to_string(), "2".to_string()]]);

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM cagg_shape_view")
        .await
        .unwrap_or_else(|e| panic!("count: {e}"));
    assert_eq!(rows, vec![vec!["2".to_string()]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_aggregate_joins_a_collection() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    seed(&srv, "cagg_join_src", "cagg_join_view", "").await;
    rows_when(&srv, "SELECT host FROM cagg_join_view", 2).await;
    srv.exec("CREATE COLLECTION cagg_join_hosts")
        .await
        .unwrap_or_else(|e| panic!("create hosts: {e}"));
    for (id, name, region) in [("h1", "a", "eu"), ("h2", "b", "us")] {
        srv.exec(&format!(
            "INSERT INTO cagg_join_hosts (id, name, region) VALUES ('{id}', '{name}', '{region}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert host {id}: {e}"));
    }

    let rows = srv
        .query_rows(
            "SELECT h.region, v.s FROM cagg_join_view v \
             JOIN cagg_join_hosts h ON v.host = h.name ORDER BY h.region",
        )
        .await
        .unwrap_or_else(|e| panic!("join: {e}"));
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0][0], "eu");
    assert_eq!(num(&rows[0][1]), 4.0);
    assert_eq!(rows[1][0], "us");
    assert_eq!(num(&rows[1][1]), 30.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_aggregate_rows_survive_restart() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    seed(&srv, "cagg_restart_rows_src", "cagg_restart_rows_view", "").await;
    let query = "SELECT bucket, host, s, a, c FROM cagg_restart_rows_view ORDER BY host";
    let before = rows_when(&srv, query, 2).await;
    assert_eq!(before.len(), 2, "{before:?}");

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) =
        TestServer::open_on_path_with_timeseries_memtable_budget(dir, FLUSH_EVERY_INGEST).await;
    let after = rows_when(&srv, query, 2).await;
    assert_eq!(after, before, "the same rows after a restart");
}

/// A `Manual` aggregate reads its last refresh, never rows after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_aggregate_reads_its_last_refresh() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    seed(
        &srv,
        "cagg_manual_rows_src",
        "cagg_manual_rows_view",
        " WITH (refresh_policy = 'manual')",
    )
    .await;
    let query = "SELECT host, c FROM cagg_manual_rows_view ORDER BY host";
    assert!(
        srv.query_rows(query)
            .await
            .unwrap_or_else(|e| panic!("read before refresh: {e}"))
            .is_empty(),
        "no row before the first REFRESH"
    );

    srv.exec("REFRESH CONTINUOUS AGGREGATE cagg_manual_rows_view")
        .await
        .unwrap_or_else(|e| panic!("refresh: {e}"));
    let refreshed = srv
        .query_rows(query)
        .await
        .unwrap_or_else(|e| panic!("read after refresh: {e}"));
    assert_eq!(
        refreshed,
        vec![
            vec!["a".to_string(), "2".to_string()],
            vec!["b".to_string(), "2".to_string()]
        ]
    );

    srv.exec("INSERT INTO cagg_manual_rows_src (ts, host, value) VALUES (5000, 'a', 1.0)")
        .await
        .unwrap_or_else(|e| panic!("insert after refresh: {e}"));
    assert_eq!(
        srv.query_rows(query)
            .await
            .unwrap_or_else(|e| panic!("read before the second refresh: {e}")),
        refreshed,
        "a row after the cut waits for the next REFRESH"
    );
}
