// SPDX-License-Identifier: BUSL-1.1

//! An aggregate inside a transaction reads the transaction's own writes on
//! every engine: staged inserts count, ROLLBACK discards them, and COMMIT
//! keeps them. Plain, grouped, and joined aggregates agree.

use crate::harness::TestServer;

/// `(engine, DDL column list)` for each engine under test. Each collection
/// holds `id` and an integer `n`.
const ENGINES: &[(&str, &str)] = &[
    (
        "document_schemaless",
        "(id STRING NOT NULL PRIMARY KEY, n INT)",
    ),
    ("document_strict", "(id STRING NOT NULL PRIMARY KEY, n INT)"),
    ("kv", "(key TEXT PRIMARY KEY, n INT)"),
    ("columnar", "COLUMNS (id TEXT, n INT)"),
];

/// The key column each engine's rows are written under.
fn key_column(engine: &str) -> &'static str {
    if engine == "kv" { "key" } else { "id" }
}

async fn setup(server: &TestServer, c: &str, engine: &str, columns: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {c} {columns} WITH (engine='{engine}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {c}: {e}"));
    let key = key_column(engine);
    for (id, n) in [("a", 1), ("b", 2)] {
        server
            .exec(&format!("INSERT INTO {c} ({key}, n) VALUES ('{id}', {n})"))
            .await
            .unwrap_or_else(|e| panic!("seed {c}: {e}"));
    }
}

async fn count_and_sum(server: &TestServer, c: &str) -> (String, String) {
    let rows = server
        .query_rows(&format!("SELECT COUNT(*), SUM(n) FROM {c}"))
        .await
        .unwrap_or_else(|e| panic!("{c}: aggregate: {e}"));
    assert_eq!(rows.len(), 1, "{c}: one row: {rows:?}");
    (rows[0][0].clone(), rows[0][1].clone())
}

async fn aggregate_reads_own_writes(engine: &str, columns: &str) {
    let server = TestServer::start().await;
    let c = format!("ryow_agg_{engine}");
    setup(&server, &c, engine, columns).await;
    let key = key_column(engine);

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("INSERT INTO {c} ({key}, n) VALUES ('c', 3)"))
        .await
        .unwrap_or_else(|e| panic!("{engine}: staged insert: {e}"));
    assert_eq!(
        count_and_sum(&server, &c).await,
        ("3".to_string(), "6".to_string()),
        "{engine}: the aggregate counts the staged row"
    );
    let grouped = server
        .query_rows(&format!(
            "SELECT n, COUNT(*) FROM {c} GROUP BY n HAVING n = 3"
        ))
        .await
        .unwrap_or_else(|e| panic!("{engine}: grouped aggregate: {e}"));
    assert_eq!(
        grouped,
        vec![vec!["3".to_string(), "1".to_string()]],
        "{engine}: the grouped aggregate sees the staged row"
    );
    server.client.simple_query("ROLLBACK").await.unwrap();
    assert_eq!(
        count_and_sum(&server, &c).await,
        ("2".to_string(), "3".to_string()),
        "{engine}: ROLLBACK discards the staged row"
    );

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("INSERT INTO {c} ({key}, n) VALUES ('d', 4)"))
        .await
        .unwrap_or_else(|e| panic!("{engine}: staged insert: {e}"));
    server.client.simple_query("COMMIT").await.unwrap();
    assert_eq!(
        count_and_sum(&server, &c).await,
        ("3".to_string(), "7".to_string()),
        "{engine}: COMMIT keeps the row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_aggregate_reads_own_writes() {
    let (engine, columns) = ENGINES[0];
    aggregate_reads_own_writes(engine, columns).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_aggregate_reads_own_writes() {
    let (engine, columns) = ENGINES[1];
    aggregate_reads_own_writes(engine, columns).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_aggregate_reads_own_writes() {
    let (engine, columns) = ENGINES[2];
    aggregate_reads_own_writes(engine, columns).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_aggregate_reads_own_writes() {
    let (engine, columns) = ENGINES[3];
    aggregate_reads_own_writes(engine, columns).await;
}

/// A timeseries aggregate on both paths reads the staged rows: the native
/// grouped scan (plain GROUP BY) and the generic aggregate (ROLLUP).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_native_and_generic_aggregates_read_own_writes() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ryow_ts (ts BIGINT TIME_KEY, host TEXT, n BIGINT) \
             WITH (engine='timeseries')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO ryow_ts (ts, host, n) VALUES (1000, 'a', 1), (2000, 'b', 2)")
        .await
        .unwrap();
    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO ryow_ts (ts, host, n) VALUES (3000, 'a', 3)")
        .await
        .unwrap();
    let mut native: Vec<String> = server
        .query_rows("SELECT host, COUNT(*) FROM ryow_ts GROUP BY host")
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    native.sort();
    assert_eq!(
        native,
        vec!["a|2", "b|1"],
        "native path sees the staged row"
    );
    let mut generic: Vec<String> = server
        .query_rows("SELECT host, COUNT(*) FROM ryow_ts GROUP BY ROLLUP (host)")
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    generic.sort();
    assert_eq!(
        generic,
        vec!["a|2", "b|1", "|3"],
        "generic path sees the staged row"
    );
    server.client.simple_query("ROLLBACK").await.unwrap();
}
