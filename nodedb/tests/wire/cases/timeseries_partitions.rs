// SPDX-License-Identifier: BUSL-1.1

//! `SHOW PARTITIONS FOR` and `REWRITE PARTITIONS FOR` read and act on the
//! partition registry of the Data Plane core that owns the collection.
//!
//! The server runs with a one-byte timeseries memtable budget, so every
//! ingest flushes its rows to a partition of its own.

use crate::harness::TestServer;

/// The memtable budget that flushes every ingest.
const FLUSH_EVERY_INGEST: usize = 1;

/// The columns `SHOW PARTITIONS FOR` reports, in order.
const COLUMNS: [&str; 7] = [
    "partition",
    "state",
    "min_ts",
    "max_ts",
    "rows",
    "size",
    "interval_ms",
];

async fn timeseries(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

async fn ingest(server: &TestServer, name: &str, timestamps: &[i64]) {
    for (i, ts) in timestamps.iter().enumerate() {
        server
            .exec(&format!(
                "INSERT INTO {name} (id, ts, value) VALUES ('r{i}', {ts}, 1.0)"
            ))
            .await
            .unwrap_or_else(|e| panic!("ingest {name} at {ts}: {e}"));
    }
}

/// Every partition row of `name`, each column split out.
async fn partitions(server: &TestServer, name: &str) -> Vec<Vec<String>> {
    server
        .query_text_joined(&format!("SHOW PARTITIONS FOR {name}"))
        .await
        .unwrap_or_else(|e| panic!("SHOW PARTITIONS FOR {name}: {e}"))
        .into_iter()
        .map(|row| row.split('\t').map(str::to_string).collect())
        .collect()
}

fn column(row: &[String], name: &str) -> String {
    let index = COLUMNS
        .iter()
        .position(|c| *c == name)
        .unwrap_or_else(|| panic!("no column {name}"));
    row.get(index)
        .cloned()
        .unwrap_or_else(|| panic!("row {row:?} has no {name}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_partitions_lists_each_flushed_partition() {
    let server = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    timeseries(&server, "tp_listed").await;
    ingest(&server, "tp_listed", &[1_000, 2_000]).await;

    let rows = partitions(&server, "tp_listed").await;
    assert_eq!(rows.len(), 2, "one partition per flushed ingest: {rows:?}");
    for row in &rows {
        assert_eq!(row.len(), COLUMNS.len(), "{row:?}");
        assert_eq!(column(row, "state"), "Sealed", "{row:?}");
        assert_eq!(column(row, "rows"), "1", "{row:?}");
    }
    let mut ranges: Vec<(String, String)> = rows
        .iter()
        .map(|row| (column(row, "min_ts"), column(row, "max_ts")))
        .collect();
    ranges.sort();
    assert_eq!(
        ranges,
        vec![
            ("1000".to_string(), "1000".to_string()),
            ("2000".to_string(), "2000".to_string()),
        ]
    );
}

/// One flush whose rows span two hours writes one partition per hour, and
/// SHOW reports the interval each was cut at.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_spanning_two_intervals_lists_two_partitions() {
    let server = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    server
        .exec(
            "CREATE COLLECTION tp_hourly \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
             WITH (engine='timeseries', partition_by='1h')",
        )
        .await
        .unwrap();
    // One statement, one record, one flush: rows in hour 0 and hour 1.
    server
        .exec(
            "INSERT INTO tp_hourly (id, ts, value) VALUES \
             ('a', 1000, 1.0), ('b', 2000, 2.0), ('c', 3600001, 3.0)",
        )
        .await
        .unwrap();

    let mut rows = partitions(&server, "tp_hourly").await;
    rows.sort_by_key(|row| column(row, "min_ts").parse::<i64>().unwrap_or_default());
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(column(&rows[0], "rows"), "2");
    assert_eq!(column(&rows[1], "rows"), "1");
    for row in &rows {
        assert_eq!(column(row, "interval_ms"), "3600000", "{row:?}");
    }
    assert_eq!(column(&rows[1], "min_ts"), "3600001");
}

/// ALTER of the interval governs later flushes only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interval_alter_governs_only_later_flushes() {
    let server = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    server
        .exec(
            "CREATE COLLECTION tp_altered \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
             WITH (engine='timeseries', partition_by='1d')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO tp_altered (id, ts, value) VALUES ('a', 1000, 1.0), ('b', 3600001, 2.0)")
        .await
        .unwrap();
    server
        .exec("ALTER TIMESERIES tp_altered SET (partition_by = '1h')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO tp_altered (id, ts, value) VALUES ('c', 1000, 1.0), ('d', 3600001, 2.0)")
        .await
        .unwrap();

    let mut intervals: Vec<String> = partitions(&server, "tp_altered")
        .await
        .iter()
        .map(|row| column(row, "interval_ms"))
        .collect();
    intervals.sort();
    assert_eq!(
        intervals,
        vec!["3600000", "3600000", "86400000"],
        "the first flush keeps its day cut, the second cuts hourly"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rewrite_partitions_keeps_every_row() {
    let server = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    timeseries(&server, "tp_rewritten").await;
    ingest(&server, "tp_rewritten", &[1_000, 2_000, 3_000]).await;
    let before = partitions(&server, "tp_rewritten").await;

    server
        .exec("REWRITE PARTITIONS FOR tp_rewritten")
        .await
        .unwrap_or_else(|e| panic!("REWRITE PARTITIONS: {e}"));

    let after = partitions(&server, "tp_rewritten").await;
    let names = |rows: &[Vec<String>]| {
        let mut names: Vec<String> = rows.iter().map(|r| column(r, "partition")).collect();
        names.sort();
        names
    };
    assert_eq!(
        names(&after),
        names(&before),
        "a rewrite keeps each partition"
    );
    let count = server
        .query_text("SELECT COUNT(*) FROM tp_rewritten")
        .await
        .unwrap_or_else(|e| panic!("count: {e}"));
    assert_eq!(count, vec!["3".to_string()], "a rewrite keeps every row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_named_collections_in_two_databases_list_their_own_partitions() {
    let server = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    for (db, timestamps) in [
        ("tp_db_one", &[1_000i64][..]),
        ("tp_db_two", &[1_000, 2_000, 3_000][..]),
    ] {
        server.exec(&format!("CREATE DATABASE {db}")).await.unwrap();
        server.exec(&format!("USE DATABASE {db}")).await.unwrap();
        timeseries(&server, "tp_shared").await;
        ingest(&server, "tp_shared", timestamps).await;
    }

    assert_eq!(partitions(&server, "tp_shared").await.len(), 3);
    server.exec("USE DATABASE tp_db_one").await.unwrap();
    assert_eq!(partitions(&server, "tp_shared").await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_ddl_refuses_a_collection_of_another_engine() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION tp_docs").await.unwrap();
    server
        .expect_error("SHOW PARTITIONS FOR tp_docs", "42809")
        .await;
    server
        .expect_error("REWRITE PARTITIONS FOR tp_docs", "42809")
        .await;
    server
        .expect_error("REWRITE PARTITIONS FOR tp_missing", "42P01")
        .await;
}
