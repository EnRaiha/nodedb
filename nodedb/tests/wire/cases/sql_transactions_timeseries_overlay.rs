// SPDX-License-Identifier: BUSL-1.1

//! Timeseries-engine `INSERT` (ingest) executes at STATEMENT time inside a
//! transaction -- staged into the per-transaction overlay with
//! read-your-own-writes on raw scans and on aggregates, a real affected-row
//! count, and `ROLLBACK` discarding the staged rows -- mirroring the columnar
//! staging. COMMIT resolves the staged ingest into the transaction's redo
//! record, which the redo install applies.
//!
//! A timeseries base row has no cross-engine surrogate identity in the scan
//! (it is keyed internally by `series_id`), so the overlay merge is
//! additive-only: read-your-own-writes surfaces staged INSERTs, and there is
//! no base-row supersede/tombstone. A same-transaction aggregate folds the
//! staged rows into its groups. Continuous aggregates read committed rows
//! only: they are maintained outside any transaction.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in a
/// simple-query response (PostgreSQL's `INSERT 0 N` count).
fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

/// Collect a single column's text across all returned rows, sorted so the
/// assertion is independent of raw-scan emission order (the overlay merge
/// appends staged rows after the base rows).
fn sorted_col(msgs: &[SimpleQueryMessage], col: &str) -> Vec<String> {
    let mut v: Vec<String> = msgs
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(col).map(str::to_string),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

async fn setup(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION sensors \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
             WITH (engine='timeseries')",
        )
        .await
        .unwrap();
    // Three committed base rows at ts 1000/2000/3000.
    for (i, ts, v) in [(1u32, 1000u64, 10u32), (2, 2000, 20), (3, 3000, 30)] {
        server
            .exec(&format!(
                "INSERT INTO sensors (id, ts, v) VALUES ('b{i}', {ts}, {v})"
            ))
            .await
            .unwrap();
    }
}

/// The load-bearing assertion: a staged in-transaction timeseries INSERT is
/// visible to a same-transaction RAW scan. Fails pre-fix — before the
/// staging path, a timeseries ingest inside a transaction was not stageable,
/// so the rows were buffered and only applied at COMMIT (invisible mid-tx).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_insert_visible_in_tx_raw_scan() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s4', 4000, 40), ('s5', 5000, 50)")
        .await
        .expect("in-tx timeseries insert should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(2),
        "in-tx timeseries INSERT must report the real row count at statement time"
    );

    // Read-your-own-writes: both staged rows appear alongside the base rows.
    let all = server
        .client
        .simple_query("SELECT v FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        sorted_col(&all, "v"),
        vec!["10", "20", "30", "40", "50"],
        "staged timeseries inserts must be visible in the same transaction \
         alongside the committed base rows"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = server
        .client
        .simple_query("SELECT v FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        sorted_col(&committed, "v"),
        vec!["10", "20", "30", "40", "50"],
        "committed timeseries inserts must persist"
    );
}

/// A time-range-narrowed in-transaction RAW scan surfaces exactly the staged
/// row whose timestamp falls in the queried window (and no base row does).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_insert_narrow_range_sees_staged_row() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s4', 4000, 40)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(1));

    // Window [3500, 4500] contains only the staged ts=4000 row (base rows are
    // at 1000/2000/3000).
    let windowed = server
        .client
        .simple_query("SELECT v FROM sensors WHERE ts >= 3500 AND ts <= 4500")
        .await
        .unwrap();
    assert_eq!(
        sorted_col(&windowed, "v"),
        vec!["40"],
        "a narrow time-range scan must surface the staged row inside the window"
    );

    // A window with no base or staged row returns nothing.
    let empty = server
        .client
        .simple_query("SELECT v FROM sensors WHERE ts >= 6000 AND ts <= 7000")
        .await
        .unwrap();
    assert!(
        sorted_col(&empty, "v").is_empty(),
        "a window matching no staged or base row must return nothing"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// `ROLLBACK` discards the staged timeseries rows (the overlay is dropped; no
/// undo-log entry and no memtable mutation happened at statement time).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_insert_rollback_discards_staged_rows() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s9', 9000, 90)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(1));

    // Visible in-tx before rollback.
    let in_tx = server
        .client
        .simple_query("SELECT v FROM sensors WHERE ts >= 8500 AND ts <= 9500")
        .await
        .unwrap();
    assert_eq!(sorted_col(&in_tx, "v"), vec!["90"]);

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after = server
        .client
        .simple_query("SELECT v FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        sorted_col(&after, "v"),
        vec!["10", "20", "30"],
        "rolled-back timeseries inserts must not persist; base rows untouched"
    );
}

/// A mid-transaction aggregate reads its own writes: the staged rows count
/// toward COUNT and SUM, exactly as the raw scan sees them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_aggregate_reads_own_writes() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s4', 4000, 40), ('s5', 5000, 50)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(2));

    // The raw scan sees 5 rows ...
    let raw = server
        .client
        .simple_query("SELECT v FROM sensors")
        .await
        .unwrap();
    assert_eq!(sorted_col(&raw, "v").len(), 5);

    // ... and so does COUNT(*): 3 committed rows plus 2 staged rows.
    let count = server
        .query_rows("SELECT COUNT(*) FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        count[0][0], "5",
        "mid-transaction COUNT(*) must include the transaction's staged rows"
    );

    // SUM(v) is 10+20+30+40+50.
    let sum = server
        .query_rows("SELECT SUM(v) FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        sum[0][0].parse::<f64>().ok(),
        Some(150.0),
        "mid-transaction SUM must include the transaction's staged rows"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    // After COMMIT the staged rows are durable and now counted.
    let count_after = server
        .query_rows("SELECT COUNT(*) FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        count_after[0][0], "5",
        "committed timeseries inserts must be counted after COMMIT"
    );
}

/// Native and generic aggregates agree on the staged rows: a plain GROUP BY,
/// a ROLLUP, a HAVING, and a time bucket all fold them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_native_and_generic_aggregates_agree() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s4', 4000, 40)")
        .await
        .unwrap();

    let native = server
        .query_rows("SELECT COUNT(*), SUM(v) FROM sensors")
        .await
        .unwrap();
    let generic = server
        .query_rows("SELECT COUNT(*), SUM(v) FROM sensors GROUP BY ROLLUP (id) HAVING COUNT(*) = 4")
        .await
        .unwrap();
    assert_eq!(native[0][0], "4", "native COUNT(*) sees the staged row");
    assert_eq!(
        generic.len(),
        1,
        "only the grand total has 4 rows: {generic:?}"
    );
    assert_eq!(native[0], generic[0], "native and generic agree");

    let buckets = server
        .query_rows(
            "SELECT time_bucket('10s', ts) AS b, COUNT(*) FROM sensors \
             GROUP BY b HAVING COUNT(*) > 0",
        )
        .await
        .unwrap();
    assert_eq!(buckets.len(), 1, "one ten-second bucket: {buckets:?}");
    assert_eq!(buckets[0][1], "4", "the bucket counts the staged row");

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// A transaction that truncated the collection aggregates only its own
/// staged rows; ROLLBACK restores the committed rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_aggregate_after_truncate_sees_only_staged_rows() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    server.exec("TRUNCATE sensors").await.unwrap();
    server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s9', 9000, 90)")
        .await
        .unwrap();
    let rows = server
        .query_rows("SELECT COUNT(*), SUM(v) FROM sensors")
        .await
        .unwrap();
    assert_eq!(rows[0][0], "1", "only the staged row survives the truncate");
    assert_eq!(rows[0][1].parse::<f64>().ok(), Some(90.0));
    server.client.simple_query("ROLLBACK").await.unwrap();

    let rows = server
        .query_rows("SELECT COUNT(*) FROM sensors")
        .await
        .unwrap();
    assert_eq!(rows[0][0], "3", "ROLLBACK restores the committed rows");
}
