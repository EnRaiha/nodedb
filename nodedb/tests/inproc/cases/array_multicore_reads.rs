// SPDX-License-Identifier: BUSL-1.1

//! Array reads and maintenance on a 4-core server.
//!
//! A cell lives on the core that owns its tile's vShard. The arrays use the
//! default `prefix_bits`, and the primary vShards of its buckets spread over
//! every core, so the 64 one-cell tiles of each fixture reach all four cores.
//! A read or maintenance op that reaches one core sees a quarter of the
//! array.
//!
//! The cell at `k` holds `v = k`. Half the cells sit in a flushed segment and
//! half in the memtable.

use std::collections::BTreeMap;

use nodedb_test_support::pgwire_harness::TestServer;

const CORES: usize = 4;
const CELLS: i64 = 64;

/// Create the 1-D array `name`. `with` is an optional `WITH (...)` clause.
async fn create_with(srv: &TestServer, name: &str, with: &str) {
    srv.exec(&format!(
        "CREATE ARRAY {name} \
         DIMS (k INT64 [0..63]) \
         ATTRS (v FLOAT64) \
         TILE_EXTENTS (1) {with}"
    ))
    .await
    .unwrap_or_else(|e| panic!("CREATE ARRAY {name}: {e}"));
}

async fn create(srv: &TestServer, name: &str) {
    create_with(srv, name, "").await;
}

/// Write `v = scale * k` for every `k` in `range`.
async fn insert(srv: &TestServer, name: &str, range: std::ops::Range<i64>, scale: f64) {
    let cells: Vec<String> = range
        .map(|k| format!("COORDS ({k}) VALUES ({})", scale * k as f64))
        .collect();
    srv.exec(&format!("INSERT INTO ARRAY {name} {}", cells.join(", ")))
        .await
        .unwrap_or_else(|e| panic!("INSERT INTO ARRAY {name}: {e}"));
}

/// An array whose cell `k` holds `scale * k`, half flushed.
async fn prepare(srv: &TestServer, name: &str, scale: f64) {
    create(srv, name).await;
    insert(srv, name, 0..CELLS / 2, scale).await;
    srv.exec(&format!("SELECT ARRAY_FLUSH('{name}')"))
        .await
        .unwrap_or_else(|e| panic!("ARRAY_FLUSH {name}: {e}"));
    insert(srv, name, CELLS / 2..CELLS, scale).await;
}

fn int(text: &str) -> i64 {
    text.parse()
        .unwrap_or_else(|e| panic!("not an integer: {text}: {e}"))
}

fn float(text: &str) -> f64 {
    text.parse()
        .unwrap_or_else(|e| panic!("not a float: {text}: {e}"))
}

/// The `(k, v)` rows of `sql`, whose first two columns are `k, v`.
async fn cells(srv: &TestServer, sql: &str) -> BTreeMap<i64, f64> {
    let rows = srv
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let map: BTreeMap<i64, f64> = rows
        .iter()
        .map(|row| (int(&row[0]), float(&row[1])))
        .collect();
    assert_eq!(map.len(), rows.len(), "{sql}: a cell came back twice");
    map
}

/// The first element of a JSON array column.
fn first(column: &str) -> serde_json::Value {
    let value: serde_json::Value = serde_json::from_str(column)
        .unwrap_or_else(|e| panic!("column {column:?} is not JSON: {e}"));
    value
        .as_array()
        .and_then(|items| items.first().cloned())
        .unwrap_or_else(|| panic!("column {column:?} is not a non-empty JSON array"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_project_returns_every_core_s_cells() {
    let srv = TestServer::start_multicores(CORES).await;
    prepare(&srv, "g", 1.0).await;

    let got = cells(&srv, "SELECT k, v FROM ARRAY_PROJECT('g', ['v'])").await;
    let want: BTreeMap<i64, f64> = (0..CELLS).map(|k| (k, k as f64)).collect();
    assert_eq!(got, want);

    let star = srv
        .query_rows("SELECT * FROM ARRAY_PROJECT('g', ['v'])")
        .await
        .expect("SELECT * FROM ARRAY_PROJECT");
    assert_eq!(star.len() as i64, CELLS, "SELECT * reads every cell");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relational_tail_runs_over_array_project() {
    let srv = TestServer::start_multicores(CORES).await;
    prepare(&srv, "g", 1.0).await;

    let filtered = cells(
        &srv,
        "SELECT k, v FROM ARRAY_PROJECT('g', ['v']) WHERE v >= 40 ORDER BY k",
    )
    .await;
    let want: BTreeMap<i64, f64> = (40..CELLS).map(|k| (k, k as f64)).collect();
    assert_eq!(filtered, want, "WHERE keeps every matching cell");

    let count = srv
        .query_rows("SELECT count(*) FROM ARRAY_PROJECT('g', ['v'])")
        .await
        .expect("count over ARRAY_PROJECT");
    assert_eq!(count.len(), 1, "a scalar count is one row: {count:?}");
    assert_eq!(int(&count[0][0]), CELLS);

    let sum = srv
        .query_rows("SELECT sum(v) FROM ARRAY_PROJECT('g', ['v']) WHERE k < 10")
        .await
        .expect("sum over ARRAY_PROJECT");
    assert_eq!(float(&sum[0][0]), 45.0, "0 + 1 + ... + 9");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_project_joins_a_collection() {
    let srv = TestServer::start_multicores(CORES).await;
    prepare(&srv, "g", 1.0).await;
    srv.exec(
        "CREATE COLLECTION lbl \
         COLUMNS (k BIGINT PRIMARY KEY, name TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE COLLECTION lbl");
    srv.exec("INSERT INTO lbl (k, name) VALUES (3, 'three'), (41, 'forty-one'), (62, 'sixty-two')")
        .await
        .expect("INSERT INTO lbl");

    let rows = srv
        .query_rows(
            "SELECT p.k, p.v, l.name FROM ARRAY_PROJECT('g', ['v']) AS p \
             JOIN lbl AS l ON p.k = l.k ORDER BY p.k",
        )
        .await
        .expect("JOIN over ARRAY_PROJECT");
    let got: Vec<(i64, f64, String)> = rows
        .iter()
        .map(|row| (int(&row[0]), float(&row[1]), row[2].clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            (3, 3.0, "three".into()),
            (41, 41.0, "forty-one".into()),
            (62, 62.0, "sixty-two".into()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_elementwise_pairs_every_core_s_cells() {
    let srv = TestServer::start_multicores(CORES).await;
    prepare(&srv, "a", 1.0).await;
    prepare(&srv, "b", 100.0).await;

    let (got, horizon) = pairs(
        &srv,
        "SELECT * FROM ARRAY_ELEMENTWISE('a', 'b', 'add', 'v')",
    )
    .await;
    assert_eq!(horizon, None, "a live read appends no summary row");
    let want: BTreeMap<i64, f64> = (0..CELLS).map(|k| (k, 101.0 * k as f64)).collect();
    assert_eq!(got, want);
}

/// The `(k, result)` pairs of an elementwise `sql`, and the below-horizon
/// flag of its summary row when it has one.
async fn pairs(srv: &TestServer, sql: &str) -> (BTreeMap<i64, f64>, Option<bool>) {
    let rows = srv
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut got = BTreeMap::new();
    let mut horizon = None;
    let mut cells = 0usize;
    for row in &rows {
        if let Some(flag) = row
            .get("truncated_before_horizon")
            .filter(|f| !f.is_empty())
        {
            horizon = Some(flag == "t" || flag == "true");
            continue;
        }
        let k = first(&row["coords"])
            .as_i64()
            .unwrap_or_else(|| panic!("coords {:?} holds no integer", row["coords"]));
        let v = first(&row["attrs"])
            .as_f64()
            .unwrap_or_else(|| panic!("attrs {:?} holds no number", row["attrs"]));
        got.insert(k, v);
        cells += 1;
    }
    assert_eq!(got.len(), cells, "{sql}: a cell paired twice: {rows:?}");
    (got, horizon)
}

/// `AS OF SYSTEM TIME` and `AS OF VALID TIME` read both operands at the
/// cutoff, on every core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_elementwise_reads_both_sides_as_of() {
    let srv = TestServer::start_multicores(CORES).await;
    prepare(&srv, "a", 1.0).await;
    prepare(&srv, "b", 100.0).await;
    for name in ["a", "b"] {
        srv.exec(&format!("SELECT ARRAY_FLUSH('{name}')"))
            .await
            .unwrap_or_else(|e| panic!("ARRAY_FLUSH {name}: {e}"));
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let between = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after epoch")
        .as_millis() as i64;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    insert(&srv, "a", 0..CELLS, 2.0).await;
    insert(&srv, "b", 0..CELLS, 200.0).await;

    let sql = "SELECT * FROM ARRAY_ELEMENTWISE('a', 'b', 'add', 'v')";
    let first_version: BTreeMap<i64, f64> = (0..CELLS).map(|k| (k, 101.0 * k as f64)).collect();
    let second_version: BTreeMap<i64, f64> = (0..CELLS).map(|k| (k, 202.0 * k as f64)).collect();

    let (live, _) = pairs(&srv, sql).await;
    assert_eq!(live, second_version, "a live read sees the overwrite");

    let (old, horizon) = pairs(&srv, &format!("{sql} AS OF SYSTEM TIME {between}")).await;
    assert_eq!(old, first_version, "both sides at the cutoff");
    assert_eq!(horizon, Some(false), "the cutoff is inside the history");

    let (both, _) = pairs(
        &srv,
        &format!("{sql} AS OF SYSTEM TIME {between} AS OF VALID TIME NOW()"),
    )
    .await;
    assert_eq!(both, first_version, "a valid-time point keeps open cells");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_elementwise_refuses_arrays_that_route_differently() {
    let srv = TestServer::start_multicores(CORES).await;
    create_with(&srv, "wide", "WITH (prefix_bits = 10)").await;
    create(&srv, "narrow").await;

    let err = srv
        .query_rows("SELECT * FROM ARRAY_ELEMENTWISE('wide', 'narrow', 'add', 'v')")
        .await
        .expect_err("different prefix_bits are refused");
    assert!(
        err.contains("route their tiles differently"),
        "unexpected error: {err}"
    );
}

/// Four flushes give every core four segments, and each core merges them on
/// the compaction. A flush or compaction that reaches one core leaves the
/// other cores' cells in their memtables and merges on one core only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_flush_and_compact_reach_every_core() {
    let srv = TestServer::start_multicores(CORES).await;
    create(&srv, "m").await;
    for round in 1..=4 {
        insert(&srv, "m", 0..CELLS, f64::from(round)).await;
        let flushed = count(&srv, "SELECT ARRAY_FLUSH('m')", "flushed").await;
        assert_eq!(flushed, 1, "round {round}");
    }

    let compacted = count(&srv, "SELECT ARRAY_COMPACT('m')", "compacted").await;
    assert_eq!(
        compacted, CORES as i64,
        "every core merges its four segments"
    );

    let got = cells(&srv, "SELECT k, v FROM ARRAY_PROJECT('m', ['v'])").await;
    let want: BTreeMap<i64, f64> = (0..CELLS).map(|k| (k, 4.0 * k as f64)).collect();
    assert_eq!(got, want, "the compacted segments hold the newest values");
}

/// The `key` count of the one row a maintenance call answers with.
async fn count(srv: &TestServer, sql: &str, key: &str) -> i64 {
    let rows = srv
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: one status row: {rows:?}");
    let value = rows[0]
        .get(key)
        .unwrap_or_else(|| panic!("{sql}: no '{key}' column: {rows:?}"));
    int(value)
}
