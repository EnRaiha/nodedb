// SPDX-License-Identifier: BUSL-1.1

//! A SELECT-list item that computes over aggregates evaluates over the group
//! row: `ROUND(AVG(v), 1)`, `SUM(v) / COUNT(*)`, a CASE over `COUNT(*)`,
//! nested functions, and an expression over a GROUP BY key.
//!
//! Each shape runs on the generic aggregate (columnar), the native grouped
//! timeseries scan, an ARRAY_SLICE relation, and a derived table. Values and
//! RowDescription OIDs are checked over the extended protocol.

use crate::harness::TestServer;

const INT8: u32 = 20;
const TEXT: u32 = 25;
const FLOAT8: u32 = 701;

/// One decoded cell, read by the type its column announces.
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Int(i64),
    Float(f64),
    Text(String),
}

/// The server's SQLSTATE and message when `e` carries them, else its
/// `Display`, which prints only `db error` for a server error.
fn db_error(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{} {}", db.code().code(), db.message()),
        None => e.to_string(),
    }
}

/// Prepares and runs `sql` over the extended protocol.
///
/// A Parse whose planning fails announces no columns and the error surfaces
/// at Execute. The rows run before any column check, so a planning error
/// panics with its SQLSTATE and message, never as an empty column list.
async fn prepared_rows(
    server: &TestServer,
    sql: &str,
) -> (Vec<(String, u32)>, Vec<tokio_postgres::Row>) {
    let statement = server
        .client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("prepare {sql}: {}", db_error(&e)));
    let rows = server
        .client
        .query(&statement, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {}", db_error(&e)));
    let columns = statement
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), c.type_().oid()))
        .collect();
    (columns, rows)
}

/// `(name, OID)` of each column, and every row decoded by those OIDs.
async fn typed_rows(server: &TestServer, sql: &str) -> (Vec<(String, u32)>, Vec<Vec<Cell>>) {
    let (columns, rows) = prepared_rows(server, sql).await;
    let rows = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(i, (_, oid))| match *oid {
                    INT8 => Cell::Int(row.get::<_, i64>(i)),
                    FLOAT8 => Cell::Float(row.get::<_, f64>(i)),
                    _ => Cell::Text(row.get::<_, String>(i)),
                })
                .collect()
        })
        .collect();
    (columns, rows)
}

/// Asserts `sql` announces `columns` and returns `rows`, floats within 1e-9.
async fn check(server: &TestServer, sql: &str, columns: &[(&str, u32)], rows: &[Vec<Cell>]) {
    let (got_columns, got_rows) = typed_rows(server, sql).await;
    let want_columns: Vec<(String, u32)> = columns
        .iter()
        .map(|(name, oid)| ((*name).to_string(), *oid))
        .collect();
    assert_eq!(got_columns, want_columns, "{sql}: columns");
    assert_eq!(got_rows.len(), rows.len(), "{sql}: {got_rows:?}");
    for (got, want) in got_rows.iter().zip(rows) {
        assert_eq!(got.len(), want.len(), "{sql}: {got:?}");
        for (g, w) in got.iter().zip(want) {
            let same = match (g, w) {
                (Cell::Float(a), Cell::Float(b)) => (a - b).abs() < 1e-9,
                (a, b) => a == b,
            };
            assert!(same, "{sql}: got {got:?}, want {want:?}");
        }
    }
}

fn f(v: f64) -> Cell {
    Cell::Float(v)
}

fn i(v: i64) -> Cell {
    Cell::Int(v)
}

fn t(v: &str) -> Cell {
    Cell::Text(v.to_string())
}

/// `k = 'a'` holds `v` 1.25 and 2.5. `k = 'b'` holds `v` 4.0.
async fn seed(server: &TestServer, c: &str, engine: &str) {
    let ddl = match engine {
        "timeseries" => format!(
            "CREATE COLLECTION {c} (ts TIMESTAMP TIME_KEY, k VARCHAR, v FLOAT) \
             WITH (engine='timeseries')"
        ),
        _ => format!(
            "CREATE COLLECTION {c} (ts TIMESTAMP, k VARCHAR, v FLOAT) WITH (engine='{engine}')"
        ),
    };
    let insert = format!(
        "INSERT INTO {c} (ts, k, v) VALUES \
         ('2026-10-01T00:00:00Z', 'a', 1.25), \
         ('2026-10-01T00:10:00Z', 'a', 2.5), \
         ('2026-10-01T01:00:00Z', 'b', 4.0)"
    );
    for sql in [ddl, insert] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{engine}: {sql}: {e}"));
    }
}

/// Every shape over the collection `c`.
async fn check_shapes(server: &TestServer, c: &str) {
    check(
        server,
        &format!("SELECT ROUND(AVG(v), 1) AS m FROM {c}"),
        &[("m", FLOAT8)],
        &[vec![f(2.6)]],
    )
    .await;
    check(
        server,
        &format!("SELECT k, ROUND(AVG(v), 1) AS m FROM {c} GROUP BY k ORDER BY k"),
        &[("k", TEXT), ("m", FLOAT8)],
        &[vec![t("a"), f(1.9)], vec![t("b"), f(4.0)]],
    )
    .await;
    check(
        server,
        &format!("SELECT k, SUM(v) / COUNT(*) AS per FROM {c} GROUP BY k ORDER BY k"),
        &[("k", TEXT), ("per", FLOAT8)],
        &[vec![t("a"), f(1.875)], vec![t("b"), f(4.0)]],
    )
    .await;
    check(
        server,
        &format!(
            "SELECT k, CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'one' END AS size \
             FROM {c} GROUP BY k ORDER BY k"
        ),
        &[("k", TEXT), ("size", TEXT)],
        &[vec![t("a"), t("many")], vec![t("b"), t("one")]],
    )
    .await;
    check(
        server,
        &format!(
            "SELECT k, ROUND(ABS(MIN(v) - MAX(v)), 2) AS spread FROM {c} GROUP BY k ORDER BY k"
        ),
        &[("k", TEXT), ("spread", FLOAT8)],
        &[vec![t("a"), f(1.25)], vec![t("b"), f(0.0)]],
    )
    .await;
    check(
        server,
        &format!("SELECT UPPER(k) AS uk, COUNT(*) + 1 AS n1 FROM {c} GROUP BY k ORDER BY k"),
        &[("uk", TEXT), ("n1", INT8)],
        &[vec![t("A"), i(3)], vec![t("B"), i(2)]],
    )
    .await;
    check(
        server,
        &format!(
            "SELECT k, ROUND(AVG(v), 1) AS m FROM {c} GROUP BY k \
             HAVING ROUND(AVG(v), 1) > 2 ORDER BY m DESC"
        ),
        &[("k", TEXT), ("m", FLOAT8)],
        &[vec![t("b"), f(4.0)]],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn computed_aggregate_items_on_the_generic_aggregate() {
    let server = TestServer::start().await;
    seed(&server, "agg_items_col", "columnar").await;
    check_shapes(&server, "agg_items_col").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn computed_aggregate_items_on_the_native_timeseries_scan() {
    let server = TestServer::start().await;
    seed(&server, "agg_items_ts", "timeseries").await;
    check_shapes(&server, "agg_items_ts").await;
}

/// The bucketed shape reads its timestamp column by name, so it decodes its
/// rows itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn computed_aggregate_items_over_time_buckets() {
    let server = TestServer::start().await;
    seed(&server, "agg_items_bucket", "timeseries").await;
    let sql = "SELECT time_bucket('1 hour', ts) AS b, ROUND(AVG(v), 3) AS m, \
               MAX(v) - MIN(v) AS width FROM agg_items_bucket GROUP BY b ORDER BY b";
    let (columns, rows) = prepared_rows(&server, sql).await;
    assert_eq!(
        columns,
        vec![
            ("b".to_string(), 1114),
            ("m".to_string(), FLOAT8),
            ("width".to_string(), FLOAT8)
        ],
        "{sql}"
    );
    let got: Vec<(f64, f64)> = rows
        .iter()
        .map(|row| (row.get::<_, f64>("m"), row.get::<_, f64>("width")))
        .collect();
    // The first hour averages 1.25 and 2.5 to 1.875; the second holds 4.0.
    assert_eq!(got.len(), 2, "{sql}: {got:?}");
    assert!((got[0].0 - 1.875).abs() < 1e-9, "{sql}: {got:?}");
    assert!((got[0].1 - 1.25).abs() < 1e-9, "{sql}: {got:?}");
    assert!((got[1].0 - 4.0).abs() < 1e-9, "{sql}: {got:?}");
    assert!(got[1].1.abs() < 1e-9, "{sql}: {got:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn computed_aggregate_items_over_an_array_slice() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE ARRAY agg_items_arr DIMS (x INT64 [0..1], y INT64 [0..1]) \
         ATTRS (v FLOAT64) TILE_EXTENTS (2, 2)",
        "INSERT INTO ARRAY agg_items_arr COORDS (0, 0) VALUES (1.25), \
         COORDS (0, 1) VALUES (2.5), COORDS (1, 0) VALUES (4.0)",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    check(
        &server,
        "SELECT x, ROUND(AVG(v), 1) AS m, SUM(v) / COUNT(*) AS per \
         FROM ARRAY_SLICE('agg_items_arr', '{x: [0, 1], y: [0, 1]}') GROUP BY x ORDER BY x",
        &[("x", INT8), ("m", FLOAT8), ("per", FLOAT8)],
        &[vec![i(0), f(1.9), f(1.875)], vec![i(1), f(4.0), f(4.0)]],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn computed_aggregate_items_over_a_derived_table() {
    let server = TestServer::start().await;
    seed(&server, "agg_items_derived", "columnar").await;
    check(
        &server,
        "SELECT k, ROUND(AVG(v), 1) AS m FROM \
         (SELECT k, v FROM agg_items_derived) AS d GROUP BY k ORDER BY k",
        &[("k", TEXT), ("m", FLOAT8)],
        &[vec![t("a"), f(1.9)], vec![t("b"), f(4.0)]],
    )
    .await;
    // A derived table of constants: the value rounds, whatever type the
    // literal carries.
    let rows = server
        .query_rows(
            "SELECT k, ROUND(AVG(v), 1) AS m FROM (SELECT 'a' AS k, 1.234 AS v) AS d GROUP BY k",
        )
        .await
        .unwrap_or_else(|e| panic!("constant derived table: {e}"));
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0][0], "a");
    let m: f64 = rows[0][1]
        .parse()
        .unwrap_or_else(|e| panic!("m '{}' is not a number: {e}", rows[0][1]));
    assert!((m - 1.2).abs() < 1e-9, "ROUND(1.234, 1) is 1.2: {rows:?}");
}
