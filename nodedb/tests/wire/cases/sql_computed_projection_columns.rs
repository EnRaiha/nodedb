// SPDX-License-Identifier: BUSL-1.1

//! A computed SELECT-list column beside a plain column, on every engine.
//!
//! Each computed column reads a stored column the SELECT list does not name
//! on its own: `SELECT name, ST_AsText(location)` reads `location`, and
//! `SELECT name, qty * 2` reads `qty`. The row must carry the value under the
//! name the row description announces, for geometry functions, nested
//! functions, arithmetic, and CASE alike.

use crate::harness::TestServer;

/// Geometry, nested-function, arithmetic, and CASE columns over the unnamed
/// `city` and `qty` columns of a row `('ali', 'temerloh', 7)`.
const MIXED_SELECT: &str = "SELECT name, upper(city), length(upper(city)), qty * 2, \
                            CASE WHEN qty > 5 THEN 'big' ELSE 'small' END FROM";

/// What [`MIXED_SELECT`] returns for the row `('ali', 'temerloh', 7)`.
const MIXED_ROW: [&str; 5] = ["ali", "TEMERLOH", "8", "14", "big"];

/// The single row `sql` returns.
async fn one_row(srv: &TestServer, sql: &str) -> Vec<String> {
    let rows = srv
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: expected one row, got {rows:?}");
    rows.into_iter().next().unwrap_or_default()
}

/// Create `collection` with `ddl`, insert the `('ali', 'temerloh', 7)` row
/// with `insert`, and check [`MIXED_SELECT`] over it.
async fn assert_mixed_select(srv: &TestServer, collection: &str, ddl: &str, insert: &str) {
    srv.exec(ddl)
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    srv.exec(insert)
        .await
        .unwrap_or_else(|e| panic!("insert into {collection}: {e}"));
    let row = one_row(srv, &format!("{MIXED_SELECT} {collection}")).await;
    assert_eq!(
        row, MIXED_ROW,
        "{collection}: every computed column must carry its value beside `name`"
    );
}

#[tokio::test]
async fn spatial_geometry_functions_beside_a_plain_column() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION sprobe (location GEOMETRY SPATIAL_INDEX, name VARCHAR) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap_or_else(|e| panic!("create sprobe: {e}"));
    srv.exec("INSERT INTO sprobe (name, location) VALUES ('Temerloh', ST_Point(102.42, 3.45))")
        .await
        .unwrap_or_else(|e| panic!("insert into sprobe: {e}"));

    let row = one_row(
        &srv,
        "SELECT name, ST_AsText(location), ST_AsText(ST_Transform(location, 3375)) FROM sprobe",
    )
    .await;
    assert_eq!(row[0], "Temerloh");
    assert_eq!(
        row[1], "POINT(102.42 3.45)",
        "ST_AsText beside `name`: {row:?}"
    );
    assert!(
        row[2].starts_with("POINT(") && row[2] != row[1],
        "the transformed point must render in the target SRID: {row:?}"
    );

    let row = one_row(
        &srv,
        "SELECT name, ST_X(location) * 2, \
         CASE WHEN ST_Y(location) > 0 THEN 'north' ELSE 'south' END, \
         upper(ST_GeometryType(location)) FROM sprobe",
    )
    .await;
    assert_eq!(row[0], "Temerloh");
    let doubled: f64 = row[1]
        .parse()
        .unwrap_or_else(|e| panic!("ST_X(location) * 2 is not a number: {row:?}: {e}"));
    assert!(
        (doubled - 204.84).abs() < 1e-9,
        "ST_X(location) * 2: {row:?}"
    );
    assert_eq!(row[2], "north", "CASE over the geometry: {row:?}");
    assert!(
        row[3].contains("POINT"),
        "a nested function over the geometry: {row:?}"
    );
}

#[tokio::test]
async fn schemaless_document_computed_columns_beside_a_plain_column() {
    let srv = TestServer::start().await;
    assert_mixed_select(
        &srv,
        "dprobe",
        "CREATE COLLECTION dprobe",
        "INSERT INTO dprobe (id, name, city, qty) VALUES ('a', 'ali', 'temerloh', 7)",
    )
    .await;
}

#[tokio::test]
async fn strict_document_computed_columns_beside_a_plain_column() {
    let srv = TestServer::start().await;
    assert_mixed_select(
        &srv,
        "stprobe",
        "CREATE COLLECTION stprobe (id TEXT PRIMARY KEY, name TEXT, city TEXT, qty INT) \
         WITH (engine='document_strict')",
        "INSERT INTO stprobe (id, name, city, qty) VALUES ('a', 'ali', 'temerloh', 7)",
    )
    .await;
}

#[tokio::test]
async fn kv_computed_columns_beside_a_plain_column() {
    let srv = TestServer::start().await;
    assert_mixed_select(
        &srv,
        "kprobe",
        "CREATE COLLECTION kprobe (k TEXT PRIMARY KEY, name TEXT, city TEXT, qty INT) \
         WITH (engine='kv')",
        "INSERT INTO kprobe (k, name, city, qty) VALUES ('a', 'ali', 'temerloh', 7)",
    )
    .await;
}

#[tokio::test]
async fn columnar_computed_columns_beside_a_plain_column() {
    let srv = TestServer::start().await;
    assert_mixed_select(
        &srv,
        "cprobe",
        "CREATE COLLECTION cprobe (id TEXT, name TEXT, city TEXT, qty INT) \
         WITH (engine='columnar')",
        "INSERT INTO cprobe (id, name, city, qty) VALUES ('a', 'ali', 'temerloh', 7)",
    )
    .await;
}

#[tokio::test]
async fn timeseries_computed_columns_beside_a_plain_column() {
    let srv = TestServer::start().await;
    assert_mixed_select(
        &srv,
        "tprobe",
        "CREATE COLLECTION tprobe (ts BIGINT TIME_KEY, name TEXT, city TEXT, qty BIGINT) \
         WITH (engine='timeseries')",
        "INSERT INTO tprobe (ts, name, city, qty) VALUES (1000, 'ali', 'temerloh', 7)",
    )
    .await;
}

/// The server flushes the columnar memtable once it holds two rows. Rows 1
/// and 2 read from a flushed segment, row 3 from the memtable.
#[tokio::test]
async fn columnar_computed_columns_read_flushed_and_memtable_rows() {
    let srv = TestServer::start_with_columnar_flush_threshold(2).await;
    srv.exec(
        "CREATE COLLECTION cflush (id INT PRIMARY KEY, city TEXT, qty INT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap_or_else(|e| panic!("create cflush: {e}"));
    srv.exec("INSERT INTO cflush (id, city, qty) VALUES (1, 'temerloh', 7), (2, 'kuantan', 3)")
        .await
        .unwrap_or_else(|e| panic!("seed flushed rows: {e}"));
    srv.exec("INSERT INTO cflush (id, city, qty) VALUES (3, 'raub', 9)")
        .await
        .unwrap_or_else(|e| panic!("seed memtable row: {e}"));

    let rows = srv
        .query_rows("SELECT id, upper(city), qty * 2 FROM cflush ORDER BY id")
        .await
        .unwrap_or_else(|e| panic!("select cflush: {e}"));
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "TEMERLOH".to_string(), "14".to_string()],
            vec!["2".to_string(), "KUANTAN".to_string(), "6".to_string()],
            vec!["3".to_string(), "RAUB".to_string(), "18".to_string()],
        ],
        "flushed and memtable rows both carry the computed columns"
    );
}

/// A slice relation evaluates its computed columns over every attr of the
/// cell. Cell `(0, 0)` holds `nir = 0.05` and `red = 0.4`.
#[tokio::test]
async fn array_slice_relation_computed_columns_beside_a_plain_column() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY aprobe \
         DIMS (x INT64 [0..3], y INT64 [0..3]) \
         ATTRS (nir FLOAT64, red FLOAT64) \
         TILE_EXTENTS (2, 2)",
    )
    .await
    .unwrap_or_else(|e| panic!("create aprobe: {e}"));
    srv.exec("INSERT INTO ARRAY aprobe COORDS (0, 0) VALUES (0.05, 0.4)")
        .await
        .unwrap_or_else(|e| panic!("insert into aprobe: {e}"));

    let row = one_row(
        &srv,
        "SELECT x, nir * 2, CASE WHEN nir > red THEN 'green' ELSE 'bare' END \
         FROM ARRAY_SLICE('aprobe', '{x: [0, 0], y: [0, 0]}')",
    )
    .await;
    assert_eq!(row[0], "0");
    let doubled: f64 = row[1]
        .parse()
        .unwrap_or_else(|e| panic!("nir * 2 is not a number: {row:?}: {e}"));
    assert!((doubled - 0.1).abs() < 1e-9, "nir * 2: {row:?}");
    assert_eq!(row[2], "bare", "CASE over the attrs: {row:?}");
}

/// A continuous aggregate reads through the provider scan. Host `a` has
/// `s = 4` over 2 rows, host `b` has `s = 30` over 2 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_aggregate_computed_columns_beside_a_plain_column() {
    let srv = TestServer::start_with_timeseries_memtable_budget(1).await;
    srv.exec(
        "CREATE COLLECTION gsrc (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap_or_else(|e| panic!("create gsrc: {e}"));
    srv.exec(
        "CREATE CONTINUOUS AGGREGATE gview ON gsrc BUCKET '1m' \
         AGGREGATE SUM(value) AS s, COUNT(*) AS c GROUP BY host \
         WITH (refresh_policy = 'manual')",
    )
    .await
    .unwrap_or_else(|e| panic!("create gview: {e}"));
    for (ts, host, value) in [
        (1_000, "a", 1.5),
        (2_000, "a", 2.5),
        (3_000, "b", 10.0),
        (4_000, "b", 20.0),
    ] {
        srv.exec(&format!(
            "INSERT INTO gsrc (ts, host, value) VALUES ({ts}, '{host}', {value})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert row at {ts}: {e}"));
    }
    srv.exec("REFRESH CONTINUOUS AGGREGATE gview")
        .await
        .unwrap_or_else(|e| panic!("refresh gview: {e}"));

    let rows = srv
        .query_rows(
            "SELECT host, c * 10, CASE WHEN s > 10 THEN 'big' ELSE 'small' END \
             FROM gview ORDER BY host",
        )
        .await
        .unwrap_or_else(|e| panic!("select gview: {e}"));
    assert_eq!(
        rows,
        vec![
            vec!["a".to_string(), "20".to_string(), "small".to_string()],
            vec!["b".to_string(), "20".to_string(), "big".to_string()],
        ],
        "the aggregate's rows carry the computed columns beside `host`"
    );
}
