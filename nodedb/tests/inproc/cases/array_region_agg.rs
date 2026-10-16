// SPDX-License-Identifier: BUSL-1.1

//! `ARRAY_AGG` bounded by a region over pgwire: a box, a polygon zone on
//! FLOAT64 lon/lat dims, a grouped zone, an empty zone, and the argument
//! errors. Half the cells sit in a flushed segment and half in the memtable.

use nodedb_test_support::pgwire_harness::TestServer;

/// The zone every test reads: lon in `[101, 103)`, lat in `[2, 4)`.
const ZONE: &str = "'POLYGON((101 2, 103 2, 103 4, 101 4, 101 2))', ARRAY['lon', 'lat']";

/// Cells inside the zone sum to 1.625 over four cells. One cell sits on the
/// zone's maximum lon edge and one far outside it.
async fn prepare_scene(srv: &TestServer) {
    srv.exec(
        "CREATE ARRAY scene \
         DIMS (lon FLOAT64 [100..106], lat FLOAT64 [0..8]) \
         ATTRS (ndvi FLOAT64) \
         TILE_EXTENTS (1, 1) \
         CELL_ORDER HILBERT",
    )
    .await
    .expect("CREATE ARRAY scene");
    srv.exec(
        "INSERT INTO ARRAY scene \
         COORDS (101.5, 2.5) VALUES (0.25), \
         COORDS (101.5, 3.5) VALUES (0.5), \
         COORDS (103.0, 3.0) VALUES (8.0)",
    )
    .await
    .expect("INSERT first half");
    srv.exec("SELECT ARRAY_FLUSH('scene')")
        .await
        .expect("ARRAY_FLUSH");
    srv.exec(
        "INSERT INTO ARRAY scene \
         COORDS (102.5, 2.5) VALUES (0.75), \
         COORDS (101.0, 3.0) VALUES (0.125), \
         COORDS (104.5, 6.5) VALUES (16.0)",
    )
    .await
    .expect("INSERT second half");
}

/// The `result` column of every row `sql` returns. NULL reads as `""`.
async fn results(srv: &TestServer, sql: &str) -> Vec<String> {
    srv.query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| row.get("result").cloned().unwrap_or_default())
        .collect()
}

fn as_f64(text: &str) -> f64 {
    text.parse()
        .unwrap_or_else(|e| panic!("result {text:?} is not a number: {e}"))
}

#[tokio::test]
async fn a_zone_aggregates_the_cells_inside_it() {
    let srv = TestServer::start().await;
    prepare_scene(&srv).await;

    let sum = results(
        &srv,
        &format!("SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'sum', NULL, NULL, {ZONE})"),
    )
    .await;
    assert_eq!(sum.len(), 1, "scalar agg returns one row: {sum:?}");
    assert_eq!(
        as_f64(&sum[0]),
        1.625,
        "the max-edge and far cells stay out"
    );

    let count = results(
        &srv,
        &format!("SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'count', NULL, NULL, {ZONE})"),
    )
    .await;
    assert_eq!(as_f64(&count[0]), 4.0);
}

#[tokio::test]
async fn a_box_aggregates_the_cells_inside_it() {
    let srv = TestServer::start().await;
    prepare_scene(&srv).await;

    let sum = results(
        &srv,
        "SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'sum', NULL, '{lon: [101, 102], lat: [2, 3]}')",
    )
    .await;
    assert_eq!(as_f64(&sum[0]), 0.375);
}

#[tokio::test]
async fn a_zone_inside_a_box_reads_both_bounds() {
    let srv = TestServer::start().await;
    prepare_scene(&srv).await;

    let sum = results(
        &srv,
        &format!(
            "SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'sum', NULL, '{{lat: [3, 8]}}', {ZONE})"
        ),
    )
    .await;
    assert_eq!(as_f64(&sum[0]), 0.625, "lat 3.0 and 3.5 inside the zone");
}

#[tokio::test]
async fn a_grouped_zone_groups_by_the_float_dim_value() {
    let srv = TestServer::start().await;
    prepare_scene(&srv).await;

    let rows = srv
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'sum', 'lon', NULL, {ZONE})"
        ))
        .await
        .expect("grouped zone agg");
    let mut groups: Vec<(f64, f64)> = rows
        .iter()
        .map(|row| (as_f64(&row["group"]), as_f64(&row["result"])))
        .collect();
    groups.sort_by(|a, b| a.0.total_cmp(&b.0));
    assert_eq!(groups, vec![(101.0, 0.125), (101.5, 0.75), (102.5, 0.75)]);
}

#[tokio::test]
async fn an_empty_zone_answers_null() {
    let srv = TestServer::start().await;
    prepare_scene(&srv).await;

    let empty = "'POLYGON((105 0, 106 0, 106 1, 105 1, 105 0))', ARRAY['lon', 'lat']";
    for reducer in ["sum", "count", "mean"] {
        let got = results(
            &srv,
            &format!("SELECT * FROM ARRAY_AGG('scene', 'ndvi', '{reducer}', NULL, NULL, {empty})"),
        )
        .await;
        assert_eq!(got, vec![String::new()], "{reducer} over an empty zone");
    }
}

#[tokio::test]
async fn region_argument_errors_name_the_problem() {
    let srv = TestServer::start().await;
    prepare_scene(&srv).await;

    srv.expect_error(
        "SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'sum', NULL, NULL, \
         'POLYGON((101 2, 103 2, 103 4, 101 2))')",
        "zone_dims",
    )
    .await;
    srv.expect_error(
        "SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'sum', NULL, NULL, \
         'LINESTRING(101 2, 103 4)', ARRAY['lon', 'lat'])",
        "POLYGON or MULTIPOLYGON",
    )
    .await;
    srv.expect_error(
        "SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'sum', NULL, '{alt: [0, 1]}')",
        "no dim 'alt'",
    )
    .await;
}
