// SPDX-License-Identifier: BUSL-1.1

//! `ARRAY_SLICE` as a relation: every SELECT clause applies over its rows.
//!
//! The fixture is a 4x4 image with two bands. Cell `(x, y)` has index
//! `k = 4x + y`, `nir = (k + 1) / 20` and `red = (16 - k) / 40`, so every
//! expected value is computed from the coordinates.

use std::collections::BTreeSet;

use nodedb_test_support::pgwire_harness::TestServer;

const SLICE: &str = "ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}')";

fn nir(x: i64, y: i64) -> f64 {
    (4 * x + y + 1) as f64 / 20.0
}

fn red(x: i64, y: i64) -> f64 {
    (16 - (4 * x + y)) as f64 / 40.0
}

fn ndvi(x: i64, y: i64) -> f64 {
    (nir(x, y) - red(x, y)) / (nir(x, y) + red(x, y))
}

async fn prepare_img(srv: &TestServer) {
    srv.exec(
        "CREATE ARRAY img \
         DIMS (x INT64 [0..3], y INT64 [0..3]) \
         ATTRS (nir FLOAT64, red FLOAT64) \
         TILE_EXTENTS (2, 2)",
    )
    .await
    .expect("CREATE ARRAY img");
    let cells: Vec<String> = (0..4)
        .flat_map(|x| {
            (0..4).map(move |y| format!("COORDS ({x}, {y}) VALUES ({}, {})", nir(x, y), red(x, y)))
        })
        .collect();
    srv.exec(&format!("INSERT INTO ARRAY img {}", cells.join(", ")))
        .await
        .expect("INSERT INTO ARRAY img");
    srv.exec("SELECT ARRAY_FLUSH('img')")
        .await
        .expect("ARRAY_FLUSH");
}

fn int(text: &str) -> i64 {
    text.parse()
        .unwrap_or_else(|e| panic!("not an integer: {text}: {e}"))
}

fn float(text: &str) -> f64 {
    text.parse()
        .unwrap_or_else(|e| panic!("not a float: {text}: {e}"))
}

/// The `(x, y)` of every row of `sql`, whose first two columns are `x, y`.
async fn coord_set(srv: &TestServer, sql: &str) -> BTreeSet<(i64, i64)> {
    srv.query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| (int(&row[0]), int(&row[1])))
        .collect()
}

/// The column names of the first row `sql` returns, in wire order.
async fn column_names(srv: &TestServer, sql: &str) -> Vec<String> {
    let msgs = srv
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    msgs.iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(row.columns().iter().map(|c| c.name().to_string()).collect())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no rows"))
}

#[tokio::test]
async fn array_slice_ndvi_projection_per_cell() {
    let srv = TestServer::start().await;
    prepare_img(&srv).await;

    let rows = srv
        .query_rows(&format!(
            "SELECT x, y, (nir - red) / (nir + red) AS ndvi FROM {SLICE}"
        ))
        .await
        .expect("NDVI projection");
    assert_eq!(rows.len(), 16, "one row per cell: {rows:?}");
    let mut seen = BTreeSet::new();
    for row in &rows {
        let (x, y) = (int(&row[0]), int(&row[1]));
        let got = float(&row[2]);
        assert!(
            (got - ndvi(x, y)).abs() < 1e-9,
            "cell ({x}, {y}): ndvi {got}, expected {}",
            ndvi(x, y)
        );
        seen.insert((x, y));
    }
    assert_eq!(seen.len(), 16, "every cell appears once");
}

#[tokio::test]
async fn array_slice_where_attr_filters_cells() {
    let srv = TestServer::start().await;
    prepare_img(&srv).await;

    let got = coord_set(&srv, &format!("SELECT x, y FROM {SLICE} WHERE nir > red")).await;
    let expected: BTreeSet<(i64, i64)> = (0..4)
        .flat_map(|x| (0..4).map(move |y| (x, y)))
        .filter(|&(x, y)| nir(x, y) > red(x, y))
        .collect();
    assert_eq!(got, expected);
    assert!(
        !expected.is_empty() && expected.len() < 16,
        "the filter keeps a strict subset"
    );
}

/// A dim bound in WHERE over a capped slice filters the cells the cap
/// selected. It never widens the cap's choice to other cells.
#[tokio::test]
async fn array_slice_where_dim_with_cap_keeps_cap_semantics() {
    let srv = TestServer::start().await;
    prepare_img(&srv).await;

    let capped = "ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}', '*', 3)";
    let selected = coord_set(&srv, &format!("SELECT x, y FROM {capped}")).await;
    assert_eq!(selected.len(), 3, "the cap reads three cells: {selected:?}");

    let got = coord_set(&srv, &format!("SELECT x, y FROM {capped} WHERE x >= 2")).await;
    let expected: BTreeSet<(i64, i64)> = selected.into_iter().filter(|&(x, _)| x >= 2).collect();
    assert_eq!(got, expected);
}

#[tokio::test]
async fn array_slice_group_by_dim_avg() {
    let srv = TestServer::start().await;
    prepare_img(&srv).await;

    let rows = srv
        .query_rows(&format!(
            "SELECT x, AVG(nir) AS mean_nir FROM {SLICE} GROUP BY x ORDER BY x"
        ))
        .await
        .expect("GROUP BY x");
    assert_eq!(rows.len(), 4, "one group per x: {rows:?}");
    for (i, row) in rows.iter().enumerate() {
        let x = int(&row[0]);
        assert_eq!(x, i as i64, "groups come back ordered by x");
        let expected = (0..4).map(|y| nir(x, y)).sum::<f64>() / 4.0;
        let got = float(&row[1]);
        assert!(
            (got - expected).abs() < 1e-9,
            "x = {x}: mean nir {got}, expected {expected}"
        );
    }
}

#[tokio::test]
async fn array_slice_order_by_attr_desc_limit() {
    let srv = TestServer::start().await;
    prepare_img(&srv).await;

    let rows = srv
        .query_rows(&format!(
            "SELECT x, y, nir FROM {SLICE} ORDER BY nir DESC LIMIT 3"
        ))
        .await
        .expect("ORDER BY nir DESC LIMIT 3");
    let got: Vec<(i64, i64)> = rows.iter().map(|r| (int(&r[0]), int(&r[1]))).collect();
    assert_eq!(got, [(3, 3), (3, 2), (3, 1)]);
    for row in &rows {
        let (x, y) = (int(&row[0]), int(&row[1]));
        assert!((float(&row[2]) - nir(x, y)).abs() < 1e-9);
    }
}

#[tokio::test]
async fn array_slice_outer_limit_respects_arg_cap() {
    let srv = TestServer::start().await;
    prepare_img(&srv).await;

    let capped = srv
        .query_rows("SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}', '*', 2) LIMIT 100")
        .await
        .expect("cap 2 under LIMIT 100");
    assert_eq!(capped.len(), 2, "the 4th-argument cap bounds the rows");

    let limited = srv
        .query_rows(&format!("SELECT * FROM {SLICE} LIMIT 5"))
        .await
        .expect("LIMIT 5");
    assert_eq!(limited.len(), 5, "an outer LIMIT bounds an uncapped slice");
}

#[tokio::test]
async fn array_slice_star_columns_are_dims_and_attrs() {
    let srv = TestServer::start().await;
    prepare_img(&srv).await;

    assert_eq!(
        column_names(&srv, &format!("SELECT * FROM {SLICE}")).await,
        ["x", "y", "nir", "red"]
    );
    assert_eq!(
        column_names(
            &srv,
            "SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}', ['red'])"
        )
        .await,
        ["x", "y", "red"]
    );
    assert_eq!(
        column_names(&srv, &format!("SELECT t.* FROM {SLICE} AS t")).await,
        ["x", "y", "nir", "red"]
    );

    let rows = srv
        .query_rows(&format!("SELECT * FROM {SLICE}"))
        .await
        .expect("SELECT *");
    assert_eq!(rows.len(), 16);
    for row in &rows {
        let (x, y) = (int(&row[0]), int(&row[1]));
        assert!((float(&row[2]) - nir(x, y)).abs() < 1e-9);
        assert!((float(&row[3]) - red(x, y)).abs() < 1e-9);
    }
}

#[tokio::test]
async fn array_slice_derived_table_matches_direct() {
    let srv = TestServer::start().await;
    prepare_img(&srv).await;

    let direct = coord_set(&srv, &format!("SELECT x, y FROM {SLICE} WHERE red < 0.2")).await;
    let derived = coord_set(
        &srv,
        &format!("SELECT x, y FROM (SELECT * FROM {SLICE}) t WHERE t.red < 0.2"),
    )
    .await;
    assert!(!direct.is_empty(), "the filter keeps some cells");
    assert_eq!(derived, direct);
}

#[tokio::test]
async fn array_agg_where_refused() {
    let srv = TestServer::start().await;
    prepare_img(&srv).await;

    srv.expect_error(
        "SELECT * FROM ARRAY_AGG('img', 'nir', 'sum') WHERE result > 1",
        "WHERE over ARRAY_AGG is not supported",
    )
    .await;
    srv.expect_error(
        "SELECT * FROM ARRAY_AGG('img', 'nir', 'sum') ORDER BY result",
        "ORDER BY over ARRAY_AGG is not supported",
    )
    .await;
}
