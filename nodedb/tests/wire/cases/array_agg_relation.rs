// SPDX-License-Identifier: BUSL-1.1

//! `ARRAY_AGG` and `ARRAY_ELEMENTWISE` as relations: every SELECT clause
//! applies over the rows the call emits, the same as over a derived table
//! that reads the call.
//!
//! The fixture is a 4x4 array `agr` with dims `t`, `x`. Cell `(t, x)` holds
//! `nir = (4t + x + 1) / 20`, so the mean of group `t` is `(4t + 2.5) / 20`
//! and the ungrouped sum is `6.8`.

use crate::harness::TestServer;

const GROUPED: &str = "ARRAY_AGG('agr', 'nir', 'mean', 't')";

async fn create_agr(server: &TestServer) {
    server
        .exec(
            "CREATE ARRAY agr DIMS (t INT64 [0..3], x INT64 [0..3]) \
             ATTRS (nir FLOAT64) TILE_EXTENTS (2, 2)",
        )
        .await
        .expect("CREATE ARRAY agr");
    let cells: Vec<String> = (0..4)
        .flat_map(|t| {
            (0..4).map(move |x| {
                format!(
                    "COORDS ({t}, {x}) VALUES ({:?})",
                    (4 * t + x + 1) as f64 / 20.0
                )
            })
        })
        .collect();
    server
        .exec(&format!("INSERT INTO ARRAY agr {}", cells.join(", ")))
        .await
        .expect("INSERT INTO ARRAY agr");
    server
        .exec("SELECT ARRAY_FLUSH('agr')")
        .await
        .expect("ARRAY_FLUSH");
}

fn group_mean(t: i64) -> f64 {
    (4 * t) as f64 / 20.0 + 2.5 / 20.0
}

fn number(text: &str) -> f64 {
    text.parse()
        .unwrap_or_else(|e| panic!("{text:?} is not a number: {e}"))
}

/// `(group, value)` of every row `sql` returns, in row order.
async fn pairs(server: &TestServer, sql: &str) -> Vec<(i64, f64)> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| (number(&row[0]) as i64, number(&row[1])))
        .collect()
}

fn assert_pairs(got: &[(i64, f64)], expected: &[(i64, f64)], sql: &str) {
    assert_eq!(got.len(), expected.len(), "{sql}: {got:?}");
    for ((g, v), (eg, ev)) in got.iter().zip(expected) {
        assert_eq!(g, eg, "{sql}: {got:?}");
        assert!((v - ev).abs() < 1e-9, "{sql}: {got:?}");
    }
}

#[tokio::test]
async fn grouped_agg_orders_by_alias_with_a_rounded_result() {
    let server = TestServer::start().await;
    create_agr(&server).await;
    let sql = format!(
        "SELECT \"group\" AS date, ROUND(result, 4) AS mean_nir FROM {GROUPED} ORDER BY date DESC"
    );
    let expected: Vec<(i64, f64)> = (0..4).rev().map(|t| (t, group_mean(t))).collect();
    assert_pairs(&pairs(&server, &sql).await, &expected, &sql);
}

#[tokio::test]
async fn grouped_agg_orders_by_group_with_limit() {
    let server = TestServer::start().await;
    create_agr(&server).await;
    let sql = format!("SELECT \"group\", result FROM {GROUPED} ORDER BY \"group\" LIMIT 2");
    assert_pairs(
        &pairs(&server, &sql).await,
        &[(0, group_mean(0)), (1, group_mean(1))],
        &sql,
    );
}

#[tokio::test]
async fn ungrouped_agg_rounds_its_result() {
    let server = TestServer::start().await;
    create_agr(&server).await;
    let sql = "SELECT ROUND(result, 2) AS total FROM ARRAY_AGG('agr', 'nir', 'sum')";
    let rows = server
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: one row: {rows:?}");
    assert!((number(&rows[0]["total"]) - 6.8).abs() < 1e-9, "{rows:?}");
}

#[tokio::test]
async fn where_on_result_keeps_matching_groups() {
    let server = TestServer::start().await;
    create_agr(&server).await;
    let sql =
        format!("SELECT \"group\", result FROM {GROUPED} WHERE result > 0.3 ORDER BY \"group\"");
    let expected: Vec<(i64, f64)> = (1..4).map(|t| (t, group_mean(t))).collect();
    assert_pairs(&pairs(&server, &sql).await, &expected, &sql);
}

#[tokio::test]
async fn count_over_grouped_agg_counts_the_kept_groups() {
    let server = TestServer::start().await;
    create_agr(&server).await;
    let sql = format!("SELECT COUNT(*) AS n FROM {GROUPED} WHERE result > 0.3");
    let rows = server
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
    assert_eq!(number(&rows[0]["n"]), 3.0, "{rows:?}");
}

#[tokio::test]
async fn direct_read_matches_the_derived_table() {
    let server = TestServer::start().await;
    create_agr(&server).await;
    let direct = format!(
        "SELECT \"group\" AS date, ROUND(result, 4) FROM {GROUPED} \
         WHERE result < 0.6 ORDER BY date DESC LIMIT 2"
    );
    let derived = format!(
        "SELECT a.\"group\" AS date, ROUND(a.result, 4) FROM (SELECT * FROM {GROUPED}) a \
         WHERE a.result < 0.6 ORDER BY date DESC LIMIT 2"
    );
    let direct_rows = pairs(&server, &direct).await;
    assert_eq!(direct_rows.len(), 2, "{direct}: {direct_rows:?}");
    assert_eq!(direct_rows, pairs(&server, &derived).await);
}

#[tokio::test]
async fn count_over_elementwise_counts_its_rows() {
    let server = TestServer::start().await;
    create_agr(&server).await;
    let sql = "SELECT COUNT(*) AS n FROM ARRAY_ELEMENTWISE('agr', 'agr', 'add', 'nir')";
    let rows = server
        .query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
    assert_eq!(number(&rows[0]["n"]), 16.0, "{rows:?}");
}
