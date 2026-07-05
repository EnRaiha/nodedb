// SPDX-License-Identifier: BUSL-1.1

//! In-transaction spatial predicate scans (`WHERE ST_DWithin(...)`) observe
//! the transaction's own uncommitted writes (read-your-own-writes).
//!
//! A mainstream SQL `INSERT INTO <spatial_collection> VALUES(...)` routes to
//! `ColumnarOp::Insert` (already staged by the columnar staging unit), not
//! `SpatialOp::Insert`. The gap closed here is the READ side:
//! `execute_spatial_scan` now merges the transaction's overlay into its
//! R-tree/full-scan result via `merge_overlay_into_spatial_scan`, decoding a
//! staged columnar row and re-evaluating the spatial predicate against it.
//!
//! NOTE on DELETE: a SQL `DELETE FROM <spatial_collection> ...` routes to
//! `ColumnarOp::Delete`, which is not in the stageable-write allow-list (only
//! `ColumnarOp::Insert` is staged today -- widening `ColumnarOp::Delete`
//! staging is a separate unit). A `DELETE` inside a transaction therefore
//! still takes the pre-existing buffered "OK now, apply at COMMIT" path, so
//! an in-tx spatial `SELECT` cannot observe a same-transaction `DELETE`
//! until `COMMIT`. This file does not test that case for that reason.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

fn rows_of(msgs: &[SimpleQueryMessage], col: &str) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(col).map(str::to_string),
            _ => None,
        })
        .collect()
}

/// Times Square, NYC -- the query anchor point used by every test below.
const QUERY_POINT: &str = "{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}";
const WITHIN_5KM: &str =
    "ST_DWithin(location, '{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}', 5000)";

async fn setup(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION geo_tx (id TEXT, location GEOMETRY SPATIAL_INDEX, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .unwrap();
    // Base row inside the 5km query region (Times Square itself).
    server
        .exec(
            "INSERT INTO geo_tx (id, location, name) \
             VALUES ('base_near', ST_Point(-73.9857, 40.7580), 'Base Near')",
        )
        .await
        .unwrap();
    // Base row far outside the query region (Paris).
    server
        .exec(
            "INSERT INTO geo_tx (id, location, name) \
             VALUES ('base_far', ST_Point(2.3522, 48.8566), 'Base Far')",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_spatial_insert_in_region_visible_in_tx_and_survives_commit() {
    let server = TestServer::start().await;
    setup(&server).await;
    let _ = QUERY_POINT; // documents the anchor used by WITHIN_5KM

    server.exec("BEGIN").await.unwrap();

    // Staged inside the txn: a new point a few hundred meters from Times
    // Square, well within the 5km query region.
    server
        .client
        .simple_query(
            "INSERT INTO geo_tx (id, location, name) \
             VALUES ('staged_near', ST_Point(-73.9880, 40.7600), 'Staged Near')",
        )
        .await
        .expect("staged spatial-collection insert should succeed at statement time");

    let in_tx = server
        .client
        .simple_query(&format!("SELECT name FROM geo_tx WHERE {WITHIN_5KM}"))
        .await
        .unwrap();
    let mut names = rows_of(&in_tx, "name");
    names.sort();
    assert_eq!(
        names,
        vec!["Base Near".to_string(), "Staged Near".to_string()],
        "in-tx spatial scan must observe the transaction's own uncommitted \
         spatial write (read-your-own-writes), alongside the matching base row"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = server
        .query_text(&format!("SELECT name FROM geo_tx WHERE {WITHIN_5KM}"))
        .await
        .unwrap();
    let mut committed_sorted = committed;
    committed_sorted.sort();
    assert_eq!(
        committed_sorted,
        vec!["Base Near".to_string(), "Staged Near".to_string()],
        "committed spatial insert must persist and remain visible post-COMMIT"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_spatial_insert_outside_region_does_not_appear() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    // Staged inside the txn: a point in Tokyo, nowhere near the 5km query
    // region around Times Square -- the predicate must be re-evaluated
    // against the staged geometry, not blindly trusted as a match.
    server
        .client
        .simple_query(
            "INSERT INTO geo_tx (id, location, name) \
             VALUES ('staged_far', ST_Point(139.6917, 35.6895), 'Staged Far')",
        )
        .await
        .unwrap();

    let in_tx = server
        .client
        .simple_query(&format!("SELECT name FROM geo_tx WHERE {WITHIN_5KM}"))
        .await
        .unwrap();
    assert_eq!(
        rows_of(&in_tx, "name"),
        vec!["Base Near".to_string()],
        "a staged geometry outside the query region must not appear, even \
         though it is present in the overlay"
    );

    // The staged-but-non-matching row must also not appear in an unfiltered
    // scan of the collection restricted to its own id -- it does exist, just
    // not within the spatial predicate's region.
    let by_id = server
        .client
        .simple_query("SELECT name FROM geo_tx WHERE id = 'staged_far'")
        .await
        .unwrap();
    assert_eq!(rows_of(&by_id, "name"), vec!["Staged Far".to_string()]);

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_spatial_insert_rollback_discards_and_leaves_base_rows_intact() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    server
        .client
        .simple_query(
            "INSERT INTO geo_tx (id, location, name) \
             VALUES ('staged_near', ST_Point(-73.9880, 40.7600), 'Staged Near')",
        )
        .await
        .unwrap();

    // Visible in-tx before rollback.
    let in_tx = server
        .client
        .simple_query(&format!("SELECT name FROM geo_tx WHERE {WITHIN_5KM}"))
        .await
        .unwrap();
    let mut names = rows_of(&in_tx, "name");
    names.sort();
    assert_eq!(
        names,
        vec!["Base Near".to_string(), "Staged Near".to_string()]
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after = server
        .query_text(&format!("SELECT name FROM geo_tx WHERE {WITHIN_5KM}"))
        .await
        .unwrap();
    assert_eq!(
        after,
        vec!["Base Near".to_string()],
        "rolled-back spatial insert must not persist"
    );

    // Unrelated base rows (including the one outside the query region) are
    // unaffected by the rollback.
    let all = server
        .query_text("SELECT id FROM geo_tx ORDER BY id")
        .await
        .unwrap();
    assert_eq!(all, vec!["base_far".to_string(), "base_near".to_string()]);
}
