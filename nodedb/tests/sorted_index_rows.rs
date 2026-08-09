// SPDX-License-Identifier: BUSL-1.1

//! What a sorted index actually answers with.
//!
//! `RANK` / `TOPK` / `RANGE` / `SORTED_COUNT` read an order-statistic tree that
//! lives in one Data-Plane core's `KvEngine`. That tree is built by scanning
//! the collection's rows on the core that holds them, and maintained by the KV
//! writes that land on that same core — so the index only ever agrees with its
//! collection if registration, maintenance, and query all resolve to the ONE
//! core the collection is homed on.
//!
//! Every test here runs on a multi-core server for that reason: the index name
//! and the collection name hash independently, so a single-core server maps
//! them to the same core by accident and cannot observe the difference.
//!
//! The load-bearing assertion is the last one — that the index answers with the
//! same row set the equivalent unindexed query returns. An index that quietly
//! disagrees with its own table is worse than no index, and only a comparison
//! against the table can catch that.
//!
//! Authorization for these same functions is covered by
//! `sorted_index_authorization.rs`.

mod common;

use common::pgwire_harness::TestServer;
use nodedb::types::{DatabaseId, VShardId};

/// Data-Plane cores the test server runs. More than one is the point: it is
/// what makes "the index is registered on a different core than its rows"
/// observable at all.
const CORES: usize = 4;

const BOARD: &str = "sidx_rows_board";
const INDEX: &str = "sidx_rows_lb";

/// The core a name is routed to, mirroring `VShardRouter::round_robin`.
fn owning_core(name: &str) -> usize {
    VShardId::from_collection_in_database(DatabaseId::DEFAULT, name).as_u32() as usize % CORES
}

/// A leaderboard with `rows` already stored, then indexed. Returns the server.
///
/// The index is created LAST so the rows it must adopt are the ones that were
/// already there — the backfill case.
async fn board_indexed_after(rows: &[(&str, i64)]) -> TestServer {
    let server = TestServer::start_multicores(CORES).await;
    create_board(&server).await;
    for (id, score) in rows {
        insert_row(&server, id, *score).await;
    }
    create_index(&server).await;
    server
}

async fn create_board(server: &TestServer) {
    server
        .exec(&format!(
            "CREATE COLLECTION {BOARD} (id STRING PRIMARY KEY, score INT) WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {BOARD}: {e}"));
}

async fn create_index(server: &TestServer) {
    server
        .exec(&format!(
            "CREATE SORTED INDEX {INDEX} ON {BOARD} (score DESC) KEY id"
        ))
        .await
        .unwrap_or_else(|e| panic!("create sorted index {INDEX}: {e}"));
}

async fn insert_row(server: &TestServer, id: &str, score: i64) {
    server
        .exec(&format!(
            "INSERT INTO {BOARD} (id, score) VALUES ('{id}', {score})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id}: {e}"));
}

/// The keys `TOPK` delivers, in rank order. Rows are `(rank, key)`.
async fn topk_keys(server: &TestServer, k: u32) -> Vec<String> {
    let rows = server
        .query_rows(&format!("SELECT * FROM TOPK({INDEX}, {k})"))
        .await
        .unwrap_or_else(|e| panic!("TOPK: {e}"));
    rows.into_iter()
        .map(|row| row.get(1).cloned().unwrap_or_default())
        .collect()
}

/// The keys `RANGE` delivers for an inclusive score window.
async fn range_keys(server: &TestServer, low: i64, high: i64) -> Vec<String> {
    let rows = server
        .query_rows(&format!("SELECT * FROM RANGE({INDEX}, {low}, {high})"))
        .await
        .unwrap_or_else(|e| panic!("RANGE: {e}"));
    let mut keys: Vec<String> = rows
        .into_iter()
        .map(|row| row.get(1).cloned().unwrap_or_default())
        .collect();
    keys.sort();
    keys
}

/// The integer a single-cell JSON reply carries under `field`.
///
/// `RANK` and `SORTED_COUNT` deliver one text column holding the Data-Plane
/// reply as JSON, so the number has to come back out of it.
fn json_field(text: &str, field: &str) -> Option<i64> {
    let needle = format!("\"{field}\":");
    let rest = text.split_once(&needle)?.1;
    let digits: String = rest
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '-')
        .collect();
    digits.parse().ok()
}

async fn rank_of(server: &TestServer, id: &str) -> Option<i64> {
    let rows = server
        .query_text(&format!("SELECT RANK({INDEX}, '{id}')"))
        .await
        .unwrap_or_else(|e| panic!("RANK: {e}"));
    json_field(rows.first()?, "rank")
}

async fn sorted_count(server: &TestServer) -> Option<i64> {
    let rows = server
        .query_text(&format!("SELECT SORTED_COUNT({INDEX})"))
        .await
        .unwrap_or_else(|e| panic!("SORTED_COUNT: {e}"));
    json_field(rows.first()?, "count")
}

/// Every stored `(id, score)`, read without going near the index.
async fn stored_rows(server: &TestServer) -> Vec<(String, i64)> {
    let rows = server
        .query_rows(&format!("SELECT id, score FROM {BOARD}"))
        .await
        .unwrap_or_else(|e| panic!("scan {BOARD}: {e}"));
    rows.into_iter()
        .map(|row| {
            let id = row.first().cloned().unwrap_or_default();
            let score = row
                .get(1)
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or_else(|| panic!("row {id} has no numeric score"));
            (id, score)
        })
        .collect()
}

/// The ids an unindexed scan yields in the order the index claims to impose:
/// score descending, ties broken by id so the comparison is total.
async fn stored_ids_by_score_desc(server: &TestServer) -> Vec<String> {
    let mut rows = stored_rows(server).await;
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows.into_iter().map(|(id, _)| id).collect()
}

/// The premise the rest of the file rests on: these two names are homed on
/// DIFFERENT cores, so an index registered or queried by its own name instead
/// of its collection's reaches a core that holds none of the rows.
#[test]
fn the_index_name_and_its_collection_are_homed_on_different_cores() {
    assert_ne!(
        owning_core(BOARD),
        owning_core(INDEX),
        "this file cannot observe the routing it exists to pin unless \
         '{BOARD}' and '{INDEX}' land on different cores"
    );
}

/// Rows that existed before `CREATE SORTED INDEX` must be reachable through it.
///
/// Creating the index empty and letting later writes fill it leaves it
/// disagreeing with its own collection from the moment it is created, and no
/// read path re-checks the table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_written_before_the_index_are_found_through_it() {
    let server = board_indexed_after(&[("p1", 10), ("p2", 30), ("p3", 20)]).await;

    assert_eq!(
        topk_keys(&server, 10).await,
        vec!["p2", "p3", "p1"],
        "TOPK must rank the rows that were already stored"
    );
    assert_eq!(sorted_count(&server).await, Some(3));
    assert_eq!(rank_of(&server, "p2").await, Some(1));
}

/// Rows written after the index exists must be maintained into it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_written_after_the_index_are_found_through_it() {
    let server = TestServer::start_multicores(CORES).await;
    create_board(&server).await;
    create_index(&server).await;

    insert_row(&server, "p1", 10).await;
    insert_row(&server, "p2", 30).await;
    insert_row(&server, "p3", 20).await;

    assert_eq!(
        topk_keys(&server, 10).await,
        vec!["p2", "p3", "p1"],
        "TOPK must rank rows inserted after registration"
    );
    assert_eq!(sorted_count(&server).await, Some(3));
}

/// An UPDATE rewrites the indexed column, so the index must re-key the row
/// rather than keep answering from the pre-update score.
///
/// A SQL `UPDATE` on a KV collection merges the assigned fields and stores the
/// merged row through the ordinary point-write path, so this pins that path.
/// The atomic write body — `KV_INCR`, `CAS`, `GETSET`, `TRANSFER` — is a second
/// route to the same store and is covered at the engine level.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_update_reorders_the_index() {
    let server = board_indexed_after(&[("p1", 10), ("p2", 30), ("p3", 20)]).await;

    server
        .exec(&format!("UPDATE {BOARD} SET score = 99 WHERE id = 'p1'"))
        .await
        .expect("update p1's score");

    assert_eq!(
        topk_keys(&server, 10).await,
        vec!["p1", "p2", "p3"],
        "the updated row must take its new position"
    );
    assert_eq!(rank_of(&server, "p1").await, Some(1));
    assert_eq!(
        sorted_count(&server).await,
        Some(3),
        "an update re-keys a row, it does not add one"
    );
}

/// A DELETE must take the row out of the index, or the deleted key keeps its
/// rank and pushes every live key below it down one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_removes_the_row_from_the_index() {
    let server = board_indexed_after(&[("p1", 10), ("p2", 30), ("p3", 20)]).await;

    server
        .exec(&format!("DELETE FROM {BOARD} WHERE id = 'p2'"))
        .await
        .expect("delete p2");

    assert_eq!(topk_keys(&server, 10).await, vec!["p3", "p1"]);
    assert_eq!(sorted_count(&server).await, Some(2));
    assert_eq!(
        rank_of(&server, "p2").await,
        None,
        "a deleted key must hold no rank"
    );
}

/// The assertion that catches an index disagreeing with its table: whatever the
/// index answers must be exactly what the collection holds, after every kind of
/// write the collection accepts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_index_answers_with_the_same_rows_as_an_unindexed_query() {
    let server = board_indexed_after(&[("p1", 10), ("p2", 30), ("p3", 20)]).await;

    insert_row(&server, "p4", 25).await;
    server
        .exec(&format!("UPDATE {BOARD} SET score = 5 WHERE id = 'p2'"))
        .await
        .expect("update p2's score");
    server
        .exec(&format!("DELETE FROM {BOARD} WHERE id = 'p3'"))
        .await
        .expect("delete p3");

    let expected = stored_ids_by_score_desc(&server).await;
    assert_eq!(
        expected.len(),
        3,
        "the collection itself must hold three rows at this point"
    );
    assert_eq!(
        topk_keys(&server, 100).await,
        expected,
        "the index must deliver the collection's rows, in the collection's order"
    );
    assert_eq!(sorted_count(&server).await, Some(expected.len() as i64));
}

/// `RANGE` names a score window, and must select the same rows a predicate over
/// that window selects — bounds included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_selects_the_same_rows_as_the_equivalent_predicate() {
    let server = board_indexed_after(&[("p1", 10), ("p2", 30), ("p3", 20), ("p4", 25)]).await;

    let (low, high) = (20, 30);
    let mut expected: Vec<String> = stored_rows(&server)
        .await
        .into_iter()
        .filter(|(_, score)| *score >= low && *score <= high)
        .map(|(id, _)| id)
        .collect();
    expected.sort();

    assert_eq!(
        expected,
        vec!["p2", "p3", "p4"],
        "the window must be the one this test means to check"
    );
    assert_eq!(
        range_keys(&server, low, high).await,
        expected,
        "RANGE must select the rows whose score falls in [{low}, {high}]"
    );
}
