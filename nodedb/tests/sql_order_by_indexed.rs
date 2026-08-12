// SPDX-License-Identifier: BUSL-1.1

//! ORDER BY / LIMIT over a collection that has a secondary index.
//!
//! An equality predicate on a `Ready` indexed column lets the planner swap the
//! table scan for an index lookup. That access path returns rows in index
//! order and used to swallow the query's ORDER BY and LIMIT with it, so
//! `WHERE indexed_col = $1 ORDER BY id DESC LIMIT 1` answered with the OLDEST
//! row and `LIMIT` became the converter's 10,000-row default. The rewrite must
//! decline whenever a sort is asked for, and must carry the row bound
//! otherwise.
//!
//! The sibling `sql_order_by` suite covers index-free collections, where the
//! rewrite never fires — these cases cannot regress there.

mod common;

use common::pgwire_harness::TestServer;

/// Rows are inserted in an order that matches neither the ascending nor the
/// descending sort, so neither can be satisfied by storage order alone.
const INSERT_ORDER: [(&str, i32); 5] = [("r3", 30), ("r1", 10), ("r5", 50), ("r2", 20), ("r4", 40)];

async fn seed_schemaless(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    for (id, n) in INSERT_ORDER {
        server
            .exec(&format!(
                "INSERT INTO {collection} {{ id: '{id}', region: 'us', n: {n} }}"
            ))
            .await
            .unwrap();
    }
    // A non-matching row: the equality predicate must still be applied.
    server
        .exec(&format!(
            "INSERT INTO {collection} {{ id: 'r9', region: 'eu', n: 90 }}"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("CREATE INDEX ON {collection}(region)"))
        .await
        .unwrap();
}

async fn seed_strict(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, region TEXT, n INT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    for (id, n) in INSERT_ORDER {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, region, n) VALUES ('{id}', 'us', {n})"
            ))
            .await
            .unwrap();
    }
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, region, n) VALUES ('r9', 'eu', 90)"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("CREATE INDEX ON {collection}(region)"))
        .await
        .unwrap();
}

/// `ORDER BY ... DESC LIMIT n` filtered by an equality on the indexed column.
/// The exact shape that returned the oldest row with the LIMIT ignored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_equality_honors_order_by_desc_and_limit_schemaless() {
    let server = TestServer::start().await;
    seed_schemaless(&server, "oi_sl").await;

    let rows = server
        .query_text("SELECT id FROM oi_sl WHERE region = 'us' ORDER BY id DESC LIMIT 2")
        .await
        .expect("indexed equality with ORDER BY + LIMIT must plan and execute");

    assert_eq!(
        rows,
        vec!["r5".to_string(), "r4".to_string()],
        "an equality on the indexed column must not cost the query its ORDER BY or LIMIT; \
         got {rows:?} (the highest ids first, capped at 2)"
    );
}

/// The ascending direction has to be honoured for the same shape — an index
/// lookup that happens to emit ascending order must not be mistaken for a
/// working sort by a test that only ever checks ASC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_equality_honors_order_by_asc_and_limit_schemaless() {
    let server = TestServer::start().await;
    seed_schemaless(&server, "oi_sl_asc").await;

    let rows = server
        .query_text("SELECT id FROM oi_sl_asc WHERE region = 'us' ORDER BY id ASC LIMIT 2")
        .await
        .expect("indexed equality with ORDER BY + LIMIT must plan and execute");

    assert_eq!(
        rows,
        vec!["r1".to_string(), "r2".to_string()],
        "ascending sort over an indexed equality; got {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_equality_honors_order_by_desc_and_limit_strict() {
    let server = TestServer::start().await;
    seed_strict(&server, "oi_st").await;

    let rows = server
        .query_text("SELECT id FROM oi_st WHERE region = 'us' ORDER BY n DESC LIMIT 2")
        .await
        .expect("indexed equality with ORDER BY + LIMIT must plan and execute");

    assert_eq!(
        rows,
        vec!["r5".to_string(), "r4".to_string()],
        "an equality on the indexed column must not cost the query its ORDER BY or LIMIT; \
         got {rows:?} (the highest n first, capped at 2)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_equality_honors_order_by_asc_and_limit_strict() {
    let server = TestServer::start().await;
    seed_strict(&server, "oi_st_asc").await;

    let rows = server
        .query_text("SELECT id FROM oi_st_asc WHERE region = 'us' ORDER BY n ASC LIMIT 2")
        .await
        .expect("indexed equality with ORDER BY + LIMIT must plan and execute");

    assert_eq!(
        rows,
        vec!["r1".to_string(), "r2".to_string()],
        "ascending sort over an indexed equality; got {rows:?}"
    );
}

/// The dropped-LIMIT half of the bug, with no ORDER BY in sight: the index
/// lookup keeps the access path, so it has to carry the row bound itself
/// instead of falling back to the converter's default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_equality_honors_limit_without_order_by_schemaless() {
    let server = TestServer::start().await;
    seed_schemaless(&server, "oi_sl_lim").await;

    let rows = server
        .query_text("SELECT id FROM oi_sl_lim WHERE region = 'us' LIMIT 2")
        .await
        .expect("indexed equality with LIMIT must plan and execute");

    assert_eq!(
        rows.len(),
        2,
        "LIMIT 2 over five matching rows must return two; got {rows:?}"
    );
    assert!(
        rows.iter().all(|id| id != "r9"),
        "the equality predicate must still exclude the 'eu' row; got {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_equality_honors_limit_without_order_by_strict() {
    let server = TestServer::start().await;
    seed_strict(&server, "oi_st_lim").await;

    let rows = server
        .query_text("SELECT id FROM oi_st_lim WHERE region = 'us' LIMIT 2")
        .await
        .expect("indexed equality with LIMIT must plan and execute");

    assert_eq!(
        rows.len(),
        2,
        "LIMIT 2 over five matching rows must return two; got {rows:?}"
    );
    assert!(
        rows.iter().all(|id| id != "r9"),
        "the equality predicate must still exclude the 'eu' row; got {rows:?}"
    );
}

/// Every matching row is still returned when no bound is given — the fix must
/// not turn the index lookup into a truncated read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_equality_without_limit_returns_all_matches() {
    let server = TestServer::start().await;
    seed_schemaless(&server, "oi_sl_all").await;

    let rows = server
        .query_text("SELECT id FROM oi_sl_all WHERE region = 'us' ORDER BY id ASC")
        .await
        .expect("indexed equality without a LIMIT must plan and execute");

    assert_eq!(
        rows,
        vec![
            "r1".to_string(),
            "r2".to_string(),
            "r3".to_string(),
            "r4".to_string(),
            "r5".to_string(),
        ],
        "all five matching rows, in the requested order; got {rows:?}"
    );
}
