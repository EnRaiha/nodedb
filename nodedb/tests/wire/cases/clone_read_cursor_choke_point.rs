// SPDX-License-Identifier: BUSL-1.1

//! `DECLARE CURSOR` materializes its SELECT via `execute_query_for_cursor`,
//! which dispatches each planned task through
//! `clone_write::intercept_authorize_and_dispatch` directly — bypassing the
//! pgwire-only clone-read merge that predated the protocol-neutral
//! `shared::clone_read` hook entirely. Before that hook existed, a cursor
//! declared against a `Shadowed` clone always materialized zero rows: the
//! clone's target holds none until a write copies one up, and nothing on
//! this path merged in the source.
//!
//! `execute_query_for_cursor` stores each dispatched task's decoded payload
//! verbatim as one cursor row — cursors carry no column-shaping step at all,
//! for a clone read or a plain one — so `FETCH` returns the raw scan blob in
//! a single `result` column, not a projected scalar. The assertion below
//! matches that real (unrelated-to-clone) shape and checks the row's content
//! by substring.

use crate::harness::TestServer;

/// This fails without the fix (FETCH returns 0 rows: the clone's target is
/// empty and nothing merges in the source) and passes with it (FETCH
/// returns one row carrying the source-only value).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declare_cursor_on_shadowed_clone_reads_source_row() {
    let server = TestServer::start().await;

    server
        .exec("CREATE DATABASE crc_src")
        .await
        .expect("create source database");
    server
        .exec("USE DATABASE crc_src")
        .await
        .expect("use source database");
    server
        .exec("CREATE COLLECTION items (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create source collection");
    server
        .exec("INSERT INTO items (id, v) VALUES ('a', 1)")
        .await
        .expect("seed source row");

    server
        .exec("USE DATABASE default")
        .await
        .expect("use default database");
    server
        .exec("CLONE DATABASE crc_tgt FROM crc_src")
        .await
        .expect("clone database (Shadowed by default)");
    server
        .exec("USE DATABASE crc_tgt")
        .await
        .expect("use cloned database");

    server.exec("BEGIN").await.expect("begin");
    server
        .exec("DECLARE crc_cursor CURSOR FOR SELECT v FROM items")
        .await
        .expect("declare cursor against Shadowed clone");
    let rows = server
        .query_rows("FETCH ALL FROM crc_cursor")
        .await
        .expect("fetch from cursor materialized on a Shadowed clone");
    server.exec("CLOSE crc_cursor").await.expect("close cursor");
    server.exec("COMMIT").await.expect("commit");

    assert_eq!(
        rows.len(),
        1,
        "the cursor must materialize exactly the source-only row through the clone: {rows:?}"
    );
    assert!(
        rows[0][0].contains("\"v\":1"),
        "the materialized row must carry the source's value: {rows:?}"
    );
}
