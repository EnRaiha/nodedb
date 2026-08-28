// SPDX-License-Identifier: BUSL-1.1

//! `REFRESH MATERIALIZED VIEW` runs its stored `SELECT` via
//! `execute_select`, which dispatches each planned task through
//! `clone_write::intercept_authorize_and_dispatch` directly from internal
//! DDL code — never through the pgwire simple-query routing path. Before
//! the protocol-neutral `shared::clone_read` hook existed, this internal
//! scan had no clone-read coverage at all (only the top-level pgwire SELECT
//! path did), so refreshing a view whose query reads a `Shadowed` clone
//! always materialized zero rows into the view's own target — the clone's
//! target collection holds none until a write copies one up, and nothing
//! on this path merged in the source. This test fails without the fix (the
//! view ends up empty) and passes with it (the view gets the source row).

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_materialized_view_scans_shadowed_clone_source() {
    let server = TestServer::start().await;

    server
        .exec("CREATE DATABASE mvc_src")
        .await
        .expect("create source database");
    server
        .exec("USE DATABASE mvc_src")
        .await
        .expect("use source database");
    server
        .exec("CREATE COLLECTION mvc_data (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create source collection");
    server
        .exec("INSERT INTO mvc_data (id, v) VALUES ('a', 1)")
        .await
        .expect("seed source row");

    server
        .exec("USE DATABASE default")
        .await
        .expect("use default database");
    server
        .exec("CLONE DATABASE mvc_tgt FROM mvc_src")
        .await
        .expect("clone database (Shadowed by default)");
    server
        .exec("USE DATABASE mvc_tgt")
        .await
        .expect("use cloned database");

    // The view's own physical target (auto-created, named after the view)
    // is a brand-new, non-cloned collection — never refreshed before this
    // point, so it starts with zero rows. `ON mvc_data` only needs the
    // named collection to exist, which the Shadowed clone descriptor
    // satisfies without holding any physical rows itself.
    server
        .exec("CREATE MATERIALIZED VIEW mvc_view ON mvc_data AS SELECT id, v FROM mvc_data")
        .await
        .expect("create materialized view over the Shadowed clone's collection");

    server
        .exec("REFRESH MATERIALIZED VIEW mvc_view")
        .await
        .expect("refresh must succeed: the view's own target is not a clone");

    let rows = server
        .query_rows("SELECT id, v FROM mvc_view")
        .await
        .expect("select from the refreshed view");
    assert_eq!(
        rows,
        vec![vec!["a".to_string(), "1".to_string()]],
        "REFRESH's internal scan must see the source-only row through the \
         Shadowed clone: {rows:?}"
    );
}
