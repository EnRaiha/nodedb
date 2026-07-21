// SPDX-License-Identifier: BUSL-1.1

//! `REFRESH MATERIALIZED VIEW` must execute the stored SELECT plan
//! (project, filter, aggregate, join) and write the computed rows to
//! the view's target collection — never copy raw source documents.
//!
//! View targets are schemaless document collections, so pgwire
//! `SELECT` returns each row as a wrapped JSON doc in column 0. Tests
//! decode that wrapper and assert on the logical row contents.

mod common;

use common::pgwire_harness::TestServer;

// SELECT * over a multi-column response is expanded by pgwire into one
// field per declared column, so tests use `query_named_rows` to recover a
// `HashMap<column_name, value>` view per row. The empty-string convention
// for absent columns matches the harness contract.

/// A view defined with a projection must materialize only the projected
/// columns; source-only columns must not appear in the view target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_applies_projection() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION mv_proj_src").await.unwrap();
    server
        .exec("INSERT INTO mv_proj_src { id: 'r1', a: 1, b: 10, c: 100 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_proj_src { id: 'r2', a: 2, b: 20, c: 200 }")
        .await
        .unwrap();

    server
        .exec("CREATE MATERIALIZED VIEW mv_proj ON mv_proj_src AS SELECT id, a FROM mv_proj_src")
        .await
        .unwrap();
    server
        .exec("REFRESH MATERIALIZED VIEW mv_proj")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT * FROM mv_proj")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "view must have both projected rows");
    for row in &rows {
        assert!(
            row.contains_key("id") && row.contains_key("a"),
            "projected columns id+a must be present in {row:?}"
        );
        assert!(
            !row.contains_key("b") && !row.contains_key("c"),
            "unprojected columns must not leak into view row {row:?}"
        );
    }
}

/// A view defined with `WHERE` must only materialize rows matching the
/// predicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_applies_where_filter() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION mv_filt_src").await.unwrap();
    server
        .exec("INSERT INTO mv_filt_src { id: 'r1', a: 1, b: 1 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_filt_src { id: 'r2', a: 2, b: 99 }")
        .await
        .unwrap();

    server
        .exec(
            "CREATE MATERIALIZED VIEW mv_filt ON mv_filt_src AS \
             SELECT id, a FROM mv_filt_src WHERE b > 5",
        )
        .await
        .unwrap();
    server
        .exec("REFRESH MATERIALIZED VIEW mv_filt")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT * FROM mv_filt")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "only the row matching WHERE b > 5 may appear"
    );
    assert_eq!(
        rows[0].get("id").map(String::as_str),
        Some("r2"),
        "surviving row must be r2"
    );
}

/// A view with GROUP BY + aggregate must produce aggregated rows, not
/// raw source docs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_applies_group_by_aggregate() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION mv_agg_src").await.unwrap();
    server
        .exec("INSERT INTO mv_agg_src { id: 'r1', bucket: 'x', n: 1 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_agg_src { id: 'r2', bucket: 'x', n: 1 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_agg_src { id: 'r3', bucket: 'y', n: 1 }")
        .await
        .unwrap();

    server
        .exec(
            "CREATE MATERIALIZED VIEW mv_agg ON mv_agg_src AS \
             SELECT bucket, COUNT(*) AS cnt FROM mv_agg_src GROUP BY bucket",
        )
        .await
        .unwrap();
    server
        .exec("REFRESH MATERIALIZED VIEW mv_agg")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT * FROM mv_agg")
        .await
        .unwrap();
    // Raw-copy bug would produce 3 rows (one per source doc) instead of 2.
    assert_eq!(
        rows.len(),
        2,
        "GROUP BY must aggregate to one row per group"
    );
}

/// Dropping and re-creating a materialized view under the same name creates a
/// new target. Before its first refresh, a scalar aggregate over that empty
/// target must return the zero-row identity rather than the predecessor's
/// cached result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_then_recreate_materialized_view_discards_cached_aggregates() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION mv_reuse_src").await.unwrap();
    server
        .exec("INSERT INTO mv_reuse_src { id: 'r1', value: 1 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_reuse_src { id: 'r2', value: 2 }")
        .await
        .unwrap();
    server
        .exec(
            "CREATE MATERIALIZED VIEW mv_reuse ON mv_reuse_src AS \
             SELECT id, value FROM mv_reuse_src",
        )
        .await
        .unwrap();
    server
        .exec("REFRESH MATERIALIZED VIEW mv_reuse")
        .await
        .unwrap();

    let predecessor = server
        .query_text("SELECT COUNT(*) FROM mv_reuse")
        .await
        .unwrap();
    assert_eq!(predecessor, vec!["2".to_string()]);

    assert!(
        server.exec("DROP COLLECTION mv_reuse PURGE").await.is_err(),
        "a materialized-view target cannot be dropped independently of its owner"
    );

    server
        .exec("DROP MATERIALIZED VIEW mv_reuse")
        .await
        .unwrap();
    server
        .exec(
            "CREATE MATERIALIZED VIEW mv_reuse ON mv_reuse_src AS \
             SELECT id, value FROM mv_reuse_src",
        )
        .await
        .unwrap();

    let replacement_rows = server.query_rows("SELECT id FROM mv_reuse").await.unwrap();
    assert!(
        replacement_rows.is_empty(),
        "new materialized-view target must start without predecessor rows: {replacement_rows:?}"
    );

    let replacement = server
        .query_text("SELECT COUNT(*) FROM mv_reuse")
        .await
        .unwrap();
    assert_eq!(
        replacement,
        vec!["0".to_string()],
        "new materialized-view target must not inherit predecessor aggregates"
    );
}

/// A materialized view owns and purges its same-name target collection.
/// Creation must therefore reject a pre-existing user collection rather than
/// silently adopting storage that DROP MATERIALIZED VIEW would later erase.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_rejects_user_collection_with_same_name() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION mv_owner_src").await.unwrap();
    server
        .exec("CREATE COLLECTION mv_owned_name")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_owned_name { id: 'keep', value: 7 }")
        .await
        .unwrap();

    let result = server
        .exec(
            "CREATE MATERIALIZED VIEW mv_owned_name ON mv_owner_src AS \
             SELECT id FROM mv_owner_src",
        )
        .await;
    assert!(
        result.is_err(),
        "materialized view must not adopt a user-owned same-name collection"
    );

    let rows = server
        .query_rows("SELECT id FROM mv_owned_name")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["keep".to_string()]]);
}

/// A view whose SELECT joins two collections must materialize the
/// joined rows, not copies of one source's raw docs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_applies_join() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION mv_join_left").await.unwrap();
    server
        .exec("CREATE COLLECTION mv_join_right")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_join_left { id: 'l1', rid: 'r1', name: 'alice' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_join_left { id: 'l2', rid: 'r2', name: 'bob' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_join_right { id: 'r1', tag: 'vip' }")
        .await
        .unwrap();
    // Deliberately no match for l2 — an INNER JOIN must drop it.

    server
        .exec(
            "CREATE MATERIALIZED VIEW mv_join ON mv_join_left AS \
             SELECT l.id AS id, r.tag AS tag \
             FROM mv_join_left l JOIN mv_join_right r ON l.rid = r.id",
        )
        .await
        .unwrap();
    server
        .exec("REFRESH MATERIALIZED VIEW mv_join")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT * FROM mv_join")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "INNER JOIN view must materialize only the matched pair, got {rows:?}"
    );
}

/// When the view's WHERE predicate narrows the source, a subsequent refresh
/// must remove view rows that no longer match.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_removes_rows_excluded_by_where() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION mv_narrow_src")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_narrow_src { id: 'r1', keep: true }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO mv_narrow_src { id: 'r2', keep: true }")
        .await
        .unwrap();
    server
        .exec(
            "CREATE MATERIALIZED VIEW mv_narrow ON mv_narrow_src AS \
             SELECT id FROM mv_narrow_src WHERE keep = true",
        )
        .await
        .unwrap();
    server
        .exec("REFRESH MATERIALIZED VIEW mv_narrow")
        .await
        .unwrap();

    // Flip r1 so the WHERE no longer selects it. A correct refresh must
    // drop r1 from the view target.
    server
        .exec("UPDATE mv_narrow_src SET keep = false WHERE id = 'r1'")
        .await
        .unwrap();
    server
        .exec("REFRESH MATERIALIZED VIEW mv_narrow")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT * FROM mv_narrow")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "WHERE-excluded row must be removed");
    assert_eq!(
        rows[0].get("id").map(String::as_str),
        Some("r2"),
        "surviving row after narrowed WHERE must be r2"
    );
}
