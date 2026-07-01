// SPDX-License-Identifier: BUSL-1.1

//! Pgwire DROP COLLECTION / DROP TABLE lifecycle.
//!
//! Covers the `IF EXISTS` idempotency contract for both spellings:
//! a DROP against a present collection must succeed and remove it; a
//! DROP IF EXISTS against an absent collection must succeed silently;
//! a DROP against an absent collection without `IF EXISTS` must error
//! with `42P01`. The redb-layer counterpart lives in
//! `collection_hard_delete.rs`; this file pins the pgwire surface.

mod common;

use common::pgwire_harness::TestServer;

/// `DROP COLLECTION IF EXISTS` on a present collection must succeed
/// and put the collection into a state where queries against it
/// error with `42P01`. Soft-delete (the default) preserves the row
/// for `UNDROP` and reports a retention-window message; `PURGE`
/// reports the bare `does not exist` message. Either is acceptable —
/// the spec is "DROP succeeded, queries no longer return rows".
#[tokio::test]
async fn drop_collection_if_exists_on_existing_collection_succeeds() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION drop_present_coll")
        .await
        .unwrap();

    srv.exec("DROP COLLECTION IF EXISTS drop_present_coll")
        .await
        .expect(
            "DROP COLLECTION IF EXISTS on an existing collection must succeed; \
             this is the documented idempotent path",
        );

    // 42P01 is the unifying SQLSTATE for "collection inaccessible" —
    // both the soft-deleted "within retention window" message and the
    // purged "does not exist" message use it. Asserting on the
    // SQLSTATE rather than a specific phrase avoids tying the test to
    // wording while still catching a "DROP silently no-ops" regression.
    srv.expect_error("SELECT 1 FROM drop_present_coll", "42P01")
        .await;
}

/// Same as above for the `DROP TABLE` spelling. The parser routes both
/// `DROP COLLECTION` and `DROP TABLE` to the same `DropCollection` AST
/// node, so this is a sibling-spelling regression guard, not a
/// duplicate.
#[tokio::test]
async fn drop_table_if_exists_on_existing_table_succeeds() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE TABLE drop_present_tbl (id TEXT, val INTEGER) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();

    srv.exec("DROP TABLE IF EXISTS drop_present_tbl")
        .await
        .expect(
            "DROP TABLE IF EXISTS on an existing table must succeed; the \
             `TABLE` spelling and `COLLECTION` spelling share one handler \
             and must behave identically",
        );

    srv.expect_error("SELECT 1 FROM drop_present_tbl", "42P01")
        .await;
}

/// `DROP COLLECTION IF EXISTS` on an absent name must succeed silently.
/// This is the documented `IF EXISTS` contract — the AST router has a
/// dedicated short-circuit at `ddl/router/ast/guards.rs` for exactly
/// this case.
#[tokio::test]
async fn drop_collection_if_exists_on_absent_collection_is_silent_success() {
    let srv = TestServer::start().await;

    srv.exec("DROP COLLECTION IF EXISTS never_created")
        .await
        .expect(
            "DROP COLLECTION IF EXISTS on a name that was never created must \
             succeed silently — this is the whole purpose of the IF EXISTS \
             modifier",
        );
}

/// Plain `DROP COLLECTION` (no `IF EXISTS`) against an absent name must
/// error with `42P01`. This is the negative pair to the IF EXISTS path
/// — proves we are not silently swallowing missing-collection errors on
/// the unmodified DROP.
#[tokio::test]
async fn drop_collection_without_if_exists_on_absent_collection_errors() {
    let srv = TestServer::start().await;

    srv.expect_error("DROP COLLECTION never_created_plain", "does not exist")
        .await;
}

/// DROP (soft-delete) then CREATE the same name must yield a FRESH,
/// EMPTY collection — the old rows must not resurrect. Soft-delete keeps
/// old rows under the same `{db}:{tenant}:{name}:` storage prefix for the
/// retention window; re-creating the name before GC runs must not expose
/// those stale rows. Re-CREATE is an explicit request for a new
/// collection (distinct from `UNDROP` recovery), so the old data must be
/// purged synchronously before the new collection is registered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_then_recreate_same_name_starts_empty() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION recycled").await.unwrap();
    srv.exec("INSERT INTO recycled (id, data) VALUES ('a', 'stale')")
        .await
        .unwrap();

    // Sanity: the row is there before the drop.
    let before = srv.query_text("SELECT data FROM recycled").await.unwrap();
    assert_eq!(
        before.len(),
        1,
        "row must exist before DROP, got {before:?}"
    );

    srv.exec("DROP COLLECTION recycled").await.unwrap();

    // Re-create the same name (before GC purges the soft-deleted data).
    srv.exec("CREATE COLLECTION recycled").await.unwrap();

    let after = srv.query_text("SELECT data FROM recycled").await.unwrap();
    assert_eq!(
        after.len(),
        0,
        "re-created collection must start empty; old rows resurrected: {after:?}"
    );
}
