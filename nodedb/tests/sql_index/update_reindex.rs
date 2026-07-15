// SPDX-License-Identifier: BUSL-1.1

//! Autocommit bulk (predicate-form) `UPDATE` must keep the plain secondary
//! index consistent with the primary document store.
//!
//! `UPDATE c SET status='archived' WHERE status='active'` routes through
//! `execute_bulk_update`. Historically that path wrote the primary document via
//! the self-committing `SparseEngine::put` and never touched the secondary
//! B-tree, so the `status` index kept pointing rows at `'active'`. A later
//! `WHERE status='archived'` then missed the updated rows and a
//! `WHERE status='active'` wrongly returned them. The reindex must happen
//! atomically with the primary write.

use super::common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_update_reconciles_secondary_index() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_bulk_update")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX ON idx_bulk_update(status)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO idx_bulk_update { id: 'a', status: 'active' }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO idx_bulk_update { id: 'b', status: 'active' }")
        .await
        .unwrap();

    // Predicate-form UPDATE → execute_bulk_update.
    server
        .exec("UPDATE idx_bulk_update SET status = 'archived' WHERE status = 'active'")
        .await
        .unwrap();

    // The index must now find both rows under the NEW value.
    let mut archived = server
        .query_text("SELECT id FROM idx_bulk_update WHERE status = 'archived'")
        .await
        .expect("indexed SELECT on new value must succeed");
    archived.sort();
    assert_eq!(
        archived,
        vec!["a".to_string(), "b".to_string()],
        "index lookup on the new value must return both updated rows; got: {archived:?}"
    );

    // And no stale entry may survive under the OLD value — this is the
    // regression: the pre-fix path left the index pointing at 'active'.
    let stale = server
        .query_text("SELECT id FROM idx_bulk_update WHERE status = 'active'")
        .await
        .expect("indexed SELECT on old value must succeed");
    assert!(
        stale.is_empty(),
        "index lookup on the old value must return no rows after the UPDATE; \
         a stale secondary-index entry survived: {stale:?}"
    );

    // The primary document store must also reflect the new value.
    let primary = server
        .query_text("SELECT status FROM idx_bulk_update WHERE id = 'a'")
        .await
        .expect("primary read must succeed");
    assert_eq!(
        primary,
        vec!["archived".to_string()],
        "primary document for id 'a' must show the updated status; got: {primary:?}"
    );
}

/// Single-row PK-form `UPDATE` routes through `execute_point_update`. The
/// non-bitemporal branch historically wrote the primary document via the
/// self-committing `SparseEngine::put` and reconciled only the vector index —
/// never the plain secondary B-tree — so the `email` index kept pointing at the
/// old value. A later `WHERE email = <new>` then missed the row and a
/// `WHERE email = <old>` wrongly returned it. A single non-strict literal
/// assignment exercises the fast binary-merge path, the highest-risk case: it
/// never decodes the document itself, so the reindex must decode both images.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_update_reconciles_secondary_index() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_point_update")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX ON idx_point_update(email)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO idx_point_update { id: 'a', email: 'old@x.z' }")
        .await
        .unwrap();

    // PK-form UPDATE with a single literal assignment → execute_point_update,
    // non-bitemporal, fast binary-merge path.
    server
        .exec("UPDATE idx_point_update SET email = 'new@x.z' WHERE id = 'a'")
        .await
        .unwrap();

    // The index must find the row under the NEW value.
    let updated = server
        .query_text("SELECT id FROM idx_point_update WHERE email = 'new@x.z'")
        .await
        .expect("indexed SELECT on new value must succeed");
    assert_eq!(
        updated,
        vec!["a".to_string()],
        "index lookup on the new value must return the updated row; got: {updated:?}"
    );

    // And no stale entry may survive under the OLD value — this is the
    // regression: the pre-fix path left the index pointing at 'old@x.z'.
    let stale = server
        .query_text("SELECT id FROM idx_point_update WHERE email = 'old@x.z'")
        .await
        .expect("indexed SELECT on old value must succeed");
    assert!(
        stale.is_empty(),
        "index lookup on the old value must return no rows after the UPDATE; \
         a stale secondary-index entry survived: {stale:?}"
    );

    // The primary document store must also reflect the new value.
    let primary = server
        .query_text("SELECT email FROM idx_point_update WHERE id = 'a'")
        .await
        .expect("primary read must succeed");
    assert_eq!(
        primary,
        vec!["new@x.z".to_string()],
        "primary document for id 'a' must show the updated email; got: {primary:?}"
    );
}
