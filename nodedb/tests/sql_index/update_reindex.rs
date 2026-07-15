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
