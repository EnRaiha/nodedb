// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal twin of `sql_drop_collection::drop_then_recreate_same_name_starts_empty`.
//!
//! DROP then CREATE the same name for a `WITH (bitemporal=true)` collection
//! must yield a FRESH, EMPTY collection. A bitemporal collection keeps its
//! system-time history in the versioned document/index tables
//! (`documents_versioned` / `indexes_versioned`), keyed under the same
//! `{db}:{tenant}:{name}:` prefix that a re-CREATE reuses. If the collection
//! purge clears only the current (non-versioned) tables, the old versioned
//! history stays addressable and resurrects on re-CREATE — surfacing as
//! non-zero row counts, resurrected audit-log versions, and/or a corrupt
//! current row. The purge must range-delete the versioned tables too.
//!
//! On the pre-fix tree these assertions FAIL: the re-created collection
//! reports the dropped collection's rows and all of its `AS OF SYSTEM TIME
//! NULL` history.

mod common;
use common::pgwire_harness::TestServer;

/// Strict bitemporal collection: DROP + re-CREATE must start empty across
/// the current-read, the audit-log (all-versions) read, and COUNT(*).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_recreate_strict_bitemporal_starts_empty() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION recycled (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .expect("create strict bitemporal collection");

    srv.exec("INSERT INTO recycled (id, v) VALUES ('a', 'v1')")
        .await
        .expect("insert a");
    srv.exec("INSERT INTO recycled (id, v) VALUES ('b', 'v1')")
        .await
        .expect("insert b");
    // Second system-time version of 'a' — the versioned store now holds
    // three doc versions (a@1, a@2, b@1) under the collection prefix.
    srv.exec("UPDATE recycled SET v = 'v2' WHERE id = 'a'")
        .await
        .expect("update a to create a second system-time version");

    // Sanity: history exists before the drop.
    let audit_before = srv
        .query_rows("SELECT * FROM recycled AS OF SYSTEM TIME NULL")
        .await
        .expect("audit query before drop");
    assert_eq!(
        audit_before.len(),
        3,
        "expected 3 system-time versions before drop, got: {audit_before:?}"
    );

    srv.exec("DROP COLLECTION recycled")
        .await
        .expect("drop bitemporal collection");

    // Re-create the same name (before GC purges the soft-deleted data).
    srv.exec(
        "CREATE COLLECTION recycled (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .expect("re-create strict bitemporal collection");

    assert_recreated_collection_is_empty(&srv, "recycled").await;
}

/// Schemaless bitemporal twin — same resurrection surface, different engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_recreate_schemaless_bitemporal_starts_empty() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION recycled_sl (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await
    .expect("create schemaless bitemporal collection");

    srv.exec("INSERT INTO recycled_sl (id, v) VALUES ('a', 'v1')")
        .await
        .expect("insert a");
    srv.exec("UPDATE recycled_sl SET v = 'v2' WHERE id = 'a'")
        .await
        .expect("update a to create a second system-time version");

    let audit_before = srv
        .query_rows("SELECT * FROM recycled_sl AS OF SYSTEM TIME NULL")
        .await
        .expect("audit query before drop");
    assert_eq!(
        audit_before.len(),
        2,
        "expected 2 system-time versions before drop, got: {audit_before:?}"
    );

    srv.exec("DROP COLLECTION recycled_sl")
        .await
        .expect("drop bitemporal collection");

    srv.exec(
        "CREATE COLLECTION recycled_sl (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await
    .expect("re-create schemaless bitemporal collection");

    assert_recreated_collection_is_empty(&srv, "recycled_sl").await;
}

/// A freshly re-created bitemporal collection must expose NO resurrected
/// state: zero current rows and zero audit-log versions. Both reads go through
/// the versioned store the old purge failed to clear, so before the fix each
/// returns the resurrected history (or a corrupt current row).
async fn assert_recreated_collection_is_empty(srv: &TestServer, name: &str) {
    let current = srv
        .query_rows(&format!("SELECT * FROM {name}"))
        .await
        .expect("current-read on re-created collection");
    assert_eq!(
        current.len(),
        0,
        "re-created bitemporal collection must have no current rows; \
         old versioned rows resurrected: {current:?}"
    );

    let audit = srv
        .query_rows(&format!("SELECT * FROM {name} AS OF SYSTEM TIME NULL"))
        .await
        .expect("audit query on re-created collection");
    assert_eq!(
        audit.len(),
        0,
        "re-created bitemporal collection must have no historical versions; \
         dropped history resurrected in audit log: {audit:?}"
    );
}
