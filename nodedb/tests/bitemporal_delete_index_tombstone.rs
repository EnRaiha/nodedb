// SPDX-License-Identifier: BUSL-1.1

//! Regression: DELETE on a bitemporal collection with a secondary index must
//! tombstone the index entry, not just the row.
//!
//! `apply_point_delete`'s bitemporal branch decodes the STORED bytes
//! (`versioned_get_current`) to walk `config.index_paths` and write a
//! `versioned_index_tombstone_in_txn` per current indexed value. For a
//! `document_strict` collection those stored bytes are a Binary Tuple, not
//! MessagePack — decoding them with the schemaless-only
//! `doc_format::decode_document` fails, so the tombstone
//! loop never runs and the old secondary-index entry survives the delete: a
//! deleted strict+bitemporal document stays findable by its old indexed
//! value. The fix routes the decode through `decode_stored_document`, which
//! dispatches on storage mode.

mod common;

use common::pgwire_harness::TestServer;

/// STRICT + bitemporal: deleting a document must hide it from a lookup on
/// its old indexed value. Before the fix, the stale secondary-index entry
/// survived the delete and the WHERE lookup below still returned the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bitemporal_delete_tombstones_secondary_index() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION bt_strict_idx (id STRING PRIMARY KEY, region STRING) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .unwrap();
    srv.exec("CREATE INDEX ON bt_strict_idx (region)")
        .await
        .unwrap();

    srv.exec("INSERT INTO bt_strict_idx (id, region) VALUES ('s1', 'us')")
        .await
        .unwrap();

    // Sanity: the secondary index finds the row by its indexed value before
    // delete.
    let before = srv
        .query_rows("SELECT id FROM bt_strict_idx WHERE region = 'us'")
        .await
        .unwrap();
    assert_eq!(
        before.len(),
        1,
        "secondary index must find the row pre-delete, got {before:?}"
    );

    srv.exec("DELETE FROM bt_strict_idx WHERE id = 's1'")
        .await
        .unwrap();

    // The deleted document must no longer be reachable through its old
    // indexed value — the tombstone must hide the stale index entry.
    let after = srv
        .query_rows("SELECT id FROM bt_strict_idx WHERE region = 'us'")
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "deleted strict+bitemporal document must not be findable via its \
         old secondary-index value, got {after:?}"
    );

    // The row itself must also be gone from a plain lookup by id.
    let by_id = srv
        .query_rows("SELECT id FROM bt_strict_idx WHERE id = 's1'")
        .await
        .unwrap();
    assert!(
        by_id.is_empty(),
        "deleted row must not resolve by id either"
    );
}

/// SCHEMALESS + bitemporal counterpart: locks in no-regression on the path
/// that was already correct (schemaless stored bytes decode fine through
/// the schemaless-only decoder, so `decode_stored_document`'s Schemaless arm
/// must remain byte-identical to `doc_format::decode_document`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_bitemporal_delete_tombstones_secondary_index() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION bt_schemaless_idx (id STRING PRIMARY KEY, region STRING) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await
    .unwrap();
    srv.exec("CREATE INDEX ON bt_schemaless_idx (region)")
        .await
        .unwrap();

    srv.exec("INSERT INTO bt_schemaless_idx (id, region) VALUES ('s1', 'us')")
        .await
        .unwrap();

    let before = srv
        .query_rows("SELECT id FROM bt_schemaless_idx WHERE region = 'us'")
        .await
        .unwrap();
    assert_eq!(before.len(), 1, "index must find the row pre-delete");

    srv.exec("DELETE FROM bt_schemaless_idx WHERE id = 's1'")
        .await
        .unwrap();

    let after = srv
        .query_rows("SELECT id FROM bt_schemaless_idx WHERE region = 'us'")
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "deleted schemaless+bitemporal document must not be findable via its \
         old secondary-index value, got {after:?}"
    );
}

/// STRICT + bitemporal UPDATE: changing an indexed value must supersede the
/// old one. A lookup on the pre-update value must return 0 rows (the stale
/// versioned-index entry is tombstoned / superseded) and a lookup on the new
/// value must return the row. This locks in current-version resolution on the
/// read path, not just delete-tombstone behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bitemporal_update_supersedes_secondary_index() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION bt_strict_upd (id STRING PRIMARY KEY, region STRING) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .unwrap();
    srv.exec("CREATE INDEX ON bt_strict_upd (region)")
        .await
        .unwrap();

    srv.exec("INSERT INTO bt_strict_upd (id, region) VALUES ('s1', 'us')")
        .await
        .unwrap();
    srv.exec("UPDATE bt_strict_upd SET region = 'eu' WHERE id = 's1'")
        .await
        .unwrap();

    let old_val = srv
        .query_rows("SELECT id FROM bt_strict_upd WHERE region = 'us'")
        .await
        .unwrap();
    assert!(
        old_val.is_empty(),
        "updated strict+bitemporal row must not be findable via its old \
         indexed value, got {old_val:?}"
    );

    let new_val = srv
        .query_rows("SELECT id FROM bt_strict_upd WHERE region = 'eu'")
        .await
        .unwrap();
    assert_eq!(
        new_val.len(),
        1,
        "updated strict+bitemporal row must be findable via its new indexed \
         value, got {new_val:?}"
    );
}

/// SCHEMALESS + bitemporal UPDATE counterpart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_bitemporal_update_supersedes_secondary_index() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION bt_schemaless_upd (id STRING PRIMARY KEY, region STRING) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await
    .unwrap();
    srv.exec("CREATE INDEX ON bt_schemaless_upd (region)")
        .await
        .unwrap();

    srv.exec("INSERT INTO bt_schemaless_upd (id, region) VALUES ('s1', 'us')")
        .await
        .unwrap();
    srv.exec("UPDATE bt_schemaless_upd SET region = 'eu' WHERE id = 's1'")
        .await
        .unwrap();

    let old_val = srv
        .query_rows("SELECT id FROM bt_schemaless_upd WHERE region = 'us'")
        .await
        .unwrap();
    assert!(
        old_val.is_empty(),
        "updated schemaless+bitemporal row must not be findable via its old \
         indexed value, got {old_val:?}"
    );

    let new_val = srv
        .query_rows("SELECT id FROM bt_schemaless_upd WHERE region = 'eu'")
        .await
        .unwrap();
    assert_eq!(
        new_val.len(),
        1,
        "updated schemaless+bitemporal row must be findable via its new \
         indexed value, got {new_val:?}"
    );
}
