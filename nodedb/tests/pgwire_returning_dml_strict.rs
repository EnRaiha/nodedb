// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` over `WITH (engine='document_strict')` collections.
//!
//! A strict row is stored as a Binary Tuple, not MessagePack, and is filed
//! under a surrogate hex storage key rather than its declared primary key.
//! Both facts are invisible to a passing test on a schemaless collection, so
//! the strict cases live here alongside the schemaless coverage in
//! `pgwire_returning_dml.rs`:
//!
//! - The MessagePack decoder does not reject a Binary Tuple. It reads the
//!   tuple's leading byte as a scalar and succeeds, so a storage-mode-blind
//!   `RETURNING` decode ships a plausible-looking row with every real column
//!   missing instead of failing.
//! - The declared primary key must survive into the returned `id`. Replacing it
//!   with the surrogate storage key hands the client a value it never wrote and
//!   cannot address the row with.

mod common;

use common::pgwire_harness::TestServer;

/// A strict collection with a declared `id` primary key holding two rows, so
/// one statement can exercise the point path (`WHERE id = ...`) and the bulk
/// path (a predicate over a non-key column) on the same shape.
async fn seed_strict(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (\
                 id TEXT PRIMARY KEY, name TEXT, score INT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (id, name, score) in [("s1", "sigma", 78), ("s2", "tau", 88)] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, name, score) VALUES ('{id}', '{name}', {score})"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}/{id}: {e}"));
    }
}

/// Bulk `UPDATE ... RETURNING id` must give back the declared primary keys, not
/// the surrogate hex storage keys the rows are filed under.
#[tokio::test]
async fn strict_bulk_update_returning_id_is_the_declared_primary_key() {
    let server = TestServer::start().await;
    seed_strict(&server, "ret_strict_bulk_upd").await;

    let mut rows = server
        .query_rows("UPDATE ret_strict_bulk_upd SET score = 5 RETURNING id, score")
        .await
        .expect("strict bulk UPDATE RETURNING should succeed");
    rows.sort();

    assert_eq!(
        rows,
        vec![
            vec!["s1".to_string(), "5".to_string()],
            vec!["s2".to_string(), "5".to_string()],
        ],
        "each row must return its own declared key: {rows:?}"
    );
}

/// Same rule on the point path.
#[tokio::test]
async fn strict_point_update_returning_id_is_the_declared_primary_key() {
    let server = TestServer::start().await;
    seed_strict(&server, "ret_strict_point_upd").await;

    let rows = server
        .query_rows("UPDATE ret_strict_point_upd SET score = 7 WHERE id = 's1' RETURNING id, score")
        .await
        .expect("strict point UPDATE RETURNING should succeed");

    assert_eq!(
        rows,
        vec![vec!["s1".to_string(), "7".to_string()]],
        "the declared key must survive into the returned row: {rows:?}"
    );
}

/// A point `DELETE ... RETURNING` must project the row's real pre-image
/// columns. Decoding the Binary Tuple pre-image as MessagePack succeeds and
/// yields a row with none of them, which is why every column is asserted.
#[tokio::test]
async fn strict_point_delete_returning_gives_the_real_pre_image_columns() {
    let server = TestServer::start().await;
    seed_strict(&server, "ret_strict_point_del").await;

    let rows = server
        .query_rows("DELETE FROM ret_strict_point_del WHERE id = 's1' RETURNING id, name, score")
        .await
        .expect("strict point DELETE RETURNING should succeed");

    assert_eq!(
        rows,
        vec![vec![
            "s1".to_string(),
            "sigma".to_string(),
            "78".to_string(),
        ]],
        "the pre-image must carry the row's stored column values: {rows:?}"
    );
}

/// Same rule on the bulk delete path, whose pre-image comes from a separate
/// read of the stored row.
#[tokio::test]
async fn strict_bulk_delete_returning_gives_the_real_pre_image_columns() {
    let server = TestServer::start().await;
    seed_strict(&server, "ret_strict_bulk_del").await;

    let mut rows = server
        .query_rows("DELETE FROM ret_strict_bulk_del WHERE score > 0 RETURNING id, name, score")
        .await
        .expect("strict bulk DELETE RETURNING should succeed");
    rows.sort();

    assert_eq!(
        rows,
        vec![
            vec!["s1".to_string(), "sigma".to_string(), "78".to_string()],
            vec!["s2".to_string(), "tau".to_string(), "88".to_string()],
        ],
        "every removed row's stored columns must be projected: {rows:?}"
    );

    let remaining = server
        .query_rows("SELECT id FROM ret_strict_bulk_del")
        .await
        .expect("read back after delete");
    assert!(
        remaining.is_empty(),
        "the delete must still remove every matched row: {remaining:?}"
    );
}
