// SPDX-License-Identifier: BUSL-1.1

//! A new incarnation of a collection starts empty on a node that still holds
//! an earlier incarnation's rows.
//!
//! Data Plane storage is keyed by collection name. A node whose reclaim of
//! the earlier incarnation never ran keeps its rows under the name. The test
//! applies a recreate the way a node applying another node's committed entry
//! does: catalog apply, the synchronous post-apply, then the post-apply
//! dispatch.

use std::sync::Arc;

use nodedb::control::catalog_entry::post_apply::{
    apply_post_apply_side_effects_sync, run_post_apply_async_side_effects,
};
use nodedb::control::catalog_entry::{CatalogEntry, apply};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, Hlc};

const TENANT: u64 = 1;
const COLLECTION: &str = "recreated_orders";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreate_from_another_node_starts_empty() {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION");
    for id in ["a", "b"] {
        server
            .exec(&format!(
                "INSERT INTO {COLLECTION} (id, val) VALUES ('{id}', 1)"
            ))
            .await
            .expect("insert into incarnation 1");
    }
    let before = server
        .query_rows(&format!("SELECT id FROM {COLLECTION}"))
        .await
        .expect("read incarnation 1");
    assert_eq!(before.len(), 2, "incarnation 1 holds its rows: {before:?}");

    // Another node dropped and recreated the name. This node never ran the
    // reclaim, so incarnation 1's rows are still under the name here.
    let first = server
        .shared
        .credentials
        .catalog()
        .get_collection(DatabaseId::DEFAULT, TENANT, COLLECTION)
        .expect("catalog read")
        .expect("incarnation 1 row");
    let mut second = first.clone();
    second.descriptor_version = 1;
    second.modification_hlc = Hlc::new(first.modification_hlc.wall_ns + 1_000_000_000, 0);
    // A create names its incarnation with its own clock.
    second.incarnation = second.modification_hlc;
    let recreate = CatalogEntry::PutCollection(Box::new(second));
    apply::apply_to(&recreate, server.shared.credentials.catalog()).expect("apply recreate");
    apply_post_apply_side_effects_sync(&recreate, &server.shared);
    run_post_apply_async_side_effects(recreate.clone(), Arc::clone(&server.shared))
        .await
        .expect("the recreate clears the name's storage");

    let after = server
        .query_rows(&format!("SELECT id FROM {COLLECTION}"))
        .await
        .expect("read incarnation 2");
    assert!(
        after.is_empty(),
        "incarnation 2 must not read incarnation 1's rows: {after:?}"
    );

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (id, val) VALUES ('c', 2)"
        ))
        .await
        .expect("incarnation 2 accepts writes");
    let written = server
        .query_rows(&format!("SELECT id FROM {COLLECTION}"))
        .await
        .expect("read incarnation 2 after a write");
    assert_eq!(written, vec![vec!["c".to_string()]]);
}

/// A one-node cluster's CREATE clears the name's storage through its
/// applier's post-apply. The node lost incarnation 1's catalog row without
/// its reclaim, so the rows are still under the name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_one_node_create_starts_empty() {
    const NAME: &str = "local_recreated_orders";
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION {NAME}"))
        .await
        .expect("CREATE COLLECTION");
    server
        .exec(&format!("INSERT INTO {NAME} (id, val) VALUES ('a', 1)"))
        .await
        .expect("insert into incarnation 1");

    apply::collection::finalize_purge(
        DatabaseId::DEFAULT.as_u64(),
        TENANT,
        NAME,
        server.shared.credentials.catalog(),
    )
    .expect("drop incarnation 1's catalog rows only");

    server
        .exec(&format!("CREATE COLLECTION {NAME}"))
        .await
        .expect("CREATE incarnation 2");
    let rows = server
        .query_rows(&format!("SELECT id FROM {NAME}"))
        .await
        .expect("read incarnation 2");
    assert!(
        rows.is_empty(),
        "incarnation 2 must not read incarnation 1's rows: {rows:?}"
    );
}
