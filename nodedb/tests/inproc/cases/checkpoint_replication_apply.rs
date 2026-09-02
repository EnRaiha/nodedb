// SPDX-License-Identifier: BUSL-1.1

//! Version-history checkpoints are replicated catalog state, not node-local
//! version bookkeeping.
//!
//! A node that did not run the statement receives the mutation as a
//! `CatalogEntry` and must end up with the same durable row. These tests drive
//! that follower path directly — apply, then the synchronous post-apply — and
//! assert on `_system.checkpoints` through the catalog readers the version
//! history SQL uses.

use nodedb::control::catalog_entry::post_apply::apply_post_apply_side_effects_sync;
use nodedb::control::catalog_entry::{CatalogEntry, apply};
use nodedb::control::security::catalog::types::CheckpointRecord;
use nodedb_test_support::pgwire_harness::TestServer;

const TENANT: u64 = 1;
const DATABASE: u64 = 3;
const COLLECTION: &str = "documents";
const DOC: &str = "doc-1";
const NAME: &str = "launch-ready";

fn record(name: &str, created_at: u64) -> CheckpointRecord {
    CheckpointRecord {
        tenant_id: TENANT,
        collection: COLLECTION.to_string(),
        doc_id: DOC.to_string(),
        checkpoint_name: name.to_string(),
        version_vector_json: "{\"n1\":4}".to_string(),
        created_by: "admin".to_string(),
        created_at,
    }
}

/// Apply an entry the way the metadata applier does: durable write first,
/// then the synchronous in-memory install.
fn apply_entry(server: &TestServer, entry: &CatalogEntry) {
    apply::apply_to(entry, server.shared.credentials.catalog()).expect("apply catalog entry");
    apply_post_apply_side_effects_sync(entry, &server.shared);
}

/// The names of every durable checkpoint row on `DOC`.
fn stored_names(server: &TestServer) -> Vec<String> {
    let mut names: Vec<String> = server
        .shared
        .credentials
        .catalog()
        .list_checkpoints(TENANT, COLLECTION, DOC, 0)
        .expect("list checkpoints")
        .into_iter()
        .map(|r| r.checkpoint_name)
        .collect();
    names.sort();
    names
}

/// A replicated `PutCheckpoint` makes the checkpoint durable on a node that
/// never parsed the statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_put_installs_the_checkpoint() {
    let server = TestServer::start().await;
    assert!(
        stored_names(&server).is_empty(),
        "no checkpoint is stored before the entry applies"
    );

    apply_entry(
        &server,
        &CatalogEntry::PutCheckpoint(Box::new(record(NAME, 1_000))),
    );

    let stored = server
        .shared
        .credentials
        .catalog()
        .get_checkpoint(TENANT, COLLECTION, DOC, NAME)
        .expect("read the checkpoint back")
        .expect("apply must write the durable row on this node");
    assert_eq!(stored.version_vector_json, "{\"n1\":4}");
    assert_eq!(stored.created_by, "admin");
    assert_eq!(stored.created_at, 1_000);
}

/// A replicated `DeleteCheckpoint` drops the row on every node, and a
/// re-delivery of it is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_delete_removes_the_checkpoint_row() {
    let server = TestServer::start().await;
    apply_entry(
        &server,
        &CatalogEntry::PutCheckpoint(Box::new(record(NAME, 1_000))),
    );
    let delete = CatalogEntry::DeleteCheckpoint {
        tenant_id: TENANT,
        collection: COLLECTION.to_string(),
        doc_id: DOC.to_string(),
        checkpoint_name: NAME.to_string(),
    };

    apply_entry(&server, &delete);
    apply_entry(&server, &delete);

    assert!(stored_names(&server).is_empty());
}

/// A replicated `CompactHistory` applies the same exclusive boundary
/// on every node, whatever order that node's catalog scan returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_range_delete_keeps_the_boundary_checkpoint() {
    let server = TestServer::start().await;
    for (name, created_at) in [("older", 99), ("boundary", 100), ("newer", 101)] {
        apply_entry(
            &server,
            &CatalogEntry::PutCheckpoint(Box::new(record(name, created_at))),
        );
    }

    apply_entry(
        &server,
        &CatalogEntry::CompactHistory {
            tenant_id: TENANT,
            database_id: DATABASE,
            collection: COLLECTION.to_string(),
            doc_id: DOC.to_string(),
            before_timestamp: 100,
            target_version_json: "{\"n1\":4}".to_string(),
        },
    );

    assert_eq!(
        stored_names(&server),
        vec!["boundary".to_string(), "newer".to_string()],
        "the range delete boundary is exclusive on the follower too"
    );
}
