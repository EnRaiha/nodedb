// SPDX-License-Identifier: BUSL-1.1
//! End-to-end cluster tests for the create-only
//! `CatalogEntry::PutCollectionIfAbsent` primitive.
//!
//! `PutCollectionIfAbsent` materializes a collection through the
//! metadata raft group ONLY when no collection of the same
//! `(database_id, tenant_id, name)` already exists — it never
//! clobbers an existing schema. This is the durable primitive CRDT
//! sync will use to announce collections without racing a
//! locally-authored definition.
//!
//! No SQL DDL emits this variant yet, so the tests propose the entry
//! directly through `metadata_proposer::propose_catalog_entry` (which
//! forwards to the metadata-group leader) and assert idempotency +
//! no-clobber by reading the replicated record on every node.

mod common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry;
use nodedb::control::security::catalog::StoredCollection;
use nodedb_types::DatabaseId;

const TENANT: u64 = 1;
const COLL: &str = "if_absent_coll";

/// Read the distinguishing fields `(bitemporal, declared_primary_key)`
/// of a collection from a node's local `SystemCatalog` redb — the same
/// record every node's applier writes.
fn coll_fields(
    node: &common::cluster_harness::TestClusterNode,
    name: &str,
) -> Option<(bool, Option<String>)> {
    node.shared
        .credentials
        .catalog()
        .get_collection(DatabaseId::DEFAULT, TENANT, name)
        .ok()
        .flatten()
        .map(|c| (c.bitemporal, c.declared_primary_key))
}

/// Propose a `PutCollectionIfAbsent` for `coll`, trying each node
/// until one accepts (the proposer forwards to the metadata leader,
/// so any node works — the loop mirrors `exec_ddl_on_any_leader`).
fn propose_if_absent(cluster: &TestCluster, coll: StoredCollection) -> Result<(), String> {
    let entry = CatalogEntry::PutCollectionIfAbsent(Box::new(coll));
    let mut last_err = String::new();
    for node in &cluster.nodes {
        match propose_catalog_entry(&node.shared, &entry) {
            Ok(_) => return Ok(()),
            Err(e) => last_err = e.to_string(),
        }
    }
    Err(format!(
        "no node accepted PutCollectionIfAbsent: {last_err}"
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn put_if_absent_creates_then_no_clobbers_then_idempotent() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    // A: the winning definition — bitemporal + a distinct PRIMARY KEY.
    let mut a = StoredCollection::new(TENANT, COLL, "tester");
    a.bitemporal = true;
    a.declared_primary_key = Some("a_key".to_string());

    // 1. Create via PutCollectionIfAbsent (collection is absent).
    propose_if_absent(&cluster, a.clone()).expect("propose A");

    // Assert A materialized on all three nodes with A's fields.
    wait_for(
        "all 3 nodes see collection with A's distinguishing fields",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| coll_fields(n, COLL) == Some((true, Some("a_key".to_string()))))
        },
    )
    .await;

    // 2. Propose a DIFFERENT definition B under the same tenant+name.
    //    Because the collection already exists, this must be a no-op.
    let mut b = StoredCollection::new(TENANT, COLL, "tester");
    b.bitemporal = false;
    b.declared_primary_key = Some("b_key".to_string());
    propose_if_absent(&cluster, b).expect("propose B");

    // Wait for B's proposal to have applied cluster-wide, then assert
    // every node STILL shows A's fields — B was skipped, no clobber.
    wait_for(
        "no-clobber: every node still shows A after B proposal",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| coll_fields(n, COLL) == Some((true, Some("a_key".to_string()))))
        },
    )
    .await;
    for node in &cluster.nodes {
        assert_eq!(
            coll_fields(node, COLL),
            Some((true, Some("a_key".to_string()))),
            "B must not clobber A on node {}",
            node.node_id
        );
    }

    // 3. Re-propose A verbatim — idempotent no-op. Still exactly one
    //    collection, unchanged.
    propose_if_absent(&cluster, a).expect("re-propose A");
    wait_for(
        "idempotent: still exactly one collection with A's fields",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|n| {
                n.cached_collection_count() == 1
                    && coll_fields(n, COLL) == Some((true, Some("a_key".to_string())))
            })
        },
    )
    .await;
    for node in &cluster.nodes {
        assert_eq!(
            node.cached_collection_count(),
            1,
            "exactly one collection on node {}",
            node.node_id
        );
        assert_eq!(
            coll_fields(node, COLL),
            Some((true, Some("a_key".to_string()))),
            "A unchanged on node {}",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
