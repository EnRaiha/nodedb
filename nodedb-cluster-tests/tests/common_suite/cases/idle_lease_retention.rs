// SPDX-License-Identifier: BUSL-1.1
//! An idle descriptor lease stays granted, and a drain still ends promptly.
//!
//! A statement's lease stays granted after its last holder finishes, so
//! sequential writes to one collection acquire it through Raft once: the
//! lease's grant, and so its expiry, does not change across the writes. A
//! drain start releases the idle lease on the node that holds it, so the
//! drain returns well inside its budget instead of waiting for the expiry.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_cluster::{DescriptorId, DescriptorKind};

use crate::common;
use common::cluster_harness::TestCluster;

const TENANT: u64 = 1;
const COLLECTION: &str = "idle_lease_docs";

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn sequential_writes_reuse_one_lease_and_a_drain_releases_it() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} (id TEXT PRIMARY KEY, body TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION");
    let node = &cluster.nodes[0];

    let own_lease = || {
        node.leases_for_descriptor(DescriptorKind::Collection, TENANT, COLLECTION)
            .into_iter()
            .find(|lease| lease.node_id == node.node_id)
    };

    let mut granted = None;
    for i in 0..5 {
        node.client
            .simple_query(&format!(
                "INSERT INTO {COLLECTION} (id, body) VALUES ('k{i}', 'v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT k{i}: {e}"));
        let lease = own_lease().expect("the statement's lease stays granted");
        match granted {
            None => granted = Some(lease.expires_at),
            Some(first) => assert_eq!(
                lease.expires_at, first,
                "write {i} re-acquired the lease instead of reusing the idle grant"
            ),
        }
    }
    assert_eq!(
        node.shared.lease_refcount.current(&DescriptorId::new(
            0,
            TENANT,
            DescriptorKind::Collection,
            COLLECTION.to_string(),
        )),
        0,
        "no statement holds the lease between writes"
    );

    // A drain on the collection releases the idle lease at its start.
    let version = node
        .shared
        .credentials
        .catalog()
        .get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, COLLECTION)
        .expect("catalog read")
        .expect("collection exists")
        .descriptor_version
        .max(1);
    let shared = Arc::clone(&node.shared);
    let id = DescriptorId::new(
        0,
        TENANT,
        DescriptorKind::Collection,
        COLLECTION.to_string(),
    );
    let drained = {
        let started = Instant::now();
        let result = nodedb::control::lease::drain_for_ddl_async(
            &shared,
            id.clone(),
            version,
            Duration::from_secs(5),
            0,
        )
        .await;
        let elapsed = started.elapsed();
        nodedb::control::lease::end_drain_async(&shared, id, nodedb_cluster::DrainOwner::Ddl)
            .await
            .expect("end the drain");
        (result, elapsed)
    };
    assert!(
        drained.0.is_ok(),
        "the drain must pass the idle lease: {:?}",
        drained.0
    );
    assert!(
        drained.1 < Duration::from_secs(5),
        "the drain waited {:?} for an idle lease",
        drained.1
    );
    assert!(
        own_lease().is_none(),
        "the drain start released the idle lease"
    );

    cluster.shutdown().await;
}
