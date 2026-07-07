// SPDX-License-Identifier: BUSL-1.1
//! Cluster smoke test: `partition_strategy` replicates through the
//! metadata Raft `PutCollection` path.
//!
//! Creates a `document_strict` collection on the leader, waits for
//! every follower to converge, then asserts that every node's
//! `StoredCollection` has `partition_strategy == CollectionHomed`.
//! This proves the new field survives the msgpack round-trip through
//! the metadata Raft log and is readable on followers.

mod common;

use std::time::Duration;

use nodedb_types::{DatabaseId, PartitionStrategy};

use common::cluster_harness::{TestCluster, TestClusterNode};

fn get_partition_strategy(node: &TestClusterNode, name: &str) -> Option<PartitionStrategy> {
    let cat = node.shared.credentials.catalog();
    let coll = cat
        .get_collection(DatabaseId::DEFAULT, 1, name)
        .ok()
        .flatten()?;
    if !coll.is_active {
        return None;
    }
    Some(coll.partition_strategy.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_strategy_replicates_to_all_nodes() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cluster_partition_strategy_smoke \
             (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    // Poll every node until each one sees the collection with the expected
    // partition_strategy. 5s is generous — typical convergence is <200ms.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);

    for node in &cluster.nodes {
        let mut converged = false;
        while std::time::Instant::now() < deadline {
            if let Some(strategy) = get_partition_strategy(node, "cluster_partition_strategy_smoke")
            {
                assert_eq!(
                    strategy,
                    PartitionStrategy::CollectionHomed,
                    "node {} has unexpected partition_strategy {:?}",
                    node.node_id,
                    strategy
                );
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            converged,
            "node {} did not converge: collection \
             'cluster_partition_strategy_smoke' not found within deadline",
            node.node_id
        );
    }
}
