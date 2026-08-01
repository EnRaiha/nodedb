// SPDX-License-Identifier: BUSL-1.1
//! End-to-end cluster tests for the descriptor lease API.
//!
//! Asserts that `SharedState::acquire_descriptor_lease` and
//! `release_descriptor_leases` flow through the metadata raft group
//! and produce the same `MetadataCache.leases` view on every node.
//! Consolidated into 3 focused tests so the whole suite runs in
//! roughly the time it takes to spawn 3 three-node clusters
//! (~2 seconds end-to-end on a warm build).

mod common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};
use nodedb_cluster::{DescriptorId, DescriptorKind};

const TENANT: u64 = 1;
const LEASE_DURATION: Duration = Duration::from_secs(60);
const WAIT_BUDGET: Duration = Duration::from_secs(2);
const POLL: Duration = Duration::from_millis(20);

fn coll_id(name: &str) -> DescriptorId {
    DescriptorId::new(0, TENANT, DescriptorKind::Collection, name.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn grant_propagates_and_release_removes() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let proposer_id = cluster.nodes[0].node_id;

    cluster.nodes[0]
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "orders",
            1,
            LEASE_DURATION,
        )
        .await
        .expect("acquire");

    wait_for("all 3 nodes observe the lease", WAIT_BUDGET, POLL, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.has_lease(DescriptorKind::Collection, TENANT, "orders", proposer_id, 1))
    })
    .await;

    cluster.nodes[0]
        .release_leases(vec![coll_id("orders")])
        .await
        .expect("release");

    wait_for(
        "all 3 nodes drop the lease after release",
        WAIT_BUDGET,
        POLL,
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_lease(DescriptorKind::Collection, TENANT, "orders", proposer_id, 1))
        },
    )
    .await;

    cluster.shutdown().await;
}

// NOTE: a `leases_keyed_per_node_id` test that has every node call
// `acquire_lease` would fail with "not leader" on the followers,
// because non-DDL lease proposals do not yet have leader forwarding.
// The "key includes node_id" property is
// structurally guaranteed by `MetadataCache.leases:
// HashMap<(DescriptorId, u64), DescriptorLease>` in the cluster
// crate and doesn't need an integration test to assert.

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn fast_path_and_version_overwrite() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let node = &cluster.nodes[0];

    let first = node
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "items",
            1,
            LEASE_DURATION,
        )
        .await
        .expect("acquire v1");

    // Fast path: re-acquire at the same version — must return the
    // exact same lease (same expires_at) without producing a fresh
    // raft entry. We assert by comparing `expires_at`: a slow-path
    // acquire would compute a NEW `expires_at = now + duration`.
    let second = node
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "items",
            1,
            LEASE_DURATION,
        )
        .await
        .expect("acquire v1 again");
    assert_eq!(
        first.expires_at, second.expires_at,
        "fast path returned the cached lease"
    );

    // Slow path: bumping the version forces a fresh propose.
    node.acquire_lease(
        DescriptorKind::Collection,
        TENANT,
        "items",
        2,
        LEASE_DURATION,
    )
    .await
    .expect("acquire v2");

    wait_for("v2 overwrites v1 on every node", WAIT_BUDGET, POLL, || {
        cluster.nodes.iter().all(|n| {
            let leases = n.leases_for_descriptor(DescriptorKind::Collection, TENANT, "items");
            leases.len() == 1 && leases[0].version == 2
        })
    })
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn batched_release_removes_all_descriptors() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let proposer_id = cluster.nodes[0].node_id;
    let names = ["a", "b", "c"];

    for name in names {
        cluster.nodes[0]
            .acquire_lease(DescriptorKind::Collection, TENANT, name, 1, LEASE_DURATION)
            .await
            .expect("acquire");
    }

    wait_for(
        "all 3 leases visible on every node",
        WAIT_BUDGET,
        POLL,
        || {
            cluster.nodes.iter().all(|n| {
                names.iter().all(|name| {
                    n.has_lease(DescriptorKind::Collection, TENANT, name, proposer_id, 1)
                })
            })
        },
    )
    .await;

    cluster.nodes[0]
        .release_leases(names.iter().map(|n| coll_id(n)).collect())
        .await
        .expect("batched release");

    wait_for(
        "all 3 leases removed on every node",
        WAIT_BUDGET,
        POLL,
        || {
            cluster.nodes.iter().all(|n| {
                names.iter().all(|name| {
                    !n.has_lease(DescriptorKind::Collection, TENANT, name, proposer_id, 1)
                })
            })
        },
    )
    .await;

    cluster.shutdown().await;
}
