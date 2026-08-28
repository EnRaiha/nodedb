// SPDX-License-Identifier: BUSL-1.1
//! End-to-end tests for descriptor lease leader-forwarding and the
//! renewal loop.
//!
//! Two focused cases on a 3-node cluster:
//!
//! 1. **follower_acquire_forwards_to_leader** — every node calls
//!    `acquire_descriptor_lease`. The two follower nodes go through
//!    the new `MetadataProposeRequest` RPC path which forwards to
//!    the leader. All 3 leases land in `MetadataCache.leases` on
//!    every node. Without forwarding, the followers would panic
//!    with `not leader`.
//! 2. **lease_renews_before_expiry** — short lease (3 seconds)
//!    plus short renewal interval (250ms) and a 50% threshold
//!    (renew when < 1.5s remaining). Acquire on the leader, wait
//!    long enough for at least one renewal cycle, assert the
//!    lease's `expires_at` advanced (it was re-acquired with a
//!    fresh expiry).

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};
use nodedb_cluster::DescriptorKind;
use nodedb_types::config::tuning::ClusterTransportTuning;

const TENANT: u64 = 1;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn follower_acquire_forwards_to_leader() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let node_ids: Vec<u64> = cluster.nodes.iter().map(|n| n.node_id).collect();

    // Every node acquires its OWN lease on the same descriptor.
    // The leader takes the local propose path; the two followers
    // hit the new MetadataProposeRequest forwarding path.
    for node in &cluster.nodes {
        node.acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "shared",
            1,
            Duration::from_secs(60),
        )
        .await
        .expect("acquire (with leader forwarding)");
    }

    // Wait for every node to observe all 3 leases. Each lease has
    // a distinct `node_id` because acquire_lease tags it with
    // `shared.node_id`, so the cache holds exactly 3 records under
    // 3 distinct (descriptor_id, node_id) keys.
    wait_for(
        "every node observes all 3 forwarded leases",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || {
            cluster.nodes.iter().all(|n| {
                let leases = n.leases_for_descriptor(DescriptorKind::Collection, TENANT, "shared");
                leases.len() == 3
                    && node_ids
                        .iter()
                        .all(|id| leases.iter().any(|l| l.node_id == *id))
            })
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn lease_renews_before_expiry() {
    // Custom tuning: 3-second lease, check every 250ms, renew at
    // 50% remaining (< 1.5s left). Within a 2.5-second test wait
    // we should observe at least one renewal.
    let tuning = ClusterTransportTuning {
        descriptor_lease_duration_secs: 3,
        descriptor_lease_renewal_check_interval_secs: 1, // min granularity
        descriptor_lease_renewal_threshold_pct: 80,
        // Match spawn_three()'s election headroom — under parallel nextest
        // load the default 150/300 ms windows trigger spurious re-elections
        // before the metadata group converges within the 10 s wait.
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 1000,
        health_ping_interval_secs: 1,
        ..ClusterTransportTuning::default()
    };

    let cluster = TestCluster::spawn_three_with_tuning(tuning)
        .await
        .expect("3-node cluster");

    // Create the collection so the renewal loop's
    // `lookup_current_version` finds it in the local catalog.
    // Without this the renewal logic treats the lease as orphaned
    // and releases it before our 1.5 s observation window — see
    // `control::lease::renewal::lookup_current_version`.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION renewable (id BIGINT PRIMARY KEY, label TEXT)")
        .await
        .expect("create renewable collection");
    common::cluster_harness::wait_for(
        "renewable visible on every node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.collection_descriptor(TENANT, "renewable").is_some())
        },
    )
    .await;

    let leader = &cluster.nodes[0];

    // Acquire on the leader. Lease has ~3s expiry from now.
    let initial = leader
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "renewable",
            1,
            Duration::from_secs(3),
        )
        .await
        .expect("initial acquire");
    let initial_expiry = initial.expires_at;

    // Wait long enough for the renewal loop to wake AT LEAST once
    // (1s tick) AND for the 80% threshold to trigger (after 0.6s
    // elapsed the remaining time is below 80% of 3s = 2.4s).
    // 1.5 seconds is comfortably past both.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Read the lease back. The renewal loop should have re-acquired
    // it, producing a strictly greater `expires_at`.
    let leases = leader.leases_for_descriptor(DescriptorKind::Collection, TENANT, "renewable");
    let renewed = leases
        .iter()
        .find(|l| l.node_id == leader.node_id)
        .expect("lease still present after renewal window");
    assert!(
        renewed.expires_at > initial_expiry,
        "renewal should have advanced expires_at: initial={:?} renewed={:?}",
        initial_expiry,
        renewed.expires_at
    );

    cluster.shutdown().await;
}
