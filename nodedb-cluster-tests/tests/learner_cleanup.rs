// SPDX-License-Identifier: BUSL-1.1
//! Steady-state learner cleanup: a 4th node joining an RF=3 cluster must not
//! linger as a learner in groups whose placement excludes it.
//!
//! ## What this guards
//!
//! When N > RF, the joining node is admitted as a learner to ALL existing data
//! groups (so it can catch up and form correctly). Once placement is authored
//! by the metadata-group leader, each data group's intended voter set has
//! exactly `min(RF, N) == 3` members. Groups that exclude node 4 from
//! placement must remove it as a learner; groups that include node 4 promote it
//! to a voter. In either case, no out-of-placement learner must remain once
//! steady state is reached.
//!
//! Assertion: for every data group, `learners ⊆ placement` — no learner
//! lingers in a group whose placement excludes it.

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

const COLL: &str = "learner_cleanup";
const RF: usize = 3;

/// Sorted learner list for `group_id` as seen by `node`'s routing table.
fn learners_seen_by(node: &common::cluster_harness::TestClusterNode, group_id: u64) -> Vec<u64> {
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let mut l = routing
        .group_info(group_id)
        .map(|i| i.learners.clone())
        .unwrap_or_default();
    l.sort_unstable();
    l
}

/// Placement for `group_id` from `node`'s routing table, sorted ascending.
fn placement_for(
    node: &common::cluster_harness::TestClusterNode,
    group_id: u64,
) -> Option<Vec<u64>> {
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    routing.group_info(group_id).and_then(|i| {
        i.placement.clone().map(|mut p| {
            p.sort_unstable();
            p
        })
    })
}

/// Data group ids (excludes metadata and sequencer groups). Read from node 0.
fn data_group_ids(cluster: &TestCluster) -> Vec<u64> {
    let routing = cluster.nodes[0]
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let mut gids: Vec<u64> = routing
        .group_ids()
        .into_iter()
        .filter(|g| {
            *g != nodedb_cluster::METADATA_GROUP_ID
                && *g != nodedb_cluster::calvin::SEQUENCER_GROUP_ID
        })
        .collect();
    gids.sort_unstable();
    gids
}

/// True when every data group on every node has no out-of-placement learners
/// AND every group's placement has been authored.
fn all_groups_clean(cluster: &TestCluster, gids: &[u64]) -> bool {
    for &gid in gids {
        for node in &cluster.nodes {
            let Some(placement) = placement_for(node, gid) else {
                return false; // placement not yet authored
            };
            let learners = learners_seen_by(node, gid);
            // Every remaining learner must be in placement (i.e., mid-promotion).
            if learners.iter().any(|l| !placement.contains(l)) {
                return false;
            }
        }
    }
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_out_of_placement_learner_after_fourth_node_joins() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION");

    // Add a 4th node. With RF=3 and N=4 the joining node is admitted as a
    // learner to all groups but placed in only ~N_groups * RF/N of them.
    let new_id = cluster
        .add_learner_node()
        .await
        .expect("add 4th node as learner")
        .node_id;
    assert_eq!(new_id, 4, "4th node must be id 4");

    let gids = data_group_ids(&cluster);
    assert!(!gids.is_empty(), "at least one data group must exist");

    // Allow generous convergence time: placement reconcile is throttled ~1s,
    // and learner cleanup runs after it, so several seconds are needed.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if all_groups_clean(&cluster, &gids) {
            break;
        }
        if Instant::now() >= deadline {
            break; // fall through to per-group assertions for a diagnosable failure
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Per-group assertions: no out-of-placement learner on any node.
    for &gid in &gids {
        for node in &cluster.nodes {
            let placement = placement_for(node, gid).unwrap_or_else(|| {
                panic!(
                    "data group {gid} on node {}: placement not authored within deadline",
                    node.node_id
                )
            });
            let learners = learners_seen_by(node, gid);
            let out_of_placement: Vec<u64> = learners
                .iter()
                .copied()
                .filter(|l| !placement.contains(l))
                .collect();
            assert!(
                out_of_placement.is_empty(),
                "data group {gid} on node {}: learners {learners:?} contain \
                 out-of-placement nodes {out_of_placement:?} (placement={placement:?}); \
                 non-placement learners must be removed by converge_leaving_learners",
                node.node_id
            );
        }
    }

    // Sanity: placement cardinality is correct (min(RF, N) = 3).
    let live = [1u64, 2, 3, 4];
    let expected_len = RF.min(live.len()); // 3
    for &gid in &gids {
        let placement = placement_for(&cluster.nodes[0], gid)
            .unwrap_or_else(|| panic!("data group {gid}: placement must be Some at this point"));
        assert_eq!(
            placement.len(),
            expected_len,
            "data group {gid}: placement {placement:?} must have {expected_len} members"
        );
        assert!(
            placement.iter().all(|n| live.contains(n)),
            "data group {gid}: placement {placement:?} must be drawn from live nodes {live:?}"
        );
    }

    cluster.shutdown().await;
}
