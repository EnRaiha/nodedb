// SPDX-License-Identifier: BUSL-1.1
//! Placement converges to `min(RF, N)` voters per data group once the cluster
//! grows beyond its replication factor.
//!
//! ## What this guards
//!
//! A cluster bootstraps RF-way (here RF=3, three founding voters per data
//! group). When a fourth node joins as a learner the node count exceeds the
//! replication factor, so each data group's *placement* set — the centrally
//! authored, metadata-Raft-replicated intended voter set — must select exactly
//! `min(RF, N) == 3` voters via rendezvous (highest-random-weight) hashing.
//! The selection is deterministic but DATA-DEPENDENT on the group id: a group
//! may keep its original `{1,2,3}` voters, or HRW may pull node 4 in and push
//! one original voter out. So the placement must be read, never assumed.
//!
//! Three things must hold once placement reconciliation settles, for every
//! data group:
//!   1. Placement is authored: `Some(P)` with `|P| == min(RF, N) == 3`, every
//!      member of `P` drawn from the live node set `{1,2,3,4}`.
//!   2. Every node in `P` has been promoted to a voter (entering learners are
//!      promoted into membership) — `P ⊆ members`.
//!   3. Voters not in `P` have left, with ONE exception: a placement-excluded
//!      voter that is the group's current leader is NOT removed. Removing a
//!      voter is a Raft `RemoveNode`; a leader removing itself would require a
//!      leadership-transfer primitive the cluster does not yet have, so that
//!      single removal is deferred. The steady state per group is therefore
//!      EITHER `members == P` (the extra voter was a follower and left) OR
//!      `members == P ∪ {leader}` (the extra voter is the leader, deferred).
//!
//! ## Shape
//!
//!  1. Spawn a 3-node cluster (RF=3), create a `document_strict` collection so
//!     a real data group is exercised, converge.
//!  2. Add a 4th node as a learner via the production join path → N=4 > RF=3.
//!  3. For each data group (excluding the metadata and sequencer groups), poll
//!     until placement converges, then assert (1)-(3) above, and that every
//!     live node's routing view agrees on the group's voter set.

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

const COLL: &str = "a4_placement";
const RF: usize = 3;

/// Sorted voter list (`members`) for `group_id` as seen by `node`'s shared
/// routing table.
fn voters_seen_by(node: &common::cluster_harness::TestClusterNode, group_id: u64) -> Vec<u64> {
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let mut v = routing
        .group_info(group_id)
        .map(|i| i.members.clone())
        .unwrap_or_default();
    v.sort_unstable();
    v
}

/// `(placement, leader)` for `group_id` from `node`'s shared routing table.
/// Placement is sorted ascending if present.
fn placement_and_leader(
    node: &common::cluster_harness::TestClusterNode,
    group_id: u64,
) -> (Option<Vec<u64>>, u64) {
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let info = routing.group_info(group_id);
    let placement = info.and_then(|i| i.placement.clone()).map(|mut p| {
        p.sort_unstable();
        p
    });
    let leader = info.map(|i| i.leader).unwrap_or(0);
    (placement, leader)
}

/// Data group ids in the cluster: every group except the metadata group (0)
/// and the Calvin sequencer group. Read from node 0's routing view.
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

/// Has group `gid` reached the converged steady state on `node`?
///
/// Converged iff: placement is `Some(P)` with `|P| == min(RF, N)`, `P` is a
/// subset of the live node set, `P ⊆ members`, and every voter outside `P` is
/// the leader (at most one such voter, the deferred leader removal).
fn group_converged(
    node: &common::cluster_harness::TestClusterNode,
    gid: u64,
    live: &[u64],
    expected_len: usize,
) -> bool {
    let (placement, leader) = placement_and_leader(node, gid);
    let Some(p) = placement else {
        return false;
    };
    if p.len() != expected_len {
        return false;
    }
    if !p.iter().all(|n| live.contains(n)) {
        return false;
    }
    let members = voters_seen_by(node, gid);
    // Entering learners promoted: every placement node is a voter.
    if !p.iter().all(|n| members.contains(n)) {
        return false;
    }
    // Down-convergence modulo the deferred leader removal: any voter not in P
    // must be the current leader.
    members.iter().all(|m| p.contains(m) || *m == leader)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn placement_converges_to_min_rf_when_node_count_exceeds_rf() {
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

    // Grow the cluster past its replication factor: node 4 joins as a learner
    // via the production join / AddLearner path. Blocks until every node sees
    // the full topology and every group has propagated.
    let new_id = cluster
        .add_learner_node()
        .await
        .expect("add 4th node as learner")
        .node_id;
    assert_eq!(new_id, 4, "4th node should be id 4");

    let live: Vec<u64> = vec![1, 2, 3, 4];
    let n = live.len();
    let expected_len = RF.min(n); // min(3, 4) == 3
    let gids = data_group_ids(&cluster);
    assert!(
        !gids.is_empty(),
        "cluster must expose at least one data group"
    );

    // Placement reconciliation runs throttled on the metadata-group leader, so
    // give it a generous window to author placement and execute the
    // promote/leave conf-changes across every data group on every node.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let all_converged = gids.iter().all(|&gid| {
            cluster
                .nodes
                .iter()
                .all(|node| group_converged(node, gid, &live, expected_len))
        });
        if all_converged {
            break;
        }
        if Instant::now() >= deadline {
            break; // fall through to the per-group asserts for a diagnosable failure
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Per-group assertions on a stable surviving node (node 0 / id 1), with a
    // cross-node agreement check on the voter set.
    let probe = &cluster.nodes[0];
    for &gid in &gids {
        let (placement, leader) = placement_and_leader(probe, gid);
        let members = voters_seen_by(probe, gid);

        // (1) Placement authored at the right cardinality, drawn from live nodes.
        let p = placement.clone().unwrap_or_else(|| {
            panic!(
                "data group {gid}: placement not authored within deadline; \
                 members={members:?}, leader={leader}"
            )
        });
        assert_eq!(
            p.len(),
            expected_len,
            "data group {gid}: placement {p:?} should have min(RF,N)={expected_len} voters; \
             members={members:?}, leader={leader}"
        );
        assert!(
            p.iter().all(|node| live.contains(node)),
            "data group {gid}: placement {p:?} not a subset of live nodes {live:?}"
        );

        // (2) Entering learners promoted: placement is a subset of voters.
        assert!(
            p.iter().all(|node| members.contains(node)),
            "data group {gid}: placement {p:?} not fully promoted into voters {members:?}; \
             leader={leader}"
        );

        // (3) Down-convergence modulo the deferred leader removal. Any voter
        // outside placement must be the current leader (a leader cannot remove
        // itself without a leadership-transfer primitive, so that one removal
        // is deferred). Hence steady state is members == P, or members == P plus
        // the leader.
        let extra: Vec<u64> = members.iter().copied().filter(|m| !p.contains(m)).collect();
        assert!(
            extra.len() <= 1,
            "data group {gid}: more than one voter outside placement {p:?}: \
             extra={extra:?}, members={members:?}, leader={leader}"
        );
        if let Some(&only) = extra.first() {
            assert_eq!(
                only, leader,
                "data group {gid}: voter {only} is outside placement {p:?} but is not the \
                 leader ({leader}) — only a placement-excluded leader may legitimately remain; \
                 members={members:?}"
            );
        }

        // (4) Cross-node agreement: every live node's routing view agrees on
        // this group's voter set.
        let views: Vec<(u64, Vec<u64>)> = cluster
            .nodes
            .iter()
            .map(|node| (node.node_id, voters_seen_by(node, gid)))
            .collect();
        assert!(
            views.iter().all(|(_, v)| *v == members),
            "data group {gid}: nodes disagree on voter set; views={views:?}"
        );
    }

    cluster.shutdown().await;
}
