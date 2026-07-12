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
//!   3. Voters not in `P` have left — with NO exception. The steady state per
//!      group is exactly `members == P`. A placement-excluded voter that is the
//!      group leader cannot `RemoveNode` itself, so leadership is first
//!      transferred to an in-placement voter; on a later tick the ex-leader is
//!      a follower and is removed like any other leaving voter. There is no
//!      surviving "extra voter is the leader" carve-out.
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
/// subset of the live node set, and the group's voter set is EXACTLY `P`
/// (entering learners promoted, every leaving voter — leader included via
/// step-aside — removed).
fn group_converged(
    node: &common::cluster_harness::TestClusterNode,
    gid: u64,
    live: &[u64],
    expected_len: usize,
) -> bool {
    let (placement, _leader) = placement_and_leader(node, gid);
    let Some(p) = placement else {
        return false;
    };
    if p.len() != expected_len {
        return false;
    }
    if !p.iter().all(|n| live.contains(n)) {
        return false;
    }
    // Voter set is exactly the placement set: every placement node promoted and
    // every leaving voter (including a stepped-aside ex-leader) removed.
    voters_seen_by(node, gid) == p
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

        // (3) Full down-convergence: the voter set is EXACTLY the placement set.
        // No voter outside placement survives — a placement-excluded leader
        // steps aside (leadership transfers to an in-placement voter) and is
        // then removed like any other leaving voter.
        assert_eq!(
            members, p,
            "data group {gid}: voter set {members:?} did not converge to exactly \
             placement {p:?} (leader={leader}); a placement-excluded voter — leader \
             or follower — must be removed"
        );

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

/// Regression: a group whose `AddLearner(self)` is deferred at join must still
/// be mounted on the joining node and converge.
///
/// Reproduces a now-fixed phantom-learner stall. When a data group G is led by
/// a NON-SEED node at the moment a fourth node joins, the join path defers G's
/// `AddLearner(node 4)` in the `JoinResponse`. Pre-fix, node 4 only mounted the
/// groups whose `AddLearner` was non-deferred, so it never created a local
/// replica for G. The later leader-side `AddLearner` then failed with
/// `GroupNotFound`, leaving node 4 a phantom learner forever:
/// `placement(G) = Some([... ,4])` while `members(G)` stayed `[1,2,3]`. The fix
/// added a tick phase that mounts any group where `self ∈ placement(G)` and the
/// node is not already hosting G, so the deferred group is caught up and
/// promoted. Without that phase this test stalls at `members == [1,2,3]`.
///
/// To force the exact trigger condition we transfer G's leadership to a voter
/// that HRW placement will EVICT once node 4 joins — making G led by a
/// non-seed-relative node and guaranteeing a real promote+evict on G.
///
/// Scope boundary: the cluster harness wires the production
/// `DataPlaneSnapshotApplier` (not a None stub), so the late mount is caught up
/// through the real `InstallSnapshot` apply path. This test asserts MEMBERSHIP
/// convergence (the reported bug); it does not separately assert per-replica
/// row contents, because a pgwire read on node 4 routes to the group leader
/// rather than its local replica and so would not isolate local restoration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn placement_converges_when_excluded_voter_leads_group() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL}_x \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION");

    let gids = data_group_ids(&cluster);
    assert!(
        !gids.is_empty(),
        "cluster must expose at least one data group"
    );

    // Determine, deterministically and BEFORE node 4 joins, the target group G
    // and the voter X that HRW placement will evict from G once the node set
    // grows to {1,2,3,4}. We need a G whose future placement pulls in node 4
    // (so exactly one of {1,2,3} is pushed out) — that excluded voter is X.
    let future = nodedb_cluster::rebalancer::placement::compute_placement(&[1, 2, 3, 4], &gids, 3);
    let (target_gid, excluded_x) = gids
        .iter()
        .find_map(|&gid| {
            let p = future.get(&gid)?;
            if p.contains(&4) {
                // The single voter in {1,2,3} absent from the new placement.
                [1u64, 2, 3]
                    .into_iter()
                    .find(|x| !p.contains(x))
                    .map(|x| (gid, x))
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            panic!(
                "no data group pulls node 4 into placement; \
                 future placement={future:?}, gids={gids:?}"
            )
        });

    // Wait for the target group to have a stable elected leader so we can read
    // a real (leader_id, term) to drive the leadership transfer.
    let lead_deadline = Instant::now() + Duration::from_secs(10);
    let (current_leader, current_term) = loop {
        let probe = &cluster.nodes[0];
        let leader = probe
            .all_group_leaders()
            .into_iter()
            .find(|(g, _)| *g == target_gid)
            .map(|(_, l)| l)
            .unwrap_or(0);
        let term = cluster
            .nodes
            .iter()
            .find_map(|node| {
                node.shared.cluster_observer.get().and_then(|obs| {
                    obs.group_status
                        .upgrade()
                        .map(|gs| gs.group_statuses())
                        .unwrap_or_default()
                        .into_iter()
                        .find(|s| s.group_id == target_gid && s.leader_id != 0)
                        .map(|s| s.term)
                })
            })
            .unwrap_or(0);
        if leader != 0 && term != 0 {
            break (leader, term);
        }
        if Instant::now() >= lead_deadline {
            panic!(
                "group {target_gid} never elected a stable leader before transfer; \
                 leader={leader}, term={term}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Force X to lead the target group via a legitimate raft leadership
    // transfer (TimeoutNow), unless X already leads it. This puts G in the
    // led-by-soon-to-be-evicted-voter state that triggered the bug.
    if current_leader != excluded_x {
        let transport = cluster.nodes[0]
            .shared
            .cluster_transport
            .as_ref()
            .expect("cluster_transport");
        transport
            .send_rpc_oneway(
                excluded_x,
                nodedb_cluster::RaftRpc::TimeoutNowRequest(nodedb_raft::TimeoutNowRequest {
                    term: current_term,
                    leader_id: current_leader,
                    group_id: target_gid,
                }),
            )
            .await
            .expect("send TimeoutNow to excluded voter");
    }

    let transfer_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let leads = cluster.nodes.iter().any(|node| {
            node.all_group_leaders()
                .into_iter()
                .any(|(g, l)| g == target_gid && l == excluded_x)
        });
        if leads {
            break;
        }
        if Instant::now() >= transfer_deadline {
            panic!(
                "group {target_gid} leadership did not transfer to X={excluded_x} \
                 within deadline (started at leader={current_leader}, term={current_term})"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Now grow past RF: node 4 joins as a learner. G's AddLearner(4) is the one
    // that gets deferred (G led by X, a non-seed-relative voter).
    let new_id = cluster
        .add_learner_node()
        .await
        .expect("add 4th node as learner")
        .node_id;
    assert_eq!(new_id, 4, "4th node should be id 4");

    let live: Vec<u64> = vec![1, 2, 3, 4];
    let expected_p = future
        .get(&target_gid)
        .cloned()
        .expect("target group has a computed placement");

    // Probe on a node guaranteed to survive in G (one of G's placement voters,
    // never the evicted X).
    let probe_id = expected_p[0];
    let probe = &cluster.nodes[(probe_id - 1) as usize];

    // The deferred group must be mounted, caught up, promoted and the excluded
    // voter evicted within the reconciliation window. Pre-fix this never
    // happens for G: it stalls at members == [1,2,3].
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if group_converged(probe, target_gid, &live, RF.min(live.len())) {
            let members = voters_seen_by(probe, target_gid);
            if members.contains(&4) && !members.contains(&excluded_x) {
                break;
            }
        }
        if Instant::now() >= deadline {
            break; // fall through to diagnosable asserts
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let (placement, leader) = placement_and_leader(probe, target_gid);
    let members = voters_seen_by(probe, target_gid);
    let p = placement.clone().unwrap_or_else(|| {
        panic!(
            "group {target_gid}: placement not authored within deadline; \
             members={members:?}, leader={leader}, expected_placement={expected_p:?}"
        )
    });

    assert_eq!(
        p, expected_p,
        "group {target_gid}: authored placement {p:?} should match HRW prediction \
         {expected_p:?}; members={members:?}, leader={leader}"
    );

    // The reported bug: node 4 (deferred-AddLearner group) must be promoted into
    // the voter set, and X must be evicted — voter set is EXACTLY the placement.
    assert!(
        members.contains(&4),
        "group {target_gid}: node 4 never mounted/promoted into voters {members:?} \
         (phantom-learner stall); placement={p:?}, leader={leader}"
    );
    assert!(
        !members.contains(&excluded_x),
        "group {target_gid}: evicted voter X={excluded_x} still present in voters \
         {members:?}; placement={p:?}, leader={leader}"
    );
    assert_eq!(
        members, p,
        "group {target_gid}: voter set {members:?} did not converge to exactly \
         placement {p:?} (leader={leader})"
    );

    // The placement-excluded leader must have stepped aside: the live leader is
    // inside placement.
    assert!(
        p.contains(&leader),
        "group {target_gid}: leader {leader} not in placement {p:?}; members={members:?}"
    );

    // Cross-node agreement on the converged voter set — among the nodes that
    // still host the group (its placement voters). A node evicted from the
    // group stops applying that group's conf-changes, so its local view of the
    // group's voter set legitimately freezes at the moment it left; only the
    // metadata-replicated `placement` is cluster-wide. So the agreement check is
    // scoped to the surviving in-group nodes, not the evicted voter.
    let views: Vec<(u64, Vec<u64>)> = cluster
        .nodes
        .iter()
        .filter(|node| p.contains(&node.node_id))
        .map(|node| (node.node_id, voters_seen_by(node, target_gid)))
        .collect();
    assert!(
        views.iter().all(|(_, v)| *v == members),
        "group {target_gid}: in-group nodes disagree on voter set; views={views:?}"
    );

    cluster.shutdown().await;
}
