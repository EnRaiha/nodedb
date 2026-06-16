// SPDX-License-Identifier: BUSL-1.1

//! Integration test: a joining node whose first seed is dead
//! falls back to the next seed in the list and still joins.
//!
//! Exercises the multi-seed retry + leader-redirect fallback in
//! `bootstrap::join::try_join_once`.
//!
//! Scenario:
//! 1. Build a healthy 3-node cluster (1, 2, 3). Node 1 is the
//!    bootstrap seed and therefore the group-0 leader.
//! 2. Kill node 1 to simulate a crashed leader. The remaining
//!    peers' in-memory topology still lists node 1 as Active (the
//!    health monitor isn't running in the test harness), but its
//!    transport address is now unreachable.
//! 3. Spawn a 4th node with `seeds = [node2_addr, node1_addr]`.
//!    Because the join loop's work list is a stack (pop from the
//!    end), node 1 is tried first. The RPC fails with a transport
//!    error, the loop falls back to node 2, which is still alive.
//!    Node 2 either accepts directly (if it has been re-elected
//!    leader) or returns a redirect back to the dead node 1 — in
//!    which case the redirect target is already in `visited` and
//!    the loop stops chasing it.
//! 4. If node 2 is still a follower when node 4 first contacts it
//!    it will redirect to node 1, which we already visited (and
//!    which was what killed the attempt in the first place). The
//!    join retry loop then backs off and re-attempts on the next
//!    outer iteration. By that time node 2 or node 3 has almost
//!    certainly won a fresh group-0 election (default election
//!    timeout 150–300 ms) and will accept directly.
//!
//! Either way, node 4 eventually joins and every surviving node
//! converges on a 4-member topology.

mod cluster_common;

use std::time::Duration;

use cluster_common::{TestNode, wait_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_falls_back_when_first_seed_is_dead() {
    // Build the initial 3-node cluster.
    let node1 = TestNode::spawn(1, vec![]).await.expect("node 1 bootstrap");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let initial_seeds = vec![node1.listen_addr()];
    let node2 = TestNode::spawn(2, initial_seeds.clone())
        .await
        .expect("node 2 join");
    let node3 = TestNode::spawn(3, initial_seeds)
        .await
        .expect("node 3 join");

    // Wait for convergence so nodes 2 and 3 know the cluster state
    // before we tear down the leader.
    let initial = [&node1, &node2, &node3];
    wait_for(
        "initial cluster converges on topology_size == 3",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || initial.iter().all(|n| n.topology_size() == 3),
    )
    .await;

    // Capture the addresses we need post-shutdown.
    let node1_addr = node1.listen_addr();
    let node2_addr = node2.listen_addr();

    // Kill node 1 — simulates a crashed leader. Nodes 2 and 3 stay
    // alive and will re-elect a new group-0 leader at their next
    // election timeout (150–300 ms).
    node1.shutdown().await;

    // Give the survivors a moment to notice and re-elect. The join
    // loop's retry-with-backoff handles slow elections too, so the
    // exact timing doesn't matter for correctness.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Spawn node 4. Put the dead node 1's address LAST in the seed
    // list so pop-order tries it first. The transport call fails
    // quickly and the loop falls back to node 2, which is alive.
    // Node 2 is either the new leader (direct accept) or redirects
    // once to a known peer; the outer retry loop handles every
    // failure case.
    let node4_seeds = vec![node2_addr, node1_addr];
    let node4 = TestNode::spawn(4, node4_seeds)
        .await
        .expect("node 4 fallback join");

    // The fallback worked iff `TestNode::spawn` returned Ok above.
    // Beyond that, the only assertion we can safely make is that
    // node 4 itself sees a multi-member topology including both
    // surviving peers — node 2 and node 3 might lag behind if the
    // `TopologyUpdate` broadcast from the new leader hits them
    // between ticks, and we don't want to pin the test on a
    // broadcast race. The per-node convergence signal we care
    // about is node 4: it received a `JoinResponse` carrying the
    // full member set.
    let node4_ids = node4.topology_ids();
    assert!(
        node4_ids.contains(&2),
        "node 4 must see node 2: {node4_ids:?}"
    );
    assert!(
        node4_ids.contains(&3),
        "node 4 must see node 3: {node4_ids:?}"
    );
    assert!(
        node4_ids.contains(&4),
        "node 4 must see itself: {node4_ids:?}"
    );

    node4.shutdown().await;
    node3.shutdown().await;
    node2.shutdown().await;
}
