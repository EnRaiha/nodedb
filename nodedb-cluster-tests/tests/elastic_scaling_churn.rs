// SPDX-License-Identifier: BUSL-1.1

//! Two-node churn during rebalance.
//!
//! `elastic_scaling.rs` covers a single new joiner. This test covers
//! the next concentric scenario: a second node joins while the moves
//! from the first round are still in flight (i.e. routing has not yet
//! caught up to the dispatched moves). Two properties matter:
//!
//! 1. **Coverage** — after both kicks, dispatched moves target both
//!    new nodes, not just whichever joined first. A planner that
//!    captures the cold set once and never re-reads it would silently
//!    pile everything onto node 4.
//! 2. **Distribution under churn** — the second-round plan, built
//!    while node 5 is now also cold, distributes targets across both
//!    new cold nodes (round-robin), proving the planner re-observes
//!    topology each tick rather than caching stale state.
//!
//! Dedup of in-flight moves (vshard X already moving 1→4 must not
//! re-dispatch as 1→5 in the next tick) is intentionally NOT asserted
//! here — that is the migration executor's responsibility, not the
//! planner's. The planner walks routing as ground truth and trusts
//! the dispatcher to be idempotent.

mod cluster_common;

use std::sync::{Arc, RwLock};
use std::time::Duration;

use nodedb_cluster::rebalancer::{
    AlwaysReadyGate, ElectionGate, LoadMetricsProvider, MigrationDispatcher, RebalancerKickHook,
    RebalancerLoop, RebalancerLoopConfig,
};
use nodedb_cluster::routing::RoutingTable;
use nodedb_cluster::swim::MemberState;
use nodedb_cluster::swim::subscriber::MembershipSubscriber;
use nodedb_cluster::topology::{ClusterTopology, NodeInfo, NodeState};
use nodedb_types::NodeId;

use cluster_common::rebalancer::{DynamicProvider, RecordingDispatcher, lm, wait_until};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_nodes_added_mid_rebalance_both_receive_moves() {
    // --- Initial state: 3 nodes, hot leader on node 1, 6 groups.
    let mut topo = ClusterTopology::new();
    for (i, id) in [1u64, 2, 3].iter().enumerate() {
        let a: std::net::SocketAddr = format!("127.0.0.1:{}", 9100 + i).parse().unwrap();
        topo.add_node(NodeInfo::new(*id, a, NodeState::Active));
    }
    let topology = Arc::new(RwLock::new(topo));

    let mut rt = RoutingTable::uniform(6, &[1, 2, 3], 1);
    for gid in 0..6 {
        rt.set_leader(gid, 1);
    }
    let routing = Arc::new(RwLock::new(rt));

    // Initial scores chosen so the cluster mean lands exactly on
    // nodes 2 and 3, putting only the new joiner strictly below the
    // mean. Score = vshards_led + bytes_mib + writes + reads (default
    // weights; qps and latency contribute nothing here).
    //   node 1: 200  (hot, leads everything)
    //   node 2: 100  (== mean for round 1: (200+100+100+0)/4 = 100)
    //   node 3: 100
    //   node 4:   0  (cold — only node strictly below mean)
    // Round 2 mean drops to 80 once node 5 (score 0) joins, so 2/3
    // become strictly above mean and the cold set is exactly {4, 5}.
    let provider = DynamicProvider::new(vec![
        lm(1, 200, 0, 0.0, 0.0),
        lm(2, 100, 0, 0.0, 0.0),
        lm(3, 100, 0, 0.0, 0.0),
    ]);

    let dispatcher = RecordingDispatcher::new();
    let gate: Arc<dyn ElectionGate> = Arc::new(AlwaysReadyGate);

    // Long natural interval — every move-batch must come from a kick.
    let rloop = Arc::new(RebalancerLoop::new(
        RebalancerLoopConfig {
            interval: Duration::from_secs(300),
            ..Default::default()
        },
        provider.clone() as Arc<dyn LoadMetricsProvider>,
        dispatcher.clone() as Arc<dyn MigrationDispatcher>,
        gate,
        routing.clone(),
        topology.clone(),
    ));
    let kick_hook = RebalancerKickHook::new(rloop.kick_handle());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn({
        let d = Arc::clone(&rloop);
        async move { d.run(shutdown_rx).await }
    });

    // ---------------------------------------------------------------
    // Round 1: node 4 joins.
    // ---------------------------------------------------------------
    {
        let mut t = topology.write().unwrap();
        let a: std::net::SocketAddr = "127.0.0.1:9103".parse().unwrap();
        t.add_node(NodeInfo::new(4, a, NodeState::Active));
    }
    provider.push(lm(4, 0, 0, 0.0, 0.0));
    kick_hook.on_state_change(
        &NodeId::try_new("node-4").expect("test fixture"),
        None,
        MemberState::Alive,
    );

    assert!(
        wait_until(Duration::from_secs(3), || dispatcher.fired()).await,
        "round 1: kick did not produce any dispatch within 3s"
    );

    let round1 = dispatcher.snapshot();
    assert!(!round1.is_empty(), "round 1 dispatched zero moves");
    // With cold = {4} only, every round-1 move must target node 4.
    for m in &round1 {
        assert_eq!(
            m.target_node, 4,
            "round 1: cold set is {{4}}, but a move targeted {m:?}"
        );
    }

    // ---------------------------------------------------------------
    // Round 2: node 5 joins WHILE round-1 moves are still "in flight".
    // The recording dispatcher does not update routing, so from the
    // planner's view the cluster is still hot-on-1 with cold = [4, 5].
    // ---------------------------------------------------------------
    dispatcher.reset_fired();
    let round1_len = round1.len();

    {
        let mut t = topology.write().unwrap();
        let a: std::net::SocketAddr = "127.0.0.1:9104".parse().unwrap();
        t.add_node(NodeInfo::new(5, a, NodeState::Active));
    }
    provider.push(lm(5, 0, 0, 0.0, 0.0));
    kick_hook.on_state_change(
        &NodeId::try_new("node-5").expect("test fixture"),
        None,
        MemberState::Alive,
    );

    assert!(
        wait_until(Duration::from_secs(3), || dispatcher.fired()).await,
        "round 2: kick did not produce any dispatch within 3s"
    );

    // Give the dispatch loop a moment to drain the full second batch.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let all = dispatcher.snapshot();
    assert!(
        all.len() > round1_len,
        "round 2 produced no new moves: total={}, round1={}",
        all.len(),
        round1_len
    );
    let round2 = &all[round1_len..];

    // (1) Coverage: round 2 must target at least one move at node 5.
    //     A planner that cached the first cold set would target only 4.
    let to_5_round2 = round2.iter().filter(|m| m.target_node == 5).count();
    assert!(
        to_5_round2 > 0,
        "round 2: expected at least one move targeting the new cold node 5, got {round2:?}"
    );

    // (2) Distribution: round 2's plan, built with cold = [4, 5],
    //     must distribute across both — round-robin in plan.rs assigns
    //     `cold_nodes[cursor % len]` per pick. With ≥2 picks we expect
    //     to see both targets represented.
    let to_4_round2 = round2.iter().filter(|m| m.target_node == 4).count();
    if round2.len() >= 2 {
        assert!(
            to_4_round2 > 0 && to_5_round2 > 0,
            "round 2: expected moves split across cold nodes 4 and 5, got to_4={to_4_round2}, to_5={to_5_round2}, moves={round2:?}"
        );
    }

    // (3) Aggregate: across both rounds, the union of targets contains
    //     both new nodes. This is the property an operator cares about
    //     when scaling up by two replicas at once.
    let total_to_4 = all.iter().filter(|m| m.target_node == 4).count();
    let total_to_5 = all.iter().filter(|m| m.target_node == 5).count();
    assert!(
        total_to_4 > 0 && total_to_5 > 0,
        "elastic add of 2 nodes must place load on both: to_4={total_to_4}, to_5={total_to_5}"
    );

    // (4) No move targets a non-Active node. Sanity guard against the
    //     planner ever picking nodes 1..=3 as cold when they were the
    //     hot/balanced set.
    for m in &all {
        assert!(
            matches!(m.target_node, 4 | 5),
            "unexpected target {} for move {m:?}",
            m.target_node
        );
    }

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}
