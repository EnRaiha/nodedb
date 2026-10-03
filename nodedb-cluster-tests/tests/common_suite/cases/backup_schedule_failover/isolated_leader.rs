// SPDX-License-Identifier: BUSL-1.1

//! A vShard 0 leader cut off from both peers fires nothing once its lease
//! lapses, and the new leader runs the due minute once.

use crate::common;
use common::cluster_harness::shared_steps::holds_vshard0_lease;
use common::cluster_harness::wait_for;

use nodedb::control::backup::schedule::envelope_name;
use nodedb::control::backup::schedule::marks::settled_through;

use super::fixture::{CONVERGE, DATABASE, Fixture, STEP};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_leader_stops_firing_and_the_new_leader_runs_the_minute_once() {
    let fx = Fixture::new().await;
    let due = fx.due;
    let leader = fx.leader();
    wait_for(
        "the leader holds the vShard 0 lease",
        CONVERGE,
        STEP,
        || {
            fx.cluster
                .nodes
                .iter()
                .any(|node| node.node_id == leader && holds_vshard0_lease(node))
        },
    )
    .await;

    fx.tick_all((due - 1) * 60 + 5).await;
    fx.wait_marks("every node holds the armed mark", due - 1)
        .await;

    // `due` comes due while the leader is cut off from both peers.
    fx.partition(leader, true);
    let isolated = fx
        .cluster
        .nodes
        .iter()
        .find(|node| node.node_id == leader)
        .expect("isolated node");
    wait_for("the isolated leader's lease lapses", CONVERGE, STEP, || {
        !holds_vshard0_lease(isolated)
    })
    .await;
    fx.wait_new_coordinator(leader).await;

    // The isolated node ticks through `due` and fires nothing.
    fx.tick_node(leader, due * 60 + 5).await;
    fx.tick_node(leader, due * 60 + 35).await;
    assert_eq!(
        fx.runs(isolated),
        (0, 0),
        "a leader without its lease fires nothing"
    );
    assert!(fx.envelopes().is_empty(), "{:?}", fx.envelopes());

    let peers = &fx.cluster.nodes;
    wait_for(
        "every group the peers host has a live leader",
        CONVERGE,
        STEP,
        || {
            peers
                .iter()
                .filter(|node| node.node_id != leader)
                .all(|node| {
                    node.all_group_leaders()
                        .into_iter()
                        .all(|(_, group_leader)| group_leader != 0 && group_leader != leader)
                })
        },
    )
    .await;

    // The peers tick after `due`, with the partition still in place. Each
    // peer's routing hints follow its own Raft, so the backup takes every
    // group from a reachable leader. A failed attempt waits out its retry
    // delay on the scheduler clock. No minute from `due + 1` through
    // `due + 4` matches, so every attempt runs `due` itself.
    let mut now_secs = (due + 1) * 60 + 5;
    for _ in 0..3 {
        fx.tick_except(leader, now_secs).await;
        if fx.local_marks().contains(&Some(due)) {
            break;
        }
        now_secs += 61;
    }
    let peers_marked = fx
        .cluster
        .nodes
        .iter()
        .filter(|node| node.node_id != leader)
        .any(|node| settled_through(&node.shared, &fx.schedule).ok().flatten() == Some(due));
    assert!(
        peers_marked,
        "the new leader runs the due minute while the partition lasts"
    );
    assert_eq!(fx.envelopes(), [envelope_name(DATABASE, due * 60_000)]);
    assert_eq!(
        fx.runs(isolated),
        (0, 0),
        "the isolated node still fires nothing"
    );
    fx.partition(leader, false);

    // Healed, the former leader catches up and finds nothing due.
    fx.wait_marks("every node holds the mark of the due minute", due)
        .await;
    fx.tick_all((due + 4) * 60 + 30).await;

    let successes: usize = fx.cluster.nodes.iter().map(|node| fx.runs(node).0).sum();
    assert_eq!(successes, 1, "the due minute runs exactly once");
    assert_eq!(fx.runs(isolated).0, 0);
    assert_eq!(fx.envelopes(), [envelope_name(DATABASE, due * 60_000)]);

    fx.shutdown().await;
}
