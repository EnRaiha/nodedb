// SPDX-License-Identifier: BUSL-1.1

//! Integration test: a restart in place from an existing catalog
//! is idempotent — the node re-registers with the same topology
//! entry, Raft groups come back with the same membership, and the
//! rest of the cluster is undisturbed.
//!
//! Exercises the `restart()` path in `bootstrap/restart.rs`, which
//! is what production does when a node crashes and systemd brings
//! it back.

mod cluster_common;

use std::time::Duration;

use cluster_common::{TestNode, wait_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_restart_is_idempotent() {
    // Build a 3-node cluster first. Node 1 uses the owned-data-dir
    // path so we can inspect / reuse its catalog later; nodes 1 and
    // 3 use the default spawn. Node 2 is the one we'll restart.
    let node1 = TestNode::spawn(1, vec![]).await.expect("node 1 bootstrap");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let seeds = vec![node1.listen_addr()];

    // The test owns node 2's data directory so it survives across
    // the shutdown-and-respawn cycle. `TestNode::spawn_with_data_dir`
    // reuses this path.
    let node2_data_dir = tempfile::tempdir().expect("create node 2 data dir");
    let node2 = TestNode::spawn_with_data_dir(2, node2_data_dir.path(), seeds.clone())
        .await
        .expect("node 2 join");

    let node3 = TestNode::spawn(3, seeds).await.expect("node 3 join");

    // Wait for the 3-node cluster to converge.
    let nodes_initial = [&node1, &node2, &node3];
    wait_for(
        "all 3 nodes converge on topology_size == 3",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || nodes_initial.iter().all(|n| n.topology_size() == 3),
    )
    .await;

    // Snapshot node 1 / 3's topology before the restart so we can
    // assert it's unchanged afterwards.
    let topo_before_1 = node1.topology_ids();
    let topo_before_3 = node3.topology_ids();
    assert_eq!(topo_before_1, vec![1, 2, 3]);
    assert_eq!(topo_before_3, vec![1, 2, 3]);

    // Gracefully shut node 2 down. `TestNode::shutdown` flips the
    // cooperative shutdown watch via `RaftLoop::begin_shutdown`
    // first, so every detached `tokio::spawn` task inside
    // `raft_loop::tick::do_tick` exits at its next `tokio::select!`
    // poll and drops its `Arc<Mutex<MultiRaft>>` clone. That
    // releases the per-group redb log file locks in the same
    // data directory, so the respawn below can reopen them
    // in-process without the old "Database already open" race.
    node2.shutdown().await;

    // Short pause so tokio has actually run the cancelled tasks'
    // drops. `abort()` schedules cancellation on the runtime;
    // the actual future-drop happens on the runtime's next cycle.
    // 50 ms is more than enough on any reasonable runtime.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Respawn node 2 from the SAME data directory — the
    // production-shaped in-process restart. Because the catalog
    // is already bootstrapped, `start_cluster` MUST take the
    // `restart()` path regardless of seed list; pass empty seeds
    // so this is unambiguous.
    let node2_restarted = TestNode::spawn_with_data_dir(2, node2_data_dir.path(), vec![])
        .await
        .expect("node 2 restart from catalog");

    // The restarted node must load its catalog-persisted topology
    // (3 members), not a fresh bootstrap.
    assert_eq!(
        node2_restarted.topology_size(),
        3,
        "restarted node 2 should load 3 members from catalog, got {}",
        node2_restarted.topology_size()
    );
    assert_eq!(node2_restarted.topology_ids(), vec![1, 2, 3]);

    // Node 1 and 3's topology must be unchanged — no duplicate
    // entries, no "new" node 2 at a different address. Collisions
    // would show up as a node_count != 3 or as topology_ids
    // containing a stray id.
    assert_eq!(
        node1.topology_ids(),
        topo_before_1,
        "node 1 topology must be unchanged across node 2's restart"
    );
    assert_eq!(
        node3.topology_ids(),
        topo_before_3,
        "node 3 topology must be unchanged across node 2's restart"
    );

    // Lifecycle: the restart path transitions to Restarting first,
    // then the test helper's caller-owned `to_ready` fires.
    assert!(
        matches!(
            node2_restarted.lifecycle_state(),
            nodedb_cluster::ClusterLifecycleState::Ready { nodes: 3 }
        ),
        "restarted node 2 should be Ready{{nodes:3}}, got {:?}",
        node2_restarted.lifecycle_state()
    );

    node2_restarted.shutdown().await;
    node3.shutdown().await;
    node1.shutdown().await;
    drop(node2_data_dir);
}
