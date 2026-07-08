// SPDX-License-Identifier: BUSL-1.1

//! The shared 3-node bringup body (`spawn_three_inner`) and its
//! post-join convergence barriers: topology size, rolling-upgrade
//! compat-mode exit, metadata-group leader stability, and per-group
//! Raft leader stability.

use std::time::Duration;

use nodedb_types::config::tuning::ClusterTransportTuning;

use super::TestCluster;
use super::types::ClusterSpawnConfig;
use crate::cluster_harness::node::TestClusterNode;
use crate::cluster_harness::wait::wait_for;

impl TestCluster {
    /// Shared 3-node spawn body. Threads an optional Raft
    /// `log_compaction_threshold` and a Raft `replication_factor` into
    /// every node's spawn; all public `spawn_three_*` entry points funnel
    /// here.
    pub(super) async fn spawn_three_inner(
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        query_tuning: nodedb_types::config::tuning::QueryTuning,
        num_cores: usize,
        log_compaction_threshold: Option<u64>,
        replication_factor: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = ClusterSpawnConfig {
            tuning,
            graph_tuning,
            query_tuning,
            num_cores,
            log_compaction_threshold,
            replication_factor,
            single_node_calvin: false,
        };

        let node1 = TestClusterNode::spawn_with_full_config(1, vec![], &config).await?;

        // Wait until node 1 has bootstrapped (topology shows itself)
        // before peers try to join. The old fixed 200ms sleep was too
        // short under heavy host load (e.g. 500+ parallel unit tests
        // sharing the same CPU pool), causing peers to dial before
        // node 1's transport was ready — failing topology convergence.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while node1.topology_size() < 1 {
            if std::time::Instant::now() >= deadline {
                return Err("node 1 failed to bootstrap within 30s".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let seeds = vec![node1.listen_addr];
        let node2 = TestClusterNode::spawn_with_full_config(2, seeds.clone(), &config).await?;

        // Wait for node 2's join to be reflected before spawning node 3.
        // Under load, spawning both peers simultaneously can overwhelm the
        // bootstrap leader's join handler, causing neither join to complete
        // within the topology convergence deadline.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while node1.topology_size() < 2 {
            if std::time::Instant::now() >= deadline {
                return Err("node 2 failed to join within 30s".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let node3 = TestClusterNode::spawn_with_full_config(3, seeds, &config).await?;

        let cluster = Self {
            nodes: vec![node1, node2, node3],
            spawn_config: config,
        };

        wait_for(
            "all 3 nodes report topology_size == 3",
            Duration::from_secs(30),
            Duration::from_millis(50),
            || cluster.nodes.iter().all(|n| n.topology_size() == 3),
        )
        .await;

        // CRITICAL: wait for every node to exit rolling-upgrade
        // compat mode before letting the test issue any DDL.
        //
        // `metadata_proposer::propose_catalog_entry` consults
        // `cluster_version_view().can_activate_feature(DISTRIBUTED_CATALOG_VERSION)`
        // and, while even one node still reports a lower wire
        // version, returns `Ok(0)` without going through the raft
        // group. The pgwire DDL handlers (CREATE USER, etc.) then
        // fall through to a LEGACY path that writes the record
        // directly on the proposing node — **with zero
        // replication** to followers. Any subsequent
        // `has_active_user` check on a follower returns false and
        // the test flakes.
        //
        // Topology has three members the moment the join request
        // completes, but the `wire_version` field on each node's
        // topology entry is updated asynchronously by the gossip
        // path. That's why `topology_size == 3` converges fast yet
        // `can_activate_feature(...)` can still be false for
        // several hundred milliseconds afterwards. Waiting here
        // closes the window deterministically — no retries, no
        // flakes, no compat-mode fallback silently breaking
        // replication.
        wait_for(
            "all 3 nodes exit rolling-upgrade compat mode",
            Duration::from_secs(30),
            Duration::from_millis(20),
            || {
                cluster.nodes.iter().all(|n| {
                    n.shared.cluster_version_view().can_activate_feature(
                        nodedb::control::rolling_upgrade::DISTRIBUTED_CATALOG_VERSION,
                    )
                })
            },
        )
        .await;

        // CRITICAL: wait for the metadata Raft group to elect a leader
        // and for every node's local view to agree on the same leader id.
        //
        // Topology convergence + rolling-upgrade exit only guarantees
        // membership and wire version are agreed; they say nothing about
        // election state. Under heavy host load (e.g. running this test
        // immediately after another full-suite cluster test exits and
        // the unit-test pool ramps back up), the initial Raft heartbeat
        // window can be missed and the first `acquire`/`propose` issued
        // by the test races a re-election — surfacing as
        // `raft error: not leader (leader hint: None)` from a
        // descriptor-lease or DDL call.
        //
        // Waiting until every node reports the same non-zero leader id
        // closes the window deterministically. Symmetric to the
        // rolling-upgrade wait above: no retries, no flakes, no
        // wasted CI minutes on cleanup of a doomed cluster bringup.
        wait_for(
            "metadata group has stable leader visible on every node",
            Duration::from_secs(30),
            Duration::from_millis(20),
            || {
                let leaders: Vec<u64> = cluster
                    .nodes
                    .iter()
                    .map(|n| n.metadata_group_leader())
                    .collect();
                let first = leaders[0];
                first != 0 && leaders.iter().all(|&l| l == first)
            },
        )
        .await;

        // CRITICAL: wait for EVERY data Raft group to elect a stable
        // leader visible on every node. Without this barrier, the
        // first data-group write after `spawn_three()` returns can
        // race a still-electing group:
        //
        // 1. Proposer's local `propose()` runs on a node that thinks
        //    it's leader (stale routing-table hint), gets an Ok back
        //    with a `log_index` that was never actually committed.
        // 2. `ProposeTracker::register((group_id, log_index))`.
        // 3. Some unrelated entry that *does* commit at that index
        //    (e.g., a leadership-change no-op) fires `tracker.complete`,
        //    waking the waiter with `Ok([])` even though the user's
        //    `INSERT` row was never replicated.
        // 4. `simple_query` returns success; the row is permanently
        //    lost.
        //
        // The metadata-group-only wait above is insufficient because
        // data groups elect independently and lag the metadata group
        // by hundreds of milliseconds under load. Waiting until every
        // group on every node reports a non-zero leader closes the
        // window deterministically.
        wait_for(
            "every Raft group has a stable leader visible on every node",
            Duration::from_secs(30),
            Duration::from_millis(20),
            || {
                // Snapshot every node's per-group leader view. A group
                // is "ready" iff every node reports the same non-zero
                // leader for it.
                let per_node: Vec<Vec<(u64, u64)>> = cluster
                    .nodes
                    .iter()
                    .map(|n| n.all_group_leaders())
                    .collect();
                if per_node.iter().any(|v| v.is_empty()) {
                    return false;
                }
                // The Calvin sequencer group is an internal Raft group that is
                // not part of the data/metadata routing topology. Cluster
                // readiness for data operations does not depend on it, and its
                // leader is surfaced to the observer on a slower/independent path
                // than the routing groups — so gating general cluster startup on
                // it makes every test (Calvin or not) flake when the sequencer
                // group's observed leader lags. Calvin tests gate on the
                // sequencer separately (`wait_for_sequencer_leader`). Exclude it
                // from the general readiness gate.
                let group_ids: std::collections::BTreeSet<u64> = per_node
                    .iter()
                    .flat_map(|v| v.iter().map(|(gid, _)| *gid))
                    .filter(|gid| *gid != nodedb_cluster::calvin::SEQUENCER_GROUP_ID)
                    .collect();
                if group_ids.is_empty() {
                    return false;
                }
                group_ids.iter().all(|gid| {
                    let leaders: Vec<u64> = per_node
                        .iter()
                        .filter_map(|v| v.iter().find(|(g, _)| g == gid).map(|(_, l)| *l))
                        .collect();
                    if leaders.len() != per_node.len() {
                        return false;
                    }
                    let first = leaders[0];
                    first != 0 && leaders.iter().all(|&l| l == first)
                })
            },
        )
        .await;

        Ok(cluster)
    }
}
