// SPDX-License-Identifier: BUSL-1.1

//! Multi-node cluster orchestration.

use std::time::Duration;

use nodedb_types::config::tuning::ClusterTransportTuning;

use super::node::TestClusterNode;
use super::wait::wait_for;

/// An in-process cluster of `TestClusterNode`s.
pub struct TestCluster {
    pub nodes: Vec<TestClusterNode>,
}

impl TestCluster {
    /// Spawn a 3-node cluster: node 1 bootstraps, nodes 2 and 3 join
    /// via node 1's pre-bound address. Waits until every node sees
    /// topology_size == 3 (10s deadline).
    pub async fn spawn_three() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning(fast_cluster_tuning()).await
    }

    /// Spawn a 3-node cluster with `num_cores` Data-Plane cores PER NODE,
    /// using the same fast election tuning as [`spawn_three`]. Exercises
    /// multi-core cross-node code paths (the per-core store fan-out).
    pub async fn spawn_three_with_cores(
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning_and_cores(fast_cluster_tuning(), num_cores).await
    }

    /// Spawn a 3-node cluster with the fast election tuning, `num_cores`
    /// Data-Plane cores per node, AND lowered variable-length MATCH expansion
    /// caps. Used to force `[*min..max]` truncation on a small graph and prove
    /// the cross-shard resume pipeline drains to the complete result set.
    pub async fn spawn_three_with_varlen_caps_and_cores(
        num_cores: usize,
        varlen_max_results: usize,
        varlen_max_frontier: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let graph_tuning = nodedb_types::config::tuning::GraphTuning {
            varlen_max_results,
            varlen_max_frontier,
            ..Default::default()
        };
        Self::spawn_three_with_tuning_graph_and_cores(
            fast_cluster_tuning(),
            graph_tuning,
            num_cores,
        )
        .await
    }

    /// Spawn a 3-node cluster with a custom `ClusterTransportTuning`.
    /// Used by the descriptor-lease renewal tests to drive the
    /// renewal loop on a much faster cadence than the production
    /// 60-second default.
    pub async fn spawn_three_with_tuning(
        tuning: ClusterTransportTuning,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning_and_cores(tuning, 1).await
    }

    /// Spawn a 3-node cluster with a custom `ClusterTransportTuning` and a
    /// specific number of Data-Plane cores per node. Graph tuning defaults
    /// (100k variable-length expansion caps).
    pub async fn spawn_three_with_tuning_and_cores(
        tuning: ClusterTransportTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning_graph_and_cores(
            tuning,
            nodedb_types::config::tuning::GraphTuning::default(),
            num_cores,
        )
        .await
    }

    /// Spawn a 3-node cluster with custom cluster-transport AND graph engine
    /// tuning plus a specific core count per node. Lets a cluster test lower
    /// the variable-length MATCH expansion caps (`varlen_max_results` /
    /// `varlen_max_frontier`) to force truncation on a small graph and prove
    /// the cross-shard resume pipeline drains to the complete result set.
    pub async fn spawn_three_with_tuning_graph_and_cores(
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let node1 = TestClusterNode::spawn_with_tuning_graph_and_cores(
            1,
            vec![],
            tuning.clone(),
            graph_tuning.clone(),
            num_cores,
        )
        .await?;

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
        let node2 = TestClusterNode::spawn_with_tuning_graph_and_cores(
            2,
            seeds.clone(),
            tuning.clone(),
            graph_tuning.clone(),
            num_cores,
        )
        .await?;

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

        let node3 = TestClusterNode::spawn_with_tuning_graph_and_cores(
            3,
            seeds,
            tuning,
            graph_tuning,
            num_cores,
        )
        .await?;

        let cluster = Self {
            nodes: vec![node1, node2, node3],
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

    /// Find a node that will accept the given DDL — retries up to
    /// 10 seconds across all nodes. Non-leader nodes surface
    /// `not metadata-group leader` errors via the pgwire error path;
    /// the retry loop tries the next node on failure so the test
    /// doesn't have to discover the leader explicitly.
    ///
    /// After the DDL is accepted, **blocks until every node's
    /// metadata applier has caught up to the proposer's applied
    /// index**. `propose_catalog_entry` already waits for the entry
    /// to be applied on the proposing node before returning, but
    /// followers apply asynchronously — without this barrier a
    /// subsequent `wait_for("x visible on every node")` would race
    /// the follower appliers and trip its timeout on the cold-start
    /// attempt. Polling the watermark directly is O(applied_index)
    /// and converges as soon as the followers drain their commit
    /// queues, so it's both strictly more correct and strictly
    /// faster than waiting on the visibility check itself.
    pub async fn exec_ddl_on_any_leader(&self, sql: &str) -> Result<usize, String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut last_err = String::new();
        while std::time::Instant::now() < deadline {
            for (idx, node) in self.nodes.iter().enumerate() {
                match node.exec(sql).await {
                    Ok(()) => {
                        self.wait_for_applied_index_convergence(idx).await;
                        return Ok(idx);
                    }
                    Err(e) => last_err = e,
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "no node accepted DDL within 10s; last error: {last_err}"
        ))
    }

    /// Block until every node's metadata applier has caught up to the
    /// proposer's current applied index. Called after every successful
    /// DDL by `exec_ddl_on_any_leader`.
    async fn wait_for_applied_index_convergence(&self, proposer_idx: usize) {
        let group_id = nodedb_cluster::METADATA_GROUP_ID;
        let target = self.nodes[proposer_idx]
            .shared
            .applied_index_watcher(group_id)
            .current();
        if target == 0 {
            return;
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let all_caught_up = self
                .nodes
                .iter()
                .all(|n| n.shared.applied_index_watcher(group_id).current() >= target);
            if all_caught_up {
                return;
            }
            if std::time::Instant::now() >= deadline {
                // Don't panic — the caller's own `wait_for` assertion
                // will report the specific visibility failure with a
                // better error than "convergence timed out".
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Block until every node's per-group applied watermark has
    /// caught up to the maximum observed across the cluster for that
    /// group. This is the deterministic barrier for "every Raft
    /// group has fully propagated" — replaces the SQL-polling
    /// pattern (`wait_for_async("rows visible from node N", ...)`)
    /// that races the follower applier under load.
    ///
    /// For every group registered on *any* node, the target is
    /// `max(applied_index across all nodes)`. Each node then waits
    /// for that group's local watcher to reach the target. New
    /// groups that show up partway through (e.g. a vshard the test
    /// has not written to yet) are handled by re-snapshotting on
    /// every iteration of the outer poll, so the helper is
    /// idempotent against late-bound group registration.
    ///
    /// Returns once every (node, group) pair has converged or the
    /// deadline expires. On expiry, falls through silently — the
    /// caller's own assertion will surface the specific row-level
    /// failure with a more useful error than "convergence timed
    /// out".
    pub async fn wait_for_full_apply_convergence(&self, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            // Targets: for each group_id seen on any node, take the
            // max applied_index. Asymmetric group membership is
            // expected — replication factor may be < node count, so
            // not every group is hosted on every node.
            let mut targets: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
            for node in &self.nodes {
                for (gid, applied) in node.shared.group_watchers().snapshot() {
                    let entry = targets.entry(gid).or_insert(0);
                    if applied > *entry {
                        *entry = applied;
                    }
                }
            }

            // Group membership is read from the routing table (the
            // authoritative source) rather than inferred from the
            // watcher registry. Inferring from the registry has a
            // cold-start race: a follower that hosts group X but
            // hasn't yet applied its first entry has no registry
            // entry, would be treated as "not hosted", and the
            // helper would return prematurely. Routing knows the
            // members + learners list as soon as the conf-change
            // commits.
            let all_caught_up = {
                let routing = self.nodes[0]
                    .shared
                    .cluster_routing
                    .as_ref()
                    .expect("cluster_routing")
                    .read()
                    .unwrap_or_else(|p| p.into_inner());

                self.nodes.iter().all(|node| {
                    let nid = node.shared.node_id;
                    let watcher = node.shared.group_watchers();
                    targets.iter().all(|(&gid, &target)| {
                        let hosts = routing
                            .group_info(gid)
                            .map(|info| info.members.contains(&nid) || info.learners.contains(&nid))
                            .unwrap_or(false);
                        if !hosts {
                            return true;
                        }
                        watcher.get_or_create(gid).current() >= target
                    })
                })
            };
            if all_caught_up {
                return;
            }
            if std::time::Instant::now() >= deadline {
                // Falls through silently — the caller's own
                // assertion will surface the specific row-level
                // failure with a more useful error than
                // "convergence timed out".
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Cooperatively shut down every node. Reverse order so peers
    /// observe their neighbours' drop without rejecting inbound
    /// traffic on an already-closed transport.
    pub async fn shutdown(self) {
        let mut nodes = self.nodes;
        while let Some(node) = nodes.pop() {
            node.shutdown().await;
        }
    }
}

/// Fast election tuning used by [`TestCluster::spawn_three`] and
/// [`TestCluster::spawn_three_with_cores`].
fn fast_cluster_tuning() -> ClusterTransportTuning {
    ClusterTransportTuning {
        // Fast health pings so the HealthMonitor re-broadcasts
        // topology within ~1s if the initial join broadcast was missed.
        health_ping_interval_secs: 1,
        // Sub-second election windows. Bootstrap defaults are 150/300ms;
        // we allow significantly more headroom (500/1000ms) because
        // integration tests share the host CPU pool with hundreds of
        // unit tests running in parallel — under that load the Raft
        // tick loop can be starved long enough that aggressive
        // 200/500ms windows trigger spurious re-elections mid-test.
        // 500/1000ms is still ~3× faster than the seconds-floor of
        // 1s/2s but stable under contention.
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 1000,
        ..ClusterTransportTuning::default()
    }
}
