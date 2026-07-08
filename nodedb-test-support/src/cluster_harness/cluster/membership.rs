// SPDX-License-Identifier: BUSL-1.1

//! Post-spawn membership growth (`add_learner_node`), DDL dispatch
//! (`exec_ddl_on_any_leader`), the convergence barriers backing both,
//! and cooperative shutdown.

use std::time::Duration;

use super::TestCluster;
use crate::cluster_harness::node::TestClusterNode;
use crate::cluster_harness::wait::wait_for;

impl TestCluster {
    /// Add a fresh node to the running cluster as a learner and return a
    /// reference to it.
    ///
    /// This drives the **production** runtime membership path end to end —
    /// no hand-copied state:
    ///
    /// 1. Spawn a brand-new full node (fresh temp data dir, its own
    ///    ephemeral QUIC + pgwire ports, 1 core, the SAME cluster config as
    ///    the original members — including the low
    ///    `log_compaction_threshold`). Its only seed is node 1's pre-bound
    ///    listen address. Because the node has no prior catalog, the
    ///    cluster-init dispatcher takes the **join** path: the node sends a
    ///    `RaftRpc::JoinRequest` to the seed.
    ///
    /// 2. The seed's `RaftLoop::join_flow` (running on its live transport)
    ///    is the real conf-change driver:
    ///    - registers the joiner's address on the leader's transport
    ///      (`transport.register_peer`) so the leader can immediately
    ///      `AppendEntries` / `InstallSnapshot` to it,
    ///    - admits it into topology, then proposes
    ///      `ConfChange::AddLearner(new_node_id)` on **every** Raft group
    ///      the node is missing (metadata + every data group),
    ///    - waits for each conf-change to commit, persists topology +
    ///      routing, and broadcasts a `TopologyUpdate` to every active peer
    ///      so the *other* existing nodes also learn the joiner's address
    ///      (peer wiring in both directions).
    ///
    ///    The `JoinResponse` carries the post-`AddLearner` routing, and the
    ///    joining node reconstructs its local `MultiRaft` with each data
    ///    group started in the `Learner` role (`add_group_as_learner`).
    ///
    /// 3. The learner now lives in every group as a non-voter. On the next
    ///    leader tick, the leader sees the learner's `next_index` is below
    ///    its compacted `snapshot_index`, so it cannot use `AppendEntries`
    ///    and instead builds a real per-group snapshot via the installed
    ///    `DataPlaneSnapshotBuilder` and streams it with `InstallSnapshot`.
    ///    The learner applies it through the `DataPlaneSnapshotApplier`.
    ///
    /// This method blocks until the new node is visible in every node's
    /// topology and every Raft group has fully propagated (via
    /// [`Self::wait_for_full_apply_convergence`]). It does **not** assert
    /// the data is present — that is the caller's job (query the learner's
    /// own pgwire client).
    ///
    /// Returns a reference to the newly-added node (the last entry in
    /// [`Self::nodes`]).
    pub async fn add_learner_node(
        &mut self,
    ) -> Result<&TestClusterNode, Box<dyn std::error::Error + Send + Sync>> {
        let new_node_id = self.nodes.iter().map(|n| n.node_id).max().unwrap_or(0) + 1;
        let seeds = vec![self.nodes[0].listen_addr];
        let cfg = self.spawn_config.clone();

        let learner = TestClusterNode::spawn_with_full_config(new_node_id, seeds, &cfg).await?;

        self.nodes.push(learner);
        let expected = self.nodes.len();

        // Wait until every node (including the new learner) sees the full
        // membership. The join broadcasts a TopologyUpdate to existing
        // peers, so this converges once that propagates.
        wait_for(
            "every node sees the new learner in topology",
            Duration::from_secs(30),
            Duration::from_millis(50),
            || self.nodes.iter().all(|n| n.topology_size() == expected),
        )
        .await;

        // Wait until every Raft group has fully propagated to every member /
        // learner. For the learner this only completes once the leader's
        // InstallSnapshot has been applied (its data-group log starts beyond
        // the compacted region, so AppendEntries alone cannot advance it).
        self.wait_for_full_apply_convergence(Duration::from_secs(30))
            .await;

        Ok(self.nodes.last().expect("learner just pushed"))
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
