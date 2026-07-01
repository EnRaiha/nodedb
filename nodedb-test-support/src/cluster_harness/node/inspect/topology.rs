// SPDX-License-Identifier: BUSL-1.1

//! Raft / topology / routing inspector methods on [`TestClusterNode`].

use crate::cluster_harness::node::lifecycle::TestClusterNode;

impl TestClusterNode {
    /// Number of nodes currently visible in this node's topology view.
    pub fn topology_size(&self) -> usize {
        self.shared
            .cluster_topology
            .as_ref()
            .map(|t| t.read().unwrap_or_else(|p| p.into_inner()).node_count())
            .unwrap_or(0)
    }

    /// Number of nodes in the `Active` state from this node's view. Unlike
    /// [`Self::topology_size`], unreachable / failed peers do NOT count —
    /// this drops as soon as the health subsystem marks a peer down, well
    /// before the ghost sweeper reaps the topology entry. Use this in
    /// tests that need to gate on "peer X is observed dead by survivors"
    /// rather than the much-later ghost reap.
    pub fn active_topology_size(&self) -> usize {
        self.shared
            .cluster_topology
            .as_ref()
            .map(|t| {
                t.read()
                    .unwrap_or_else(|p| p.into_inner())
                    .active_nodes()
                    .len()
            })
            .unwrap_or(0)
    }

    /// Observed metadata-group leader id from this node's local Raft
    /// state, or `0` if no leader is known yet (election in progress).
    /// Polled by the cluster harness `spawn_three()` to gate test
    /// execution on a stable leader — otherwise tests racing the first
    /// election see `not leader (leader hint: None)` errors when CPU
    /// pressure delays the initial heartbeats past topology convergence.
    pub fn metadata_group_leader(&self) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .group_statuses()
            .into_iter()
            .find(|g| g.group_id == nodedb_cluster::METADATA_GROUP_ID)
            .map(|g| g.leader_id)
            .unwrap_or(0)
    }

    /// Snapshot of `(group_id, leader_id)` for every Raft group hosted
    /// on this node. Used by the harness to gate cluster startup on
    /// every group having a stable leader — without this, the first
    /// data-group write (after `spawn_three()` returns) can race the
    /// data-group leader election: the proposer forwards to the
    /// routing-table-hinted leader (still `0` because the data group
    /// hasn't elected yet), local propose returns Ok with a bogus
    /// log_index that nobody has actually committed, the apply path
    /// never finds that index, and the row is silently lost while the
    /// proposer's tracker fires Ok on a no-op or unrelated entry.
    pub fn all_group_leaders(&self) -> Vec<(u64, u64)> {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return Vec::new();
        };
        observer
            .group_status
            .group_statuses()
            .into_iter()
            .map(|g| (g.group_id, g.leader_id))
            .collect()
    }

    /// Highest compacted snapshot index for `group_id` from this node's
    /// local Raft state, or `0` if the group isn't hosted here or hasn't
    /// compacted yet. A non-zero value means the log has been compacted
    /// past the start — a lagging peer below this index can only be caught
    /// up via `InstallSnapshot`, never `AppendEntries`. Used by the
    /// install-snapshot end-to-end test to assert compaction happened on
    /// the leader *before* the learner joined.
    pub fn group_snapshot_index(&self, group_id: u64) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .group_statuses()
            .into_iter()
            .find(|g| g.group_id == group_id)
            .map(|g| g.snapshot_index)
            .unwrap_or(0)
    }

    /// Maximum compacted snapshot index across every **data** Raft group
    /// hosted on this node (i.e. excluding the metadata group, id 0). A
    /// non-zero value proves at least one data group's log has compacted
    /// past the start, so a fresh learner on that group must be caught up
    /// via `InstallSnapshot`.
    pub fn max_data_group_snapshot_index(&self) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .group_statuses()
            .into_iter()
            .filter(|g| g.group_id != nodedb_cluster::METADATA_GROUP_ID)
            .map(|g| g.snapshot_index)
            .max()
            .unwrap_or(0)
    }

    /// Observed data-group (group 1) leader id from this node's local Raft
    /// state, or `0` if no leader is known yet.
    pub fn data_group_leader(&self) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .group_statuses()
            .into_iter()
            .find(|g| g.group_id == 1)
            .map(|g| g.leader_id)
            .unwrap_or(0)
    }

    /// Count of `DocumentOp::BackfillIndex` handler invocations on
    /// this node's Data Plane since startup. A CREATE INDEX against a
    /// cluster must fan out backfill to every node — this counter
    /// exposes whether the local core actually executed the primitive
    /// (a positive value) versus merely replicating Raft state from
    /// the coordinator (counter stays 0).
    pub fn document_index_backfill_count(&self) -> u64 {
        self.shared
            .system_metrics
            .as_ref()
            .map(|m| {
                m.document_index_backfills
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .unwrap_or(0)
    }

    /// Direct accessor for the `applied_index` watermark — used by
    /// the lease tests to assert that the fast-path acquire did NOT
    /// advance raft.
    pub fn metadata_applied_index(&self) -> u64 {
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.applied_index
    }

    /// Force the routing table on this node to point `group_id` at `fake_leader`,
    /// creating a stale route.
    ///
    /// When the gateway on this node next dispatches to `group_id`, it will send
    /// the request to `fake_leader` instead of the real leader. The remote node
    /// (which is NOT the leader for that group) will return `TypedClusterError::NotLeader`,
    /// causing `retry_not_leader` to update the routing table and retry against
    /// the real leader. This is the canonical way to exercise the NotLeader retry
    /// path in tests without needing a real leadership change (which is slow and
    /// flaky).
    pub fn force_stale_route_for_test(&self, group_id: u64, fake_leader: u64) {
        if let Some(ref routing) = self.shared.cluster_routing {
            let mut table = routing.write().unwrap_or_else(|p| p.into_inner());
            table.set_leader(group_id, fake_leader);
        }
    }

    /// Read the current `not_leader_retry_count` from this node's shared gateway.
    ///
    /// Returns 0 if the gateway has not been constructed yet (shouldn't happen
    /// in tests since the harness wires the gateway during spawn).
    pub fn not_leader_retry_count(&self) -> u64 {
        self.shared
            .gateway
            .as_ref()
            .map(|gw| gw.not_leader_retry_count())
            .unwrap_or(0)
    }
}
