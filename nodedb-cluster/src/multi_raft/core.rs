// SPDX-License-Identifier: BUSL-1.1

//! `MultiRaft` struct, constructors, group lifecycle, tick, observability.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use tracing::info;

use nodedb_raft::node::RaftConfig;
use nodedb_raft::{RaftNode, Ready};

use crate::error::{ClusterError, Result};
use crate::raft_storage::RedbLogStorage;
use crate::routing::RoutingTable;

/// Snapshot of a single Raft group's state for observability.
#[derive(Debug, Clone, serde::Serialize)]
pub struct GroupStatus {
    pub group_id: u64,
    /// Role as a human-readable string ("Leader", "Follower", "Candidate", "Learner").
    pub role: String,
    pub leader_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    pub member_count: usize,
    pub learner_count: usize,
    pub vshard_count: usize,
}

/// Membership snapshot for a hosted Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMembership {
    pub group_id: u64,
    pub leader_id: u64,
    /// Voting members, including this node when it is a voter.
    pub voters: Vec<u64>,
    /// Non-voting learners, including this node when it is a learner.
    pub learners: Vec<u64>,
}

/// Multi-Raft coordinator managing multiple Raft groups on a single node.
///
/// This coordinator:
/// - Manages all Raft groups hosted on this node
/// - Batches heartbeats across groups sharing the same leader
/// - Routes incoming RPCs to the correct group
/// - Collects `Ready` output from all groups for the caller to execute
pub struct MultiRaft {
    /// This node's ID.
    pub(super) node_id: u64,
    /// Raft groups hosted on this node (group_id → RaftNode).
    pub(super) groups: HashMap<u64, RaftNode<RedbLogStorage>>,
    /// Routing table (vShard → group mapping).
    pub(super) routing: RoutingTable,
    /// Default election timeout range.
    pub(super) election_timeout_min: Duration,
    pub(super) election_timeout_max: Duration,
    /// Heartbeat interval.
    pub(super) heartbeat_interval: Duration,
    /// Data directory for persistent Raft log storage.
    pub(super) data_dir: PathBuf,
}

/// Aggregated output from all Raft groups after a tick.
#[derive(Debug, Default)]
pub struct MultiRaftReady {
    /// Per-group ready output: (group_id, Ready).
    pub groups: Vec<(u64, Ready)>,
}

impl MultiRaftReady {
    pub fn is_empty(&self) -> bool {
        self.groups.iter().all(|(_gid, r)| r.is_empty())
    }

    /// Total committed entries across all groups.
    pub fn total_committed(&self) -> usize {
        self.groups
            .iter()
            .map(|(_, r)| r.committed_entries.len())
            .sum()
    }
}

impl MultiRaft {
    pub fn new(node_id: u64, routing: RoutingTable, data_dir: PathBuf) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
            routing,
            election_timeout_min: Duration::from_secs(2),
            election_timeout_max: Duration::from_secs(5),
            heartbeat_interval: Duration::from_millis(50),
            data_dir,
        }
    }

    /// Configure election timeout range.
    pub fn with_election_timeout(mut self, min: Duration, max: Duration) -> Self {
        self.election_timeout_min = min;
        self.election_timeout_max = max;
        self
    }

    /// Configure heartbeat interval.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Initialize a Raft group on this node as a voting member.
    ///
    /// `peers` is the list of other voters in the group (excluding self).
    /// For a learner-start group, use `add_group_as_learner` instead.
    pub fn add_group(&mut self, group_id: u64, peers: Vec<u64>) -> Result<()> {
        self.add_group_inner(group_id, peers, vec![], false)
    }

    /// Initialize a Raft group on this node as a non-voting learner.
    ///
    /// The local node boots in the `Learner` role and will not stand for
    /// election until it is promoted by a `PromoteLearner` conf change.
    ///
    /// `voters` is the full voter set of the group (excluding self).
    /// `learners` is the learner set of the group excluding self — usually
    /// empty unless multiple learners are being admitted in the same round.
    pub fn add_group_as_learner(
        &mut self,
        group_id: u64,
        voters: Vec<u64>,
        learners: Vec<u64>,
    ) -> Result<()> {
        self.add_group_inner(group_id, voters, learners, true)
    }

    fn add_group_inner(
        &mut self,
        group_id: u64,
        peers: Vec<u64>,
        learners: Vec<u64>,
        starts_as_learner: bool,
    ) -> Result<()> {
        let config = RaftConfig {
            node_id: self.node_id,
            group_id,
            peers,
            learners,
            observers: vec![],
            starts_as_learner,
            starts_as_observer: false,
            election_timeout_min: self.election_timeout_min,
            election_timeout_max: self.election_timeout_max,
            heartbeat_interval: self.heartbeat_interval,
        };

        let storage_path = self.data_dir.join(format!("raft/group-{group_id}.redb"));
        let storage = RedbLogStorage::open(&storage_path).map_err(|e| ClusterError::Transport {
            detail: format!("failed to open raft storage for group {group_id}: {e}"),
        })?;
        let node = RaftNode::new(config, storage);
        self.groups.insert(group_id, node);

        info!(
            node = self.node_id,
            group = group_id,
            as_learner = starts_as_learner,
            path = %storage_path.display(),
            "added raft group with persistent storage"
        );
        Ok(())
    }

    /// Tick all Raft groups. Returns aggregated ready output.
    pub fn tick(&mut self) -> MultiRaftReady {
        let mut ready = MultiRaftReady::default();

        for (&group_id, node) in &mut self.groups {
            node.tick();
            let r = node.take_ready();
            if !r.is_empty() {
                ready.groups.push((group_id, r));
            }
        }

        ready
    }

    pub fn routing(&self) -> &RoutingTable {
        &self.routing
    }

    pub fn routing_mut(&mut self) -> &mut RoutingTable {
        &mut self.routing
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Whether this node hosts the given Raft group.
    pub fn contains_group(&self, group_id: u64) -> bool {
        self.groups.contains_key(&group_id)
    }

    /// IDs of every Raft group hosted on this node, including groups
    /// that do not own vShards (for example the Calvin sequencer).
    pub fn group_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.groups.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Snapshot the actual Raft membership rather than the vShard routing view.
    pub fn group_membership(&self, group_id: u64) -> Option<GroupMembership> {
        let node = self.groups.get(&group_id)?;
        let mut voters = node.voters().to_vec();
        let mut learners = node.learners().to_vec();
        match node.role() {
            nodedb_raft::NodeRole::Learner => learners.push(self.node_id),
            nodedb_raft::NodeRole::Observer => {}
            _ => voters.push(self.node_id),
        }
        voters.sort_unstable();
        voters.dedup();
        learners.sort_unstable();
        learners.dedup();
        Some(GroupMembership {
            group_id,
            leader_id: node.leader_id(),
            voters,
            learners,
        })
    }

    /// Mutable access to the underlying Raft groups (for testing / bootstrap).
    pub fn groups_mut(&mut self) -> &mut HashMap<u64, RaftNode<RedbLogStorage>> {
        &mut self.groups
    }

    /// Snapshot of all Raft group states for observability.
    pub fn group_statuses(&self) -> Vec<GroupStatus> {
        let mut statuses = Vec::with_capacity(self.groups.len());
        for (&group_id, node) in &self.groups {
            let vshard_count = self.routing.vshards_for_group(group_id).len();
            let self_is_voter = !matches!(
                node.role(),
                nodedb_raft::NodeRole::Learner | nodedb_raft::NodeRole::Observer
            );

            statuses.push(GroupStatus {
                group_id,
                role: format!("{:?}", node.role()),
                leader_id: node.leader_id(),
                term: node.current_term(),
                commit_index: node.commit_index(),
                last_applied: node.last_applied(),
                last_log_index: node.last_log_index(),
                member_count: node.voters().len() + usize::from(self_is_voter),
                learner_count: node.learners().len()
                    + usize::from(node.role() == nodedb_raft::NodeRole::Learner),
                vshard_count,
            });
        }
        statuses.sort_by_key(|s| s.group_id);
        statuses
    }

    /// Get the leader for a given vShard (from local group state).
    pub fn leader_for_vshard(&self, vshard_id: u32) -> Result<Option<u64>> {
        let group_id = self.routing.group_for_vshard(vshard_id)?;
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let lid = node.leader_id();
        Ok(if lid == 0 { None } else { Some(lid) })
    }

    /// Propose a command to the Raft group that owns the given vShard.
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose(&mut self, vshard_id: u32, data: Vec<u8>) -> Result<(u64, u64)> {
        let group_id = self.routing.group_for_vshard(vshard_id)?;
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let log_index = node.propose(data)?;
        Ok((group_id, log_index))
    }

    /// Returns `true` if this node is currently the leader of `group_id`.
    ///
    /// Returns `false` when the group does not exist on this node or when the
    /// node is a follower, candidate, or learner in the group.
    pub fn is_group_leader(&self, group_id: u64) -> bool {
        use nodedb_raft::state::NodeRole;
        self.groups
            .get(&group_id)
            .map(|n| n.role() == NodeRole::Leader)
            .unwrap_or(false)
    }

    /// Propose a command directly to a specific Raft group (e.g. the
    /// metadata group, which has no vShard mapping).
    ///
    /// Returns the committed log index on success.
    pub fn propose_to_group(&mut self, group_id: u64, data: Vec<u8>) -> Result<u64> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        Ok(node.propose(data)?)
    }

    /// Read committed log entries for a Raft group in the inclusive index
    /// range `[lo, hi]`.
    ///
    /// `hi` is clamped to the group's `commit_index` so callers that pass
    /// `u64::MAX` never read uncommitted entries.
    ///
    /// Used by the Calvin scheduler's rebuild path to replay sequenced
    /// transactions from the sequencer Raft log after a restart.
    ///
    /// Returns `Err(ClusterError::Raft(RaftError::LogCompacted))` if `lo`
    /// has been compacted into a snapshot (caller must install a snapshot
    /// instead of replaying from log).
    pub fn read_committed_entries(
        &self,
        group_id: u64,
        lo: u64,
        hi: u64,
    ) -> Result<Vec<nodedb_raft::message::LogEntry>> {
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let entries = node.log_entries_range(lo, hi)?;
        Ok(entries.to_vec())
    }
}

// Re-export LogEntry so callers of `read_committed_entries` can name the type.
pub use nodedb_raft::LogEntry;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn single_node_multi_raft() {
        let dir = tempfile::tempdir().unwrap();
        // uniform(4, ...) creates 4 data groups (1..=4) plus metadata group 0.
        let rt = RoutingTable::uniform(4, &[1], 1);
        let mut mr = MultiRaft::new(1, rt.clone(), dir.path().to_path_buf());

        for gid in rt.group_ids() {
            mr.add_group(gid, vec![]).unwrap();
        }
        // 4 data groups + 1 metadata group.
        assert_eq!(mr.group_count(), 5);

        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let ready = mr.tick();
        assert_eq!(ready.groups.len(), 5);
    }

    #[test]
    fn propose_routes_to_correct_group() {
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(4, &[1], 1);
        let mut mr = MultiRaft::new(1, rt.clone(), dir.path().to_path_buf());

        for gid in rt.group_ids() {
            mr.add_group(gid, vec![]).unwrap();
        }
        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        mr.tick();
        for (gid, ready) in mr.tick().groups {
            if let Some(last) = ready.committed_entries.last() {
                mr.advance_applied(gid, last.index).unwrap();
            }
        }

        // vshard 0 maps to data group 1, vshard 256 also maps to group 1 (256 % 4 + 1 = 1).
        let (_gid, idx) = mr.propose(0, b"cmd-shard-0".to_vec()).unwrap();
        assert!(idx > 0);

        let (_gid, idx) = mr.propose(256, b"cmd-shard-256".to_vec()).unwrap();
        assert!(idx > 0);
    }

    #[test]
    fn add_group_as_learner_starts_in_learner_role() {
        use nodedb_raft::NodeRole;
        let dir = tempfile::tempdir().unwrap();
        // uniform(1, ...) creates data group 1 plus metadata group 0.
        let rt = RoutingTable::uniform(1, &[1, 2], 2);
        let mut mr = MultiRaft::new(2, rt, dir.path().to_path_buf());

        // Data group 1: join as learner (node 1 is the voter, we're node 2 = learner).
        mr.add_group_as_learner(1, vec![1], vec![]).unwrap();

        let node = mr.groups.get(&1).unwrap();
        assert_eq!(node.role(), NodeRole::Learner);
        assert_eq!(node.voters(), &[1]);
    }

    #[test]
    fn group_membership_includes_non_routing_learner_group() {
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(2, rt, dir.path().to_path_buf());
        let non_routing_group = u64::MAX - 7;
        mr.add_group_as_learner(non_routing_group, vec![1], vec![])
            .unwrap();

        assert!(mr.group_ids().contains(&non_routing_group));
        assert_eq!(
            mr.group_membership(non_routing_group),
            Some(GroupMembership {
                group_id: non_routing_group,
                leader_id: 0,
                voters: vec![1],
                learners: vec![2],
            })
        );
    }
}
