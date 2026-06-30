// SPDX-License-Identifier: BUSL-1.1

//! Raft configuration-change propose/apply with learner semantics.
//!
//! `propose_conf_change` writes a `ConfChange` payload (see
//! `crate::conf_change::ConfChange`) into the group leader's Raft log as a
//! regular entry with a special prefix byte. The entry replicates via the
//! normal `AppendEntries` channel; no new transport is needed.
//!
//! `apply_conf_change` is called by the tick loop when a committed entry
//! is identified as a conf change. It updates both the in-memory
//! `RaftNode` peer set and the `RoutingTable`:
//!
//! - `AddNode` → voter added to `RaftNode.peers` and `routing.members`.
//! - `RemoveNode` → voter removed from both.
//! - `AddLearner` → learner added to `RaftNode.learners` and `routing.learners`.
//! - `PromoteLearner` → learner moved from `learners` to `members` in both;
//!   if the promoted peer is *this* node, also flips the local role from
//!   `Learner` to `Follower`.
//! - `RemoveLearner` → learner removed from `RaftNode.learners` and
//!   `routing.learners`; voters (members) are not touched.

use tracing::debug;

use crate::conf_change::{ConfChange, ConfChangeType};
use crate::error::{ClusterError, Result};

use super::core::MultiRaft;

impl MultiRaft {
    /// Propose a configuration change to a Raft group.
    ///
    /// The change is serialized into the group's Raft log as a
    /// regular entry with a distinguishing prefix byte. It
    /// replicates through the normal `AppendEntries` path and is
    /// applied by every follower replica when the entry commits
    /// (see `apply_conf_change`).
    ///
    /// # Single-voter vs. multi-voter groups
    ///
    /// Single-voter groups commit inside `node.propose` itself
    /// (see `nodedb_raft::node::RaftNode::propose` single-voter
    /// branch). In that case the commit has already happened by
    /// the time we return, so we safely apply the change inline:
    /// any caller that reads routing immediately after the
    /// propose sees the final state.
    ///
    /// Multi-voter groups commit asynchronously once enough
    /// followers have replicated the entry. The apply then
    /// happens on the tick loop after it observes the updated
    /// `commit_index`. We MUST NOT inline-apply in that case —
    /// if the leader steps down before replication completes, a
    /// new leader may truncate the log entry and the local state
    /// would be permanently ahead of the committed state with no
    /// rollback path. Callers that need to wait for the apply
    /// should poll the routing table (see
    /// `raft_loop::join::wait_for_routing_contains_learner`).
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose_conf_change(
        &mut self,
        group_id: u64,
        change: &ConfChange,
    ) -> Result<(u64, u64)> {
        let (log_index, committed_immediately) = {
            let node = self
                .groups
                .get_mut(&group_id)
                .ok_or(ClusterError::GroupNotFound { group_id })?;
            let data = change.to_entry_data()?;
            let log_index = node.propose(data)?;
            // A single-voter group self-commits inside `propose`:
            // its `commit_index` is bumped to the new `log_index`
            // before we return. Detecting this is the one safe
            // trigger for an inline apply.
            let committed_immediately = node.commit_index() >= log_index;
            (log_index, committed_immediately)
        };

        if committed_immediately {
            self.apply_conf_change(group_id, change)?;
        }
        Ok((group_id, log_index))
    }

    /// Apply a committed configuration change to this node's view of the
    /// given Raft group.
    ///
    /// This is called from the tick loop for every committed entry
    /// detected as a conf-change (via `ConfChange::from_entry_data`). It
    /// must be idempotent with respect to no-op changes so replaying the
    /// log after a crash does not double-apply.
    pub fn apply_conf_change(&mut self, group_id: u64, change: &ConfChange) -> Result<()> {
        let self_node_id = self.node_id;

        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;

        match change.change_type {
            ConfChangeType::AddNode => {
                // Direct voter add (used for legacy or bootstrap paths).
                node.add_peer(change.node_id);
                // One write guard serves both the `group_info` read and the
                // `set_group_members` write — taking a read guard first then
                // a write guard on the same RwLock would deadlock.
                let mut rt = self.routing.write().unwrap_or_else(|p| p.into_inner());
                if let Some(info) = rt.group_info(group_id)
                    && !info.members.contains(&change.node_id)
                {
                    let mut new_members = info.members.clone();
                    new_members.push(change.node_id);
                    rt.set_group_members(group_id, new_members);
                }
            }
            ConfChangeType::RemoveNode => {
                node.remove_peer(change.node_id);
                let mut rt = self.routing.write().unwrap_or_else(|p| p.into_inner());
                if let Some(info) = rt.group_info(group_id) {
                    let new_members: Vec<u64> = info
                        .members
                        .iter()
                        .copied()
                        .filter(|&id| id != change.node_id)
                        .collect();
                    rt.set_group_members(group_id, new_members);
                }
            }
            ConfChangeType::AddLearner => {
                // Non-voting add: peer enters learners on both the
                // RaftNode and the routing table. Voting quorum does not
                // change.
                node.add_learner(change.node_id);
                self.routing
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .add_group_learner(group_id, change.node_id);
            }
            ConfChangeType::PromoteLearner => {
                // Learner → voter. RaftNode and routing both update.
                // If this is our own promotion, we also need to flip the
                // local role from `Learner` to `Follower` so subsequent
                // ticks run election timeouts normally.
                if change.node_id == self_node_id {
                    // A learner-start node intentionally does not store itself in
                    // `RaftConfig.learners`, so `promote_learner(self)` cannot be
                    // the gate for updating its routing snapshot.
                    node.promote_self_to_voter();
                } else {
                    node.promote_learner(change.node_id);
                }
                // The committed conf change is authoritative. This is idempotent
                // and also handles self-promotion on a joining replica.
                self.routing
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .promote_group_learner(group_id, change.node_id);
            }
            ConfChangeType::RemoveLearner => {
                // Non-voting removal: safe at any time — learners are not in
                // quorum, commit, or election paths.
                node.remove_learner(change.node_id);
                self.routing
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove_group_learner(group_id, change.node_id);
            }
        }

        debug!(
            node = self.node_id,
            group = group_id,
            change_type = ?change.change_type,
            target_node = change.node_id,
            voters = ?self.groups.get(&group_id).map(|n| n.voters().to_vec()),
            learners = ?self.groups.get(&group_id).map(|n| n.learners().to_vec()),
            "applied conf change"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RoutingTable;
    use nodedb_raft::NodeRole;

    use super::super::core::MultiRaft;

    fn new_mr(node_id: u64, group_ids: &[u64]) -> MultiRaft {
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(group_ids.len() as u64, &[node_id], 1);
        let mut mr = MultiRaft::new(node_id, rt, dir.path().to_path_buf());
        std::mem::forget(dir); // Keep temp dir alive for the duration of the test.
        for &gid in group_ids {
            mr.add_group(gid, vec![]).unwrap();
        }
        mr
    }

    #[test]
    fn apply_add_learner_updates_routing_and_raftnode() {
        let mut mr = new_mr(1, &[0]);
        let change = ConfChange {
            change_type: ConfChangeType::AddLearner,
            node_id: 2,
        };
        mr.apply_conf_change(0, &change).unwrap();

        // RaftNode: learner tracked, voters unchanged.
        let node = mr.groups.get(&0).unwrap();
        assert_eq!(node.learners(), &[2]);
        assert!(node.voters().is_empty());

        // Routing: learners populated, members untouched.
        let rt = mr.routing();
        let rt = rt.read().unwrap();
        let info = rt.group_info(0).unwrap();
        assert_eq!(info.learners, vec![2]);
        assert_eq!(info.members, vec![1]); // Self.
    }

    #[test]
    fn apply_promote_learner_moves_peer_to_voters() {
        let mut mr = new_mr(1, &[0]);
        mr.apply_conf_change(
            0,
            &ConfChange {
                change_type: ConfChangeType::AddLearner,
                node_id: 2,
            },
        )
        .unwrap();
        mr.apply_conf_change(
            0,
            &ConfChange {
                change_type: ConfChangeType::PromoteLearner,
                node_id: 2,
            },
        )
        .unwrap();

        let node = mr.groups.get(&0).unwrap();
        assert_eq!(node.voters(), &[2]);
        assert!(node.learners().is_empty());

        let rt = mr.routing();
        let rt = rt.read().unwrap();
        let info = rt.group_info(0).unwrap();
        assert_eq!(info.learners, Vec::<u64>::new());
        assert!(info.members.contains(&2));
    }

    #[test]
    fn apply_promote_self_flips_role() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = RoutingTable::uniform(1, &[1], 1);
        rt.add_group_learner(0, 2);
        let mut mr = MultiRaft::new(2, rt, dir.path().to_path_buf());
        mr.add_group_as_learner(0, vec![1], vec![]).unwrap();

        mr.apply_conf_change(
            0,
            &ConfChange {
                change_type: ConfChangeType::PromoteLearner,
                node_id: 2,
            },
        )
        .unwrap();

        assert_eq!(mr.groups.get(&0).unwrap().role(), NodeRole::Follower);
        let rt = mr.routing();
        let rt = rt.read().unwrap();
        let info = rt.group_info(0).unwrap();
        assert_eq!(info.members, vec![1, 2]);
        assert!(info.learners.is_empty());
    }

    #[test]
    fn apply_remove_learner_drops_from_learners_only() {
        let mut mr = new_mr(1, &[0]);
        // Add learner first.
        mr.apply_conf_change(
            0,
            &ConfChange {
                change_type: ConfChangeType::AddLearner,
                node_id: 2,
            },
        )
        .unwrap();

        // Confirm it's present.
        assert_eq!(mr.groups.get(&0).unwrap().learners(), &[2]);

        // Remove the learner.
        mr.apply_conf_change(
            0,
            &ConfChange {
                change_type: ConfChangeType::RemoveLearner,
                node_id: 2,
            },
        )
        .unwrap();

        // RaftNode: learner gone, voters untouched.
        let node = mr.groups.get(&0).unwrap();
        assert!(node.learners().is_empty());
        assert!(node.voters().is_empty());

        // Routing: learners empty, members (self) untouched.
        let rt = mr.routing();
        let rt = rt.read().unwrap();
        let info = rt.group_info(0).unwrap();
        assert!(info.learners.is_empty());
        assert_eq!(info.members, vec![1]);
    }

    #[test]
    fn apply_remove_learner_noop_for_voter_and_absent() {
        let mut mr = new_mr(1, &[0]);

        // Removing a voter via RemoveLearner must be a no-op (does not
        // touch the voter list).
        mr.apply_conf_change(
            0,
            &ConfChange {
                change_type: ConfChangeType::RemoveLearner,
                node_id: 1,
            },
        )
        .unwrap();
        let rt = mr.routing();
        let rt = rt.read().unwrap();
        let info = rt.group_info(0).unwrap();
        assert_eq!(
            info.members,
            vec![1],
            "voter must not be removed by RemoveLearner"
        );

        // Removing an absent peer must also be a no-op.
        drop(rt);
        mr.apply_conf_change(
            0,
            &ConfChange {
                change_type: ConfChangeType::RemoveLearner,
                node_id: 99,
            },
        )
        .unwrap();
    }
}
