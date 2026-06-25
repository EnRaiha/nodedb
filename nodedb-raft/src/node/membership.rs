// SPDX-License-Identifier: BUSL-1.1

//! Dynamic group membership mutation.
//!
//! Split out from [`super::core`] so the struct/constructor file stays
//! focused on state; this file owns everything that mutates the voter or
//! learner set at runtime. All mutations also update the `LeaderState`
//! per-peer replication tracking when the node is currently the leader,
//! so a newly added peer immediately starts receiving `AppendEntries`.

use tracing::info;

use crate::state::NodeRole;
use crate::storage::LogStorage;

use super::core::RaftNode;

impl<S: LogStorage> RaftNode<S> {
    /// Replace the voter list wholesale.
    ///
    /// Computes the diff against the previous voter list and updates
    /// `LeaderState` per-peer tracking for added/removed voters (only if
    /// this node is currently the leader). Learners are not touched.
    pub(super) fn set_voters(&mut self, new_voters: Vec<u64>) {
        let last_index = self.log.last_index();

        if let Some(ref mut leader) = self.leader_state {
            for &peer in &new_voters {
                if !self.config.peers.contains(&peer) && !self.config.learners.contains(&peer) {
                    leader.add_peer(peer, last_index);
                    info!(
                        node = self.config.node_id,
                        group = self.config.group_id,
                        peer,
                        "added voter to leader tracking"
                    );
                }
            }
            for &peer in &self.config.peers {
                if !new_voters.contains(&peer) && !self.config.learners.contains(&peer) {
                    leader.remove_peer(peer);
                    info!(
                        node = self.config.node_id,
                        group = self.config.group_id,
                        peer,
                        "removed voter from leader tracking"
                    );
                }
            }
        }

        self.config.peers = new_voters;
    }

    /// Add a single voter peer to this group.
    ///
    /// No-op if `peer` is self, already a voter, or currently a learner
    /// (use [`promote_learner`] to convert a learner into a voter).
    ///
    /// [`promote_learner`]: Self::promote_learner
    pub fn add_peer(&mut self, peer: u64) {
        if peer == self.config.node_id
            || self.config.peers.contains(&peer)
            || self.config.learners.contains(&peer)
        {
            return;
        }
        let mut new_peers = self.config.peers.clone();
        new_peers.push(peer);
        self.set_voters(new_peers);
    }

    /// Remove a voter peer from this group.
    pub fn remove_peer(&mut self, peer: u64) {
        if !self.config.peers.contains(&peer) {
            return;
        }
        let new_peers: Vec<u64> = self
            .config
            .peers
            .iter()
            .copied()
            .filter(|&id| id != peer)
            .collect();
        self.set_voters(new_peers);
    }

    /// Add a non-voting learner peer.
    ///
    /// Learners receive replicated log entries but do not vote and do not
    /// count toward the commit quorum. If this node is currently the
    /// leader, the learner is immediately added to `LeaderState`
    /// replication tracking so the next heartbeat ships entries to it.
    ///
    /// No-op if `peer` is self, already a voter, or already a learner.
    pub fn add_learner(&mut self, peer: u64) {
        if peer == self.config.node_id
            || self.config.peers.contains(&peer)
            || self.config.learners.contains(&peer)
        {
            return;
        }

        let last_index = self.log.last_index();
        if let Some(ref mut leader) = self.leader_state {
            leader.add_peer(peer, last_index);
        }
        self.config.learners.push(peer);

        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            peer,
            "added learner peer"
        );
    }

    /// Remove a learner peer (e.g., join was rolled back before promotion).
    pub fn remove_learner(&mut self, peer: u64) {
        if !self.config.learners.contains(&peer) {
            return;
        }
        if let Some(ref mut leader) = self.leader_state {
            leader.remove_peer(peer);
        }
        self.config.learners.retain(|&id| id != peer);

        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            peer,
            "removed learner peer"
        );
    }

    /// Promote an existing learner to a full voter.
    ///
    /// Called on the leader after observing the learner has caught up
    /// (its `match_index` >= the group's `commit_index`). The `LeaderState`
    /// entry is left in place — it already tracks the peer's next/match
    /// index — but the peer now counts toward the commit quorum.
    ///
    /// Returns `true` if the promotion happened, `false` if `peer` was not
    /// a learner.
    pub fn promote_learner(&mut self, peer: u64) -> bool {
        if !self.config.learners.contains(&peer) {
            return false;
        }
        self.config.learners.retain(|&id| id != peer);
        if !self.config.peers.contains(&peer) {
            self.config.peers.push(peer);
        }

        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            peer,
            "promoted learner to voter"
        );
        true
    }

    /// Promote *this* node from learner to voter role.
    ///
    /// Used when a follow-up conf change committed the local node's
    /// promotion — the node transitions out of `Learner` role so its
    /// subsequent ticks will run election timeouts like a normal follower.
    pub fn promote_self_to_voter(&mut self) {
        if self.role == NodeRole::Learner {
            self.role = NodeRole::Follower;
            self.config.starts_as_learner = false;
            // Election deadline is already set from `new()`; leave it.
            info!(
                node = self.config.node_id,
                group = self.config.group_id,
                "promoted self from learner to follower"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;
    use std::time::{Duration, Instant};

    fn cfg(node_id: u64, peers: Vec<u64>) -> RaftConfig {
        RaftConfig {
            node_id,
            group_id: 0,
            peers,
            learners: vec![],
            observers: vec![],
            starts_as_learner: false,
            starts_as_observer: false,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            log_compaction_threshold: None,
        }
    }

    fn force_leader(node: &mut RaftNode<MemStorage>) {
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        // Drain vote messages.
        let _ = node.take_ready();
        // Reply to own candidacy (for multi-voter configs, skip).
    }

    #[test]
    fn add_learner_does_not_change_quorum() {
        // Start: self + 2 voters. Quorum = 2 (out of 3).
        let mut node = RaftNode::new(cfg(1, vec![2, 3]), MemStorage::new());
        assert_eq!(node.config.quorum(), 2);

        node.add_learner(4);
        assert_eq!(node.learners(), &[4]);
        // Quorum must NOT include the learner.
        assert_eq!(node.config.quorum(), 2);
        assert_eq!(node.config.cluster_size(), 3);
    }

    #[test]
    fn promote_learner_grows_quorum() {
        let mut node = RaftNode::new(cfg(1, vec![2]), MemStorage::new());
        assert_eq!(node.config.quorum(), 2); // 2 voters → quorum 2.

        node.add_learner(3);
        assert_eq!(node.config.quorum(), 2);

        let promoted = node.promote_learner(3);
        assert!(promoted);
        assert_eq!(node.voters(), &[2, 3]);
        assert!(node.learners().is_empty());
        // 3 voters → quorum 2.
        assert_eq!(node.config.cluster_size(), 3);
        assert_eq!(node.config.quorum(), 2);
    }

    #[test]
    fn remove_learner_drops_peer() {
        let mut node = RaftNode::new(cfg(1, vec![2]), MemStorage::new());
        node.add_learner(3);
        assert_eq!(node.learners(), &[3]);
        node.remove_learner(3);
        assert!(node.learners().is_empty());
    }

    #[test]
    fn add_learner_on_leader_starts_tracking() {
        // Single-voter cluster — self becomes leader on first tick.
        let mut node = RaftNode::new(cfg(1, vec![]), MemStorage::new());
        force_leader(&mut node);
        assert_eq!(node.role(), NodeRole::Leader);

        // Propose something so last_index > 0.
        let _ = node.propose(b"x".to_vec()).unwrap();
        let _ = node.take_ready();

        node.add_learner(2);
        // Leader now tracks peer 2's match_index (initially 0).
        assert_eq!(node.match_index_for(2), Some(0));

        // Replicating to all should include peer 2 in outgoing messages.
        node.replicate_to_all();
        let ready = node.take_ready();
        let targets: Vec<u64> = ready.messages.iter().map(|(p, _)| *p).collect();
        assert!(
            targets.contains(&2),
            "learner should receive AE, got {targets:?}"
        );
    }

    #[test]
    fn promote_self_flips_role() {
        let mut c = cfg(2, vec![1]);
        c.starts_as_learner = true;
        let mut node = RaftNode::new(c, MemStorage::new());
        assert_eq!(node.role(), NodeRole::Learner);
        node.promote_self_to_voter();
        assert_eq!(node.role(), NodeRole::Follower);
    }
}
