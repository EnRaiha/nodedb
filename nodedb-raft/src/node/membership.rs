// SPDX-License-Identifier: BUSL-1.1

//! Dynamic group membership mutation.
//!
//! Split out from [`super::core`] so the struct/constructor file stays
//! focused on state; this file owns everything that mutates the voter or
//! learner set at runtime. All mutations also update the `LeaderState`
//! per-peer replication tracking when the node is currently the leader,
//! so a newly added peer immediately starts receiving `AppendEntries`.

use std::time::Instant;

use tracing::info;

use crate::error::{RaftError, Result};
use crate::message::TimeoutNowRequest;
use crate::state::{LeadershipTransfer, NodeRole};
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
    /// Send `peer` one last `AppendEntries` before it is dropped from the
    /// configuration.
    ///
    /// A node learns an entry is committed only from a later `AppendEntries`
    /// carrying an advanced `leader_commit`. The entry that removes a node is
    /// no exception — but by the time it commits, the leader is about to stop
    /// replicating to that node, so the ordinary path never delivers the news.
    /// The removed node is then left holding its own removal uncommitted
    /// forever: it never applies the change, and every view it derives from
    /// that log stays stale for a group it has already left.
    ///
    /// Call this immediately BEFORE [`Self::remove_peer`], while `peer` is
    /// still tracked. No-op unless this node leads and still tracks `peer`.
    pub fn notify_removed_peer(&mut self, peer: u64) {
        if self.role != NodeRole::Leader || !self.config.peers.contains(&peer) {
            return;
        }
        self.send_append_entries(peer);
    }

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
    /// Called from the conf-change apply path (committed entry) — the
    /// catch-up precondition is enforced by the PROPOSE side (see
    /// `nodedb-cluster` `propose_conf_change`), not here: at apply time the
    /// group's commit_index has already advanced past the promote entry
    /// itself, so checking `match_index >= commit_index` here would
    /// spuriously reject promotions that were valid when proposed. This
    /// function is therefore unconditional and idempotent.
    ///
    /// The `LeaderState` entry is left in place — it already tracks the
    /// peer's next/match index — but the peer now counts toward the commit
    /// quorum.
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

    /// Begin transferring leadership to an in-placement voter `target`.
    ///
    /// `target` must be a current voter peer of this group (it is checked
    /// against the voter set, not any external routing metadata; learners are
    /// excluded because they cannot win an election). The leader stops
    /// accepting new proposals so the log frontier is fixed, then emits a
    /// `TimeoutNow` to `target` as soon as `target` has replicated up to that
    /// frontier. If `target` is still lagging the trigger is deferred and
    /// retried from the `AppendEntries` ack path until it catches up.
    ///
    /// Idempotent: a second call while a transfer is already pending is a
    /// no-op (`Ok`), so fire-and-forget retries never produce duplicate
    /// triggers. The transfer carries no vote and does not change any term —
    /// `target` simply runs a normal election at `term + 1`.
    pub fn transfer_leadership(&mut self, target: u64) -> Result<()> {
        if self.role != NodeRole::Leader {
            return Err(RaftError::NotLeader {
                leader_hint: if self.leader_id != 0 {
                    Some(self.leader_id)
                } else {
                    None
                },
            });
        }

        if target == self.config.node_id || !self.config.peers.contains(&target) {
            return Err(RaftError::InvalidTransferTarget { target });
        }

        // Already transferring: idempotent no-op vs fire-and-forget retries.
        if self.leadership_transfer.is_some() {
            return Ok(());
        }

        let deadline = Instant::now() + self.config.election_timeout_max;
        self.leadership_transfer = Some(LeadershipTransfer {
            target,
            emitted: false,
            deadline,
        });

        // Emit immediately if the target is already at the frontier; otherwise
        // the ack hook retries once it catches up.
        self.try_emit_timeout_now();
        Ok(())
    }

    /// Emit a `TimeoutNow` to the leadership-transfer target if a transfer is
    /// pending, has not yet been emitted, and the target's `match_index` has
    /// reached the leader's log frontier.
    ///
    /// The `emitted` guard is load-bearing: it makes the trigger fire at most
    /// once even though this is called from every `AppendEntries` ack.
    pub(super) fn try_emit_timeout_now(&mut self) {
        let (target, emitted) = match self.leadership_transfer.as_ref() {
            Some(t) => (t.target, t.emitted),
            None => return,
        };
        if emitted {
            return;
        }
        if self.match_index_for(target) != Some(self.last_log_index()) {
            return;
        }

        self.ready.timeout_now.push((
            target,
            TimeoutNowRequest {
                term: self.hard_state.current_term,
                leader_id: self.config.node_id,
                group_id: self.config.group_id,
            },
        ));

        if let Some(t) = self.leadership_transfer.as_mut() {
            t.emitted = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;
    use std::time::Duration;

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

    /// Elect a leader in a multi-voter config: `force_leader` only carries a
    /// single-voter group to leadership, so grant the votes a real election
    /// needs.
    fn elect_with_voters(peers: Vec<u64>) -> RaftNode<MemStorage> {
        let mut node = RaftNode::new(cfg(1, peers.clone()), MemStorage::new());
        crate::test_support::force_election(&mut node);
        let term = node.current_term();
        for peer in peers {
            if node.role() == NodeRole::Leader {
                break;
            }
            node.handle_request_vote_response(
                peer,
                &crate::message::RequestVoteResponse {
                    term,
                    vote_granted: true,
                },
            );
        }
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();
        node
    }

    fn force_leader(node: &mut RaftNode<MemStorage>) {
        crate::test_support::force_election(node);
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

    /// `promote_learner` is the apply-path function: unconditional and
    /// idempotent, because at apply time the commit index has already
    /// advanced past the promote entry itself. The catch-up precondition is
    /// enforced on the PROPOSE side (cluster `propose_conf_change`), so a
    /// not-yet-caught-up learner can still be promoted here when the entry
    /// is applied — this test pins the apply-path contract.
    #[test]
    fn promote_learner_apply_path_is_unconditional() {
        // Single-voter cluster so the no-op commits immediately.
        let mut node = RaftNode::new(cfg(1, vec![]), MemStorage::new());
        force_leader(&mut node);
        assert_eq!(node.role(), NodeRole::Leader);
        assert!(
            node.commit_index() > 0,
            "no-op must commit in single-voter cluster"
        );

        node.add_learner(3);
        // Learner 3 has match_index 0 — below commit index. Apply path must
        // still promote (idempotent, authoritative conf change).
        let promoted = node.promote_learner(3);
        assert!(promoted, "apply-path promotion must be unconditional");
        assert!(node.voters().contains(&3));
        assert!(!node.learners().contains(&3));
    }

    #[test]
    fn promote_learner_allows_caught_up() {
        let mut node = RaftNode::new(cfg(1, vec![]), MemStorage::new());
        force_leader(&mut node);
        assert!(node.commit_index() > 0);

        node.add_learner(3);
        // Learner catches up fully.
        if let Some(ls) = node.leader_state.as_mut() {
            ls.set_match_index(3, node.log.last_index());
        }

        let promoted = node.promote_learner(3);
        assert!(promoted, "caught-up learner must be promotable");
        assert!(node.voters().contains(&3));
    }

    /// A departing voter must be told its own removal committed. Without the
    /// final message it never applies the change, and anything it derives from
    /// that log stays stale for a group it has already left.
    #[test]
    fn a_removed_peer_is_told_before_replication_stops() {
        let mut node = elect_with_voters(vec![2, 3]);
        node.volatile.commit_index = node.log.last_index();

        node.notify_removed_peer(2);
        node.remove_peer(2);

        let ready = node.take_ready();
        let to_departing: Vec<_> = ready.messages.iter().filter(|(p, _)| *p == 2).collect();
        assert_eq!(
            to_departing.len(),
            1,
            "the departing voter must get exactly one final AppendEntries"
        );
        assert_eq!(
            to_departing[0].1.leader_commit,
            node.commit_index(),
            "the final message must carry the commit index that covers the removal"
        );
        assert!(!node.voters().contains(&2), "peer is removed afterwards");
    }

    /// Ordering matters: once the peer is gone from the configuration there is
    /// no replication state to send from, so a late call must do nothing
    /// rather than resurrect it.
    #[test]
    fn notifying_after_removal_is_a_no_op() {
        let mut node = elect_with_voters(vec![2, 3]);

        node.remove_peer(2);
        node.notify_removed_peer(2);

        let ready = node.take_ready();
        assert!(
            !ready.messages.iter().any(|(p, _)| *p == 2),
            "a removed peer must not be sent to"
        );
    }

    /// Only the leader replicates, so only the leader can deliver this news.
    #[test]
    fn a_follower_does_not_notify_a_removed_peer() {
        let mut node = RaftNode::new(cfg(1, vec![2, 3]), MemStorage::new());
        assert_eq!(node.role(), NodeRole::Follower);

        node.notify_removed_peer(2);

        let ready = node.take_ready();
        assert!(ready.messages.is_empty(), "a follower sends nothing");
    }
}
