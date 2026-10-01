// SPDX-License-Identifier: BUSL-1.1

//! Check-quorum: a leader that can no longer reach a majority steps down.
//!
//! A partition does not notify the leader on the minority side. Left alone it
//! keeps its role indefinitely, accepting proposals that can never commit and
//! answering leader-only queries from a log the real leader has moved past.
//! Raft's remedy is check-quorum: the leader tracks when a majority of voters
//! last acknowledged it, and demotes itself once that gap reaches an election
//! timeout — by which point any surviving majority has had time to elect a
//! successor.
//!
//! # What counts as contact
//!
//! Contact is measured from `AppendEntries` **responses**, not from replication
//! progress. Any response proves the peer is reachable and still recognises
//! this term, which is the whole question check-quorum asks. Whether the
//! peer's log has caught up is a different question. Counting `match_index`
//! instead breaks in two cases a healthy cluster routinely hits:
//!
//! - A follower rebuilding from a snapshot, or backtracking after a log
//!   conflict, answers every heartbeat while its `match_index` sits far behind.
//! - Under a sustained write burst the leader's last index outruns the acks
//!   still in flight, so even fully healthy followers trail it.
//!
//! In both cases a `match_index` test sees no contact and deposes a leader
//! whose quorum is intact.
//!
//! # When contact happened
//!
//! Contact is dated at the **send time** of the request a quorum answered,
//! the same anchor the leader lease uses (see [`super::leader_lease`]). The
//! arrival time of a response says nothing about when the follower last heard
//! this leader. Both checks read one clock, so the lease never outlives the
//! contact that check-quorum counts.

use std::time::Instant;

use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Open the contact record of a new leadership term at `now`.
    ///
    /// Called on winning an election. A quorum of voters just granted their
    /// votes, which is contact by definition. The lease starts empty: a vote
    /// grant does not make a voter refuse other candidates.
    pub(super) fn arm_quorum_window(&mut self, now: Instant) {
        self.last_quorum_contact = Some(now);
        self.quorum_window.clear();
        self.lease.begin_term();
    }

    /// Move contact up to the send time of the latest round a quorum of
    /// voters answered. No-op for any role but leader.
    ///
    /// Called from the `AppendEntries` response path, and from
    /// [`RaftNode::tick`] so a single-voter group renews on its own quorum of
    /// one. It has no peers to answer and never reaches the response path.
    pub(super) fn refresh_quorum_contact(&mut self, now: Instant) {
        if self.role != NodeRole::Leader {
            return;
        }
        if self.config.peers.is_empty() {
            self.last_quorum_contact = Some(now);
            return;
        }
        let Some(anchor) = self.settle_lease() else {
            return;
        };
        if self.last_quorum_contact.is_none_or(|last| anchor > last) {
            self.last_quorum_contact = Some(anchor);
        }
    }

    /// Push the last quorum contact back to `at` (for testing). The lease
    /// anchor moves back with it: both read the same clock.
    pub fn quorum_contact_at_override(&mut self, at: Instant) {
        self.last_quorum_contact = Some(at);
        self.lease.anchor = self.lease.anchor.map(|anchor| anchor.min(at));
    }

    /// Whether the leader has gone an entire election timeout without a
    /// quorum answering. Always false off the leader path.
    ///
    /// `election_timeout_max` is deliberate: a follower starts its own
    /// election somewhere in `[min, max]`, so waiting for the upper bound
    /// means a leader only steps down once every follower has had the chance
    /// to move on without it. Stepping down at `min` would demote leaders
    /// during ordinary jitter.
    pub(super) fn quorum_contact_lost(&self, now: Instant) -> bool {
        if self.role != NodeRole::Leader {
            return false;
        }
        self.last_quorum_contact
            .is_some_and(|last| now.duration_since(last) >= self.config.election_timeout_max)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::message::{AppendEntriesResponse, LogEntry, RequestVoteResponse};
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;
    use crate::test_support::{force_election, test_config};

    fn elect(cfg: RaftConfig) -> RaftNode<MemStorage> {
        let peers = cfg.peers.clone();
        let mut node = RaftNode::new(cfg, MemStorage::new());
        force_election(&mut node);
        let term = node.current_term();
        for peer in peers {
            if node.role() == NodeRole::Leader {
                break;
            }
            node.handle_request_vote_response(
                peer,
                &RequestVoteResponse {
                    term,
                    vote_granted: true,
                },
            );
        }
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();
        node
    }

    fn leader(peers: Vec<u64>) -> RaftNode<MemStorage> {
        elect(test_config(1, peers))
    }

    /// Age the contact window past the step-down threshold.
    fn go_silent(node: &mut RaftNode<MemStorage>) {
        node.quorum_contact_at_override(Instant::now() - Duration::from_secs(1));
    }

    /// The round of the latest `AppendEntries` the leader sent.
    fn latest_round(node: &RaftNode<MemStorage>) -> u64 {
        node.lease.next_round - 1
    }

    /// A rejection is contact. A follower backtracking through a log conflict
    /// answers every round while its `match_index` stays put; deposing that
    /// leader would be a false positive.
    #[test]
    fn a_rejecting_follower_still_counts_as_contact() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);

        node.handle_append_entries_response(
            2,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: false,
                last_log_index: 0,
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Leader,
            "a reachable follower that rejects must refresh contact"
        );
    }

    /// Contact must not depend on replication progress. A leader well ahead of
    /// its followers is the normal state under load, not evidence of a lost
    /// quorum.
    #[test]
    fn a_lagging_follower_still_counts_as_contact() {
        let mut node = leader(vec![2, 3]);
        for i in 0..64u8 {
            node.propose(vec![i]).unwrap();
        }
        go_silent(&mut node);

        // Peer 2 answers, acking an index far behind the leader's last.
        node.handle_append_entries_response(
            2,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: 1,
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Leader,
            "a lagging but reachable follower must refresh contact"
        );
    }

    /// The real case: nobody answers, so the leader demotes itself.
    #[test]
    fn silence_from_every_voter_steps_the_leader_down() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Follower,
            "no voter answered within the election timeout"
        );
    }

    /// One answer out of two peers is a minority in a three-voter group.
    #[test]
    fn a_single_answer_is_not_a_quorum_in_a_five_voter_group() {
        let mut node = leader(vec![2, 3, 4, 5]);
        go_silent(&mut node);

        node.handle_append_entries_response(
            2,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: node.last_log_index(),
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Follower,
            "self plus one of four peers is short of a quorum of three"
        );
    }

    /// A single-voter group has nobody to hear from and must never depose
    /// itself over it.
    #[test]
    fn a_single_voter_leader_never_steps_down() {
        let mut node = leader(vec![]);
        go_silent(&mut node);

        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);
    }

    /// Stepping down must leave no leader-term state behind for the next term
    /// to inherit.
    #[test]
    fn stepping_down_clears_the_contact_window() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);
        node.tick();

        assert_eq!(node.role(), NodeRole::Follower);
        assert!(node.last_quorum_contact.is_none());
        assert!(node.quorum_window.is_empty());
    }

    /// A demoted leader is an ordinary follower: it accepts the new leader's
    /// entries and applies them.
    #[test]
    fn a_demoted_leader_follows_the_next_one() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);
        node.tick();
        assert_eq!(node.role(), NodeRole::Follower);

        let term = node.current_term() + 1;
        let resp = node.handle_append_entries(&crate::message::AppendEntriesRequest {
            term,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term,
                index: 1,
                data: b"from-the-new-leader".to_vec(),
            }],
            leader_commit: 1,
            group_id: 1,
            round: 1,
            replicated_floor: 0,
        });

        assert!(resp.success);
        assert_eq!(node.leader_id(), 2);
        assert_eq!(node.current_term(), term);
        let ready = node.take_ready();
        assert_eq!(ready.committed_entries.len(), 1);
        assert_eq!(ready.committed_entries[0].data, b"from-the-new-leader");
    }

    /// Contact is a sliding window, not a one-off: answers must keep arriving.
    /// A peer that answered once and then went quiet does not hold the term
    /// open forever.
    #[test]
    fn contact_must_be_renewed_by_later_answers() {
        let mut node = leader(vec![2, 3]);

        node.handle_append_entries_response(
            2,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: node.last_log_index(),
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );
        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);

        // Nothing further arrives.
        go_silent(&mut node);
        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Follower,
            "a stale answer must not renew the window indefinitely"
        );
    }

    /// Contact is dated when the answered request left the leader. A
    /// response that took long to arrive proves nothing about the time since.
    #[test]
    fn contact_is_dated_at_send_time() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);
        let round = latest_round(&node);
        let silent_since = node.last_quorum_contact.expect("leader has contact");
        // The answered request left after the silence began, and well before
        // its answer is handled below.
        let sent = silent_since + Duration::from_millis(100);
        node.lease.set_sent_at(round, sent);

        node.handle_append_entries_response(
            2,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: node.last_log_index(),
                round,
                needs_snapshot: false,
            },
        );

        assert_eq!(
            node.last_quorum_contact,
            Some(sent),
            "contact must be the send time, not the arrival time"
        );
    }

    /// A follower never runs the leader-side check.
    #[test]
    fn a_follower_is_never_deposed_by_check_quorum() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        assert_eq!(node.role(), NodeRole::Follower);
        assert!(!node.quorum_contact_lost(std::time::Instant::now()));
        node.refresh_quorum_contact(std::time::Instant::now());
        assert!(node.last_quorum_contact.is_none());
    }

    /// Answers from a learner carry no weight: learners are not voters and
    /// cannot keep a leader's term alive.
    #[test]
    fn a_learner_answer_does_not_count_toward_quorum() {
        let mut cfg = test_config(1, vec![2, 3]);
        cfg.learners = vec![9];
        let mut node = elect(cfg);
        go_silent(&mut node);

        node.handle_append_entries_response(
            9,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: node.last_log_index(),
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Follower,
            "only voters can renew the contact window"
        );
    }

    /// The step-down waits for the upper bound of the election timeout, so
    /// ordinary jitter below it does not demote a healthy leader.
    #[test]
    fn contact_is_not_lost_before_the_upper_election_bound() {
        let node = leader(vec![2, 3]);
        let contact = node.last_quorum_contact.expect("leader has contact");
        let cfg_max = node.config.election_timeout_max;
        assert!(!node.quorum_contact_lost(contact + cfg_max - Duration::from_nanos(1)));
        assert!(node.quorum_contact_lost(contact + cfg_max));
    }
}
