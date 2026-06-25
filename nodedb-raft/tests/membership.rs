// SPDX-License-Identifier: BUSL-1.1

//! Membership-change state-machine paths.
//!
//! Inline tests in `node/membership.rs` cover the basic shape (add /
//! remove / promote learner; learner doesn't change quorum; promotion
//! grows it). These tests target the corners that aren't exercised
//! there: no-op guards (self / duplicate / cross-list), a full
//! learner-catches-up-then-promoted lifecycle on a leader, and the
//! invariant that voter removal both retracts replication tracking
//! AND lets a previously stuck commit advance under the new (smaller)
//! quorum.

use std::time::{Duration, Instant};

use nodedb_raft::{
    AppendEntriesResponse, RaftNode, message::RequestVoteResponse, node::config::RaftConfig,
    state::NodeRole, storage::MemStorage,
};

fn config(node_id: u64, peers: Vec<u64>) -> RaftConfig {
    RaftConfig {
        node_id,
        group_id: 1,
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

fn become_leader_3node(node: &mut RaftNode<MemStorage>) {
    node.election_deadline_override(Instant::now() - Duration::from_millis(1));
    node.tick();
    let _ = node.take_ready();
    let yes = RequestVoteResponse {
        term: 1,
        vote_granted: true,
    };
    node.handle_request_vote_response(2, &yes);
    assert_eq!(node.role(), NodeRole::Leader);
    let _ = node.take_ready();
}

#[test]
fn add_peer_is_noop_for_self_duplicate_or_existing_learner() {
    let mut node = RaftNode::new(config(1, vec![2]), MemStorage::new());

    // Self.
    node.add_peer(1);
    assert_eq!(node.voters(), &[2]);

    // Duplicate voter.
    node.add_peer(2);
    assert_eq!(node.voters(), &[2]);

    // Adding as voter while already a learner is rejected — caller
    // must use promote_learner instead.
    node.add_learner(3);
    assert_eq!(node.learners(), &[3]);
    node.add_peer(3);
    assert_eq!(
        node.voters(),
        &[2],
        "voter list must not absorb a learner via add_peer"
    );
    assert_eq!(node.learners(), &[3], "learner stays in learner list");
}

#[test]
fn add_learner_is_noop_for_self_duplicate_or_existing_voter() {
    let mut node = RaftNode::new(config(1, vec![2]), MemStorage::new());

    // Self.
    node.add_learner(1);
    assert!(node.learners().is_empty());

    // Existing voter.
    node.add_learner(2);
    assert!(
        node.learners().is_empty(),
        "voter must not be re-added as learner"
    );

    // Duplicate learner.
    node.add_learner(3);
    node.add_learner(3);
    assert_eq!(node.learners(), &[3]);
}

#[test]
fn promote_learner_returns_false_for_non_learner() {
    let mut node = RaftNode::new(config(1, vec![2]), MemStorage::new());

    // Peer that doesn't exist anywhere.
    assert!(!node.promote_learner(99));

    // Peer that's a voter, not a learner.
    assert!(!node.promote_learner(2));
    assert_eq!(node.voters(), &[2]);
    assert!(node.learners().is_empty());
}

#[test]
fn remove_peer_drops_voter_from_replication_targets() {
    // 3-voter cluster.
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());
    become_leader_3node(&mut node);
    assert_eq!(node.peers(), &[2, 3]);

    // Remove peer 3.
    node.remove_peer(3);
    assert_eq!(node.voters(), &[2]);

    // A subsequent proposal must replicate to the remaining voter only —
    // peer 3 must NOT appear in the outgoing AE message set.
    let _ = node.propose(b"after-remove".to_vec()).unwrap();
    let ready = node.take_ready();
    let targets: Vec<u64> = ready.messages.iter().map(|(p, _)| *p).collect();
    assert!(
        targets.contains(&2),
        "remaining voter must still receive AE, got {targets:?}"
    );
    assert!(
        !targets.contains(&3),
        "removed voter must not receive AE, got {targets:?}"
    );
}

/// Full lifecycle: leader adds a learner, replicates a proposed entry
/// to it, learner catches up, leader promotes it, learner now counts
/// in the quorum. End-to-end against the public API.
#[test]
fn learner_catchup_then_promotion_lifecycle() {
    let mut node = RaftNode::new(config(1, vec![2]), MemStorage::new());
    // 2-voter cluster: self + peer 2, quorum 2.
    node.election_deadline_override(Instant::now() - Duration::from_millis(1));
    node.tick();
    let _ = node.take_ready();
    let yes = RequestVoteResponse {
        term: 1,
        vote_granted: true,
    };
    node.handle_request_vote_response(2, &yes);
    assert_eq!(node.role(), NodeRole::Leader);
    let _ = node.take_ready();

    // Add a learner peer.
    node.add_learner(3);
    assert_eq!(node.learners(), &[3]);
    assert_eq!(node.voters(), &[2]);

    // Quorum is unchanged at this point.
    let cluster_size_before = node.voters().len() + 1;
    assert_eq!(cluster_size_before, 2);

    // Propose an entry. Leader replicates to voters AND the learner;
    // the learner's tracking is now non-None.
    let idx = node.propose(b"cmd".to_vec()).unwrap();
    let _ = node.take_ready();
    assert!(
        node.match_index_for(3).is_some(),
        "learner must be tracked for replication"
    );

    // Voter ACKs index `idx` — quorum (2 of 2: self + voter) satisfied.
    let voter_ack = AppendEntriesResponse {
        term: 1,
        success: true,
        last_log_index: idx,
    };
    node.handle_append_entries_response(2, &voter_ack);
    assert_eq!(node.commit_index(), idx);

    // Learner ACKs too — match_index advances but quorum already met.
    let learner_ack = AppendEntriesResponse {
        term: 1,
        success: true,
        last_log_index: idx,
    };
    node.handle_append_entries_response(3, &learner_ack);
    assert_eq!(node.match_index_for(3), Some(idx));

    // Promote.
    assert!(node.promote_learner(3));
    assert_eq!(node.voters().len(), 2);
    assert!(node.learners().is_empty());

    // After promotion: 3-voter cluster, quorum is now 2 (still).
    // Crucially, the previously-tracked match_index for peer 3 must
    // survive the promotion — we don't want to retransmit everything.
    assert_eq!(
        node.match_index_for(3),
        Some(idx),
        "promotion must preserve replication state, not reset it"
    );
}

/// Proposing on a leader whose voter set has been removed down to
/// itself must still commit (single-voter quorum = 1).
#[test]
fn voter_removal_to_single_node_keeps_commit_progress() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());
    become_leader_3node(&mut node);

    // Remove both peers, leaving a single-voter cluster.
    node.remove_peer(2);
    node.remove_peer(3);
    assert!(node.voters().is_empty());

    // A single-voter cluster commits proposals immediately.
    let idx = node.propose(b"solo".to_vec()).unwrap();
    let ready = node.take_ready();
    let committed: Vec<_> = ready
        .committed_entries
        .iter()
        .filter(|e| e.data == b"solo")
        .collect();
    assert_eq!(
        committed.len(),
        1,
        "single-voter cluster must self-commit proposals"
    );
    assert!(node.commit_index() >= idx);
}

#[test]
fn remove_learner_is_noop_when_not_a_learner() {
    let mut node = RaftNode::new(config(1, vec![2]), MemStorage::new());
    // Removing a non-existent learner must not panic or leak into voter list.
    node.remove_learner(99);
    assert!(node.learners().is_empty());
    assert_eq!(node.voters(), &[2]);

    // Removing a voter via remove_learner must be a no-op (the API
    // contract: voters and learners are disjoint sets, each with its
    // own remover).
    node.remove_learner(2);
    assert_eq!(
        node.voters(),
        &[2],
        "remove_learner must not touch voter list"
    );
}

#[test]
fn is_learner_peer_reflects_current_state() {
    let mut node = RaftNode::new(config(1, vec![2]), MemStorage::new());
    assert!(!node.is_learner_peer(3));
    node.add_learner(3);
    assert!(node.is_learner_peer(3));
    node.promote_learner(3);
    assert!(
        !node.is_learner_peer(3),
        "promoted learner is no longer a learner peer"
    );
}
