// SPDX-License-Identifier: BUSL-1.1

//! Election state-machine corner cases.
//!
//! Inline tests in `node/rpc.rs` and `node/core.rs` cover the happy
//! path (single-node election, three-node election + replication,
//! basic vote grant/reject, leader step-down on higher term). These
//! tests target the corners that are easy to break silently:
//! idempotent vote grants, the §5.4.1 log-up-to-date rule, candidate
//! step-down on a higher-term AE arriving mid-election, and the
//! invariant that consecutive failed elections strictly increment the
//! term.

use std::time::{Duration, Instant};

use nodedb_raft::{
    AppendEntriesRequest, LogEntry, RaftNode,
    message::{RequestVoteRequest, RequestVoteResponse},
    node::config::RaftConfig,
    state::NodeRole,
    storage::MemStorage,
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

fn force_election(node: &mut RaftNode<MemStorage>) {
    node.election_deadline_override(Instant::now() - Duration::from_millis(1));
    node.tick();
}

/// Re-delivering a RequestVote from the same candidate in the same term
/// must be granted again. The RPC layer can't distinguish a retry from
/// a duplicate, and the candidate's progress depends on idempotent grants.
#[test]
fn vote_grant_is_idempotent_for_same_candidate() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());

    let req = RequestVoteRequest {
        term: 1,
        candidate_id: 2,
        last_log_index: 0,
        last_log_term: 0,
        group_id: 1,
    };
    let r1 = node.handle_request_vote(&req);
    assert!(r1.vote_granted);

    // Retry — must still be granted, term unchanged.
    let r2 = node.handle_request_vote(&req);
    assert!(
        r2.vote_granted,
        "same-term retry from same candidate must re-grant"
    );
    assert_eq!(r2.term, 1);
}

/// Stale-term RequestVote (term < current_term) must be rejected with
/// the current term carried in the response so the candidate can
/// step down.
#[test]
fn stale_term_request_vote_rejected_with_current_term() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());

    // Drive the node up to term 5 via a higher-term AE.
    let bump = AppendEntriesRequest {
        term: 5,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
        group_id: 1,
    };
    let _ = node.handle_append_entries(&bump);
    assert_eq!(node.current_term(), 5);

    let stale = RequestVoteRequest {
        term: 3,
        candidate_id: 3,
        last_log_index: 0,
        last_log_term: 0,
        group_id: 1,
    };
    let resp = node.handle_request_vote(&stale);
    assert!(!resp.vote_granted);
    assert_eq!(
        resp.term, 5,
        "rejection must surface current term for caller step-down"
    );
}

/// Raft §5.4.1: a candidate whose log is shorter or has a smaller
/// last-log term than the voter must NOT receive the vote, even if
/// the voter has not yet voted in this term.
#[test]
fn vote_denied_when_candidate_log_not_up_to_date() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());

    // Seed the voter's log with two entries at term 2 via AE.
    let seed = AppendEntriesRequest {
        term: 2,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![
            LogEntry {
                term: 2,
                index: 1,
                data: vec![],
            },
            LogEntry {
                term: 2,
                index: 2,
                data: vec![],
            },
        ],
        leader_commit: 0,
        group_id: 1,
    };
    assert!(node.handle_append_entries(&seed).success);
    assert_eq!(node.current_term(), 2);

    // Candidate at term 3 but with a shorter log (last_log at term 1).
    let stale_log = RequestVoteRequest {
        term: 3,
        candidate_id: 3,
        last_log_index: 5, // long, but...
        last_log_term: 1,  // ...older term — loses to our term-2 tail
        group_id: 1,
    };
    let resp = node.handle_request_vote(&stale_log);
    assert!(
        !resp.vote_granted,
        "older last-log-term must lose §5.4.1 even with longer log"
    );

    // Same last-log term, shorter log — also loses.
    let shorter = RequestVoteRequest {
        term: 4,
        candidate_id: 3,
        last_log_index: 1,
        last_log_term: 2,
        group_id: 1,
    };
    let resp = node.handle_request_vote(&shorter);
    assert!(!resp.vote_granted, "same term, shorter log must lose");

    // Equal last-log index/term at a higher election term — wins.
    let equal = RequestVoteRequest {
        term: 5,
        candidate_id: 3,
        last_log_index: 2,
        last_log_term: 2,
        group_id: 1,
    };
    let resp = node.handle_request_vote(&equal);
    assert!(
        resp.vote_granted,
        "equal last-log info must satisfy up-to-date rule"
    );
}

/// A candidate that receives a higher-term AppendEntries during its
/// election must step down to follower in the new term — not stay a
/// candidate and silently let two leaders coexist.
#[test]
fn candidate_steps_down_on_higher_term_append_entries() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());
    force_election(&mut node);
    assert_eq!(node.role(), NodeRole::Candidate);
    assert_eq!(node.current_term(), 1);

    // A different leader appears at a higher term.
    let intruder = AppendEntriesRequest {
        term: 7,
        leader_id: 3,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
        group_id: 1,
    };
    let resp = node.handle_append_entries(&intruder);
    assert!(resp.success);
    assert_eq!(node.role(), NodeRole::Follower);
    assert_eq!(node.current_term(), 7);
    assert_eq!(node.leader_id(), 3);
}

/// Each fresh election must increment the term. Two timeouts in a row
/// (both failed for lack of quorum) must end on term N+2, not N+1 —
/// otherwise repeated elections silently collapse and forward progress
/// stalls.
#[test]
fn consecutive_failed_elections_strictly_increment_term() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());

    force_election(&mut node);
    assert_eq!(node.role(), NodeRole::Candidate);
    let term1 = node.current_term();
    assert_eq!(term1, 1);
    let _ = node.take_ready();

    // Second timeout while still a candidate (no votes received).
    force_election(&mut node);
    assert_eq!(node.role(), NodeRole::Candidate);
    let term2 = node.current_term();
    assert_eq!(term2, 2, "candidate timeout must bump term");

    // And again.
    force_election(&mut node);
    assert_eq!(node.current_term(), 3);
}

/// A voter that has already granted a vote to candidate A in term T
/// must reject candidate B in the same term T — even if B's log is
/// up-to-date.
#[test]
fn second_candidate_rejected_in_same_term_after_vote_already_cast() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());

    let a = RequestVoteRequest {
        term: 1,
        candidate_id: 2,
        last_log_index: 0,
        last_log_term: 0,
        group_id: 1,
    };
    assert!(node.handle_request_vote(&a).vote_granted);

    let b = RequestVoteRequest {
        term: 1,
        candidate_id: 3,
        last_log_index: 0,
        last_log_term: 0,
        group_id: 1,
    };
    let resp = node.handle_request_vote(&b);
    assert!(
        !resp.vote_granted,
        "voter must not grant two distinct candidates in the same term"
    );
}

/// A vote response whose term is higher than the candidate's current
/// term must cause an immediate step-down. The candidate cannot keep
/// counting votes in a stale term.
#[test]
fn candidate_steps_down_on_higher_term_vote_response() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());
    force_election(&mut node);
    assert_eq!(node.role(), NodeRole::Candidate);
    assert_eq!(node.current_term(), 1);

    let high_term_resp = RequestVoteResponse {
        term: 9,
        vote_granted: false,
    };
    node.handle_request_vote_response(2, &high_term_resp);

    assert_eq!(node.role(), NodeRole::Follower);
    assert_eq!(node.current_term(), 9);
}

/// Vote responses arriving after the candidate has already won (and
/// become leader) must not cause role regressions or term drift.
#[test]
fn late_vote_response_after_election_won_is_ignored() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::new());
    force_election(&mut node);
    let _ = node.take_ready();

    let yes = RequestVoteResponse {
        term: 1,
        vote_granted: true,
    };
    node.handle_request_vote_response(2, &yes);
    assert_eq!(node.role(), NodeRole::Leader);

    // A late "yes" from peer 3 arriving after we already won.
    node.handle_request_vote_response(3, &yes);
    assert_eq!(node.role(), NodeRole::Leader);
    assert_eq!(node.current_term(), 1);
}
