// SPDX-License-Identifier: BUSL-1.1

//! Shared test fixtures for the RPC handler test suites.

use std::time::{Duration, Instant};

use crate::message::RequestVoteResponse;
use crate::node::config::RaftConfig;
use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::MemStorage;

/// Standard 3-voter (or N-voter) test config with no learners/observers.
pub(super) fn test_config(node_id: u64, peers: Vec<u64>) -> RaftConfig {
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

/// Config where `node_id` is itself an observer (no peers).
pub(super) fn observer_self_config(node_id: u64) -> RaftConfig {
    RaftConfig {
        node_id,
        group_id: 1,
        peers: vec![],
        learners: vec![],
        observers: vec![],
        starts_as_learner: false,
        starts_as_observer: true,
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
        log_compaction_threshold: None,
    }
}

/// Elect node 1 as leader with 2 voters and 1 observer (peer 5).
///
/// Returns `(leader_node, observer_node)`.
pub(super) fn setup_leader_with_observer() -> (RaftNode<MemStorage>, RaftNode<MemStorage>) {
    let mut node1 = RaftNode::new(
        RaftConfig {
            node_id: 1,
            group_id: 1,
            peers: vec![2],
            learners: vec![],
            observers: vec![5],
            starts_as_learner: false,
            starts_as_observer: false,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            log_compaction_threshold: None,
        },
        MemStorage::new(),
    );
    let observer = RaftNode::new(observer_self_config(5), MemStorage::new());

    // Force node 1 into candidate.
    node1.election_deadline = Instant::now() - Duration::from_millis(1);
    node1.tick();
    let _ = node1.take_ready();
    // Voter 2 grants its vote.
    node1.handle_request_vote_response(
        2,
        &RequestVoteResponse {
            term: 1,
            vote_granted: true,
        },
    );
    assert_eq!(node1.role(), NodeRole::Leader);
    let _ = node1.take_ready();

    (node1, observer)
}
