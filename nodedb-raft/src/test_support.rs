// SPDX-License-Identifier: BUSL-1.1

//! Test-only drivers shared across the crate's unit-test modules.

use std::time::{Duration, Instant};

use crate::message::PreVoteResponse;
use crate::node::core::RaftNode;
use crate::storage::LogStorage;

/// Push `node` past its election timeout and grant the pre-vote round every
/// peer, so it reaches the real election that `tick` alone no longer starts.
///
/// Leaves `Ready` undrained, so callers keep whatever they assert on. A
/// single-voter group has no peers to answer and is elected by the tick.
pub(crate) fn force_election<S: LogStorage>(node: &mut RaftNode<S>) {
    node.election_deadline_override(Instant::now() - Duration::from_millis(1));
    node.tick();
    let term = node.current_term();
    for peer in node.peers().to_vec() {
        node.handle_pre_vote_response(
            peer,
            &PreVoteResponse {
                term,
                vote_granted: true,
            },
        );
    }
}

/// A single-group config with the crate's standard test timings and no
/// auto-compaction. Callers override the fields the test cares about.
pub(crate) fn test_config(node_id: u64, peers: Vec<u64>) -> crate::node::config::RaftConfig {
    crate::node::config::RaftConfig {
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

/// Drive a single-voter node to leadership and apply its initial election
/// no-op so `last_applied` tracks the log.
pub(crate) fn leader_with_applied_noop(
    config: crate::node::config::RaftConfig,
) -> RaftNode<crate::storage::MemStorage> {
    let mut node = RaftNode::new(config, crate::storage::MemStorage::new());
    node.election_deadline_override(Instant::now() - Duration::from_millis(1));
    node.tick();
    assert_eq!(node.role(), crate::state::NodeRole::Leader);
    let ready = node.take_ready();
    if let Some(last) = ready.committed_entries.last() {
        node.advance_applied(last.index);
    }
    node
}

/// Stand in for a data-plane apply that reached durability: advance the
/// delivery watermark AND the durable floor, as the apply loop does once the
/// write funnel's fsync barrier has returned.
pub(crate) fn apply_durably(node: &mut RaftNode<crate::storage::MemStorage>, index: u64) {
    node.advance_applied(index);
    node.save_durable_applied_index(index).unwrap();
}
