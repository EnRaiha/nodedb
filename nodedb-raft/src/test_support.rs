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
