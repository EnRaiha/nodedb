// SPDX-License-Identifier: BUSL-1.1

//! Send one peer's batch of per-group Raft messages in order.
//!
//! A link failure ends the batch, since the rest fail the same way. A typed
//! refusal answers one group only, for example a group the peer does not
//! host yet. The batch moves on to the next group, so one unhosted group
//! never holds back another group's heartbeat. A refusal is not a Raft
//! response, so it never counts as contact with the peer.

use std::future::Future;

use tokio::sync::watch;
use tracing::{debug, warn};

use crate::error::Result;

/// Send each `(group_id, message)` to `peer` in order through `send`.
///
/// `on_response` runs for each Raft response. `kind` names the message in
/// logs. Returns early on shutdown or on the first link failure.
pub(super) async fn drive_peer_batch<M, R, Fut>(
    peer: u64,
    kind: &'static str,
    messages: Vec<(u64, M)>,
    shutdown_rx: &mut watch::Receiver<bool>,
    mut send: impl FnMut(M) -> Fut,
    mut on_response: impl FnMut(u64, R),
) where
    Fut: Future<Output = Result<R>>,
{
    for (group_id, message) in messages {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => return,
            result = send(message) => match result {
                Ok(response) => on_response(group_id, response),
                Err(e) if e.is_link_failure() => {
                    warn!(group_id, peer, kind, error = %e, "raft RPC failed; skipping the peer's remaining groups");
                    return;
                }
                Err(e) => debug!(group_id, peer, kind, error = %e, "raft RPC refused"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::error::ClusterError;

    /// Drive a batch whose sends answer with `answer(message)`. Returns the
    /// groups that got a Raft response.
    async fn run(messages: Vec<(u64, u64)>, answer: impl Fn(u64) -> Result<u64>) -> Vec<u64> {
        let (_tx, mut rx) = watch::channel(false);
        let answered = Mutex::new(Vec::new());
        drive_peer_batch(
            2,
            "append_entries",
            messages,
            &mut rx,
            |message| std::future::ready(answer(message)),
            |group_id, _response| answered.lock().unwrap().push(group_id),
        )
        .await;
        answered.into_inner().unwrap()
    }

    #[tokio::test]
    async fn a_refusal_does_not_skip_the_next_groups_heartbeat() {
        let answered = run(vec![(4, 4), (0, 0)], |group| {
            if group == 4 {
                Err(ClusterError::GroupNotFound { group_id: 4 })
            } else {
                Ok(group)
            }
        })
        .await;
        assert_eq!(answered, vec![0], "group 0's heartbeat still goes out");
    }

    #[tokio::test]
    async fn a_link_failure_ends_the_batch() {
        let answered = run(vec![(1, 1), (4, 4), (0, 0)], |group| {
            if group == 4 {
                Err(ClusterError::Transport {
                    detail: "connection lost".into(),
                })
            } else {
                Ok(group)
            }
        })
        .await;
        assert_eq!(answered, vec![1]);
    }

    #[tokio::test]
    async fn an_open_circuit_ends_the_batch() {
        let answered = run(vec![(4, 4), (0, 0)], |_| {
            Err(ClusterError::CircuitOpen {
                node_id: 2,
                failures: 5,
            })
        })
        .await;
        assert!(answered.is_empty());
    }
}
