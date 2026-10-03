// SPDX-License-Identifier: BUSL-1.1

//! Send one peer's batch of per-group Raft messages concurrently.
//!
//! Sends start in group order and run side by side. One group whose answer
//! waits on the peer's disk never delays another group's heartbeat.
//!
//! A link failure stops the sends not yet started, since they fail the same
//! way. Sends already in flight can still complete. An answer settles before
//! the next send starts when it is ready by then, so a failure that the
//! transport reports at once, such as an open circuit, stops the rest.
//!
//! A typed refusal answers one group only, for example a group the peer does
//! not host yet. The other groups go on. A refusal is not a Raft response, so
//! it never counts as contact with the peer.

use std::future::Future;

use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::error::Result;

/// Send each `(group_id, message)` to `peer` through `send`, concurrently.
///
/// `on_response` runs for each Raft response, one at a time, in arrival
/// order. `kind` names the message in logs. Returns on shutdown, or once
/// every started send has finished.
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
    let mut in_flight = FuturesUnordered::new();
    let mut link_failed = false;
    for (group_id, message) in messages {
        // Answers that are ready settle before the next send starts.
        while let Some(Some((answered, result))) = in_flight.next().now_or_never() {
            link_failed |= settle(peer, kind, answered, result, &mut on_response);
        }
        if link_failed {
            break;
        }
        in_flight.push(send(message).map(move |result| (group_id, result)));
    }
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => return,
            next = in_flight.next() => match next {
                Some((group_id, result)) => {
                    settle(peer, kind, group_id, result, &mut on_response);
                }
                None => return,
            },
        }
    }
}

/// Handle the outcome of one group's send. Returns whether the link to the
/// peer failed.
fn settle<R>(
    peer: u64,
    kind: &'static str,
    group_id: u64,
    result: Result<R>,
    on_response: &mut impl FnMut(u64, R),
) -> bool {
    match result {
        Ok(response) => {
            on_response(group_id, response);
            false
        }
        Err(e) if e.is_link_failure() => {
            warn!(group_id, peer, kind, error = %e, "raft RPC failed; no further groups go to the peer this round");
            true
        }
        Err(e) => {
            debug!(group_id, peer, kind, error = %e, "raft RPC refused");
            false
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

    type Pending = std::pin::Pin<Box<dyn Future<Output = Result<u64>> + Send>>;

    /// Group 1's answer waits until group 2 has answered. Sent one after the
    /// other, the batch never finishes.
    #[tokio::test]
    async fn a_slow_group_does_not_delay_another_groups_send() {
        let (_tx, mut rx) = watch::channel(false);
        let gate = std::sync::Arc::new(tokio::sync::Notify::new());
        let answered = Mutex::new(Vec::new());
        let send = |group: u64| -> Pending {
            let gate = std::sync::Arc::clone(&gate);
            Box::pin(async move {
                if group == 1 {
                    gate.notified().await;
                }
                Ok(group)
            })
        };
        let batch = drive_peer_batch(
            2,
            "append_entries",
            vec![(1, 1), (2, 2)],
            &mut rx,
            send,
            |group_id, _response| {
                answered.lock().unwrap().push(group_id);
                if group_id == 2 {
                    gate.notify_one();
                }
            },
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), batch)
            .await
            .expect("group 2 answers while group 1 waits");
        assert_eq!(answered.into_inner().unwrap(), vec![2, 1]);
    }

    /// A link failure stops the sends not yet started. A send already in
    /// flight still completes.
    #[tokio::test]
    async fn a_link_failure_lets_the_sends_in_flight_finish() {
        let (_tx, mut rx) = watch::channel(false);
        let gate = std::sync::Arc::new(tokio::sync::Notify::new());
        let sent = Mutex::new(Vec::new());
        let answered = Mutex::new(Vec::new());
        let send = |group: u64| -> Pending {
            sent.lock().unwrap().push(group);
            let gate = std::sync::Arc::clone(&gate);
            Box::pin(async move {
                match group {
                    1 => {
                        gate.notified().await;
                        Ok(group)
                    }
                    4 => {
                        gate.notify_one();
                        Err(ClusterError::Transport {
                            detail: "connection lost".into(),
                        })
                    }
                    _ => Ok(group),
                }
            })
        };
        drive_peer_batch(
            2,
            "append_entries",
            vec![(1, 1), (4, 4), (0, 0)],
            &mut rx,
            send,
            |group_id, _response| answered.lock().unwrap().push(group_id),
        )
        .await;
        assert_eq!(
            sent.into_inner().unwrap(),
            vec![1, 4],
            "group 0 is never sent"
        );
        assert_eq!(answered.into_inner().unwrap(), vec![1]);
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
