// SPDX-License-Identifier: BUSL-1.1

//! Outbound RPC to a known peer: one attempt, its circuit-breaker record,
//! and the retry loop around it.
//!
//! Only a link failure counts against the peer's circuit breaker. That is a
//! failed connect, handshake, stream open or write, a read timeout, or a lost
//! or reset connection. Every answer the peer sends counts as a success, a
//! typed refusal included. A refusal goes back to the caller as its typed
//! error and is not resent.

use std::time::Duration;

use tracing::debug;

use crate::circuit_breaker::{Admission, RetryPolicy};
use crate::error::{ClusterError, Result};
use crate::rpc_codec::{self, RaftRpc};
use crate::transport::frame_io::read_envelope_or_finish;

use super::transport::NexarTransport;

/// How one attempt ended.
#[derive(Debug)]
enum Attempt {
    /// The peer answered. The result is final: a response, or the typed
    /// error the peer refused the request with.
    Answered(Result<RaftRpc>),
    /// The peer's replay window refused the frame. The peer is up, and a
    /// retry goes out under a fresh sequence number.
    FrameRefused(ClusterError),
    /// The link to the peer failed.
    LinkFailed(ClusterError),
}

/// Sort the outcome of one send into an [`Attempt`].
///
/// The outer error is a link failure. The inner result is what the peer
/// sent back, or why its reply is unreadable.
fn classify(target: u64, sent: Result<Result<RaftRpc>>) -> Attempt {
    match sent {
        Err(link) => Attempt::LinkFailed(link),
        Ok(Ok(RaftRpc::FrameRefused(refusal))) => Attempt::FrameRefused(ClusterError::Transport {
            detail: format!("node {target} refused the frame: {}", refusal.detail),
        }),
        Ok(Ok(RaftRpc::RequestRefused(refusal))) => Attempt::Answered(Err(refusal.into_error())),
        Ok(answer) => Attempt::Answered(answer),
    }
}

impl NexarTransport {
    /// Send an RPC to a peer with retry and circuit breaker.
    ///
    /// The response read is bounded by the transport's default `rpc_timeout`.
    /// For RPCs whose handler legitimately blocks far longer than a normal
    /// request/response round-trip (e.g. a routed Calvin submit-and-await, which
    /// the leader-side handler holds open until the transaction is sequenced AND
    /// completion-acked), use [`send_rpc_with_read_timeout`](Self::send_rpc_with_read_timeout)
    /// so the generic short timeout does not abort the call while the remote
    /// handler is still legitimately working.
    pub async fn send_rpc(&self, target: u64, rpc: RaftRpc) -> Result<RaftRpc> {
        self.send_rpc_with_read_timeout(target, rpc, self.rpc_timeout)
            .await
    }

    /// [`send_rpc`](Self::send_rpc) with an explicit response-read timeout.
    ///
    /// `read_timeout` bounds the wait for the response envelope on each attempt
    /// (the connect / handshake / write phases still use the transport's pooled
    /// connection). Callers pass a value derived from the remote handler's own
    /// deadline (plus a margin) so a long-running handler is not aborted early.
    pub async fn send_rpc_with_read_timeout(
        &self,
        target: u64,
        rpc: RaftRpc,
        read_timeout: Duration,
    ) -> Result<RaftRpc> {
        self.check_not_severed(target)?;
        // Encode the inner RPC once (codec errors are not retryable).
        // Each retry wraps it in a fresh envelope so the seq advances
        // per attempt — a retry is a new frame, not a replayed frame.
        let inner = rpc_codec::encode(&rpc, &self.auth.epoch)?;

        let mut last_err = None;
        for attempt in 0..self.retry_policy.max_attempts {
            if attempt > 0 {
                let delay = self.retry_policy.delay_for_attempt(attempt - 1);
                debug!(target, attempt, ?delay, "retrying RPC");
                tokio::time::sleep(delay).await;
            }
            // The breaker admits each attempt right before it goes out.
            let admission = self.circuit_breaker.check(target)?;

            match self.attempt(target, &inner, read_timeout, admission).await {
                Attempt::Answered(answer) => return answer,
                Attempt::FrameRefused(e) => last_err = Some(e),
                Attempt::LinkFailed(e) if RetryPolicy::is_retryable(&e) => last_err = Some(e),
                Attempt::LinkFailed(e) => return Err(e),
            }
        }

        Err(last_err.unwrap_or_else(|| ClusterError::Transport {
            detail: format!("send_rpc to node {target}: all attempts exhausted"),
        }))
    }

    /// Send a recovery probe: one attempt that an open circuit never refuses.
    ///
    /// The probe's outcome decides the circuit. An answer closes it, and a
    /// link failure reopens it. The health monitor and the reachability
    /// driver probe peers this way, so a peer whose circuit is open is still
    /// probed and can recover.
    pub async fn send_probe_rpc(&self, target: u64, rpc: RaftRpc) -> Result<RaftRpc> {
        self.check_not_severed(target)?;
        let inner = rpc_codec::encode(&rpc, &self.auth.epoch)?;
        let admission = self.circuit_breaker.admit_probe(target);
        match self
            .attempt(target, &inner, self.rpc_timeout, admission)
            .await
        {
            Attempt::Answered(answer) => answer,
            Attempt::FrameRefused(e) | Attempt::LinkFailed(e) => Err(e),
        }
    }

    /// One attempt, recorded on the circuit breaker under `admission`.
    ///
    /// A link failure also evicts the cached connection, so the next attempt
    /// dials a fresh one.
    async fn attempt(
        &self,
        target: u64,
        inner: &[u8],
        read_timeout: Duration,
        admission: Admission,
    ) -> Attempt {
        let outcome = classify(
            target,
            self.try_send_once(target, inner, read_timeout).await,
        );
        match &outcome {
            Attempt::LinkFailed(_) => {
                self.circuit_breaker.record_failure(target, admission);
                self.evict_peer(target);
            }
            Attempt::Answered(_) | Attempt::FrameRefused(_) => {
                self.circuit_breaker.record_success(target, admission);
            }
        }
        outcome
    }

    /// Single-attempt RPC send (no retry, no circuit breaker). `inner` is the
    /// encoded RPC. It is wrapped in a fresh envelope once the stream is
    /// open.
    ///
    /// The outer error is a link failure. The inner result is the peer's
    /// reply. A stream the peer finished without a reply is an inner error:
    /// the peer refused the request before its handler, and a resend gets
    /// the same answer.
    async fn try_send_once(
        &self,
        target: u64,
        inner: &[u8],
        read_timeout: Duration,
    ) -> Result<Result<RaftRpc>> {
        let conn = self.get_or_connect(target).await?;
        self.verify_connection_target(&conn, target)?;

        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| ClusterError::Transport {
            detail: format!("open_bi to node {target}: {e}"),
        })?;

        let envelope = self.wrap_inner(inner)?;
        send.write_all(&envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write to node {target}: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish send to node {target}: {e}"),
        })?;

        let response_envelope =
            tokio::time::timeout(read_timeout, read_envelope_or_finish(&mut recv))
                .await
                .map_err(|_| ClusterError::Transport {
                    detail: format!(
                        "RPC timeout ({}ms) to node {target}",
                        read_timeout.as_millis()
                    ),
                })??;
        let Some(response_envelope) = response_envelope else {
            return Ok(Err(ClusterError::RemoteUntyped {
                detail: format!("node {target} finished the stream without a reply"),
            }));
        };

        // Envelope / MAC / replay-window / codec errors are not transport
        // errors — return them wrapped in Ok so retry logic doesn't retry
        // a failed MAC as if it were a flaky network.
        Ok(self.parse_inbound(&response_envelope, Some(target)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_codec::{FrameRefusal, PongResponse, RequestRefusal};

    #[test]
    fn a_typed_refusal_is_a_final_answer() {
        let refusal = RaftRpc::RequestRefused(RequestRefusal::from(ClusterError::GroupNotFound {
            group_id: 4,
        }));
        match classify(1, Ok(Ok(refusal))) {
            Attempt::Answered(Err(ClusterError::GroupNotFound { group_id: 4 })) => {}
            other => panic!("expected a final GroupNotFound, got {other:?}"),
        }
    }

    #[test]
    fn a_frame_refusal_is_resent() {
        let refusal = RaftRpc::FrameRefused(FrameRefusal {
            detail: "stale sequence".into(),
        });
        assert!(matches!(
            classify(1, Ok(Ok(refusal))),
            Attempt::FrameRefused(_)
        ));
    }

    #[test]
    fn only_the_outer_error_is_a_link_failure() {
        let link = ClusterError::Transport {
            detail: "connection lost".into(),
        };
        assert!(matches!(classify(1, Err(link)), Attempt::LinkFailed(_)));

        let unreadable = ClusterError::Codec {
            detail: "bad crc".into(),
        };
        assert!(matches!(
            classify(1, Ok(Err(unreadable))),
            Attempt::Answered(Err(ClusterError::Codec { .. }))
        ));

        let pong = RaftRpc::Pong(PongResponse {
            responder_id: 1,
            topology_version: 2,
        });
        assert!(matches!(
            classify(1, Ok(Ok(pong))),
            Attempt::Answered(Ok(RaftRpc::Pong(_)))
        ));
    }
}
