// SPDX-License-Identifier: BUSL-1.1

//! Outbound RPC: encode, wrap in authenticated envelope, dial/reuse,
//! send-and-receive, retry + circuit breaker.
//!
//! Every outbound RPC is wrapped in an
//! [`auth_envelope`](crate::rpc_codec::auth_envelope)-framed wire message
//! carrying `from_node_id = self.auth.local_node_id`, a per-peer monotonic
//! `seq`, and an HMAC-SHA256 MAC. Responses are the same shape and are
//! run through the per-peer replay window before being decoded.

use std::net::SocketAddr;

use futures::Stream;
use tracing::debug;

use crate::circuit_breaker::RetryPolicy;
use crate::error::{ClusterError, Result};
use crate::rpc_codec::{
    self, RaftRpc, ShufflePushChunk, ShufflePushEnd, ShufflePushRequest, TypedClusterError,
    auth_envelope,
};
use crate::transport::config::SNI_HOSTNAME;
use crate::transport::server;
use crate::wire_version::handshake_io::perform_version_handshake_client;

use super::transport::NexarTransport;

impl NexarTransport {
    /// Send an RPC to an address directly (for bootstrap/join before peer
    /// IDs are known).
    ///
    /// The **entire** operation — handshake, stream open, write, read — is
    /// bounded by `self.rpc_timeout`. The response envelope's
    /// `from_node_id` is consulted for the inbound replay window only
    /// after MAC verification.
    pub async fn send_rpc_to_addr(&self, addr: SocketAddr, rpc: RaftRpc) -> Result<RaftRpc> {
        tokio::time::timeout(self.rpc_timeout, self.send_rpc_to_addr_inner(addr, rpc))
            .await
            .map_err(|_| ClusterError::Transport {
                detail: format!("RPC timeout ({}ms) to {addr}", self.rpc_timeout.as_millis()),
            })?
    }

    async fn send_rpc_to_addr_inner(&self, addr: SocketAddr, rpc: RaftRpc) -> Result<RaftRpc> {
        let envelope = self.wrap_outbound(&rpc)?;

        let conn = self
            .listener
            .endpoint()
            .connect_with(self.client_config.clone(), addr, SNI_HOSTNAME)
            .map_err(|e| ClusterError::Transport {
                detail: format!("connect to {addr}: {e}"),
            })?
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("handshake with {addr}: {e}"),
            })?;

        // Perform the wire-version handshake on the first bidi stream.
        {
            let (mut hs_send, mut hs_recv) =
                conn.open_bi().await.map_err(|e| ClusterError::Transport {
                    detail: format!("open handshake stream to {addr}: {e}"),
                })?;
            perform_version_handshake_client(&mut hs_send, &mut hs_recv).await?;
            let _ = hs_send.finish();
        }

        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| ClusterError::Transport {
            detail: format!("open_bi to {addr}: {e}"),
        })?;

        send.write_all(&envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write to {addr}: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish send to {addr}: {e}"),
        })?;

        let response_envelope = server::read_envelope(&mut recv).await?;
        self.parse_inbound(&response_envelope)
    }

    /// Send an RPC to a peer with retry and circuit breaker.
    pub async fn send_rpc(&self, target: u64, rpc: RaftRpc) -> Result<RaftRpc> {
        // Encode the inner RPC once (codec errors are not retryable).
        // Each retry wraps it in a fresh envelope so the seq advances
        // per attempt — a retry is a new frame, not a replayed frame.
        let inner = rpc_codec::encode(&rpc)?;

        let mut last_err = None;
        for attempt in 0..self.retry_policy.max_attempts {
            // Check circuit breaker before each attempt.
            self.circuit_breaker.check(target)?;

            if attempt > 0 {
                let delay = self.retry_policy.delay_for_attempt(attempt - 1);
                debug!(target, attempt, ?delay, "retrying RPC");
                tokio::time::sleep(delay).await;
            }

            let envelope = self.wrap_inner(&inner)?;
            match self.try_send_once(target, &envelope).await {
                Ok(resp) => {
                    self.circuit_breaker.record_success(target);
                    return resp;
                }
                Err(e) if RetryPolicy::is_retryable(&e) => {
                    self.circuit_breaker.record_failure(target);
                    // Evict stale connection so retry gets a fresh one.
                    self.evict_peer(target);
                    last_err = Some(e);
                }
                Err(e) => {
                    // Non-retryable error — fail immediately.
                    self.circuit_breaker.record_failure(target);
                    return Err(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| ClusterError::Transport {
            detail: format!("send_rpc to node {target}: all attempts exhausted"),
        }))
    }

    /// Open a streaming RPC to `target` and return a stream of result chunks.
    ///
    /// Sibling of [`try_send_once`](Self::try_send_once) for multi-frame
    /// responses: opens a bidi stream on the pooled connection, writes the
    /// request envelope (typically a `RaftRpc::ExecuteStreamRequest`), finishes
    /// the send side, and returns a [`Stream`] that loops
    /// [`server::read_envelope`] until the terminal `RPC_EXECUTE_STREAM_END`
    /// frame:
    ///
    /// - `RPC_EXECUTE_STREAM_CHUNK` → yields `Ok((payload, watermark_lsn))`.
    /// - `RPC_EXECUTE_STREAM_END{error: None}` → ends the stream cleanly.
    /// - `RPC_EXECUTE_STREAM_END{error: Some(e)}` → yields `Err` then ends.
    /// - a transport / decode error before the END frame → yields `Err` then
    ///   ends (the connection dropped mid-stream).
    ///
    /// There is NO retry wrapper on the stream body — retry only applies to the
    /// eager pre-first-frame phase, which the caller (coordinator) owns. Once
    /// the first frame has been observed, any error is terminal.
    pub async fn send_rpc_stream(
        &self,
        target: u64,
        rpc: RaftRpc,
    ) -> Result<impl Stream<Item = Result<(Vec<u8>, u64)>> + Send + use<>> {
        let envelope = self.wrap_outbound(&rpc)?;
        let conn = self.get_or_connect(target).await?;

        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| ClusterError::Transport {
            detail: format!("open_bi (stream) to node {target}: {e}"),
        })?;

        send.write_all(&envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write stream request to node {target}: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish stream request to node {target}: {e}"),
        })?;

        // Clone the shared auth so the returned stream owns its envelope-parsing
        // state and borrows neither `self` nor `conn` beyond the held `recv`.
        let auth = self.auth.clone();
        let local_node_id = self.auth.local_node_id;

        Ok(async_stream::try_stream! {
            // Keep the send half alive for the duration of the stream so the
            // server-side `accept_bi` stream is not reset early.
            let _send = send;
            loop {
                let envelope = server::read_envelope(&mut recv).await?;
                let (fields, inner_frame) =
                    auth_envelope::parse_envelope(&envelope, &auth.mac_key)?;
                // Replay-window check, skipping self-addressed frames (same
                // reasoning as `parse_inbound`).
                if fields.from_node_id != local_node_id {
                    auth.peer_seq_in.accept(fields.from_node_id, fields.seq)?;
                }
                match rpc_codec::decode(inner_frame)? {
                    RaftRpc::ExecuteStreamChunk(chunk) => {
                        yield (chunk.payload, chunk.watermark_lsn);
                    }
                    RaftRpc::ExecuteStreamEnd(end) => {
                        match end.error {
                            None => return,
                            Some(e) => {
                                Err(stream_terminal_error(e))?;
                                return;
                            }
                        }
                    }
                    other => {
                        Err(ClusterError::Transport {
                            detail: format!(
                                "unexpected frame in streaming response: {other:?}"
                            ),
                        })?;
                        return;
                    }
                }
            }
        })
    }

    /// Open a cross-node streaming-shuffle push to `target` (E1).
    ///
    /// Producer → receiver direction (the mirror of [`send_rpc_stream`], which
    /// streams a response back): opens a bidi stream on the pooled connection,
    /// writes the [`ShufflePushRequest`] envelope, then one [`ShufflePushChunk`]
    /// envelope per pre-batched payload, then exactly one [`ShufflePushEnd`]
    /// (clean EOF), and `finish()`es the send half. It does **not** read a
    /// reply — the server deposits the chunks and never writes back, so the
    /// helper fire-and-finishes.
    ///
    /// Each `batches` element is a standalone msgpack array of rows (the same
    /// convention as `RowBatch.payload`). E4 — the planner-side caller — is
    /// responsible for computing `partition_hash(row, keys) % num_parts` and
    /// grouping rows into per-partition batches before calling this; E1's
    /// helper takes already-partitioned payloads.
    pub async fn send_shuffle_push(
        &self,
        target: u64,
        req: ShufflePushRequest,
        batches: Vec<Vec<u8>>,
    ) -> Result<()> {
        let conn = self.get_or_connect(target).await?;
        let (mut send, _recv) = conn.open_bi().await.map_err(|e| ClusterError::Transport {
            detail: format!("open_bi (shuffle push) to node {target}: {e}"),
        })?;

        // Opening frame.
        let req_envelope = self.wrap_outbound(&RaftRpc::ShufflePushRequest(req))?;
        send.write_all(&req_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write shuffle push request to node {target}: {e}"),
            })?;

        // One chunk frame per pre-batched payload.
        for payload in batches {
            let chunk_envelope =
                self.wrap_outbound(&RaftRpc::ShufflePushChunk(ShufflePushChunk { payload }))?;
            send.write_all(&chunk_envelope)
                .await
                .map_err(|e| ClusterError::Transport {
                    detail: format!("write shuffle push chunk to node {target}: {e}"),
                })?;
        }

        // Terminal clean-EOF frame, then finish the send half.
        let end_envelope =
            self.wrap_outbound(&RaftRpc::ShufflePushEnd(ShufflePushEnd { error: None }))?;
        send.write_all(&end_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write shuffle push end to node {target}: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish shuffle push to node {target}: {e}"),
        })?;
        Ok(())
    }

    /// Single-attempt RPC send (no retry, no circuit breaker).
    async fn try_send_once(
        &self,
        target: u64,
        envelope: &[u8],
    ) -> std::result::Result<Result<RaftRpc>, ClusterError> {
        let conn = self.get_or_connect(target).await?;

        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| ClusterError::Transport {
            detail: format!("open_bi to node {target}: {e}"),
        })?;

        send.write_all(envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write to node {target}: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish send to node {target}: {e}"),
        })?;

        let response_envelope =
            tokio::time::timeout(self.rpc_timeout, server::read_envelope(&mut recv))
                .await
                .map_err(|_| ClusterError::Transport {
                    detail: format!(
                        "RPC timeout ({}ms) to node {target}",
                        self.rpc_timeout.as_millis()
                    ),
                })??;

        // Envelope / MAC / replay-window / codec errors are not transport
        // errors — return them wrapped in Ok so retry logic doesn't retry
        // a failed MAC as if it were a flaky network.
        Ok(self.parse_inbound(&response_envelope))
    }

    /// Encode and wrap an RPC in an authenticated envelope.
    fn wrap_outbound(&self, rpc: &RaftRpc) -> Result<Vec<u8>> {
        let inner = rpc_codec::encode(rpc)?;
        self.wrap_inner(&inner)
    }

    /// Wrap an already-encoded inner frame in an authenticated envelope.
    fn wrap_inner(&self, inner: &[u8]) -> Result<Vec<u8>> {
        let seq = self.auth.peer_seq_out.next();
        let mut out = Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + inner.len());
        auth_envelope::write_envelope(
            self.auth.local_node_id,
            seq,
            inner,
            &self.auth.mac_key,
            &mut out,
        )?;
        Ok(out)
    }

    /// Parse an inbound envelope: verify MAC, check replay window, decode
    /// inner RPC.
    ///
    /// Self-addressed frames skip the replay-window check. In a single-node
    /// test (or when a node genuinely dispatches an RPC to itself over the
    /// transport) the client and server share one `AuthContext`, which
    /// means one `peer_seq_in` window is updated by *both* the server-side
    /// request-accept and the client-side response-accept. Without this
    /// guard the second accept trips on its own first — the envelope
    /// was never replayed, the same window simply saw traffic from both
    /// directions for `peer_id == local_node_id`.
    fn parse_inbound(&self, envelope: &[u8]) -> Result<RaftRpc> {
        let (fields, inner_frame) = auth_envelope::parse_envelope(envelope, &self.auth.mac_key)?;
        if fields.from_node_id != self.auth.local_node_id {
            self.auth
                .peer_seq_in
                .accept(fields.from_node_id, fields.seq)?;
        }
        rpc_codec::decode(inner_frame)
    }
}

/// Map a terminal [`TypedClusterError`] carried by `ExecuteStreamEnd` into a
/// [`ClusterError`] for the stream's `Err` item.
///
/// A terminal error is reached only AFTER at least the stream was established,
/// so by the coordinator's retry-vs-stream contract it is never retried — it is
/// surfaced as-is. The original typed shape is preserved in the detail string.
fn stream_terminal_error(err: TypedClusterError) -> ClusterError {
    let detail = format!("{err:?}");
    ClusterError::StreamTerminal { error: err, detail }
}
