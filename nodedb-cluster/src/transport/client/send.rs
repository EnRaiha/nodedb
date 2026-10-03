// SPDX-License-Identifier: BUSL-1.1

//! Outbound RPC: encode, wrap in authenticated envelope, dial/reuse,
//! send-and-receive. The retry loop and the circuit breaker live in
//! [`super::attempt`].
//!
//! Every outbound RPC is wrapped in an
//! [`auth_envelope`](crate::rpc_codec::auth_envelope)-framed wire message
//! carrying `from_node_id = self.auth.local_node_id`, a per-peer monotonic
//! `seq`, and an HMAC-SHA256 MAC. Responses are the same shape and are
//! run through the per-peer replay window before being decoded.

use std::net::SocketAddr;

use futures::Stream;
use rustls::pki_types::CertificateDer;

use crate::error::{ClusterError, Result};
use crate::rpc_codec::{self, RaftRpc, TypedClusterError, auth_envelope};
use crate::transport::config::SNI_HOSTNAME;
use crate::transport::frame_io::read_envelope;
use crate::transport::peer_identity_verifier::{
    VerifyOutcome, spki_pin_from_cert_der, verify_peer_identity,
};
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
        tokio::time::timeout(
            self.rpc_timeout,
            self.send_rpc_to_addr_inner(addr, rpc, true),
        )
        .await
        .map_err(|_| ClusterError::Transport {
            detail: format!("RPC timeout ({}ms) to {addr}", self.rpc_timeout.as_millis()),
        })?
    }

    /// Send to an address supplied by a response from the pinned bootstrap
    /// issuer. The redirect is already authenticated by the issuer envelope,
    /// so the redirected peer is authenticated by cluster CA + envelope MAC
    /// rather than by the issuer's leaf pin.
    pub(crate) async fn send_rpc_to_authenticated_redirect(
        &self,
        addr: SocketAddr,
        rpc: RaftRpc,
    ) -> Result<RaftRpc> {
        tokio::time::timeout(
            self.rpc_timeout,
            self.send_rpc_to_addr_inner(addr, rpc, false),
        )
        .await
        .map_err(|_| ClusterError::Transport {
            detail: format!("RPC timeout ({}ms) to {addr}", self.rpc_timeout.as_millis()),
        })?
    }

    async fn send_rpc_to_addr_inner(
        &self,
        addr: SocketAddr,
        rpc: RaftRpc,
        enforce_bootstrap_pin: bool,
    ) -> Result<RaftRpc> {
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

        if enforce_bootstrap_pin && let Some(expected_spki) = self.bootstrap_peer_spki {
            let peer_cert = conn
                .peer_identity()
                .and_then(|identity| identity.downcast::<Vec<CertificateDer<'static>>>().ok())
                .and_then(|chain| chain.first().cloned())
                .ok_or_else(|| ClusterError::Transport {
                    detail: format!("bootstrap peer {addr} supplied no leaf certificate"),
                })?;
            let actual_spki = spki_pin_from_cert_der(peer_cert.as_ref()).map_err(|error| {
                ClusterError::Transport {
                    detail: format!("bootstrap peer {addr} has invalid leaf: {error}"),
                }
            })?;
            if actual_spki != expected_spki {
                return Err(ClusterError::Transport {
                    detail: format!("bootstrap peer {addr} does not match join-token issuer"),
                });
            }
        }

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

        // Numbered once the stream is open (see `wrap_inner`).
        let envelope = self.wrap_outbound(&rpc)?;
        send.write_all(&envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write to {addr}: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish send to {addr}: {e}"),
        })?;

        let response_envelope = read_envelope(&mut recv).await?;
        match self.parse_inbound(&response_envelope, None)? {
            RaftRpc::FrameRefused(refusal) => Err(ClusterError::Transport {
                detail: format!("{addr} refused the frame: {}", refusal.detail),
            }),
            RaftRpc::RequestRefused(refusal) => Err(refusal.into_error()),
            response => Ok(response),
        }
    }

    /// Fire-and-forget one-way RPC: encode, send, and return without reading a
    /// reply. For best-effort notifications (e.g. `TimeoutNow`) where the caller
    /// expects no response and tolerates loss — no retry or circuit-breaker, so
    /// a dropped frame is the caller's concern to recover from.
    pub async fn send_rpc_oneway(&self, target: u64, rpc: RaftRpc) -> Result<()> {
        let inner = rpc_codec::encode(&rpc, &self.auth.epoch)?;
        let conn = self.get_or_connect(target).await?;
        self.verify_connection_target(&conn, target)?;
        let (mut send, _recv) = conn.open_bi().await.map_err(|e| ClusterError::Transport {
            detail: format!("oneway open_bi to node {target}: {e}"),
        })?;
        let envelope = self.wrap_inner(&inner)?;
        send.write_all(&envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("oneway write to node {target}: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("oneway finish to node {target}: {e}"),
        })?;
        Ok(())
    }

    /// Open a streaming RPC to `target` and return a stream of result chunks.
    ///
    /// Sibling of the one-shot send in [`super::attempt`] for multi-frame
    /// responses: opens a bidi stream on the pooled connection, writes the
    /// request envelope (typically a `RaftRpc::ExecuteStreamRequest`), finishes
    /// the send side, and returns a [`Stream`] that loops
    /// [`read_envelope`] until the terminal `RPC_EXECUTE_STREAM_END`
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
        let inner = rpc_codec::encode(&rpc, &self.auth.epoch)?;
        let conn = self.get_or_connect(target).await?;
        self.verify_connection_target(&conn, target)?;

        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| ClusterError::Transport {
            detail: format!("open_bi (stream) to node {target}: {e}"),
        })?;

        let envelope = self.wrap_inner(&inner)?;
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
                let envelope = read_envelope(&mut recv).await?;
                let (fields, inner_frame) =
                    auth_envelope::parse_envelope(&envelope, &auth.mac_key)?;
                if fields.from_node_id != target {
                    Err(ClusterError::Transport {
                        detail: format!(
                            "streaming response identity mismatch: expected node {target}, got {}",
                            fields.from_node_id
                        ),
                    })?;
                }
                // Replay-window check, skipping self-addressed frames (same
                // reasoning as `parse_inbound`).
                if fields.from_node_id != local_node_id {
                    auth.peer_seq_in.accept(fields.from_node_id, fields.seq)?;
                }
                match rpc_codec::decode(inner_frame, &auth.epoch)? {
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
                    RaftRpc::FrameRefused(refusal) => {
                        Err(ClusterError::Transport {
                            detail: format!(
                                "node {target} refused the stream request: {}",
                                refusal.detail
                            ),
                        })?;
                        return;
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

    /// Encode and wrap an RPC in an authenticated envelope.
    fn wrap_outbound(&self, rpc: &RaftRpc) -> Result<Vec<u8>> {
        let inner = rpc_codec::encode(rpc, &self.auth.epoch)?;
        self.wrap_inner(&inner)
    }

    /// Wrap an already-encoded inner frame in an authenticated envelope.
    ///
    /// Callers wrap once the frame's stream is open, right before the write.
    /// A number taken earlier would wait out the connect and the stream
    /// credit while later frames go ahead of it, and reach the peer further
    /// out of order than its replay window allows.
    pub(super) fn wrap_inner(&self, inner: &[u8]) -> Result<Vec<u8>> {
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

    /// Check that `conn` reaches the TLS identity pinned for `target`.
    pub(super) fn verify_connection_target(
        &self,
        conn: &quinn::Connection,
        target: u64,
    ) -> Result<()> {
        if !self.identity_store.enforces_peer_identity()
            || self.identity_store.bootstrap_window_open()
        {
            return Ok(());
        }
        let expected =
            self.identity_store
                .get_node_info(target)
                .ok_or_else(|| ClusterError::Transport {
                    detail: format!("target node {target} has no pinned topology identity"),
                })?;
        let identity = conn
            .peer_identity()
            .ok_or_else(|| ClusterError::Transport {
                detail: format!("target node {target} did not present a TLS identity"),
            })?;
        let certs: &Vec<rustls::pki_types::CertificateDer<'static>> = identity
            .downcast_ref()
            .ok_or_else(|| ClusterError::Transport {
                detail: format!("target node {target} presented an unsupported TLS identity"),
            })?;
        let cert = certs.first().ok_or_else(|| ClusterError::Transport {
            detail: format!("target node {target} presented no leaf certificate"),
        })?;
        match verify_peer_identity(&expected, cert.as_ref()) {
            VerifyOutcome::Accepted { .. } => Ok(()),
            VerifyOutcome::Rejected => Err(ClusterError::Transport {
                detail: format!("TLS identity does not match target node {target}"),
            }),
        }
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
    pub(super) fn parse_inbound(
        &self,
        envelope: &[u8],
        expected_node_id: Option<u64>,
    ) -> Result<RaftRpc> {
        let (fields, inner_frame) = auth_envelope::parse_envelope(envelope, &self.auth.mac_key)?;
        if let Some(expected) = expected_node_id
            && fields.from_node_id != expected
        {
            return Err(ClusterError::Transport {
                detail: format!(
                    "response identity mismatch: expected node {expected}, got {}",
                    fields.from_node_id
                ),
            });
        }
        if fields.from_node_id != self.auth.local_node_id {
            self.auth
                .peer_seq_in
                .accept(fields.from_node_id, fields.seq)?;
        }
        rpc_codec::decode(inner_frame, &self.auth.epoch)
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
    ClusterError::StreamTerminal {
        error: Box::new(err),
        detail,
    }
}
