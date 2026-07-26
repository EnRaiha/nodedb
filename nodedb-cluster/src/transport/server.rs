// SPDX-License-Identifier: BUSL-1.1

//! Inbound Raft RPC handling.
//!
//! Accepts connections from the QUIC endpoint, dispatches incoming bidi
//! streams to a [`RaftRpcHandler`], and writes back the response frame.
//!
//! # Authenticated wire envelope
//!
//! Every on-wire message is an [`auth_envelope`]-wrapped
//! [`rpc_codec`] frame. The envelope carries `from_node_id`, a per-peer
//! monotonic `seq`, and an HMAC-SHA256 MAC. [`handle_stream`]:
//!
//! 1. reads one envelope from the QUIC stream,
//! 2. verifies the MAC against the cluster MAC key held by
//!    [`AuthContext`],
//! 3. rejects replays via the per-peer sliding window,
//!    3b. verifies the TLS peer certificate identity against the topology pin,
//! 4. decodes the inner frame and dispatches to the handler,
//! 5. wraps the handler's response in its own authenticated envelope
//!    with `from_node_id = local_node_id` and a fresh outbound seq for
//!    the caller's id.
//!
//! # Cooperative shutdown
//!
//! Every long-lived `.await` is wrapped in a `tokio::select!` over a
//! `watch::Receiver<bool>` shutdown signal that is cloned into every
//! spawned child task, so graceful shutdown promptly releases handler
//! Arcs held by grandchild per-stream tasks.
//!
//! [`auth_envelope`]: crate::rpc_codec::auth_envelope
//! [`rpc_codec`]: crate::rpc_codec

use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::error::{ClusterError, Result};
use crate::forward::ChunkSink;
use crate::rpc_codec::{
    self, ExecuteStreamChunk, ExecuteStreamEnd, MAX_RPC_PAYLOAD_SIZE, RaftRpc, auth_envelope,
};
use crate::transport::auth_context::AuthContext;
use crate::transport::identity_admission::enrollment_matches;
use crate::transport::peer_identity_store::PeerIdentityStore;
use crate::transport::peer_identity_verifier::{
    IDENTITY_MISMATCH_QUIC_ERROR, VerifyOutcome, verify_peer_identity,
};
use crate::transport::rpc_handler::RaftRpcHandler;
use crate::wire_version::handshake_io::{local_version_range, perform_version_handshake_server};

use super::stream_dispatch;

/// Transport-local [`ChunkSink`] that writes one `RPC_EXECUTE_STREAM_CHUNK`
/// envelope per chunk to a QUIC send stream.
///
/// Each chunk gets a fresh outbound `seq` (mirroring the one-shot response
/// path in [`handle_stream`]). The `write_all` is awaited inline so QUIC flow
/// control throttles the producer — the chunk MUST NOT be detached into a
/// spawned task.
struct QuicChunkSink<'a> {
    send: &'a mut quinn::SendStream,
    auth: &'a AuthContext,
}

impl ChunkSink for QuicChunkSink<'_> {
    async fn send_chunk(
        &mut self,
        payload: Vec<u8>,
        watermark_lsn: u64,
        // The `ExecuteStream` wire chunk only carries `watermark_lsn`; the
        // per-collection read version is surfaced separately on the shuffle
        // produce reply, not on this streaming path.
        _read_version_lsn: u64,
    ) -> Result<()> {
        let rpc = RaftRpc::ExecuteStreamChunk(ExecuteStreamChunk {
            payload,
            watermark_lsn,
        });
        let inner = rpc_codec::encode(&rpc)?;
        let seq = self.auth.peer_seq_out.next();
        let mut envelope = Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + inner.len());
        auth_envelope::write_envelope(
            self.auth.local_node_id,
            seq,
            &inner,
            &self.auth.mac_key,
            &mut envelope,
        )?;
        self.send
            .write_all(&envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write stream chunk: {e}"),
            })
    }
}

/// Extract the peer's leaf certificate DER bytes from a QUIC connection.
///
/// Returns `None` if the peer did not present a certificate (insecure
/// transport) or if the runtime-type downcast fails.
fn peer_leaf_cert_der(conn: &quinn::Connection) -> Option<Vec<u8>> {
    let identity = conn.peer_identity()?;
    let certs: &Vec<CertificateDer<'static>> = identity.downcast_ref()?;
    certs.first().map(|c| c.as_ref().to_vec())
}

/// Handle all bidi streams on a single connection.
///
/// Exits cleanly (Ok) on shutdown, on normal connection close,
/// or on unrecoverable transport error.
pub(crate) async fn handle_connection<H: RaftRpcHandler, S: PeerIdentityStore + ?Sized>(
    conn: quinn::Connection,
    handler: Arc<H>,
    auth: Arc<AuthContext>,
    identity_store: Arc<S>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    // Extract the peer cert once per connection; it does not change.
    let peer_cert_der: Option<Vec<u8>> = peer_leaf_cert_der(&conn);
    let peer_addr = conn.remote_address();

    // Perform the wire-version handshake on the first bidi stream before
    // dispatching any RPCs. The client opens a dedicated stream for this
    // exchange; subsequent streams on the same connection are RPC streams.
    let agreed_version = {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
                // Spurious change — retry the accept.
                conn.accept_bi().await
            }
            result = conn.accept_bi() => result,
        };

        let (mut hs_send, mut hs_recv) = match accepted {
            Ok(streams) => streams,
            Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(()),
            Err(quinn::ConnectionError::LocallyClosed) => return Ok(()),
            Err(e) => {
                return Err(ClusterError::Transport {
                    detail: format!("accept handshake stream from {peer_addr}: {e}"),
                });
            }
        };

        let local = local_version_range();
        match perform_version_handshake_server(&conn, &mut hs_send, &mut hs_recv).await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    peer_addr = %peer_addr,
                    local_min = %local.min,
                    local_max = %local.max,
                    error = %e,
                    "wire version handshake failed; closing connection"
                );
                // perform_version_handshake_server already closed the QUIC
                // connection on range mismatch; propagate the error so the
                // caller logs it and the connection task exits.
                return Err(e);
            }
        }
    };

    debug!(
        peer_addr = %peer_addr,
        agreed_version = %agreed_version,
        "wire version handshake complete"
    );

    loop {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            result = conn.accept_bi() => result,
        };

        let (send, recv) = match accepted {
            Ok(streams) => streams,
            Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(()),
            Err(quinn::ConnectionError::LocallyClosed) => return Ok(()),
            Err(e) => {
                return Err(ClusterError::Transport {
                    detail: format!("accept_bi: {e}"),
                });
            }
        };

        let ctx = StreamContext {
            handler: handler.clone(),
            auth: auth.clone(),
            identity_store: identity_store.clone(),
            peer_cert_der: peer_cert_der.clone(),
            conn: conn.clone(),
            shutdown: shutdown.clone(),
        };
        tokio::spawn(async move {
            if let Err(e) = handle_stream(ctx, send, recv).await {
                debug!(error = %e, "raft RPC stream error");
            }
        });
    }
}

/// Per-stream context passed to [`handle_stream`].
///
/// Bundles the shared, connection-scoped handles so [`handle_stream`] stays
/// under the `too_many_arguments` threshold while remaining generic over
/// handler and identity-store types.
struct StreamContext<H: RaftRpcHandler, S: PeerIdentityStore + ?Sized> {
    handler: Arc<H>,
    auth: Arc<AuthContext>,
    identity_store: Arc<S>,
    peer_cert_der: Option<Vec<u8>>,
    conn: quinn::Connection,
    shutdown: watch::Receiver<bool>,
}

fn reject_peer_identity(conn: &quinn::Connection, node_id: u64) -> Result<()> {
    warn!(node_id, "peer identity mismatch — closing connection");
    conn.close(IDENTITY_MISMATCH_QUIC_ERROR, b"peer identity mismatch");
    Err(ClusterError::Transport {
        detail: format!("peer identity mismatch for node {node_id}"),
    })
}

/// Handle a single bidi stream: read request → dispatch → write response.
///
/// Every long-lived await is racing a shutdown signal — see the
/// module docstring for the rationale.
async fn handle_stream<H: RaftRpcHandler, S: PeerIdentityStore + ?Sized>(
    ctx: StreamContext<H, S>,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) -> Result<()> {
    let StreamContext {
        handler,
        auth,
        identity_store,
        peer_cert_der,
        conn,
        mut shutdown,
    } = ctx;
    let work = async {
        // 1. Read one envelope.
        let envelope = read_envelope(&mut recv).await?;
        let (fields, inner_frame) = auth_envelope::parse_envelope(&envelope, &auth.mac_key)?;

        // 2. Replay window — under the advertised from_node_id (MAC-verified).
        //    Self-addressed frames skip the window: when a node dispatches
        //    an RPC to itself over the transport, the shared `AuthContext`
        //    means one window is updated by both the server-side request
        //    accept (here) and the client-side response accept (in
        //    `send.rs::parse_inbound`). Skipping when `from == local`
        //    keeps the two flows from tripping on each other's entries —
        //    a self-addressed frame can't have been replayed by an
        //    external attacker by definition.
        if fields.from_node_id != auth.local_node_id {
            auth.peer_seq_in.accept(fields.from_node_id, fields.seq)?;
        }

        // 3. Decode before the identity decision so an unknown, CA-verified
        // peer can be restricted to exactly one enrollment operation.
        let request = rpc_codec::decode(inner_frame)?;
        validate_join_sender(&request, fields.from_node_id)?;

        // 3b. Bind the MAC-authenticated node id to the mTLS leaf identity.
        // Unknown identities may submit only a JoinRequest whose node id and
        // advertised pins exactly match that leaf. Every other RPC fails
        // closed until the successful join is visible in topology.
        if fields.from_node_id != auth.local_node_id && identity_store.enforces_peer_identity() {
            let cert_der = peer_cert_der
                .as_deref()
                .ok_or_else(|| ClusterError::Transport {
                    detail: "authenticated cluster peer did not present a leaf certificate".into(),
                })?;
            match identity_store.get_node_info(fields.from_node_id) {
                Some(ref info) => match verify_peer_identity(info, cert_der) {
                    VerifyOutcome::Accepted { method } => {
                        debug!(
                            node_id = fields.from_node_id,
                            ?method,
                            "peer identity verified"
                        );
                    }
                    VerifyOutcome::Rejected => {
                        reject_peer_identity(&conn, fields.from_node_id)?;
                    }
                },
                None if enrollment_matches(
                    &request,
                    fields.from_node_id,
                    cert_der,
                    &*identity_store,
                ) =>
                {
                    debug!(
                        node_id = fields.from_node_id,
                        "accepted identity-bound cluster join enrollment"
                    );
                }
                None => {
                    reject_peer_identity(&conn, fields.from_node_id)?;
                }
            }
        }

        // 4b. Streaming path: an `ExecuteStreamRequest` produces a multi-frame
        //     response — N `RPC_EXECUTE_STREAM_CHUNK` envelopes (each written
        //     inline so QUIC flow control throttles the producer) followed by
        //     exactly one `RPC_EXECUTE_STREAM_END` envelope, then `finish()`.
        //     The non-streaming path below is unchanged: one response envelope.
        if let RaftRpc::ExecuteStreamRequest(req) = request {
            let terminal = {
                let sink = QuicChunkSink {
                    send: &mut send,
                    auth: &auth,
                };
                handler.handle_rpc_streaming(req, sink).await
            };

            let end_rpc = RaftRpc::ExecuteStreamEnd(ExecuteStreamEnd { error: terminal });
            let end_inner = rpc_codec::encode(&end_rpc)?;
            let end_seq = auth.peer_seq_out.next();
            let mut end_envelope =
                Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + end_inner.len());
            auth_envelope::write_envelope(
                auth.local_node_id,
                end_seq,
                &end_inner,
                &auth.mac_key,
                &mut end_envelope,
            )?;
            send.write_all(&end_envelope)
                .await
                .map_err(|e| ClusterError::Transport {
                    detail: format!("write stream end: {e}"),
                })?;
            send.finish().map_err(|e| ClusterError::Transport {
                detail: format!("finish stream response: {e}"),
            })?;
            return Ok::<(), ClusterError>(());
        }

        // 4c. Cross-node streaming shuffle (E1): a `ShufflePushRequest` is the
        //     opening frame of a producer → receiver stream. The producer keeps
        //     writing `ShufflePushChunk` envelopes on the SAME bidi stream
        //     (this read half), terminated by exactly one `ShufflePushEnd`.
        //     The server reads inbound frames, deposits them via the handler,
        //     and writes NO reply — the producer fire-and-finishes (mirroring
        //     the response-direction `send_rpc_stream`, which finishes its send
        //     half before reading). The loop exits on the `End` frame or on a
        //     clean stream close (`read_envelope` surfacing a transport error
        //     after the producer's `finish()`).
        if let RaftRpc::ShufflePushRequest(req) = request {
            let shuffle_id = req.shuffle_id;
            let part = req.part;
            let side = req.side;
            handler.on_shuffle_request(req).await;

            loop {
                let frame_envelope = match read_envelope(&mut recv).await {
                    Ok(e) => e,
                    // Producer closed the stream without (or after) an End.
                    // A graceful finish surfaces here as a transport read
                    // error; treat it as end-of-stream rather than propagating.
                    Err(_) => return Ok::<(), ClusterError>(()),
                };
                let (frame_fields, frame_inner) =
                    auth_envelope::parse_envelope(&frame_envelope, &auth.mac_key)?;
                if frame_fields.from_node_id != fields.from_node_id {
                    reject_peer_identity(&conn, frame_fields.from_node_id)?;
                }
                if frame_fields.from_node_id != auth.local_node_id {
                    auth.peer_seq_in
                        .accept(frame_fields.from_node_id, frame_fields.seq)?;
                }
                match rpc_codec::decode(frame_inner)? {
                    RaftRpc::ShufflePushChunk(chunk) => {
                        handler
                            .on_shuffle_chunk(shuffle_id, part, side, chunk.payload)
                            .await?;
                    }
                    RaftRpc::ShufflePushEnd(end) => {
                        handler
                            .on_shuffle_end(shuffle_id, part, side, end.error)
                            .await;
                        return Ok::<(), ClusterError>(());
                    }
                    other => {
                        return Err(ClusterError::Transport {
                            detail: format!("unexpected frame in shuffle push stream: {other:?}"),
                        });
                    }
                }
            }
        }

        // 4d/4e/4f. One-shot shuffle RPCs (ShuffleProduce / ShuffleConsume /
        //     ShuffleAggregateConsume). Each is a single request/response — no
        //     additional frames on `recv`. Handled in a shared helper to keep
        //     this function under the file-size limit.
        let request =
            match stream_dispatch::try_handle_oneshot_rpc(&*handler, request, &mut send, &auth)
                .await?
            {
                None => return Ok::<(), ClusterError>(()),
                Some(req) => req,
            };

        let response = handler.handle_rpc(request).await?;

        // 5. Wrap the response in its own envelope. `from = local_node_id`,
        //    `seq = next outbound seq scoped to the caller`.
        let response_inner = rpc_codec::encode(&response)?;
        let response_seq = auth.peer_seq_out.next();
        let mut response_envelope =
            Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + response_inner.len());
        auth_envelope::write_envelope(
            auth.local_node_id,
            response_seq,
            &response_inner,
            &auth.mac_key,
            &mut response_envelope,
        )?;

        send.write_all(&response_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write response: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish response: {e}"),
        })?;
        Ok::<(), ClusterError>(())
    };

    tokio::select! {
        biased;
        _ = shutdown.changed() => Ok(()),
        result = work => result,
    }
}

fn validate_join_sender(request: &RaftRpc, authenticated_node_id: u64) -> Result<()> {
    if let RaftRpc::JoinRequest(join) = request
        && join.node_id != authenticated_node_id
    {
        return Err(ClusterError::Transport {
            detail: format!(
                "join request node_id {} does not match authenticated sender {}",
                join.node_id, authenticated_node_id
            ),
        });
    }
    Ok(())
}

/// Read a complete authenticated envelope from a QUIC receive stream.
///
/// Reads the fixed envelope pre-header (version + from_node_id + seq +
/// inner_len), then the inner frame, then the MAC tag. Returns the full
/// envelope bytes for caller-side parsing.
pub(crate) async fn read_envelope(recv: &mut quinn::RecvStream) -> Result<Vec<u8>> {
    // Envelope header is version(1) + from_node_id(8) + seq(8) + inner_len(4).
    const ENV_HDR_LEN: usize = 21;

    let mut hdr = [0u8; ENV_HDR_LEN];
    recv.read_exact(&mut hdr)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("read envelope header: {e}"),
        })?;

    let inner_len = u32::from_le_bytes([hdr[17], hdr[18], hdr[19], hdr[20]]);
    if inner_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!(
                "envelope inner length {inner_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"
            ),
        });
    }

    let total = ENV_HDR_LEN + inner_len as usize + rpc_codec::MAC_LEN;
    let mut buf = vec![0u8; total];
    buf[..ENV_HDR_LEN].copy_from_slice(&hdr);
    if total > ENV_HDR_LEN {
        recv.read_exact(&mut buf[ENV_HDR_LEN..])
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("read envelope payload+mac: {e}"),
            })?;
    }

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_codec::JoinRequest;

    #[test]
    fn join_request_must_match_authenticated_sender() {
        let request = RaftRpc::JoinRequest(JoinRequest {
            node_id: 9,
            listen_addr: "127.0.0.1:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            spiffe_id: None,
            spki_pin: Some(vec![1; 32]),
        });
        assert!(validate_join_sender(&request, 9).is_ok());
        let error = validate_join_sender(&request, 8).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match authenticated sender 8")
        );
    }
}
