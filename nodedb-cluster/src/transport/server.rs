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
    self, ExecuteRequest, ExecuteStreamChunk, ExecuteStreamEnd, MAX_RPC_PAYLOAD_SIZE, RaftRpc,
    ShufflePushRequest, TypedClusterError, auth_envelope,
};
use crate::topology::NodeInfo;
use crate::transport::auth_context::AuthContext;
use crate::transport::peer_identity_verifier::{
    IDENTITY_MISMATCH_QUIC_ERROR, VerifyOutcome, verify_peer_identity,
};
use crate::wire_version::handshake_io::{local_version_range, perform_version_handshake_server};

/// Topology-decoupled lookup for per-node identity pins.
///
/// The server needs to check the TLS peer cert against the pinned
/// `NodeInfo` for the MAC-verified `from_node_id`, but it must not
/// take a direct dependency on `ClusterState` or `ClusterTopology`
/// (which would create a circular crate dependency and would be hard
/// to test).  Implementors wrap whatever topology store they have.
///
/// The `NoopIdentityStore` below is used in insecure-transport mode
/// and in unit tests that do not exercise the identity layer.
///
/// The `find_by_spki` and `find_by_spiffe` methods are called by the
/// TLS-layer [`PinnedClientVerifier`] and [`PinnedServerVerifier`]
/// during the QUIC handshake (before `node_id` is known from the MAC
/// envelope). They search the topology by the cert's identity rather
/// than by node_id.
///
/// [`PinnedClientVerifier`]: crate::transport::config::PinnedClientVerifier
/// [`PinnedServerVerifier`]: crate::transport::config::PinnedServerVerifier
pub trait PeerIdentityStore: Send + Sync + 'static {
    /// Return the `NodeInfo` for the given node_id, or `None` if
    /// the node is not in the topology (treat as bootstrap window).
    fn get_node_info(&self, node_id: u64) -> Option<NodeInfo>;

    /// Return the `NodeInfo` for the node whose pinned SPKI fingerprint
    /// matches `spki`, or `None` if no node in the topology has that pin.
    ///
    /// Used by the TLS-layer verifiers during the handshake (before the
    /// MAC envelope reveals `node_id`).
    fn find_by_spki(&self, spki: &[u8; 32]) -> Option<NodeInfo>;

    /// Return the `NodeInfo` for the node whose pinned SPIFFE id matches
    /// `spiffe_id`, or `None` if no node has that id.
    ///
    /// Used by the TLS-layer verifiers during the handshake.
    fn find_by_spiffe(&self, spiffe_id: &str) -> Option<NodeInfo>;
}

/// Always returns `None`, accepting every peer as a bootstrap window.
///
/// Used when mTLS is disabled (insecure transport) or in unit tests
/// that focus on HMAC / codec layers rather than identity binding.
pub struct NoopIdentityStore;

impl PeerIdentityStore for NoopIdentityStore {
    fn get_node_info(&self, _node_id: u64) -> Option<NodeInfo> {
        None
    }

    fn find_by_spki(&self, _spki: &[u8; 32]) -> Option<NodeInfo> {
        None
    }

    fn find_by_spiffe(&self, _spiffe_id: &str) -> Option<NodeInfo> {
        None
    }
}

/// Trait for handling incoming Raft RPCs.
///
/// Implementors receive a request [`RaftRpc`] and return the corresponding
/// response variant. The transport calls this for each incoming bidi stream.
pub trait RaftRpcHandler: Send + Sync + 'static {
    fn handle_rpc(&self, rpc: RaftRpc)
    -> impl std::future::Future<Output = Result<RaftRpc>> + Send;

    /// Handle a streaming `ExecuteStreamRequest`: execute the plan and push
    /// each result frame to `sink`. Returns `None` on a clean EOF or
    /// `Some(err)` on a terminal failure. The transport writes one
    /// `RPC_EXECUTE_STREAM_END` envelope carrying this outcome after the call
    /// returns. See [`crate::forward::PlanExecutor::execute_plan_streaming`].
    fn handle_rpc_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> impl std::future::Future<Output = Option<TypedClusterError>> + Send;

    /// Cross-node streaming shuffle (E1) — opening frame of a `ShufflePush`
    /// stream. Lazily creates the receiver inbox for `(shuffle_id, part, side)`
    /// carrying `producer_count` and `num_parts`.
    fn on_shuffle_request(
        &self,
        req: ShufflePushRequest,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Deposit one shuffle chunk payload into the receiver inbox. Bounded —
    /// the implementation blocks while the inbox buffer is full so QUIC flow
    /// control back-pressures the producer.
    fn on_shuffle_chunk(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Terminal frame for one producer of a `ShufflePush` stream: record the
    /// `End` (advancing the per-part build barrier) and capture any terminal
    /// error.
    fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<TypedClusterError>,
    ) -> impl std::future::Future<Output = ()> + Send;
}

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
    async fn send_chunk(&mut self, payload: Vec<u8>, watermark_lsn: u64) -> Result<()> {
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
pub(crate) async fn handle_connection<H: RaftRpcHandler, S: PeerIdentityStore>(
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
struct StreamContext<H: RaftRpcHandler, S: PeerIdentityStore> {
    handler: Arc<H>,
    auth: Arc<AuthContext>,
    identity_store: Arc<S>,
    peer_cert_der: Option<Vec<u8>>,
    conn: quinn::Connection,
    shutdown: watch::Receiver<bool>,
}

/// Handle a single bidi stream: read request → dispatch → write response.
///
/// Every long-lived await is racing a shutdown signal — see the
/// module docstring for the rationale.
async fn handle_stream<H: RaftRpcHandler, S: PeerIdentityStore>(
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

        // 3b. Peer identity check — binds the MAC-verified node_id to the
        //     TLS certificate.  Self-addressed frames skip the check by the
        //     same reasoning as the replay window above.
        if fields.from_node_id != auth.local_node_id
            && let Some(cert_der) = &peer_cert_der
        {
            let node_info = identity_store.get_node_info(fields.from_node_id);
            match node_info {
                Some(ref info) => match verify_peer_identity(info, cert_der) {
                    VerifyOutcome::Accepted { method } => {
                        debug!(
                            node_id = fields.from_node_id,
                            ?method,
                            "peer identity verified"
                        );
                    }
                    VerifyOutcome::BootstrapAccepted => {
                        warn!(
                            node_id = fields.from_node_id,
                            "peer identity not pinned — bootstrap window accepted"
                        );
                    }
                    VerifyOutcome::Rejected => {
                        warn!(
                            node_id = fields.from_node_id,
                            "peer identity mismatch — closing connection"
                        );
                        conn.close(IDENTITY_MISMATCH_QUIC_ERROR, b"peer identity mismatch");
                        return Err(ClusterError::Transport {
                            detail: format!(
                                "peer identity mismatch for node {}",
                                fields.from_node_id
                            ),
                        });
                    }
                },
                None => {
                    // Node not yet in topology — bootstrap window.
                    warn!(
                        node_id = fields.from_node_id,
                        "node not in topology — bootstrap window accepted"
                    );
                }
            }
        }

        // 4. Decode inner RPC and hand to handler.
        let request = rpc_codec::decode(inner_frame)?;

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
