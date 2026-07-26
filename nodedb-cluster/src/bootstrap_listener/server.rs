// SPDX-License-Identifier: BUSL-1.1

//! Bootstrap-listener server loop and handler trait.
//!
//! The nodedb crate provides a concrete `BootstrapHandler` impl that
//! pairs a loaded `ClusterCa` (for issuing leaves) with a token
//! verifier (keyed by the local `cluster_secret`). `spawn_listener`
//! wires the handler into a nexar QUIC listener using a self-signed
//! server cert.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::error::{ClusterError, Result};

use super::protocol::{
    BootstrapCredsRequest, BootstrapCredsResponse, DELIVERY_ACK, MAX_FRAME_BYTES, decode_request,
    encode_response,
};

/// Host-side handler called for every validated `BootstrapCredsRequest`.
///
/// The nodedb crate supplies an implementation that (a) verifies the
/// token, (b) issues a leaf cert for `req.node_id` under the cluster
/// CA, (c) returns the bundle. The listener itself is transport glue
/// only — no crypto policy lives here.
pub trait BootstrapHandler: Send + Sync + 'static {
    fn handle<'a>(
        &'a self,
        req: BootstrapCredsRequest,
        remote_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = BootstrapCredsResponse> + Send + 'a>>;

    fn confirm_delivery<'a>(
        &'a self,
        _req: &'a BootstrapCredsRequest,
        _response: &'a BootstrapCredsResponse,
        _remote_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn abort_delivery<'a>(
        &'a self,
        _req: &'a BootstrapCredsRequest,
        _response: &'a BootstrapCredsResponse,
        _remote_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
}

/// Spawn a tokio task that accepts QUIC connections on `listen_addr`
/// and dispatches each to `handler`.
///
/// Uses the issuer node's existing cluster certificate. Each join token binds
/// that certificate's SPKI, so the joiner authenticates the specific issuer
/// before transmitting the bearer token.
///
/// The listener runs until `shutdown` fires. The returned `JoinHandle`
/// completes once the loop has drained.
pub fn spawn_listener<H: BootstrapHandler>(
    listen_addr: SocketAddr,
    issuer_cert: rustls::pki_types::CertificateDer<'static>,
    issuer_key: rustls::pki_types::PrivateKeyDer<'static>,
    handler: Arc<H>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    // rustls requires a process-level `CryptoProvider` before any
    // server/client config is built. Normal server startup installs
    // one long before this call; install here too so unit tests
    // (which skip the init path) and embedded uses don't panic.
    // `install_default` returns Err on second install and we don't
    // care which wins — any provider is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let server_config = nexar::transport::tls::make_server_config(issuer_cert, issuer_key)
        .map_err(|e| ClusterError::Transport {
            detail: format!("bootstrap listener server config: {e}"),
        })?;

    let listener =
        nexar::TransportListener::bind_with_config(listen_addr, server_config).map_err(|e| {
            ClusterError::Transport {
                detail: format!("bootstrap listener bind {listen_addr}: {e}"),
            }
        })?;
    let local = listener.local_addr();

    info!(%local, "bootstrap-listener: listening");

    let join = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("bootstrap-listener: shutdown");
                        break;
                    }
                }
                accept = listener.accept() => {
                    let conn = match accept {
                        Ok(c) => c,
                        Err(e) => {
                            warn!(error = %e, "bootstrap-listener: accept failed");
                            continue;
                        }
                    };
                    let remote = conn.remote_address();
                    let handler = Arc::clone(&handler);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(conn, handler).await {
                            warn!(%remote, error = %e, "bootstrap-listener: connection error");
                        }
                    });
                }
            }
        }
    });
    Ok((local, join))
}

async fn handle_connection<H: BootstrapHandler>(
    conn: quinn::Connection,
    handler: Arc<H>,
) -> Result<()> {
    let remote_addr = conn.remote_address();
    // We accept exactly one bidi stream per connection. The joiner
    // closes the connection after receiving the response.
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("accept_bi: {e}"),
        })?;

    let req_bytes = read_frame(&mut recv).await?;
    let req: BootstrapCredsRequest = decode_request(&req_bytes)?;
    debug!(
        node_id = req.node_id,
        "bootstrap-listener: handling request"
    );

    let resp = handler.handle(req.clone(), remote_addr).await;
    let resp_bytes = encode_response(&resp)?;
    if let Err(error) = write_frame(&mut send, &resp_bytes).await {
        if resp.ok {
            handler.abort_delivery(&req, &resp, remote_addr).await;
        }
        return Err(error);
    }
    if resp.ok {
        let ack = match read_frame(&mut recv).await {
            Ok(ack) => ack,
            Err(error) => {
                handler.abort_delivery(&req, &resp, remote_addr).await;
                return Err(error);
            }
        };
        if ack.as_slice() != DELIVERY_ACK {
            handler.abort_delivery(&req, &resp, remote_addr).await;
            return Err(ClusterError::Transport {
                detail: "bootstrap credential delivery acknowledgement mismatch".into(),
            });
        }
        if let Err(error) = handler.confirm_delivery(&req, &resp, remote_addr).await {
            handler.abort_delivery(&req, &resp, remote_addr).await;
            return Err(error);
        }
    }
    send.finish().map_err(|e| ClusterError::Transport {
        detail: format!("finish stream: {e}"),
    })?;
    // Keep the connection alive until the peer has acknowledged the
    // response and closed. Without this, `handle_connection` returns,
    // drops `conn`, and quinn tears down the connection before the
    // last outgoing packets are flushed — the client sees the read
    // error "connection lost" instead of the response.
    let _ = send.stopped().await;
    conn.closed().await;
    Ok(())
}

async fn read_frame(recv: &mut quinn::RecvStream) -> Result<Vec<u8>> {
    let mut hdr = [0u8; 4];
    recv.read_exact(&mut hdr)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("read length prefix: {e}"),
        })?;
    let len = u32::from_be_bytes(hdr) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(ClusterError::Transport {
            detail: format!("frame too large: {len} > {MAX_FRAME_BYTES}"),
        });
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("read frame body: {e}"),
        })?;
    Ok(buf)
}

async fn write_frame(send: &mut quinn::SendStream, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| ClusterError::Transport {
        detail: "frame over u32".into(),
    })?;
    send.write_all(&len.to_be_bytes())
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("write length prefix: {e}"),
        })?;
    send.write_all(bytes)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("write frame body: {e}"),
        })?;
    Ok(())
}
