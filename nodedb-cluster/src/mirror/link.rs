// SPDX-License-Identifier: BUSL-1.1

//! Cross-cluster QUIC link: connection establishment, cluster-id
//! authentication, and exponential backoff reconnect.
//!
//! [`CrossClusterLink`] is the mirror-side manager for a single outbound
//! connection to a source cluster.  It is `Send + Sync` and lives on the
//! Control Plane (Tokio).
//!
//! # Lifecycle
//!
//! 1. Call [`CrossClusterLink::connect`] to establish the initial QUIC
//!    connection and run the [`handshake`] exchange.
//! 2. On success the link is in `Connected` state; callers open bidi streams
//!    to stream AppendEntries or snapshot chunks.
//! 3. On disconnect, call [`CrossClusterLink::schedule_reconnect`].  The
//!    link drives an exponential backoff loop (base 500 ms, max 30 s, ±25 %
//!    jitter) and re-runs the handshake before returning.
//! 4. The source enforces `PeerRole::Observer`: any voter-class RPC from
//!    this link is rejected by the source's RPC handler.
//!
//! # Observer-role contract
//!
//! This link is used exclusively for mirror replication.  The mirror never
//! sends `RequestVote`, `ConfChange`, or any write proposal over this link.
//! The source's RPC handler validates this and returns an error for any
//! voter-class message.

use std::net::SocketAddr;
use std::time::Duration;

use rand::RngExt as _;
use tokio::sync::Mutex;
use tracing::{info, warn};

use super::error::MirrorError;
use super::handshake::{
    MIRROR_HELLO_ERR_BAD_VERSION, MIRROR_HELLO_ERR_CLUSTER_ID, MIRROR_HELLO_ERR_OBSERVER_ONLY,
    MIRROR_PROTOCOL_VERSION, MirrorHello, MirrorHelloAck, recv_ack, send_hello,
};
use super::throttle::SendThrottle;

/// Base reconnect delay.
const RECONNECT_BASE_MS: u64 = 500;
/// Maximum reconnect delay (30 seconds, as per spec).
const RECONNECT_MAX_MS: u64 = 30_000;
/// Jitter fraction: ±25 % of the current delay.
const JITTER_FRACTION: f64 = 0.25;

/// State of the cross-cluster link.
#[derive(Debug)]
enum LinkState {
    /// No connection is open.
    Disconnected,
    /// A QUIC connection is open and the handshake has completed.
    Connected(quinn::Connection),
}

/// Cross-cluster QUIC link from a mirror to its source cluster.
///
/// Manages connection establishment, cluster-id authentication, exponential
/// backoff reconnect, and per-mirror bytes-in-flight throttle.
pub struct CrossClusterLink {
    /// Source cluster's cluster-id string.  The source verifies that the
    /// `source_cluster` field in the handshake matches its own id.
    source_cluster_id: String,
    /// Database id on the source cluster being mirrored.
    source_database_id: String,
    /// Remote address of the source cluster's mirror-listener endpoint.
    source_addr: SocketAddr,
    /// QUIC client config used to open outbound connections.
    client_config: quinn::ClientConfig,
    /// QUIC endpoint used to originate connections.
    endpoint: quinn::Endpoint,
    /// Current connection state, protected by a mutex so `schedule_reconnect`
    /// can be awaited concurrently.
    state: Mutex<LinkState>,
    /// Bytes-in-flight throttle shared with the snapshot / log sender.
    pub throttle: SendThrottle,
}

impl CrossClusterLink {
    /// Create a new (disconnected) link.
    ///
    /// Call [`connect`](Self::connect) to open the initial connection.
    pub fn new(
        source_cluster_id: String,
        source_database_id: String,
        source_addr: SocketAddr,
        endpoint: quinn::Endpoint,
        client_config: quinn::ClientConfig,
        throttle: SendThrottle,
    ) -> Self {
        Self {
            source_cluster_id,
            source_database_id,
            source_addr,
            client_config,
            endpoint,
            state: Mutex::new(LinkState::Disconnected),
            throttle,
        }
    }

    /// The source cluster-id this link is targeting.
    pub fn source_cluster_id(&self) -> &str {
        &self.source_cluster_id
    }

    /// Establish the initial QUIC connection and run the cross-cluster
    /// handshake.  Returns the [`MirrorHelloAck`] on success.
    ///
    /// If the source rejects the connection (cluster-id mismatch, observer
    /// violation, etc.) this returns a [`MirrorError`] immediately — no
    /// backoff is applied, because these are hard-configuration errors that
    /// won't be fixed by retrying.
    pub async fn connect(&self, last_applied_lsn: u64) -> Result<MirrorHelloAck, MirrorError> {
        let conn = self.dial().await?;
        let ack = self.run_handshake(&conn, last_applied_lsn).await?;
        let mut state = self.state.lock().await;
        *state = LinkState::Connected(conn);
        Ok(ack)
    }

    /// Open a new QUIC bidi stream on the existing connection.
    ///
    /// Returns an error when the link is in `Disconnected` state; the caller
    /// should call [`schedule_reconnect`](Self::schedule_reconnect) first.
    pub async fn open_bidi_stream(
        &self,
    ) -> Result<(quinn::SendStream, quinn::RecvStream), MirrorError> {
        let state = self.state.lock().await;
        match &*state {
            LinkState::Disconnected => Err(MirrorError::Transport {
                detail: "cross-cluster link is disconnected".into(),
            }),
            LinkState::Connected(conn) => {
                conn.open_bi().await.map_err(|e| MirrorError::Transport {
                    detail: format!("open bidi stream to source: {e}"),
                })
            }
        }
    }

    /// Reconnect with exponential backoff after a disconnect.
    ///
    /// Drives the following sequence:
    /// 1. Mark link as `Disconnected`, reset throttle.
    /// 2. Sleep for the current backoff duration.
    /// 3. Dial + handshake.
    /// 4. On success, mark `Connected` and return the ack.
    /// 5. On failure, double the delay (capped at `RECONNECT_MAX_MS`) and
    ///    repeat from step 2.
    pub async fn schedule_reconnect(
        &self,
        last_applied_lsn: u64,
    ) -> Result<MirrorHelloAck, MirrorError> {
        {
            let mut state = self.state.lock().await;
            *state = LinkState::Disconnected;
        }
        self.throttle.reset();

        let mut delay_ms = RECONNECT_BASE_MS;

        loop {
            let jitter = jitter_for(delay_ms);
            let sleep_ms = delay_ms.saturating_add_signed(jitter);
            info!(
                source_cluster = %self.source_cluster_id,
                source_addr = %self.source_addr,
                sleep_ms,
                "mirror link: reconnecting after disconnect"
            );
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

            match self.dial().await {
                Err(e) => {
                    warn!(
                        source_cluster = %self.source_cluster_id,
                        error = %e,
                        "mirror link: dial failed, will retry"
                    );
                }
                Ok(conn) => match self.run_handshake(&conn, last_applied_lsn).await {
                    Err(e @ MirrorError::ClusterIdMismatch { .. })
                    | Err(e @ MirrorError::ObserverRoleViolation { .. })
                    | Err(e @ MirrorError::ProtocolVersionMismatch { .. })
                    | Err(e @ MirrorError::MirrorPromoted { .. }) => {
                        // Hard config errors: surface immediately.
                        return Err(e);
                    }
                    Err(e) => {
                        warn!(
                            source_cluster = %self.source_cluster_id,
                            error = %e,
                            "mirror link: handshake failed, will retry"
                        );
                    }
                    Ok(ack) => {
                        let mut state = self.state.lock().await;
                        *state = LinkState::Connected(conn);
                        return Ok(ack);
                    }
                },
            }

            delay_ms = (delay_ms * 2).min(RECONNECT_MAX_MS);
        }
    }

    /// Dial the source cluster address.
    async fn dial(&self) -> Result<quinn::Connection, MirrorError> {
        self.endpoint
            .connect_with(
                self.client_config.clone(),
                self.source_addr,
                &self.source_cluster_id,
            )
            .map_err(|e| MirrorError::Transport {
                detail: format!("connect to source {}: {e}", self.source_addr),
            })?
            .await
            .map_err(|e| MirrorError::Transport {
                detail: format!("QUIC handshake with source {}: {e}", self.source_addr),
            })
    }

    /// Run the cross-cluster mirror handshake on a freshly opened connection.
    async fn run_handshake(
        &self,
        conn: &quinn::Connection,
        last_applied_lsn: u64,
    ) -> Result<MirrorHelloAck, MirrorError> {
        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| MirrorError::Transport {
            detail: format!("open handshake stream: {e}"),
        })?;

        let hello = MirrorHello {
            source_cluster: self.source_cluster_id.clone(),
            source_database_id: self.source_database_id.clone(),
            last_applied_lsn,
            protocol_version: MIRROR_PROTOCOL_VERSION,
        };
        send_hello(&mut send, &hello).await?;
        let _ = send.finish();

        let ack = recv_ack(&mut recv).await?;

        if !ack.accepted {
            return Err(match ack.error_code {
                MIRROR_HELLO_ERR_CLUSTER_ID => MirrorError::ClusterIdMismatch {
                    declared: self.source_cluster_id.clone(),
                    remote: ack.source_cluster_id,
                },
                MIRROR_HELLO_ERR_OBSERVER_ONLY => MirrorError::ObserverRoleViolation {
                    detail: ack.error_detail,
                },
                MIRROR_HELLO_ERR_BAD_VERSION => MirrorError::ProtocolVersionMismatch {
                    local: MIRROR_PROTOCOL_VERSION,
                    detail: ack.error_detail,
                },
                other => MirrorError::Transport {
                    detail: format!(
                        "source rejected mirror handshake: code={other:#04x} {}",
                        ack.error_detail
                    ),
                },
            });
        }

        // Verify the source's self-reported cluster-id matches what we expect.
        if ack.source_cluster_id != self.source_cluster_id {
            return Err(MirrorError::ClusterIdMismatch {
                declared: self.source_cluster_id.clone(),
                remote: ack.source_cluster_id,
            });
        }

        Ok(ack)
    }
}

/// Compute ±`JITTER_FRACTION` jitter for a given delay, returning a signed
/// offset.  The offset is at most ±25 % of `delay_ms`.
fn jitter_for(delay_ms: u64) -> i64 {
    let max = (delay_ms as f64 * JITTER_FRACTION) as i64;
    if max == 0 {
        return 0;
    }
    rand::rng().random_range(-max..=max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_bounds() {
        for delay in [500u64, 1000, 5000, 30_000] {
            for _ in 0..200 {
                let j = jitter_for(delay);
                let max = (delay as f64 * JITTER_FRACTION) as i64;
                assert!(
                    j.abs() <= max,
                    "jitter {j} out of bounds ±{max} for delay {delay}"
                );
            }
        }
    }

    #[test]
    fn backoff_capped_at_max() {
        let mut d: u64 = RECONNECT_BASE_MS;
        for _ in 0..30 {
            d = (d * 2).min(RECONNECT_MAX_MS);
        }
        assert_eq!(d, RECONNECT_MAX_MS);
    }
}
