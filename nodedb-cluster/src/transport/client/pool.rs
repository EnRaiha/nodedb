// SPDX-License-Identifier: BUSL-1.1

//! Per-peer QUIC connection pool: registration, dialling, eviction, warm-up.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tracing::debug;

use crate::error::{ClusterError, Result};
use crate::transport::config::SNI_HOSTNAME;
use crate::wire_version::handshake_io::{local_version_range, perform_version_handshake_client};

use super::transport::NexarTransport;

impl NexarTransport {
    /// Register a peer's address for outbound connections.
    pub fn register_peer(&self, node_id: u64, addr: SocketAddr) {
        let mut addrs = self.peer_addrs.write().unwrap_or_else(|p| p.into_inner());
        addrs.insert(node_id, addr);
        debug!(node_id, %addr, "peer registered");
    }

    /// Pre-warm the QUIC connection cache for a peer by performing the full
    /// dial + handshake and inserting the connection into the peer cache. On
    /// success, the next `send_rpc(target, ...)` skips the dial entirely and
    /// reuses the cached `quinn::Connection`.
    ///
    /// Caller MUST have called [`register_peer`] first — this function
    /// resolves the peer address from the `peer_addrs` map. Used by the
    /// startup `warm_peers` phase so the first replicated request after boot
    /// doesn't pay a cold-connect penalty.
    ///
    /// [`register_peer`]: Self::register_peer
    pub async fn warm_peer(&self, node_id: u64) -> Result<()> {
        self.get_or_connect(node_id).await.map(|_| ())
    }

    /// Get the stable ID of the cached connection to a peer.
    ///
    /// Returns `None` if no connection is cached or the connection is closed.
    /// Used during migrations to detect if the peer connection changed
    /// (indicating possible node replacement).
    pub fn peer_connection_stable_id(&self, target: u64) -> Option<usize> {
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        peers.get(&target).and_then(|conn| {
            if conn.close_reason().is_none() {
                Some(conn.stable_id())
            } else {
                None
            }
        })
    }

    /// The pooled connection to `target`, if it is open.
    fn pooled_connection(&self, target: u64) -> Option<quinn::Connection> {
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        peers
            .get(&target)
            .filter(|conn| conn.close_reason().is_none())
            .cloned()
    }

    /// Get an existing connection to a peer, or establish a new one.
    ///
    /// Dials to one peer are single-flight. A caller that finds no open
    /// connection waits for the peer's dial gate. It then reuses the
    /// connection an earlier dial pooled meanwhile, or shares the failure of
    /// a dial that ended while it waited. Only a caller with neither dials,
    /// so a burst of sends to one peer opens one connection.
    pub(super) async fn get_or_connect(&self, target: u64) -> Result<quinn::Connection> {
        if let Some(conn) = self.pooled_connection(target) {
            return Ok(conn);
        }
        let addr = {
            let addrs = self.peer_addrs.read().unwrap_or_else(|p| p.into_inner());
            addrs
                .get(&target)
                .copied()
                .ok_or(ClusterError::NodeUnreachable { node_id: target })?
        };

        let waiting_since = Instant::now();
        let gate = self.dial_gates.gate(target);
        // Held across this peer's dial only. Eviction takes no gate.
        let mut last_dial = gate.lock().await;
        if let Some(conn) = self.pooled_connection(target) {
            return Ok(conn);
        }
        if let Some(failure) = last_dial.failed_since(waiting_since) {
            return Err(ClusterError::Transport {
                detail: format!(
                    "dial to node {target} at {addr} failed while this send waited: {failure}"
                ),
            });
        }
        self.drop_closed_connection(target);
        match self.dial(target, addr).await {
            Ok(conn) => {
                last_dial.failure = None;
                let mut peers = self.peers.write().unwrap_or_else(|p| p.into_inner());
                peers.insert(target, conn.clone());
                Ok(conn)
            }
            Err(error) => {
                last_dial.failure = Some(DialFailure {
                    at: Instant::now(),
                    detail: error.to_string(),
                });
                Err(error)
            }
        }
    }

    /// Connect to `target` at `addr` and negotiate the wire version.
    async fn dial(&self, target: u64, addr: SocketAddr) -> Result<quinn::Connection> {
        // Connect — bounded by rpc_timeout so a hung QUIC handshake
        // (peer not yet serving) doesn't block for the full 30s idle timeout.
        let connecting = self
            .listener
            .endpoint()
            .connect_with(self.client_config.clone(), addr, SNI_HOSTNAME)
            .map_err(|e| ClusterError::Transport {
                detail: format!("connect to node {target} at {addr}: {e}"),
            })?;
        let conn = tokio::time::timeout(self.rpc_timeout, connecting)
            .await
            .map_err(|_| ClusterError::Transport {
                detail: format!(
                    "handshake timeout ({}ms) with node {target} at {addr}",
                    self.rpc_timeout.as_millis()
                ),
            })?
            .map_err(|e| ClusterError::Transport {
                detail: format!("handshake with node {target} at {addr}: {e}"),
            })?;

        debug!(target, %addr, "connected to peer");

        // Open a dedicated bidi stream for the wire-version handshake.
        // This must complete before any RPC stream is opened on this connection.
        let agreed = {
            let (mut hs_send, mut hs_recv) =
                conn.open_bi().await.map_err(|e| ClusterError::Transport {
                    detail: format!("open handshake stream to node {target} at {addr}: {e}"),
                })?;
            let version = tokio::time::timeout(
                self.rpc_timeout,
                perform_version_handshake_client(&mut hs_send, &mut hs_recv),
            )
            .await
            .map_err(|_| ClusterError::Transport {
                detail: format!(
                    "handshake timeout ({}ms) with node {target} at {addr}",
                    self.rpc_timeout.as_millis()
                ),
            })??;
            // Finish the handshake send stream — the server reads it until FIN.
            let _ = hs_send.finish();
            version
        };

        let local = local_version_range();
        debug!(
            target,
            %addr,
            agreed_version = %agreed,
            local_min = %local.min,
            local_max = %local.max,
            "wire version negotiated"
        );

        // Cache the agreed version keyed on the QUIC connection's stable id.
        self.store_agreed_version(conn.stable_id(), agreed);
        Ok(conn)
    }

    /// Drop the pooled connection to `target` if it is closed.
    fn drop_closed_connection(&self, target: u64) {
        let closed = {
            let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
            peers
                .get(&target)
                .filter(|conn| conn.close_reason().is_some())
                .map(quinn::Connection::stable_id)
        };
        if let Some(stable_id) = closed {
            self.evict_connection(target, stable_id);
        }
    }

    /// Drop the connection `stable_id` from the pool, so the next send to
    /// `target` dials a fresh one. A newer connection pooled for `target`
    /// stays.
    pub(super) fn evict_connection(&self, target: u64, stable_id: usize) {
        {
            let mut peers = self.peers.write().unwrap_or_else(|p| p.into_inner());
            if peers
                .get(&target)
                .is_some_and(|conn| conn.stable_id() == stable_id)
            {
                peers.remove(&target);
            }
        }
        self.evict_agreed_version(stable_id);
    }
}

/// The last failed dial to one peer.
#[derive(Debug)]
struct DialFailure {
    /// When the dial ended.
    at: Instant,
    detail: String,
}

/// Outcome of the last dial to one peer, behind the peer's dial gate.
#[derive(Debug, Default)]
struct LastDial {
    failure: Option<DialFailure>,
}

impl LastDial {
    /// The failure of a dial that ended at or after `since`.
    fn failed_since(&self, since: Instant) -> Option<&str> {
        self.failure
            .as_ref()
            .filter(|failure| failure.at >= since)
            .map(|failure| failure.detail.as_str())
    }
}

/// One dial gate per peer. A gate is held across a dial to its peer only.
#[derive(Debug, Default)]
pub(super) struct DialGates {
    gates: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<LastDial>>>>,
}

impl DialGates {
    /// The dial gate of `target`. The map holds one gate per peer ever
    /// dialled, so it stays bounded by the cluster's nodes.
    fn gate(&self, target: u64) -> Arc<tokio::sync::Mutex<LastDial>> {
        let mut gates = self.gates.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(gates.entry(target).or_default())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::transport::tests::serve_echo;
    use super::*;
    use crate::transport::credentials::TransportCredentials;

    /// A burst of sends to one peer opens one connection.
    #[tokio::test]
    async fn concurrent_callers_share_one_dial() {
        let (_server, client, _shutdown) = serve_echo().await;
        let callers = (0..16).map(|_| {
            let client = Arc::clone(&client);
            async move { client.get_or_connect(1).await.map(|conn| conn.stable_id()) }
        });

        let ids = futures::future::try_join_all(callers).await.expect("dial");

        assert!(ids.windows(2).all(|pair| pair[0] == pair[1]), "{ids:?}");
        assert_eq!(client.peer_connection_stable_id(1), ids.first().copied());
    }

    /// Callers that waited behind a failed dial share its failure. None of
    /// them dials again, so a peer that is down costs one dial timeout.
    #[tokio::test]
    async fn waiters_share_a_failed_dial() {
        // Bound but never answering: every dial runs into the handshake timeout.
        let silent = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind");
        let client = Arc::new(
            NexarTransport::with_timeout(
                2,
                "127.0.0.1:0".parse().expect("addr"),
                Duration::from_millis(300),
                TransportCredentials::Insecure,
            )
            .expect("transport"),
        );
        client.register_peer(1, silent.local_addr().expect("addr"));
        let callers = (0..8).map(|_| {
            let client = Arc::clone(&client);
            async move { client.get_or_connect(1).await }
        });

        let outcomes = futures::future::join_all(callers).await;

        let shared = outcomes
            .iter()
            .filter(|outcome| {
                outcome
                    .as_ref()
                    .is_err_and(|e| e.to_string().contains("while this send waited"))
            })
            .count();
        assert!(outcomes.iter().all(Result::is_err));
        assert_eq!(shared, outcomes.len() - 1, "one dial, its failure shared");
    }

    /// Eviction removes only the connection that failed. A newer connection
    /// pooled for the same peer stays.
    #[tokio::test]
    async fn eviction_spares_a_newer_connection() {
        let (_server, client, _shutdown) = serve_echo().await;
        let conn = client.get_or_connect(1).await.expect("dial");
        let stale = conn.stable_id().wrapping_add(1);

        client.evict_connection(1, stale);
        assert_eq!(client.peer_connection_stable_id(1), Some(conn.stable_id()));

        client.evict_connection(1, conn.stable_id());
        assert_eq!(client.peer_connection_stable_id(1), None);
    }
}
