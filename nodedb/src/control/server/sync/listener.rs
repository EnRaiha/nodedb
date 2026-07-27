// SPDX-License-Identifier: BUSL-1.1

//! WebSocket listener for NodeDB-Lite sync connections.
//!
//! Accepts loopback-only `ws://` connections on the Tokio Control Plane for a
//! local TLS-terminating proxy. Each connection spawns a sync session with full
//! RLS, audit, DLQ, and rate limiting. Public plaintext binds are rejected.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::control::security::jwt::JwtConfig;
use crate::control::state::SharedState;

use super::rate_limit::RateLimitConfig;
use super::session_handler::handle_sync_session;

/// Configuration for the sync WebSocket listener.
#[derive(Debug, Clone)]
pub struct SyncListenerConfig {
    pub listen_addr: SocketAddr,
    pub max_sessions: usize,
    pub idle_timeout_secs: u64,
    pub jwt_config: JwtConfig,
    pub rate_limit: RateLimitConfig,
}

impl Default for SyncListenerConfig {
    fn default() -> Self {
        Self {
            // Loopback, not `0.0.0.0`: the listen address always comes from
            // `ServerConfig::sync_addr()` in production, so the default must
            // be the conservative one rather than an implicit all-interfaces
            // bind for anything that fills it in from `Default`.
            listen_addr: std::net::SocketAddr::from((
                std::net::Ipv4Addr::LOCALHOST,
                crate::config::server::DEFAULT_SYNC_PORT,
            )),
            max_sessions: 1024,
            idle_timeout_secs: 300,
            jwt_config: JwtConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

/// Sync listener state (shared across all sessions).
pub struct SyncListenerState {
    pub active_sessions: AtomicU64,
    pub connections_accepted: AtomicU64,
    pub connections_rejected: AtomicU64,
    pub config: SyncListenerConfig,
}

impl SyncListenerState {
    pub fn new(config: SyncListenerConfig) -> Self {
        Self {
            active_sessions: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            config,
        }
    }

    pub fn can_accept(&self) -> bool {
        self.active_sessions.load(Ordering::Relaxed) < self.config.max_sessions as u64
    }

    pub fn session_opened(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn session_closed(&self) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn session_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }
}

/// Bind the sync WebSocket listener socket.
///
/// Separate from [`serve_sync_listener`] so boot can bind every protocol
/// socket up front — before any accept loop is spawned and before the node
/// joins the cluster — and fail loudly on a port conflict while nothing is
/// yet exposed. See `bootstrap::listeners::bind_listeners`.
pub async fn bind_sync_listener(addr: SocketAddr) -> crate::Result<TcpListener> {
    // Plaintext `ws://` sync must terminate TLS at a loopback proxy: reject any
    // public bind here so both the fail-fast boot path (`bind_listeners`) and
    // the convenience `start_sync_listener` path are covered by one guard.
    if !addr.ip().is_loopback() {
        return Err(crate::Error::Config {
            detail: format!(
                "plaintext sync listener {addr} must bind to loopback behind a TLS-terminating proxy"
            ),
        });
    }
    TcpListener::bind(&addr)
        .await
        .map_err(|e| crate::Error::Config {
            detail: format!("bind sync listener to {addr}: {e}"),
        })
}

/// Start the sync WebSocket listener with full security context.
///
/// Binds and serves in one step. Boot uses [`bind_sync_listener`] +
/// [`serve_sync_listener`] instead so the bind is fail-fast; this is the
/// convenience path for callers that own the whole lifecycle (tests, tools).
pub async fn start_sync_listener(
    config: SyncListenerConfig,
    shared: Option<Arc<SharedState>>,
) -> crate::Result<Arc<SyncListenerState>> {
    let listener = bind_sync_listener(config.listen_addr).await?;
    Ok(serve_sync_listener(listener, config, shared).await)
}

/// Serve sync sessions on an already-bound listener.
///
/// Infallible: the only failure mode is the bind, which the caller has
/// already cleared.
pub async fn serve_sync_listener(
    listener: TcpListener,
    config: SyncListenerConfig,
    shared: Option<Arc<SharedState>>,
) -> Arc<SyncListenerState> {
    // Surface the actually-bound address. For a fixed port this is a no-op; for
    // an ephemeral port (`:0`) it records the OS-assigned port so the caller can
    // discover where the listener is reachable.
    let mut config = config;
    if let Ok(bound) = listener.local_addr() {
        config.listen_addr = bound;
    }

    let state = Arc::new(SyncListenerState::new(config));

    info!(addr = %state.config.listen_addr, "sync WebSocket listener started");

    // Spawn presence TTL sweep timer (before moving `shared` into accept loop).
    if let Some(ref shared) = shared {
        let presence = Arc::clone(&shared.presence);
        let sweep_interval_ms = presence.read().await.sweep_interval_ms();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(sweep_interval_ms));
            loop {
                interval.tick().await;
                let mut mgr = presence.write().await;
                let outbound = mgr.sweep_expired();
                let senders = mgr.senders().clone();
                drop(mgr); // Release lock before fan-out.
                outbound.send_all(&senders);
            }
        });
    }

    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        accept_loop(listener, state_clone, shared).await;
    });

    state
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<SyncListenerState>,
    shared: Option<Arc<SharedState>>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                if !state.can_accept() {
                    state.session_rejected();
                    warn!(%addr, "sync: max sessions reached, rejecting");
                    continue;
                }

                state.session_opened();
                let state_clone = Arc::clone(&state);
                let shared_clone = shared.clone();

                tokio::spawn(async move {
                    match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws) => {
                            info!(%addr, "sync: WebSocket connection established");
                            handle_sync_session(ws, addr, &state_clone, shared_clone).await;
                        }
                        Err(e) => {
                            warn!(%addr, error = %e, "sync: WebSocket upgrade failed");
                        }
                    }
                    state_clone.session_closed();
                });
            }
            Err(e) => {
                warn!(error = %e, "sync: accept failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    /// Binding to an address that's already occupied must surface as `Err`,
    /// not panic or silently succeed — this is the behavior `bind_listeners`
    /// relies on to fail boot on a sync port conflict instead of logging a
    /// non-fatal warning and coming up sync-less.
    #[tokio::test]
    async fn bind_sync_listener_returns_err_on_occupied_port() {
        // Reserve an ephemeral port via a real listener so we know it's taken.
        let occupied = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind ephemeral listener to reserve a port");
        let addr = occupied
            .local_addr()
            .expect("local addr of reserved listener");

        let result = bind_sync_listener(addr).await;

        assert!(
            result.is_err(),
            "expected bind_sync_listener to return Err when the address is already bound"
        );
    }

    /// `start_sync_listener` must propagate the same bind failure — it is the
    /// path tests and tools use, and it must not diverge from the boot path.
    #[tokio::test]
    async fn start_sync_listener_returns_err_on_occupied_port() {
        let occupied = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind ephemeral listener to reserve a port");
        let addr = occupied
            .local_addr()
            .expect("local addr of reserved listener");

        let cfg = SyncListenerConfig {
            listen_addr: addr,
            ..Default::default()
        };

        assert!(
            start_sync_listener(cfg, None).await.is_err(),
            "expected start_sync_listener to return Err when the address is already bound"
        );
    }

    /// The default must not be an implicit all-interfaces bind: production
    /// always sets `listen_addr` from `ServerConfig::sync_addr()`, so anything
    /// falling back to `Default` should get the conservative address, and its
    /// port must agree with the config default.
    #[test]
    fn default_listen_addr_is_loopback_on_the_config_default_port() {
        let cfg = SyncListenerConfig::default();
        assert_eq!(cfg.listen_addr.ip(), Ipv4Addr::LOCALHOST);
        assert_eq!(
            cfg.listen_addr.port(),
            crate::config::server::DEFAULT_SYNC_PORT
        );
    }

    #[test]
    fn default_plaintext_sync_listener_is_loopback_only() {
        assert!(SyncListenerConfig::default().listen_addr.ip().is_loopback());
    }

    #[tokio::test]
    async fn public_plaintext_sync_bind_is_rejected() {
        let config = SyncListenerConfig {
            listen_addr: "0.0.0.0:9090".parse().unwrap(),
            ..SyncListenerConfig::default()
        };
        let error = match start_sync_listener(config, None).await {
            Ok(_) => panic!("public plaintext listener unexpectedly started"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("TLS-terminating proxy"));
    }
}
