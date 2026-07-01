// SPDX-License-Identifier: BUSL-1.1

//! WebSocket listener for NodeDB-Lite sync connections.
//!
//! Accepts `ws://` connections on the Tokio Control Plane. Each connection
//! spawns a sync session with full RLS, audit, DLQ, and rate limiting.

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
            listen_addr: std::net::SocketAddr::from(([0, 0, 0, 0], 9090)),
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

/// Start the sync WebSocket listener with full security context.
pub async fn start_sync_listener(
    config: SyncListenerConfig,
    shared: Option<Arc<SharedState>>,
) -> crate::Result<Arc<SyncListenerState>> {
    let listener =
        TcpListener::bind(&config.listen_addr)
            .await
            .map_err(|e| crate::Error::Config {
                detail: format!("bind sync listener to {}: {e}", config.listen_addr),
            })?;

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

    Ok(state)
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
