// SPDX-License-Identifier: BUSL-1.1

//! Protocol listener spawning for all non-native listeners.

use std::sync::Arc;

use anyhow::Context;
use tokio::net::TcpListener;
use tracing::info;

use crate::ServerConfig;
use crate::control::cluster::ClusterHandle;
use crate::control::server::ilp_listener::IlpListener;
use crate::control::server::listener::Listener;
use crate::control::server::pgwire::listener::PgListener;
use crate::control::server::resp::RespListener;
use crate::control::shutdown::ShutdownBus;
use crate::control::startup::StartupGate;
use crate::control::state::SharedState;

/// The pre-bound protocol listeners passed to [`spawn_protocol_listeners`].
///
/// Every socket here is already bound by [`bind_listeners`], so spawning
/// cannot fail on a port conflict.
pub struct ProtocolListeners {
    pub pg_listener: PgListener,
    pub http_listener: TcpListener,
    pub sync_listener: TcpListener,
    pub ilp_listener: Option<IlpListener>,
    pub resp_listener: Option<RespListener>,
}

/// Shared infrastructure resources for the listener spawner.
pub struct ListenerInfra {
    pub conn_semaphore: Arc<tokio::sync::Semaphore>,
    pub startup_gate: Arc<StartupGate>,
    pub shutdown_bus: ShutdownBus,
}

/// Spawn all non-native protocol listeners as background tasks.
///
/// The native listener is not spawned here — it is run on the main task
/// by the caller after this returns.
///
/// Infallible by construction: every socket was bound by [`bind_listeners`]
/// before this point, so a port conflict has already aborted boot while
/// nothing was exposed. Nothing here may silently swallow a bind failure.
pub async fn spawn_protocol_listeners(
    listeners: ProtocolListeners,
    shared: Arc<SharedState>,
    config: &ServerConfig,
    infra: ListenerInfra,
    base_acceptor: Option<tokio_rustls::TlsAcceptor>,
    cluster_handle: &Option<Arc<ClusterHandle>>,
) {
    let ProtocolListeners {
        pg_listener,
        http_listener,
        sync_listener,
        ilp_listener,
        resp_listener,
    } = listeners;
    let ListenerInfra {
        conn_semaphore,
        startup_gate,
        shutdown_bus,
    } = infra;
    let tls_for = |enabled: bool| -> Option<tokio_rustls::TlsAcceptor> {
        if enabled { base_acceptor.clone() } else { None }
    };
    let tls_flags = config.server.tls.as_ref();
    let pgwire_tls_enabled = tls_flags.is_some_and(|t| t.pgwire);
    let http_tls_enabled = tls_flags.is_some_and(|t| t.http);
    let resp_tls_enabled = tls_flags.is_some_and(|t| t.resp);
    let ilp_tls_enabled = tls_flags.is_some_and(|t| t.ilp);

    // pgwire listener.
    let auth_mode = config.auth.mode.clone();
    let shared_pg = Arc::clone(&shared);
    let conn_sem_pg = Arc::clone(&conn_semaphore);
    let pgwire_tls = tls_for(pgwire_tls_enabled);
    let startup_gate_pg = Arc::clone(&startup_gate);
    let bus_pg = shutdown_bus.clone();
    tokio::spawn(async move {
        if let Err(e) = pg_listener
            .run(
                shared_pg,
                auth_mode,
                pgwire_tls,
                conn_sem_pg,
                startup_gate_pg,
                bus_pg,
            )
            .await
        {
            tracing::error!(error = %e, "pgwire listener failed");
        }
    });

    // HTTP API server (on the socket bound by `bind_listeners`).
    let shared_http = Arc::clone(&shared);
    let http_auth_mode = config.auth.mode.clone();
    let http_tls = if http_tls_enabled {
        config.server.tls.clone()
    } else {
        None
    };
    let bus_http = shutdown_bus.clone();
    tokio::spawn(async move {
        if let Err(e) = crate::control::server::http::server::run(
            http_listener,
            shared_http,
            http_auth_mode,
            http_tls.as_ref(),
            bus_http,
        )
        .await
        {
            tracing::error!(error = %e, "HTTP API server failed");
        }
    });

    // ILP TCP listener (if configured).
    if let Some(ilp) = ilp_listener {
        let shared_ilp = Arc::clone(&shared);
        let ilp_auth_mode = config.auth.mode.clone();
        let conn_sem_ilp = Arc::clone(&conn_semaphore);
        let ilp_tls = tls_for(ilp_tls_enabled);
        let startup_gate_ilp = Arc::clone(&startup_gate);
        let bus_ilp = shutdown_bus.clone();
        tokio::spawn(async move {
            if let Err(e) = ilp
                .run(
                    shared_ilp,
                    ilp_auth_mode,
                    conn_sem_ilp,
                    ilp_tls,
                    startup_gate_ilp,
                    bus_ilp,
                )
                .await
            {
                tracing::error!(error = %e, "ILP listener failed");
            }
        });
    }

    // RESP (Redis-compatible) listener (if configured).
    if let Some(resp) = resp_listener {
        let shared_resp = Arc::clone(&shared);
        let conn_sem_resp = Arc::clone(&conn_semaphore);
        let resp_tls = tls_for(resp_tls_enabled);
        let startup_gate_resp = Arc::clone(&startup_gate);
        let bus_resp = shutdown_bus.clone();
        tokio::spawn(async move {
            if let Err(e) = resp
                .run(
                    shared_resp,
                    conn_sem_resp,
                    resp_tls,
                    startup_gate_resp,
                    bus_resp,
                )
                .await
            {
                tracing::error!(error = %e, "RESP listener failed");
            }
        });
    }

    // Sync WebSocket listener for NodeDB-Lite clients (socket already bound).
    let sync_config = crate::control::server::sync::listener::SyncListenerConfig {
        listen_addr: config.sync_addr(),
        ..Default::default()
    };
    let sync_state = crate::control::server::sync::listener::serve_sync_listener(
        sync_listener,
        sync_config,
        Some(Arc::clone(&shared)),
        shutdown_bus.clone(),
    )
    .await;
    info!(
        addr = %sync_state.config.listen_addr,
        max_sessions = sync_state.config.max_sessions,
        "sync WebSocket listener started"
    );

    // Signal readiness to systemd and cluster lifecycle.
    if let Some(handle) = cluster_handle {
        let nodes = handle.topology.read().map(|t| t.node_count()).unwrap_or(1);
        handle.lifecycle.to_ready(nodes);
    }
    nodedb_cluster::readiness::notify_ready();
}

/// Every protocol socket, bound before any accept loop starts.
pub struct BoundListeners {
    pub native: Listener,
    pub pgwire: PgListener,
    pub http: TcpListener,
    pub sync: TcpListener,
    pub ilp: Option<IlpListener>,
    pub resp: Option<RespListener>,
}

/// Bind all protocol listeners to their configured addresses.
///
/// This is the single fail-fast point for listener setup: it runs before the
/// node waits on cluster readiness and before any accept loop is spawned, so
/// a port conflict on *any* protocol — including HTTP and sync, which serve
/// from detached tasks — aborts boot while nothing is exposed yet. Never
/// move a bind out of here into a spawned task; that is how a server ends up
/// running for days missing a listener behind one warning line.
pub async fn bind_listeners(config: &ServerConfig) -> anyhow::Result<BoundListeners> {
    let native = crate::control::server::listener::Listener::bind(config.native_addr()).await?;
    let pgwire =
        crate::control::server::pgwire::listener::PgListener::bind(config.pgwire_addr()).await?;
    let http = TcpListener::bind(config.http_addr())
        .await
        .with_context(|| format!("bind HTTP API listener to {}", config.http_addr()))?;
    let sync = crate::control::server::sync::listener::bind_sync_listener(config.sync_addr())
        .await
        .context("sync listener failed to bind")?;
    let ilp = if let Some(ilp_addr) = config.ilp_addr() {
        Some(crate::control::server::ilp_listener::IlpListener::bind(ilp_addr).await?)
    } else {
        None
    };
    let resp = if let Some(resp_addr) = config.resp_addr() {
        Some(crate::control::server::resp::RespListener::bind(resp_addr).await?)
    } else {
        None
    };
    Ok(BoundListeners {
        native,
        pgwire,
        http,
        sync,
        ilp,
        resp,
    })
}
