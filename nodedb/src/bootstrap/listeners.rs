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
use crate::control::server::reserved_socket::ReservedSocket;
use crate::control::server::resp::RespListener;
use crate::control::shutdown::ShutdownBus;
use crate::control::startup::{ReadyGate, StartupGate};
use crate::control::state::SharedState;

/// The listening client-protocol sockets passed to
/// [`spawn_protocol_listeners`].
///
/// Every socket here was bound by [`bind_listeners`] and opened by
/// [`open_listeners`], so spawning cannot fail on a port conflict.
pub struct ProtocolListeners {
    pub pg_listener: PgListener,
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

/// Spawn all non-native client-protocol listeners as background tasks.
///
/// The native listener is not spawned here — it is run on the main task
/// by the caller after this returns. The HTTP server is spawned earlier by
/// [`spawn_http_listener`].
///
/// Infallible by construction: every socket was bound by [`bind_listeners`]
/// and opened by [`open_listeners`] before this point, so a port conflict
/// has already aborted boot. Nothing here may silently swallow a bind
/// failure.
pub async fn spawn_protocol_listeners(
    listeners: ProtocolListeners,
    shared: Arc<SharedState>,
    config: &ServerConfig,
    infra: ListenerInfra,
    base_acceptor: Option<tokio_rustls::TlsAcceptor>,
    cluster_handle: &ClusterHandle,
) {
    let ProtocolListeners {
        pg_listener,
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
    let nodes = cluster_handle
        .topology
        .read()
        .map(|t| t.node_count())
        .unwrap_or(1);
    cluster_handle.lifecycle.to_ready(nodes);
    nodedb_cluster::readiness::notify_ready();
}

/// Spawn the HTTP API server on `http_listener`.
///
/// Boot calls this before it waits for the node to become ready, so
/// orchestrator probes can watch startup. Until the `Serving` phase only the
/// probe and metrics routes answer (see
/// `control::server::http::startup_gate`).
pub fn spawn_http_listener(
    http_listener: TcpListener,
    shared: Arc<SharedState>,
    config: &ServerConfig,
    shutdown_bus: ShutdownBus,
) {
    let http_auth_mode = config.auth.mode.clone();
    let http_tls = config.server.tls.as_ref().filter(|tls| tls.http).cloned();
    tokio::spawn(async move {
        if let Err(e) = crate::control::server::http::server::run(
            http_listener,
            shared,
            http_auth_mode,
            http_tls.as_ref(),
            shutdown_bus,
        )
        .await
        {
            tracing::error!(error = %e, "HTTP API server failed");
        }
    });
}

/// Every protocol socket, bound before the node waits to become ready.
///
/// The HTTP socket listens from the start, so probes answer during boot. The
/// client-protocol sockets are bound but not listening.
pub struct BoundListeners {
    pub http: TcpListener,
    pub clients: ClientSockets,
}

/// The client-protocol sockets, bound but not yet listening.
///
/// A bound socket that does not listen refuses each connection attempt at
/// once. Boot listens through [`open_listeners`] only once the node can
/// serve, so no client waits in a kernel accept queue through boot.
pub struct ClientSockets {
    pub native: ReservedSocket,
    pub pgwire: ReservedSocket,
    pub sync: ReservedSocket,
    pub ilp: Option<ReservedSocket>,
    pub resp: Option<ReservedSocket>,
}

/// Every client-protocol socket, listening.
pub struct OpenListeners {
    pub native: Listener,
    pub pgwire: PgListener,
    pub sync: TcpListener,
    pub ilp: Option<IlpListener>,
    pub resp: Option<RespListener>,
}

/// Bind all protocol listeners to their configured addresses.
///
/// This is the single fail-fast point for listener setup: it runs before the
/// node waits on cluster readiness and before any accept loop is spawned, so
/// a port conflict on *any* protocol — including HTTP and sync, which serve
/// from detached tasks — aborts boot early. Never move a bind out of here
/// into a spawned task; that is how a server ends up running for days
/// missing a listener behind one warning line.
pub fn bind_listeners(config: &ServerConfig) -> anyhow::Result<BoundListeners> {
    let reserve = |name: &str, addr: std::net::SocketAddr| {
        ReservedSocket::bind(addr).with_context(|| format!("bind {name} listener to {addr}"))
    };
    let http = reserve("HTTP API", config.http_addr())?
        .listen()
        .context("listen on the HTTP API address")?;
    let native = reserve("native protocol", config.native_addr())?;
    let pgwire = reserve("pgwire", config.pgwire_addr())?;
    let sync = crate::control::server::sync::listener::reserve_sync_listener(config.sync_addr())
        .context("sync listener failed to bind")?;
    let ilp = config
        .ilp_addr()
        .map(|addr| reserve("ILP", addr))
        .transpose()?;
    let resp = config
        .resp_addr()
        .map(|addr| reserve("RESP", addr))
        .transpose()?;
    Ok(BoundListeners {
        http,
        clients: ClientSockets {
            native,
            pgwire,
            sync,
            ilp,
            resp,
        },
    })
}

/// Start listening on every client-protocol socket, then fire
/// `serving_gate`.
///
/// Boot calls this once the node can serve and before any accept loop is
/// spawned. The gate advances the startup sequencer to
/// [`StartupPhase::Serving`](crate::control::startup::StartupPhase::Serving),
/// which opens every HTTP route and lets `/healthz` report `ok`, at the same
/// point the client protocols start listening. A socket that cannot listen
/// fails the gate and aborts boot: another process started listening on its
/// address after the bind.
pub fn open_listeners(
    clients: ClientSockets,
    serving_gate: ReadyGate,
) -> anyhow::Result<OpenListeners> {
    let ClientSockets {
        native,
        pgwire,
        sync,
        ilp,
        resp,
    } = clients;
    let open = || -> crate::Result<OpenListeners> {
        Ok(OpenListeners {
            native: Listener::from_listener(native.listen()?)?,
            pgwire: PgListener::from_listener(pgwire.listen()?)?,
            sync: sync.listen()?,
            ilp: ilp
                .map(|socket| IlpListener::from_listener(socket.listen()?))
                .transpose()?,
            resp: resp
                .map(|socket| RespListener::from_listener(socket.listen()?))
                .transpose()?,
        })
    };
    match open() {
        Ok(open) => {
            serving_gate.fire();
            Ok(open)
        }
        Err(error) => {
            serving_gate.fail(error.to_string());
            Err(error.into())
        }
    }
}
