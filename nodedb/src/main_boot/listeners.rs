// SPDX-License-Identifier: BUSL-1.1

//! Connection admission setup, listener binding, the startup banner,
//! signal-handler spawn, and shared TLS acceptor construction.

use std::sync::Arc;
use std::time::Duration;

use nodedb::ServerConfig;
use nodedb::bootstrap;
use nodedb::bootstrap::tls::build_tls_acceptor;
use nodedb::control::cluster::ClusterHandle;
use nodedb::control::server::ilp_listener::IlpListener;
use nodedb::control::server::listener::Listener;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::server::resp::RespListener;
use nodedb::control::shutdown::ShutdownBus;
use nodedb::control::state::SharedState;

/// Every value the tail of `main()` needs after connection admission +
/// listener setup, bundled so the call site doesn't juggle nine
/// separate `let`s.
pub(crate) struct ListenerSetup {
    pub(crate) conn_semaphore: Arc<tokio::sync::Semaphore>,
    pub(crate) admission_registry: Arc<nodedb::control::server::admission::AdmissionRegistry>,
    pub(crate) listener: Listener,
    pub(crate) pg_listener: PgListener,
    pub(crate) http_listener: tokio::net::TcpListener,
    pub(crate) sync_listener: tokio::net::TcpListener,
    pub(crate) ilp_listener: Option<IlpListener>,
    pub(crate) resp_listener: Option<RespListener>,
    pub(crate) base_acceptor: Option<tokio_rustls::TlsAcceptor>,
    pub(crate) native_tls_enabled: bool,
}

/// Create the connection semaphore + admission registry, bind all
/// listeners, print the startup banner, spawn signal handlers, and
/// build the shared TLS acceptor. Pure relocation of what used to be
/// inline in `main()` between background-loop spawn and cluster-ready
/// wait.
pub(crate) async fn setup(
    shared: &Arc<SharedState>,
    config: &ServerConfig,
    cluster_mode_str: &str,
    shutdown_bus: &ShutdownBus,
    cluster_handle: Option<Arc<ClusterHandle>>,
) -> anyhow::Result<ListenerSetup> {
    // Create shared connection semaphore — enforced across all listeners.
    let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(config.server.max_connections));

    // Reuse the admission registry owned by SharedState so every transport
    // observes the same runtime database and tenant connection quotas.
    let admission_registry = Arc::clone(&shared.admission_registry);
    tracing::info!(
        max_connections = config.server.max_connections,
        "connection limit configured"
    );

    // Bind all listeners — every protocol, including HTTP and sync — before
    // starting any accept loop, so a port conflict fails boot here rather
    // than after the node is already serving other protocols.
    let bootstrap::listeners::BoundListeners {
        native: listener,
        pgwire: pg_listener,
        http: http_listener,
        sync: sync_listener,
        ilp: ilp_listener,
        resp: resp_listener,
    } = bootstrap::listeners::bind_listeners(config).await?;

    // Startup banner (and trust-mode warning if applicable).
    bootstrap::credentials::print_startup_banner(config, cluster_mode_str);

    // Spawn graceful shutdown and force-stop signal handlers.
    bootstrap::signal::spawn_signal_handlers(
        Arc::clone(shared),
        Arc::clone(&conn_semaphore),
        config.server.max_connections,
        shutdown_bus.clone(),
        cluster_handle,
    );

    // Build shared TLS acceptor if configured. Per-protocol flags control
    // which listeners actually use it — `tls_for(flag)` returns None when
    // the flag is false, disabling TLS on that protocol.
    let base_acceptor: Option<tokio_rustls::TlsAcceptor> = match &config.server.tls {
        Some(tls) => {
            let check_interval = Duration::from_secs(tls.cert_reload_interval_secs.unwrap_or(3600));
            let (_tls_rx, _tls_tx) = nodedb::control::server::tls_reload::start_tls_reloader(
                tls,
                check_interval,
                Arc::clone(shared),
            )?;
            let acceptor: tokio_rustls::TlsAcceptor = build_tls_acceptor(tls)?;
            tracing::info!(
                reload_interval_secs = check_interval.as_secs(),
                "TLS enabled with hot rotation"
            );
            Some(acceptor)
        }
        None => None,
    };

    let tls_flags = config.server.tls.as_ref();
    let native_tls_enabled = tls_flags.is_some_and(|t| t.native);

    Ok(ListenerSetup {
        conn_semaphore,
        admission_registry,
        listener,
        pg_listener,
        http_listener,
        sync_listener,
        ilp_listener,
        resp_listener,
        base_acceptor,
        native_tls_enabled,
    })
}
