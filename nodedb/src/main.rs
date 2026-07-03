// SPDX-License-Identifier: BUSL-1.1

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nodedb::ServerConfig;
use nodedb::bootstrap;
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::server::apply_env_overrides;
use nodedb::control::startup::{StartupPhase, StartupSequencer};
use nodedb::control::state::SharedState;
use tracing::info;

use bootstrap::tls::build_tls_acceptor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Operator subcommand dispatch (L.4): handled before config load
    // + tracing init so `nodedb regen-certs`, `nodedb rotate-ca`,
    // `nodedb join-token` exit cleanly without spinning up the
    // server's global allocator arenas or file locks. A first arg
    // that doesn't match a known subcommand is treated as a config
    // file path and falls through to the normal server bootstrap.
    let cli_args: Vec<String> = std::env::args().skip(1).collect();
    match nodedb::ctl::parse_subcommand(&cli_args) {
        Ok(Some(cmd)) => std::process::exit(nodedb::ctl::run_subcommand(cmd)),
        Ok(None) => {}
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    }

    // Resolve config file path.
    // Priority: CLI arg (highest) > NODEDB_CONFIG env var > default.
    let config_path: Option<PathBuf> = cli_args
        .iter()
        .find(|a| !a.starts_with("--"))
        .map(PathBuf::from)
        .or_else(|| std::env::var("NODEDB_CONFIG").ok().map(PathBuf::from));

    // Load config first (needed for log format).
    // Environment variable overrides are applied after tracing is initialised
    // (see below) so that info!/warn! messages are actually emitted.
    let mut config = match config_path {
        Some(ref path) => ServerConfig::from_file(path)?,
        None => ServerConfig::default(),
    };

    // Apply env overrides once now (before tracing) so that log_format is
    // correct in case NODEDB_DATA_DIR / NODEDB_MEMORY_LIMIT also affect it.
    // The overrides are re-applied silently here; the real log messages
    // will be emitted by the second call after the subscriber is registered.
    apply_env_overrides(&mut config);

    // Initialize tracing subscriber (format + filter from config / RUST_LOG).
    bootstrap::tracing_init::init_tracing(&config);

    // Root span: entered for the lifetime of the process. Provides structured
    // context fields (service name, version, host, pid, node_id) on every log
    // event. node_id starts at 0 for single-node; cluster wiring records the
    // real value below once the cluster handle is resolved.
    let root_span = tracing::info_span!(
        "service",
        service.name = "nodedb",
        service.version = nodedb::version::VERSION,
        host = %nodedb::version::hostname(),
        pid = std::process::id(),
        node_id = 0u64,
    );
    // Use enter() (borrows) rather than entered() (consumes) so that root_span
    // remains accessible for the late record() call after cluster wiring.
    let _root_guard = root_span.enter();

    // Re-apply env overrides now that tracing is initialised so that
    // info!/warn! messages are actually emitted for operators.
    apply_env_overrides(&mut config);

    match &config_path {
        None => info!("no config file provided, using defaults"),
        Some(path)
            if std::env::var("NODEDB_CONFIG").is_ok() && std::env::args().nth(1).is_none() =>
        {
            info!(
                path = %path.display(),
                "config file loaded from NODEDB_CONFIG"
            );
        }
        Some(_) => {}
    }

    let cluster_mode_str = if config.cluster.is_some() {
        "cluster"
    } else {
        "single-node"
    };
    info!(
        target: "boot",
        version = nodedb::version::VERSION,
        git_commit = nodedb::version::GIT_COMMIT,
        build_date = nodedb::version::BUILD_DATE,
        build_profile = nodedb::version::BUILD_PROFILE,
        rust_version = nodedb::version::RUST_VERSION,
        wire_format_version = nodedb::version::WIRE_FORMAT_VERSION,
        features = nodedb::version::features_str(),
        host = %nodedb::version::hostname(),
        pid = std::process::id(),
        pgwire_port = config.server.ports.pgwire,
        http_port = config.server.ports.http,
        native_port = config.server.ports.native,
        cluster_mode = cluster_mode_str,
        cores = config.server.data_plane_cores,
        memory_limit = config.server.memory_limit,
        "nodedb starting",
    );

    // Validate engine config.
    config.engines.validate()?;

    // Construct the gate-based startup sequencer. Gates for each phase are
    // registered before the subsystem that owns that phase begins its work,
    // and fired immediately after it reports ready. The `startup_gate` is
    // installed on `SharedState` after `open()` returns so every code path
    // that calls `await_phase` can observe phase transitions in real time.
    let (startup_seq, startup_gate) = StartupSequencer::new();

    // Register all gates up-front so the sequencer knows every phase has
    // an owner. Phases that have no concurrent sub-tasks get a single gate
    // that is fired inline.
    let wal_gate = startup_seq.register_gate(StartupPhase::WalRecovery, "wal");
    let catalog_gate =
        startup_seq.register_gate(StartupPhase::ClusterCatalogOpen, "cluster-catalog");
    let raft_gate =
        startup_seq.register_gate(StartupPhase::RaftMetadataReplay, "raft-metadata-replay");
    let schema_gate =
        startup_seq.register_gate(StartupPhase::SchemaCacheWarmup, "schema-cache-warmup");
    let sanity_gate =
        startup_seq.register_gate(StartupPhase::CatalogSanityCheck, "catalog-sanity-check");
    let data_groups_gate =
        startup_seq.register_gate(StartupPhase::DataGroupsReplay, "data-groups-replay");
    let transport_gate = startup_seq.register_gate(StartupPhase::TransportBind, "transport-bind");
    let warm_peers_gate = startup_seq.register_gate(StartupPhase::WarmPeers, "warm-peers");
    let health_loop_gate = startup_seq.register_gate(StartupPhase::HealthLoopStart, "health-loop");
    let gateway_enable_gate =
        startup_seq.register_gate(StartupPhase::GatewayEnable, "gateway-enable");

    // Initialize memory governor (per-engine budgets + global ceiling).
    let byte_budgets = config.engines.to_byte_budgets(config.server.memory_limit);
    let governor = nodedb::memory::init_governor(config.server.memory_limit, &byte_budgets)?;

    // Open WAL, validate, replay, and load tombstone set.
    let (wal, wal_records, replay_tombstones) = bootstrap::wal_init::init_wal(&config)?;
    wal_gate.fire();

    // Create SPSC bridge: Dispatcher (Control Plane) + CoreChannelDataSide (Data Plane).
    let num_cores = config.server.data_plane_cores;
    let (mut dispatcher, data_sides) = Dispatcher::new(num_cores, 1024);

    // Create Event Bus: per-core ring buffers (Data Plane → Event Plane).
    let (event_producers, event_consumers) = nodedb::event::bus::create_event_bus(num_cores);

    // Start Data Plane cores on dedicated OS threads (thread-per-core).
    // Each core gets: jemalloc arena pinning + eventfd-driven wake + WAL replay + event producer.
    let system_metrics = Arc::new(nodedb::control::metrics::SystemMetrics::new());

    // Create the shared scan-quiesce registry up front so every Data
    // Plane core and (below) `SharedState::open` reference the same
    // instance. The registry is the integration point between Control
    // Plane purge-time `begin_drain` and per-core scan-time
    // `try_start_scan` — splitting it would make drain a no-op.
    let quiesce = nodedb::bridge::quiesce::CollectionQuiesce::new();

    // Shared ordinal clock for bitemporal `system_from` key suffixes.
    // One instance per server — all Data Plane cores reference the same
    // Arc so edge keys are globally strictly monotonic.
    let hlc = Arc::new(nodedb_types::OrdinalClock::new());

    // Load the persisted ND-array catalog once, before spawning cores.
    let array_catalog = bootstrap::data_plane::load_array_catalog(&config);

    // Create the quarantine registry before spawning cores.
    let quarantine_registry =
        std::sync::Arc::new(nodedb::storage::quarantine::QuarantineRegistry::new());

    // Create once and share with both Data Plane cores and SharedState so
    // ALTER DATABASE SET QUOTA updates live caps immediately for all cores.
    let maintenance_budget =
        Arc::new(nodedb::control::maintenance::MaintenanceBudgetTracker::new());

    let _core_handles = bootstrap::data_plane::spawn_data_plane_cores(
        &config,
        data_sides,
        event_producers,
        Arc::clone(&wal_records),
        replay_tombstones.clone(),
        &mut dispatcher,
        bootstrap::data_plane::CoreSharedResources {
            governor: Arc::clone(&governor),
            quiesce: Arc::clone(&quiesce),
            hlc: Arc::clone(&hlc),
            array_catalog: Arc::clone(&array_catalog),
            quarantine_registry: Arc::clone(&quarantine_registry),
            system_metrics: Arc::clone(&system_metrics),
            maintenance_budget: Arc::clone(&maintenance_budget),
        },
    )?;

    // Event Plane resources (spawned after SharedState is created — needs it for trigger dispatch).
    let watermark_store = Arc::new(
        nodedb::event::watermark::WatermarkStore::open(&config.server.data_dir)
            .expect("failed to open event plane watermark store"),
    );
    let trigger_dlq = Arc::new(std::sync::Mutex::new(
        nodedb::event::trigger::TriggerDlq::open(&config.server.data_dir)
            .expect("failed to open trigger DLQ"),
    ));

    // Initialize cluster mode if configured.
    let cluster_handle = if let Some(ref cluster_cfg) = config.cluster {
        cluster_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!("cluster config: {e}"))?;
        let handle = nodedb::control::cluster::init_cluster(
            cluster_cfg,
            &config.server.data_dir,
            &config.tuning.cluster_transport,
        )
        .await?;
        Some(handle)
    } else {
        None
    };

    // Create shared state with persistent system catalog.
    let mut shared = SharedState::open(
        dispatcher,
        Arc::clone(&wal),
        &config.catalog_path(),
        &config.auth,
        config.tuning.clone(),
        Arc::clone(&quiesce),
        Arc::clone(&array_catalog),
    )?;

    // Install startup gate, wire subsystems and cluster handles into SharedState.
    bootstrap::state_wiring::wire_state(
        &mut shared,
        &config,
        &startup_gate,
        cluster_handle.as_ref(),
        bootstrap::state_wiring::SharedStateComponents {
            quarantine_registry: Arc::clone(&quarantine_registry),
            governor: Arc::clone(&governor),
            system_metrics: Arc::clone(&system_metrics),
            array_catalog: Arc::clone(&array_catalog),
            maintenance_budget: Arc::clone(&maintenance_budget),
        },
        &root_span,
    )?;

    // Wire global quota ceiling from server config so `ALTER DATABASE SET QUOTA`
    // can validate the sum-of-database-quotas against the cluster's physical
    // resources. `memory_limit` and `max_connections` are the only dimensions
    // the server config currently constrains; storage and QPS pass through as
    // zero (= no ceiling) until [server.storage_limit] / [server.qps_limit]
    // land. The ALTER handler treats zero on any dimension as "skip that check".
    {
        use nodedb::control::security::catalog::GlobalQuotaCeiling;
        let mem_u64 = u64::try_from(config.server.memory_limit).unwrap_or(u64::MAX);
        let conn_u64 = u64::try_from(config.server.max_connections).unwrap_or(u64::MAX);
        shared.set_quota_ceiling(GlobalQuotaCeiling {
            max_memory_bytes: mem_u64,
            max_storage_bytes: 0,
            max_qps: 0,
            max_connections: conn_u64,
        });
    }

    // Apply login rate-limit capacities from cluster config (or defaults).
    {
        let (ip_cap, user_cap) = config
            .cluster
            .as_ref()
            .map(|c| {
                (
                    c.login_attempts_per_ip_per_min,
                    c.login_attempts_per_user_per_min,
                )
            })
            .unwrap_or((30, 10));
        shared.rate_limiter.set_login_capacities(ip_cap, user_cap);
    }

    // System catalog (redb) is open — fire the ClusterCatalogOpen gate.
    catalog_gate.fire();

    // Replay surrogate WAL records into the in-memory registry.
    bootstrap::credentials::replay_surrogate_wal(&shared, &wal_records);

    // Recover any in-progress MOVE TENANT operations from the journal.
    // This runs synchronously before accepting connections so that
    // in-flight tenant moves are resolved before any client can issue
    // new ones against the same tenant.
    nodedb::control::server::shared::ddl::neutral::tenant::move_tenant::recovery::recover_all(
        &shared,
    )
    .await;

    // Bootstrap superuser credential (or warn about trust mode).
    bootstrap::credentials::bootstrap_superuser(&shared, &config)?;

    // All shutdown signals flow through the canonical
    // `ShutdownWatch` held on `SharedState`. The local
    // `shutdown_rx` binding below is a raw-receiver view of
    // that same watch, preserved so the existing listener APIs
    // (`PgListener::run`, `HttpServer::run`, `IlpListener::run`,
    // `RespListener::run`, `spawn_cold_storage_loop`,
    // `spawn_checkpoint_loop`, and the lease renewal loop)
    // keep their `watch::Receiver<bool>` parameter unchanged.
    // New code SHOULD use `shared.shutdown.subscribe()`.
    let shutdown_rx = shared.shutdown.raw_receiver();

    // Unified shutdown bus: phased drain with per-phase 500 ms budgets.
    // `ShutdownBus::initiate()` signals the flat `ShutdownWatch` so all
    // existing `watch::Receiver<bool>` subscribers wake up as well.
    let (shutdown_bus, _shutdown_bus_handle) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    // Wire system metrics so the bus records `nodedb_shutdown_phase_duration_seconds{phase}`
    // for each phase transition during graceful shutdown.
    shutdown_bus.set_metrics(Arc::clone(&system_metrics));

    // Test-only injection: if NODEDB_TEST_SLOW_DRAIN_TASK=1, register a drain
    // task that sleeps for 2s without calling report_drained, to verify the
    // offender-abort path in integration tests. This code path is guarded
    // by an env var so it is never activated in production.
    if std::env::var("NODEDB_TEST_SLOW_DRAIN_TASK").as_deref() == Ok("1") {
        let mut guard = shutdown_bus.register_task(
            nodedb::control::shutdown::ShutdownPhase::DrainingListeners,
            "test_slow_task",
            None,
        );
        tokio::spawn(async move {
            guard.await_signal().await;
            // Intentionally do NOT call report_drained — tests the offender path.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            drop(guard); // This will log the "dropped without report_drained" warning.
        });
    }

    // Start cluster Raft loop if in cluster mode. The returned
    // receiver flips to `true` after the metadata raft group has
    // applied its first entry on this node — see
    // `nodedb-cluster::RaftLoop::subscribe_ready`. We hold on to it
    // and await it just before binding client-facing listeners so
    // the first DDL after process start cannot race against an
    // uninitialized metadata group.
    let raft_ready_rx: Option<tokio::sync::watch::Receiver<bool>> =
        if let Some(ref handle) = cluster_handle {
            Some(nodedb::control::cluster::start_raft(
                handle,
                Arc::clone(&shared),
                &config.server.data_dir,
                shutdown_rx.clone(),
                &config.tuning.cluster_transport,
            )?)
        } else {
            None
        };

    // Spawn the descriptor lease renewal loop. Returns None on
    // single-node clusters (no metadata raft handle wired) — the
    // returned JoinHandle is dropped on the floor because the loop
    // subscribes to `shutdown_rx` and exits cleanly on Ctrl+C.
    let _lease_renewal = nodedb::control::lease::LeaseRenewalLoop::spawn(
        Arc::clone(&shared),
        &config.tuning.cluster_transport,
        shutdown_rx.clone(),
    )
    .map(|(join, metrics)| {
        shared.loop_metrics_registry.register(metrics);
        join
    });

    // Start response poller (routes Data Plane responses to waiting sessions).
    bootstrap::background_loops::spawn_response_poller(&shared);

    // Spawn all persistent background loops and subsystems.
    // The returned EventPlane handle MUST be held for the server's lifetime —
    // dropping it shuts down every event consumer and the Data Plane will
    // silently drop every WriteEvent it emits afterward.
    let _event_plane = bootstrap::background_loops::spawn_background_loops(
        &shared,
        bootstrap::background_loops::EventPlaneComponents {
            wal: Arc::clone(&wal),
            event_consumers,
            watermark_store,
            trigger_dlq,
        },
        &config,
        num_cores,
        shutdown_rx.clone(),
    );

    // Create shared connection semaphore — enforced across all listeners.
    let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(config.server.max_connections));

    // Per-database and per-tenant connection semaphore registry.
    // Populated at runtime when ALTER DATABASE/TENANT SET QUOTA configures max_connections.
    let admission_registry = Arc::new(nodedb::control::server::admission::AdmissionRegistry::new());
    info!(
        max_connections = config.server.max_connections,
        "connection limit configured"
    );

    // Bind all listeners before starting accept loops.
    let (listener, pg_listener, ilp_listener, resp_listener) =
        bootstrap::listeners::bind_listeners(&config).await?;

    // Startup banner (and trust-mode warning if applicable).
    bootstrap::credentials::print_startup_banner(&config, cluster_mode_str);

    // Spawn graceful shutdown and force-stop signal handlers.
    bootstrap::signal::spawn_signal_handlers(
        Arc::clone(&shared),
        Arc::clone(&conn_semaphore),
        config.server.max_connections,
        shutdown_bus.clone(),
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
                Arc::clone(&shared),
            )?;
            let acceptor: tokio_rustls::TlsAcceptor = build_tls_acceptor(tls)?;
            info!(
                reload_interval_secs = check_interval.as_secs(),
                "TLS enabled with hot rotation"
            );
            Some(acceptor)
        }
        None => None,
    };

    // Per-protocol TLS: returns the acceptor only if the protocol flag is true.
    let tls_for = |enabled: bool| -> Option<tokio_rustls::TlsAcceptor> {
        if enabled { base_acceptor.clone() } else { None }
    };
    let tls_flags = config.server.tls.as_ref();
    let native_tls_enabled = tls_flags.is_some_and(|t| t.native);

    // Wait for raft readiness, run catalog sanity check, warm peer cache, fire gates.
    bootstrap::cluster_ready::await_cluster_ready(
        &shared,
        raft_ready_rx,
        bootstrap::cluster_ready::ClusterReadyGates {
            raft_gate,
            schema_gate,
            sanity_gate,
            data_groups_gate,
            transport_gate,
            warm_peers_gate,
            health_loop_gate,
            gateway_enable_gate,
        },
    )
    .await?;

    // Spawn all non-native protocol listeners.
    bootstrap::listeners::spawn_protocol_listeners(
        bootstrap::listeners::ProtocolListeners {
            pg_listener,
            ilp_listener,
            resp_listener,
        },
        Arc::clone(&shared),
        &config,
        bootstrap::listeners::ListenerInfra {
            conn_semaphore: Arc::clone(&conn_semaphore),
            startup_gate: Arc::clone(&startup_gate),
            shutdown_bus: shutdown_bus.clone(),
        },
        base_acceptor.clone(),
        &cluster_handle,
    )
    .await;

    // Native protocol TLS.
    let native_tls = tls_for(native_tls_enabled);

    // Run native listener on main task.
    let native_auth_mode = config.auth.mode.clone();
    listener
        .run(nodedb::control::server::listener::ListenerRunParams {
            state: shared,
            auth_mode: native_auth_mode,
            tls_acceptor: native_tls,
            conn_semaphore,
            startup_gate: Arc::clone(&startup_gate),
            bus: shutdown_bus.clone(),
            admission: admission_registry,
        })
        .await?;

    info!("server shutting down");
    nodedb_cluster::readiness::notify_stopping();

    // The native listener returned because the phased shutdown bus signaled
    // DrainingListeners. The signal handler task is concurrently awaiting
    // the bus sequencer to walk every phase (including offender-abort at
    // budget). If we `exit(0)` here, the signal handler gets killed
    // mid-sequence and offender-abort logs never get emitted.
    //
    // Wait for the bus to reach `Closed` before exiting. The signal handler
    // also calls `exit(0)` after its sequencer await — whichever reaches
    // it first wins the race, and both paths guarantee the sequencer has
    // completed first.
    shutdown_bus
        .handle()
        .await_phase(nodedb::control::shutdown::ShutdownPhase::Closed)
        .await;

    // Data Plane cores run on std::thread (not Tokio) and block in an
    // infinite eventfd poll loop. They have no shutdown signal — they
    // rely on process exit. Explicitly exit so they don't keep the
    // process alive after the Control Plane has drained.
    std::process::exit(0);
}
