// SPDX-License-Identifier: BUSL-1.1

//! Spawn and shutdown lifecycle for [`TestClusterNode`].
//!
//! A `TestClusterNode` owns one full NodeDB server instance configured
//! for cluster mode. Spawn via [`TestClusterNode::spawn`], passing the
//! node id and the seed list (empty for the bootstrap node; otherwise
//! a list of already-running peer addresses). On return the node has:
//!
//! - Pre-bound QUIC transport (so the listen address is known before
//!   peers need it).
//! - `SharedState` wired with credentials, metadata_cache, and the
//!   cluster handles (topology / routing / transport / applied-index
//!   watcher).
//! - Data Plane core running on a spawn_blocking task.
//! - Response poller running on a tokio task.
//! - Event Plane spawned.
//! - Raft loop + QUIC RPC server started via `start_raft` (installs
//!   the production `MetadataCommitApplier` with `Weak<SharedState>`
//!   so committed `CollectionDdl::Create` entries trigger Data Plane
//!   registers on every node).
//! - pgwire listener bound on an ephemeral port.
//! - `tokio_postgres::Client` connected to that listener.
//!
//! Shutdown flips every shutdown channel, aborts background tasks,
//! and drops the TempDir.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::config::server::ClusterSettings;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::event::{EventPlane, create_event_bus};
use nodedb::wal::WalManager;
use nodedb_types::config::tuning::ClusterTransportTuning;

/// Running cluster node.
pub struct TestClusterNode {
    pub node_id: u64,
    pub listen_addr: SocketAddr,
    pub pg_addr: SocketAddr,
    /// Native (MessagePack) protocol listener port. Bound on an ephemeral port
    /// so `NativeClient::connect("127.0.0.1:<native_port>")` works in tests.
    pub native_port: u16,
    pub client: tokio_postgres::Client,
    pub shared: Arc<SharedState>,
    pub(super) _data_dir: tempfile::TempDir,
    pub(super) _conn_handle: tokio::task::JoinHandle<()>,
    pub(super) pg_shutdown_bus: nodedb::control::shutdown::ShutdownBus,
    pub(super) poller_shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(super) cluster_shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(super) core_stop_txs: Vec<std::sync::mpsc::Sender<()>>,
    pub(super) _pg_handle: tokio::task::JoinHandle<()>,
    pub(super) _native_handle: tokio::task::JoinHandle<()>,
    pub(super) _poller_handle: tokio::task::JoinHandle<()>,
    pub(super) _core_handles: Vec<tokio::task::JoinHandle<()>>,
    pub(super) _event_plane: EventPlane,
}

impl TestClusterNode {
    /// Spawn a cluster node.
    ///
    /// - `node_id` — non-zero unique id within the cluster.
    /// - `seed_nodes` — empty for the bootstrap node; otherwise the
    ///   pre-bound listen address of at least one already-running
    ///   peer (typically node 1).
    pub async fn spawn(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_tuning(node_id, seed_nodes, ClusterTransportTuning::default()).await
    }

    /// Spawn a cluster node with a custom `ClusterTransportTuning`.
    /// Used by tests that need to override the descriptor lease
    /// duration or renewal cadence to drive renewal within a
    /// short test budget.
    pub async fn spawn_with_tuning(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_tuning_and_cores(node_id, seed_nodes, tuning, 1).await
    }

    /// Spawn a cluster node with a custom `ClusterTransportTuning` and a
    /// specific number of Data-Plane cores. Used to exercise multi-core
    /// code paths in cluster tests. Graph tuning defaults (100k varlen caps).
    pub async fn spawn_with_tuning_and_cores(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_tuning_graph_and_cores(
            node_id,
            seed_nodes,
            tuning,
            nodedb_types::config::tuning::GraphTuning::default(),
            num_cores,
        )
        .await
    }

    /// Spawn a cluster node with custom cluster-transport AND graph engine
    /// tuning plus a specific core count. The `graph_tuning` knob lets cluster
    /// tests lower the variable-length MATCH expansion caps to drive truncation
    /// (and exercise the cross-shard resume drain) on small graphs.
    pub async fn spawn_with_tuning_graph_and_cores(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_tuning_graph_query_and_cores(
            node_id,
            seed_nodes,
            tuning,
            graph_tuning,
            nodedb_types::config::tuning::QueryTuning::default(),
            num_cores,
        )
        .await
    }

    /// Spawn a cluster node with custom cluster-transport, graph engine
    /// tuning, query execution tuning, and a specific core count.
    ///
    /// The `query_tuning` knob lets cluster tests override per-core Data Plane
    /// parameters (e.g. `columnar_flush_threshold`) to exercise flush behaviour
    /// on small datasets.
    ///
    /// Delegates to [`Self::spawn_with_full_config`] with
    /// `log_compaction_threshold = None` (auto-compaction disabled).
    pub async fn spawn_with_tuning_graph_query_and_cores(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        query_tuning: nodedb_types::config::tuning::QueryTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_full_config(
            node_id,
            seed_nodes,
            tuning,
            graph_tuning,
            query_tuning,
            num_cores,
            None,
        )
        .await
    }

    /// Lowest-level cluster-node spawn. In addition to the tuning knobs of
    /// [`Self::spawn_with_tuning_graph_query_and_cores`], this accepts the
    /// Raft `log_compaction_threshold`: when `Some(n)`, every Raft group on
    /// this node auto-compacts its log once it has more than `n` applied
    /// entries past the snapshot index. A low value forces the leader's
    /// data-group log to compact past the start after a handful of writes,
    /// which is what makes a freshly-joined learner unreachable via
    /// `AppendEntries` and forces a real `InstallSnapshot`.
    ///
    /// Every other spawn entry point delegates here with `None`.
    pub async fn spawn_with_full_config(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        query_tuning: nodedb_types::config::tuning::QueryTuning,
        num_cores: usize,
        log_compaction_threshold: Option<u64>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let data_dir = tempfile::tempdir()?;
        let data_dir_path: PathBuf = data_dir.path().to_path_buf();

        // Pre-bind the QUIC transport on a random port so we know the
        // listen address before wiring seeds / cluster settings.
        let transport = Arc::new(nodedb_cluster::NexarTransport::new(
            node_id,
            "127.0.0.1:0".parse()?,
            nodedb_cluster::TransportCredentials::Insecure,
        )?);
        let listen_addr = transport.local_addr();

        // Build cluster settings. Empty `seed_nodes` → single-node
        // bootstrap by listing only our own address.
        let seeds = if seed_nodes.is_empty() {
            vec![listen_addr]
        } else {
            seed_nodes
        };
        let cluster_settings = ClusterSettings {
            node_id,
            listen: listen_addr,
            seed_nodes: seeds,
            num_groups: 2,
            replication_factor: 3,
            force_bootstrap: false,
            tls: None,
            max_active_sessions: 0,
            login_attempts_per_ip_per_min: 30,
            login_attempts_per_user_per_min: 10,
            insecure_transport: true,
            log_compaction_threshold,
        };

        // Open WAL + dispatcher + event bus.
        let wal = Arc::new(WalManager::open_for_testing(
            &data_dir_path.join("test.wal"),
        )?);
        let (dispatcher, data_sides) = Dispatcher::new(num_cores, 1024);
        let (event_producers, event_consumers) = create_event_bus(num_cores);

        // Credential store backed by the system catalog — required for
        // CREATE COLLECTION to exercise the full persistence path.
        let credentials = Arc::new(
            nodedb::control::security::credential::store::CredentialStore::open(
                &data_dir_path.join("system.redb"),
            )?,
        );
        let mut shared =
            SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials);

        // Initialise the cluster using the pre-bound transport.
        let handle = nodedb::control::cluster::init_cluster_with_transport(
            &cluster_settings,
            transport.clone(),
            &data_dir_path,
            &tuning,
        )
        .await?;

        // Wire cluster handles into SharedState (mirrors main.rs).
        // `Arc::get_mut` is valid here: `shared` has not been cloned.
        if let Some(state) = Arc::get_mut(&mut shared) {
            state.node_id = handle.node_id;
            state.cluster_topology = Some(Arc::clone(&handle.topology));
            state.cluster_routing = Some(Arc::clone(&handle.routing));
            state.cluster_transport = Some(Arc::clone(&handle.transport));
            state.metadata_cache = Arc::clone(&handle.metadata_cache);
            state.group_watchers = Arc::clone(&handle.group_watchers);
            // Fixed test KEK so backup tests produce encrypted envelopes.
            state.backup_kek = Some(Arc::new([0x42u8; 32]));
            // Durable producer registry, sharing the credential store's
            // already-open catalog (mirrors production `SharedState::open`).
            // Required for sync handshake fencing to replicate via the
            // metadata Raft group on cluster nodes.
            if let Some(catalog) = state.credentials.catalog() {
                match nodedb::control::sync_producer::registry::SyncProducerRegistry::open(
                    Arc::new(catalog.clone()),
                ) {
                    Ok(reg) => state.producer_registry = Some(Arc::new(reg)),
                    Err(e) => {
                        return Err(format!(
                            "SyncProducerRegistry::open failed in test harness: {e}"
                        )
                        .into());
                    }
                }
            }
        } else {
            return Err("SharedState already cloned before cluster wire-up".into());
        }

        // Start one Data-Plane core loop per core. Each core gets its own SPSC
        // data side and event producer; per-core stores live under the shared
        // data dir keyed by `idx` (graph/core-{idx}.redb, etc.).
        let mut core_stop_txs = Vec::with_capacity(num_cores);
        let mut core_handles = Vec::with_capacity(num_cores);
        for (idx, (data_side, event_producer)) in
            data_sides.into_iter().zip(event_producers).enumerate()
        {
            let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
            let core_handle =
                crate::core_loop_runner::spawn_core_loop(crate::core_loop_runner::CoreLoopSpawn {
                    idx,
                    data_side,
                    core_dir: data_dir_path.clone(),
                    core_array_catalog: shared.array_catalog.clone(),
                    event_producer,
                    core_metrics: shared.system_metrics.clone(),
                    governor: shared.governor.clone(),
                    replay: None,
                    graph_tuning: graph_tuning.clone(),
                    query_tuning: query_tuning.clone(),
                    stop_rx: core_stop_rx,
                });
            core_stop_txs.push(core_stop_tx);
            core_handles.push(core_handle);
        }

        // Response poller (Data Plane → control plane routing).
        let shared_poller = Arc::clone(&shared);
        let (poller_shutdown_tx, mut poller_shutdown_rx) = tokio::sync::watch::channel(false);
        let poller_handle = tokio::spawn(async move {
            loop {
                shared_poller.poll_and_route_responses();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    _ = poller_shutdown_rx.changed() => break,
                }
            }
        });

        // Event Plane (triggers, CDC, scheduler).
        let watermark_store = Arc::new(nodedb::event::watermark::WatermarkStore::open(
            &data_dir_path,
        )?);
        let trigger_dlq = Arc::new(std::sync::Mutex::new(
            nodedb::event::trigger::TriggerDlq::open(&data_dir_path)?,
        ));
        let event_plane = EventPlane::spawn(
            event_consumers,
            Arc::clone(&wal),
            watermark_store,
            Arc::clone(&shared),
            trigger_dlq,
            Arc::clone(&shared.cdc_router),
            Arc::clone(&shared.shutdown),
        );

        // Start Raft + install MetadataCommitApplier.
        let (cluster_shutdown_tx, cluster_shutdown_rx) = tokio::sync::watch::channel(false);
        nodedb::control::cluster::start_raft(
            &handle,
            Arc::clone(&shared),
            &data_dir_path,
            cluster_shutdown_rx.clone(),
            &tuning,
        )?;

        // CRDT constraint reconcile loop (leader-gated). The production server
        // spawns this from `spawn_background_loops`, which the harness does not
        // call; wire it directly so cluster tests exercise constraint delivery
        // to every replica's validator. Registered on `shared.loop_registry`,
        // so cluster shutdown stops it with the other loops.
        nodedb::bootstrap::constraint_reconcile::spawn_constraint_reconcile(Arc::clone(&shared));

        // Spawn the descriptor lease renewal loop on the same
        // shutdown channel as raft so cluster shutdown stops it
        // cleanly. Returns None on single-node clusters that
        // never wired metadata_raft (the harness always wires it,
        // so this returns Some in practice for cluster tests).
        let _lease_renewal = nodedb::control::lease::LeaseRenewalLoop::spawn(
            Arc::clone(&shared),
            &tuning,
            cluster_shutdown_rx,
        )
        .map(|(join, metrics)| {
            shared.loop_metrics_registry.register(metrics);
            join
        });

        // Construct the gateway and install it (plus its DDL invalidator) on
        // SharedState, mirroring what main.rs does before listeners bind.
        //
        // We use a raw-pointer write because `shared` has already been cloned
        // by the response poller task, making `Arc::get_mut` return None.
        // This is sound at this point in setup because:
        //   1. The response poller only calls `poll_and_route_responses()`,
        //      which never touches the `gateway` or `gateway_invalidator` fields.
        //   2. No other concurrent task reads those fields before the pgwire
        //      listener binds (a few lines below).
        //   3. The write completes before the pgwire listener spawns, so the
        //      happens-before relationship is guaranteed.
        {
            let shared_for_gw = Arc::clone(&shared);
            let gateway = Arc::new(nodedb::control::gateway::Gateway::new(shared_for_gw));
            let invalidator = Arc::new(nodedb::control::gateway::PlanCacheInvalidator::new(
                &gateway.plan_cache,
            ));
            // SAFETY: no concurrent reads of `gateway` / `gateway_invalidator`
            // at this point (see comment above). Fields start as `None` and
            // are written once here before any listener starts.
            unsafe {
                let state = Arc::as_ptr(&shared) as *mut nodedb::control::state::SharedState;
                (*state).gateway = Some(Arc::clone(&gateway));
                (*state).gateway_invalidator = Some(invalidator);
            }
        }

        // pgwire listener.
        // In the test harness, use the startup gate already on SharedState
        // (a pre-fired placeholder from `new_inner`). This means the listener
        // accepts immediately without a startup-phase delay.
        let pg_listener = PgListener::bind("127.0.0.1:0".parse()?).await?;
        let pg_addr = pg_listener.local_addr();
        let (pg_shutdown_bus, _) =
            nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let shared_pg = Arc::clone(&shared);
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_pg = pg_shutdown_bus.clone();
        let pg_handle = tokio::spawn(async move {
            let _ = pg_listener
                .run(
                    shared_pg,
                    AuthMode::Trust,
                    None,
                    Arc::new(tokio::sync::Semaphore::new(128)),
                    test_startup_gate,
                    bus_pg,
                )
                .await;
        });

        // Native (MessagePack) listener — same SharedState, ephemeral port,
        // trust-mode auth. Uses the pre-fired startup gate so it accepts
        // immediately without a startup-phase wait (same as the pgwire listener).
        let native_listener =
            nodedb::control::server::listener::Listener::bind("127.0.0.1:0".parse()?)
                .await
                .map_err(|e| format!("bind native listener: {e}"))?;
        let native_port = native_listener.local_addr().port();
        let shared_native = Arc::clone(&shared);
        let native_startup_gate = Arc::clone(&shared.startup);
        let bus_native = pg_shutdown_bus.clone();
        let native_handle = tokio::spawn(async move {
            let _ = native_listener
                .run(nodedb::control::server::listener::ListenerRunParams {
                    state: shared_native,
                    auth_mode: AuthMode::Trust,
                    tls_acceptor: None,
                    conn_semaphore: Arc::new(tokio::sync::Semaphore::new(128)),
                    startup_gate: native_startup_gate,
                    bus: bus_native,
                    admission: Arc::new(
                        nodedb::control::server::admission::AdmissionRegistry::new(),
                    ),
                })
                .await;
        });

        // Give the listeners a moment to start accepting.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect tokio_postgres client.
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
            pg_addr.port()
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("pgwire connect failed: {e}"))?;
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(Self {
            node_id,
            listen_addr,
            pg_addr,
            native_port,
            client,
            shared,
            _data_dir: data_dir,
            _conn_handle: conn_handle,
            pg_shutdown_bus,
            poller_shutdown_tx,
            cluster_shutdown_tx,
            core_stop_txs,
            _pg_handle: pg_handle,
            _native_handle: native_handle,
            _poller_handle: poller_handle,
            _core_handles: core_handles,
            _event_plane: event_plane,
        })
    }

    /// Execute a simple query; returns an error message on SQL error.
    pub async fn exec(&self, sql: &str) -> Result<(), String> {
        match self.client.simple_query(sql).await {
            Ok(_) => Ok(()),
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Cooperatively shut down every background task this node owns.
    pub async fn shutdown(self) {
        self.pg_shutdown_bus.initiate();
        let _ = self.cluster_shutdown_tx.send(true);
        let _ = self.poller_shutdown_tx.send(true);
        for tx in &self.core_stop_txs {
            let _ = tx.send(());
        }
        // Give tokio a chance to drop the task futures before TempDir
        // is dropped — otherwise redb file locks can linger.
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }
}

/// Panic-safe teardown. Without this, a test that panics (e.g. a
/// `wait_for` tripping its budget) would drop `TestClusterNode`
/// without ever calling the async `shutdown()`, leaving every
/// background task still running:
///
/// - `watch::Sender`s close on drop but DO NOT transmit their last
///   value, so the raft / pgwire / poller loops block on
///   `select { shutdown.changed() }` forever.
/// - `JoinHandle`s on drop DETACH the task instead of cancelling it.
/// - Those detached tasks keep the tempdir's redb files open, so
///   `TempDir::drop` either hangs or the whole test process sticks
///   around until nextest kills it at `slow-timeout` (previously
///   ~2 minutes of wasted CI time per flaky cluster test).
///
/// The Drop here fires the watch senders synchronously and aborts
/// every JoinHandle we own. `abort()` is non-blocking: the next time
/// the task hits an `.await` it gets cancelled and releases its
/// resources, including the redb handles. Combined with the
/// already-present `core_stop_tx` drop (which disconnects the
/// blocking Data Plane loop), this guarantees the node tears down
/// in milliseconds instead of minutes.
impl Drop for TestClusterNode {
    fn drop(&mut self) {
        self.pg_shutdown_bus.initiate();
        let _ = self.cluster_shutdown_tx.send(true);
        let _ = self.poller_shutdown_tx.send(true);
        // `core_stop_tx` is a std mpsc Sender; dropping it disconnects
        // the receiver the spawn_blocking data-plane loop polls, so
        // no explicit signal needed here.
        self._conn_handle.abort();
        self._pg_handle.abort();
        self._native_handle.abort();
        self._poller_handle.abort();
        for h in &self._core_handles {
            h.abort();
        }
    }
}

pub(super) fn pg_error_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        format!(
            "{}: {} (SQLSTATE {})",
            db_err.severity(),
            db_err.message(),
            db_err.code().code()
        )
    } else {
        format!("{e:?}")
    }
}
