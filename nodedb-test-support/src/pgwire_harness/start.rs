// SPDX-License-Identifier: BUSL-1.1

//! Single-core `TestServer::start`, plus `take_dir` for handing the data
//! directory to a subsequent restart.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::event::{EventPlane, create_event_bus};
use nodedb::wal::WalManager;

use super::support::{bind_http_listener, bind_native_listener, init_test_memory_governor};
use super::types::{TestClient, TestDataDir, TestServer};

/// Knobs for spawning a `TestServer`. `Default` reproduces the historical
/// `TestServer::start` behaviour: trust-mode auth, lockout disabled.
pub(super) struct StartConfig {
    /// pgwire authentication mode.
    pub auth_mode: AuthMode,
    /// When `Some((max_failed, lockout_secs))`, configures the credential
    /// store's lockout policy before it is shared. `None` leaves lockout
    /// disabled (`max_failed_logins = 0`).
    pub lockout: Option<(u32, u64)>,
}

impl Default for StartConfig {
    fn default() -> Self {
        Self {
            auth_mode: AuthMode::Trust,
            lockout: None,
        }
    }
}

#[allow(dead_code)]
impl TestServer {
    /// Spawn a single-core NodeDB server and connect via pgwire (trust mode).
    pub async fn start() -> Self {
        Self::start_with_config(StartConfig::default()).await
    }

    /// Spawn a single-core server in pgwire **password mode** (SCRAM-SHA-256)
    /// with the credential lockout policy enabled (`5` failures → `300s`).
    ///
    /// The harness user `nodedb` keeps password `nodedb`; the returned
    /// client authenticates with it. Tests can then mutate the credential
    /// store and open further connections to exercise the SCRAM auth path.
    pub async fn start_password() -> Self {
        Self::start_with_config(StartConfig {
            auth_mode: AuthMode::Password,
            lockout: Some((5, 300)),
        })
        .await
    }

    /// Spawn a single-core NodeDB server and connect via pgwire.
    pub(super) async fn start_with_config(cfg: StartConfig) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let (event_producers, event_consumers) = create_event_bus(1);

        // Use catalog-backed credential store (required for CREATE FUNCTION/TRIGGER/PROCEDURE).
        let catalog_path = dir.path().join("system.redb");
        let mut credential_store =
            nodedb::control::security::credential::store::CredentialStore::open(&catalog_path)
                .unwrap();
        // Apply the lockout policy before the store is shared — `set_lockout_policy`
        // needs `&mut`, so it cannot be called once wrapped in an `Arc`.
        if let Some((max_failed, lockout_secs)) = cfg.lockout {
            credential_store.set_lockout_policy(max_failed, lockout_secs, 0);
        }
        let credentials = Arc::new(credential_store);
        // Provision the harness superuser `nodedb` so Trust-mode strict
        // identity resolution accepts the default test connection. The
        // bootstrap exception in the handler only fires when the store
        // is empty, which would break as soon as any DDL creates a user.
        let _ = credentials.create_user(
            "nodedb",
            "nodedb",
            nodedb::types::TenantId::new(1),
            vec![nodedb::control::security::identity::Role::Superuser],
        );
        // Ensure the built-in `default` database (id 0) is present in the
        // catalog so `USE DATABASE default` and `\c default` work in tests.
        // Idempotent: no-op if the descriptor is already there.
        if let Some(cat) = credentials.catalog() {
            let _ = cat.bootstrap_default_database();
        }
        let mut shared =
            SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials);
        // Inject a fixed test KEK so backup tests produce encrypted envelopes.
        // Deterministic 32-byte key — same value every test run.
        if let Some(s) = Arc::get_mut(&mut shared) {
            s.backup_kek = Some(Arc::new([0x42u8; 32]));
            s.governor = init_test_memory_governor();
        }
        let shared = shared;

        // Data Plane core. Share the SharedState's array_catalog so DDL
        // mutations made by the SQL converter are visible to the handler
        // (without this, CP and DP would each carry independent catalogs
        // and `OpenArray` post-DROP-and-recreate would see stale state).
        let mut core_stop_txs = Vec::new();
        let mut core_handles = Vec::new();
        for (idx, (data_side, event_producer)) in
            data_sides.into_iter().zip(event_producers).enumerate()
        {
            let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
            let core_handle =
                crate::core_loop_runner::spawn_core_loop(crate::core_loop_runner::CoreLoopSpawn {
                    idx,
                    data_side,
                    core_dir: dir.path().to_path_buf(),
                    core_array_catalog: shared.array_catalog.clone(),
                    event_producer,
                    core_metrics: None,
                    governor: shared.governor.clone(),
                    replay: None,
                    graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
                    stop_rx: core_stop_rx,
                });
            core_stop_txs.push(core_stop_tx);
            core_handles.push(core_handle);
        }

        // Response poller.
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

        let watermark_store =
            Arc::new(nodedb::event::watermark::WatermarkStore::open(dir.path()).unwrap());
        let trigger_dlq = Arc::new(std::sync::Mutex::new(
            nodedb::event::trigger::TriggerDlq::open(dir.path()).unwrap(),
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

        // PgWire listener.
        let pg_listener = PgListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let pg_addr = pg_listener.local_addr();

        // Create a shutdown bus wrapping the shared.shutdown watch so that
        // bus.initiate() also signals the flat ShutdownWatch.
        let (shutdown_bus, _) =
            nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(128));
        let shared_pg = Arc::clone(&shared);
        // Use the startup gate already on SharedState (a pre-fired placeholder
        // from `new_inner`). The listener starts accepting immediately.
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_pg = shutdown_bus.clone();
        let pg_sem = Arc::clone(&conn_semaphore);
        let listener_auth_mode = cfg.auth_mode.clone();
        let pg_handle = tokio::spawn(async move {
            pg_listener
                .run(
                    shared_pg,
                    listener_auth_mode,
                    None,
                    pg_sem,
                    test_startup_gate,
                    bus_pg,
                )
                .await
                .unwrap();
        });

        // Native (MessagePack) listener — same SharedState, ephemeral port.
        let (native_port, native_handle) =
            bind_native_listener(&shared, &shutdown_bus, Arc::clone(&conn_semaphore)).await;
        let (http_port, http_handle) = bind_http_listener(&shared, &shutdown_bus).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect client. Password / certificate mode supplies the harness
        // user's credentials so the SCRAM handshake completes.
        let conn_str = match cfg.auth_mode {
            AuthMode::Password | AuthMode::Certificate => format!(
                "host=127.0.0.1 port={} user=nodedb password=nodedb dbname=nodedb",
                pg_addr.port()
            ),
            AuthMode::Trust => format!(
                "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
                pg_addr.port()
            ),
        };
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("pgwire connect failed");

        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Self {
            client: TestClient::new(client),
            pg_port: pg_addr.port(),
            native_port,
            http_port,
            shared,
            conn_handle: Some(conn_handle),
            shutdown_bus: Some(shutdown_bus),
            poller_shutdown_tx: Some(poller_shutdown_tx),
            core_stop_txs: Some(core_stop_txs),
            pg_handle: Some(pg_handle),
            native_handle: Some(native_handle),
            http_handle: Some(http_handle),
            poller_handle: Some(poller_handle),
            core_handles: Some(core_handles),
            event_plane: Some(event_plane),
            _dir: dir,
        }
    }

    /// Consume the data directory from a live server so it outlives the
    /// server's lifetime. The server continues to run until dropped, but
    /// ownership of the temp dir moves to the caller so the files survive
    /// the `Drop` of `TestServer`.
    ///
    /// The returned `TestDataDir` must be kept alive until the caller is
    /// done with the on-disk state (i.e., after `open_on_path` returns).
    pub fn take_dir(mut self) -> (Self, TestDataDir) {
        // Replace the TempDir inside self with a new one (data plane has
        // already loaded everything, so the new "empty" dir is unused).
        // We do this by reconstructing with a sentinel. The original dir
        // is returned to the caller via TestDataDir.
        let original_dir = {
            // SAFETY: we swap the dir out before drop so neither the old
            // nor the new TempDir is double-freed.
            let placeholder = tempfile::tempdir().unwrap();
            std::mem::replace(&mut self._dir, placeholder)
        };
        (self, TestDataDir(original_dir))
    }
}
