// SPDX-License-Identifier: BUSL-1.1

//! Shared bring-up helpers used by every `TestServer` constructor: the
//! native-protocol listener bind, and the memory-governor wiring.

use std::sync::Arc;

use nodedb::config::auth::AuthMode;
use nodedb::control::server::listener::{Listener, ListenerRunParams};
use nodedb::control::state::SharedState;

/// Build a `MemoryGovernor` for integration tests using the **production**
/// wiring (`nodedb::memory::init_governor` over a default `EngineConfig`),
/// so the harness can never diverge from how a real server distributes its
/// memory budget. A hand-rolled all-engines map here once masked a bug
/// where production registered only a subset of engines and the first write
/// to any unregistered engine was rejected with `resources exhausted`.
///
/// An 8 GiB ceiling keeps even the smallest per-engine slice generous
/// enough that integration workloads never trip engine-level pressure.
/// Panics if `GovernorConfig` validation fails — an 8 GiB uniform budget
/// over the default `EngineConfig` never does.
pub(super) fn init_test_memory_governor() -> Arc<nodedb_mem::MemoryGovernor> {
    let ceiling: usize = 8 * 1024 * 1024 * 1024; // 8 GiB
    let budgets = nodedb::config::EngineConfig::default().to_byte_budgets(ceiling);
    nodedb::memory::init_governor(ceiling, &budgets).expect("harness governor config is valid")
}

/// Bind a native (MessagePack) protocol listener on `127.0.0.1:0` and
/// spawn its accept loop. Returns the listener's local port plus the
/// handle to await on shutdown.
pub(super) async fn bind_native_listener(
    shared: &Arc<SharedState>,
    shutdown_bus: &nodedb::control::shutdown::ShutdownBus,
    conn_semaphore: Arc<tokio::sync::Semaphore>,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = Listener::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind native listener");
    let port = listener.local_addr().port();
    let state = Arc::clone(shared);
    let startup_gate = Arc::clone(&shared.startup);
    let bus = shutdown_bus.clone();
    // The registry `SharedState` carries, not a fresh one: quota DDL installs
    // its caps there. A private registry leaves the native listener enforcing
    // an empty set of caps.
    let admission = Arc::clone(&shared.admission_registry);
    let handle = tokio::spawn(async move {
        let _ = listener
            .run(ListenerRunParams {
                state,
                auth_mode: AuthMode::Trust,
                tls_acceptor: None,
                conn_semaphore,
                startup_gate,
                bus,
                admission,
            })
            .await;
    });
    (port, handle)
}

/// Bind an HTTP (REST/axum) listener on `127.0.0.1:0` and spawn its accept
/// loop. Returns the listener's local port plus the handle to await on
/// shutdown.
pub(super) async fn bind_http_listener(
    shared: &Arc<SharedState>,
    shutdown_bus: &nodedb::control::shutdown::ShutdownBus,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind http listener");
    let port = listener.local_addr().expect("http local addr").port();
    let shared_clone = Arc::clone(shared);
    let bus_clone = shutdown_bus.clone();
    let handle = tokio::spawn(async move {
        let _ = nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_clone,
            AuthMode::Trust,
            None,
            bus_clone,
        )
        .await;
    });
    (port, handle)
}
