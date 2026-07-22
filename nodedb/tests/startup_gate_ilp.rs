// SPDX-License-Identifier: BUSL-1.1

//! Integration test: ILP listener is gated on GatewayEnable.
//!
//! The test:
//! 1. Builds a minimal node with a real StartupSequencer (gate held).
//! 2. Binds a real ILP TCP socket.
//! 3. Launches `ilp_listener.run(...)` in a task — it blocks at `await_phase`.
//! 4. Connects a raw TCP stream to the bound port (TCP handshake succeeds
//!    immediately since the port is open; the kernel queues the connection).
//! 5. Sends one ILP line and shuts down the write side (sends FIN).
//! 6. Fires the gate after 300 ms.
//! 7. Reads until EOF — the server closes its side only after accepting and
//!    processing the connection, which requires the gate to have fired.
//! 8. Asserts the EOF arrived after ≥ 250 ms.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::ilp_listener::IlpListener;
use nodedb::control::startup::{StartupPhase, StartupSequencer};
use nodedb::control::state::SharedState;

mod common;

/// Test-scoped owners that must outlive the spawned ILP listener.
/// Critically, `_data_sides` owns the Data Plane SPSC consumer halves —
/// dropping it closes the rings and deadlocks the listener's flush
/// dispatch.
struct GatedOwners {
    _sequencer: StartupSequencer,
    _dir: tempfile::TempDir,
    _data_sides: Vec<nodedb::bridge::dispatch::CoreChannelDataSide>,
}

fn make_gated_state() -> (
    Arc<SharedState>,
    nodedb::control::startup::ReadyGate,
    GatedOwners,
) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("gate_ilp_test.wal");
    let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, data_sides) = Dispatcher::new(1, 64);
    let mut shared = SharedState::new(dispatcher, wal).unwrap();

    let (sequencer, gate) = StartupSequencer::new();
    let gateway_gate =
        sequencer.register_gate(StartupPhase::GatewayEnable, "gateway-enable-ilp-test");

    Arc::get_mut(&mut shared)
        .expect("SharedState not yet cloned")
        .startup = Arc::clone(&gate);

    (
        shared,
        gateway_gate,
        GatedOwners {
            _sequencer: sequencer,
            _dir: dir,
            _data_sides: data_sides,
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ilp_accept_blocked_until_gateway_enable() {
    let (shared, gw_gate, _owners) = make_gated_state();
    let startup_gate = Arc::clone(&shared.startup);

    // Bind a real ILP TCP socket on an ephemeral port.
    let ilp_listener = IlpListener::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("ILP bind failed");
    let ilp_addr = ilp_listener.local_addr();

    // Spawn the listener — it blocks inside `await_phase(GatewayEnable)`.
    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_ilp = Arc::clone(&shared);
    let gate_for_listener = Arc::clone(&startup_gate);
    let bus_ilp = shutdown_bus.clone();
    tokio::spawn(async move {
        let _ = ilp_listener
            .run(
                shared_ilp,
                AuthMode::Trust,
                Arc::new(tokio::sync::Semaphore::new(128)),
                None,
                gate_for_listener,
                bus_ilp,
            )
            .await;
    });

    // Give the listener task time to reach `await_phase`.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Connect. The TCP handshake completes immediately (kernel accepts it into
    // the listen backlog). The ILP listener has not called accept() yet.
    let mut stream = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(ilp_addr))
        .await
        .expect("ILP connect timed out")
        .expect("ILP TCP connect failed");

    // Shut down the write side without sending a line. The handler must
    // still accept (after the gate fires), read to EOF, and close — that
    // round-trip is what the test measures. Sending a real ILP line would
    // route the request through `dispatch_to_data_plane` which awaits a
    // response from a Data Plane core that this minimal harness doesn't
    // run, deadlocking the connection close past the test timeout.
    stream.shutdown().await.ok();

    // Start timing. The server won't close its side until it accepts and
    // processes the connection, which is blocked until the gate fires.
    let start = Instant::now();

    // Fire the gate after 300 ms in a background task.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        gw_gate.fire();
    });

    // Read until EOF — blocks until the server closes its write side.
    let mut sink = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut sink))
        .await
        .expect("ILP read_to_end timed out");

    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(250),
        "ILP server-side close arrived too fast ({elapsed:?}): gate did not block accept"
    );
}
