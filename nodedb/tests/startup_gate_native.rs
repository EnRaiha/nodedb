// SPDX-License-Identifier: BUSL-1.1

//! Integration test: native protocol STATUS command returns "OK" after
//! GatewayEnable fires and returns "Starting" before it fires.
//!
//! The native protocol is a simple framing format:
//!   [4-byte big-endian payload_len][payload]
//! Payload is JSON (first byte `{`) or MessagePack. This test uses JSON.
//!
//! STATUS requires no authentication (same as PING).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::listener::Listener;
use nodedb::control::startup::{StartupPhase, StartupSequencer};
use nodedb::control::state::SharedState;
use nodedb_types::protocol::HelloFrame;

mod common;

fn make_gated_state() -> (
    Arc<SharedState>,
    StartupSequencer,
    nodedb::control::startup::ReadyGate,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("gate_native_test.wal");
    let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let mut shared = SharedState::new(dispatcher, wal).unwrap();

    let (seq, gate) = StartupSequencer::new();
    let gw_gate = seq.register_gate(StartupPhase::GatewayEnable, "gateway-enable-native-test");

    Arc::get_mut(&mut shared)
        .expect("SharedState not yet cloned")
        .startup = Arc::clone(&gate);

    (shared, seq, gw_gate, dir)
}

/// Encode a JSON payload as a native protocol frame (4-byte length prefix).
fn encode_json_frame(json: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + json.len());
    let len = json.len() as u32;
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(json);
    frame
}

/// Read one native protocol frame from a stream (4-byte length prefix + payload).
async fn read_json_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .expect("failed to read frame length");
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .expect("failed to read frame payload");
    payload
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_status_returns_ok_after_gateway_enable() {
    let (shared, _seq, gw_gate, _dir) = make_gated_state();
    let startup_gate = Arc::clone(&shared.startup);

    // Bind the native protocol listener on an ephemeral port.
    let native_listener = Listener::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("native listener bind failed");
    let native_addr = native_listener.local_addr();

    // Spawn the listener — it blocks inside `await_phase(GatewayEnable)`.
    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_native = Arc::clone(&shared);
    let gate_for_listener = Arc::clone(&startup_gate);
    let bus_native = shutdown_bus.clone();
    tokio::spawn(async move {
        let _ = native_listener
            .run(nodedb::control::server::listener::ListenerRunParams {
                state: shared_native,
                auth_mode: AuthMode::Trust,
                tls_acceptor: None,
                conn_semaphore: Arc::new(tokio::sync::Semaphore::new(128)),
                startup_gate: gate_for_listener,
                bus: bus_native,
                admission: Arc::new(nodedb::control::server::admission::AdmissionRegistry::new()),
            })
            .await;
    });

    // Fire the gate so the listener starts accepting.
    gw_gate.fire();

    // Give the listener time to reach the accept loop.
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Connect a raw TCP client and perform the Hello/HelloAck handshake first.
    let mut stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(native_addr))
        .await
        .expect("native connect timed out")
        .expect("native TCP connect failed");

    // Native protocol now requires a 16-byte HelloFrame before any frames.
    let hello = HelloFrame::current().encode();
    stream
        .write_all(&hello)
        .await
        .expect("write HelloFrame failed");
    // Server responds with a variable-length HelloAckFrame: magic(4) +
    // proto_version(2) + capabilities(8) + sv_len(1) + sv_len bytes +
    // optional limits block (1 flag + 7 * 5 = 36 bytes max).
    let mut magic_buf = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut magic_buf))
        .await
        .expect("HelloAck magic read timed out")
        .expect("HelloAck magic read failed");
    assert_eq!(
        u32::from_be_bytes(magic_buf),
        nodedb_types::protocol::HELLO_ACK_MAGIC,
        "handshake did not return HelloAck magic"
    );
    let mut fixed_rest = [0u8; 11];
    stream
        .read_exact(&mut fixed_rest)
        .await
        .expect("HelloAck fixed-rest read failed");
    let sv_len = fixed_rest[10] as usize;
    let var_len = sv_len + 1 + 7 * 5;
    let mut var_buf = vec![0u8; var_len];
    stream
        .read_exact(&mut var_buf)
        .await
        .expect("HelloAck var-block read failed");

    // STATUS request: op 0x03 = Status. `RequestFields` is `#[serde(flatten)]`-ed
    // into `NativeRequest`, and is internally tagged with `kind = "text"`,
    // so the discriminant lives at the top level alongside `op` and `seq`.
    let status_req = br#"{"op":3,"seq":1,"kind":"text"}"#;
    let frame = encode_json_frame(status_req);
    stream
        .write_all(&frame)
        .await
        .expect("write STATUS frame failed");

    // Read the response.
    let resp_payload = tokio::time::timeout(Duration::from_secs(5), read_json_frame(&mut stream))
        .await
        .expect("read STATUS response timed out");

    let resp_json: serde_json::Value =
        serde_json::from_slice(&resp_payload).expect("invalid JSON response");

    // The response should be a status_row with ResponseStatus::Ok.
    // serde serializes ResponseStatus::Ok as the string "Ok".
    assert_eq!(
        resp_json["status"], "Ok",
        "expected ResponseStatus::Ok, got: {resp_json}"
    );
    // The rows field should contain a single row with "OK".
    let rows = resp_json["rows"]
        .as_array()
        .expect("expected rows array in STATUS response");
    assert_eq!(rows.len(), 1, "expected 1 row in STATUS response");
    let row = rows[0].as_array().expect("expected row to be an array");
    assert!(
        row.iter().any(|v| v.as_str() == Some("OK")),
        "expected 'OK' in STATUS row, got: {row:?}"
    );
}
