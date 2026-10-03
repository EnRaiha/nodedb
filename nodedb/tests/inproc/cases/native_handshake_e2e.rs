// SPDX-License-Identifier: BUSL-1.1

//! End-to-end native protocol handshake tests.
//!
//! Boots a real NodeDB server with the native TCP listener and exercises the
//! Hello/HelloAck exchange with a real client connection.

use std::sync::Arc;
use std::time::Duration;

use nodedb::config::auth::AuthMode;
use nodedb::control::server::listener::Listener;
use nodedb_test_support::booted_state::{BootOptions, BootedState};

/// A one-node cluster with the native protocol listener running.
struct NativeTestServer {
    addr: std::net::SocketAddr,
    shared: BootedState,
    shutdown_bus: nodedb::control::shutdown::ShutdownBus,
    _listener_handle: tokio::task::JoinHandle<()>,
}

impl NativeTestServer {
    async fn start() -> Self {
        let shared = BootedState::boot(BootOptions::default());
        shared
            .credentials
            .bootstrap_trust_superuser("nodedb")
            .expect("bootstrap trust superuser");
        let (shutdown_bus, _) =
            nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));

        let listener = Listener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr();

        let shared_listener = Arc::clone(&*shared);
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_listener = shutdown_bus.clone();
        let _listener_handle = tokio::spawn(async move {
            listener
                .run(nodedb::control::server::listener::ListenerRunParams {
                    state: shared_listener,
                    auth_mode: AuthMode::Trust,
                    tls_acceptor: None,
                    conn_semaphore: Arc::new(tokio::sync::Semaphore::new(128)),
                    startup_gate: test_startup_gate,
                    bus: bus_listener,
                    admission: Arc::new(
                        nodedb::control::server::admission::AdmissionRegistry::new(),
                    ),
                })
                .await
                .unwrap();
        });

        // Give the listener a moment to start accepting.
        tokio::time::sleep(Duration::from_millis(50)).await;

        Self {
            addr,
            shared,
            shutdown_bus,
            _listener_handle,
        }
    }

    /// Stop the listener, then the node. Dropping `shared` stops the node.
    async fn shutdown(self) {
        self.shutdown_bus.initiate();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A connect helper that performs only the handshake, without authentication,
/// using raw TCP + the native protocol handshake framing.
async fn connect_and_handshake(
    addr: std::net::SocketAddr,
) -> Result<u16, Box<dyn std::error::Error>> {
    use nodedb_types::protocol::handshake::{
        HelloAckFrame, HelloFrame, PROTO_VERSION_MAX, PROTO_VERSION_MIN,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(addr).await?;

    // Send HelloFrame.
    let hello = HelloFrame::current();
    stream.write_all(&hello.encode()).await?;
    stream.flush().await?;

    // Read 4-byte magic.
    let mut magic_buf = [0u8; 4];
    stream.read_exact(&mut magic_buf).await?;
    let magic = u32::from_be_bytes(magic_buf);

    if magic == nodedb_types::protocol::HELLO_ERROR_MAGIC_U32 {
        let mut header = [0u8; 2];
        stream.read_exact(&mut header).await?;
        let msg_len = header[1] as usize;
        let mut msg = vec![0u8; msg_len];
        stream.read_exact(&mut msg).await?;
        return Err(format!(
            "server rejected handshake (code {}): {}",
            header[0],
            String::from_utf8_lossy(&msg)
        )
        .into());
    }

    assert_eq!(
        magic,
        nodedb_types::protocol::HELLO_ACK_MAGIC,
        "expected HelloAck magic"
    );

    // Read fixed rest: proto_version(2) + capabilities(8) + sv_len(1).
    let mut fixed_rest = [0u8; 11];
    stream.read_exact(&mut fixed_rest).await?;
    let sv_len = fixed_rest[10] as usize;
    let var_len = sv_len + 1 + 7 * 5;
    let mut var_buf = vec![0u8; var_len];
    stream.read_exact(&mut var_buf).await?;

    let mut ack_buf = Vec::with_capacity(4 + 11 + var_len);
    ack_buf.extend_from_slice(&magic_buf);
    ack_buf.extend_from_slice(&fixed_rest);
    ack_buf.extend_from_slice(&var_buf);

    let ack = HelloAckFrame::decode(&ack_buf).ok_or("failed to decode HelloAckFrame")?;

    assert!(
        ack.proto_version >= PROTO_VERSION_MIN && ack.proto_version <= PROTO_VERSION_MAX,
        "proto_version {} out of range [{PROTO_VERSION_MIN}, {PROTO_VERSION_MAX}]",
        ack.proto_version
    );
    assert!(
        ack.server_version.contains("NodeDB"),
        "server_version '{}' does not contain 'NodeDB'",
        ack.server_version
    );

    Ok(ack.proto_version)
}

#[tokio::test]
async fn native_handshake_connect_succeeds() {
    let server = NativeTestServer::start().await;
    let result = connect_and_handshake(server.addr).await;
    server.shutdown().await;

    assert!(result.is_ok(), "handshake failed: {result:?}");
    assert_eq!(result.unwrap(), 1, "negotiated proto_version should be 1");
}

#[tokio::test]
async fn native_trust_auth_uses_configured_durable_identity() {
    let server = NativeTestServer::start().await;
    let body = serde_json::json!({"method": "trust"});

    let (identity, warning) = nodedb::control::server::session_auth::authenticate(
        &server.shared,
        &AuthMode::Trust,
        &body,
        "127.0.0.1:1",
    )
    .await
    .expect("configured trust authentication");

    assert_eq!(identity.username, "nodedb");
    assert_ne!(identity.user_id, 0, "trust identity must be durable");
    assert!(identity.is_superuser);
    assert!(warning.is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn native_trust_auth_rejects_unknown_explicit_username() {
    let server = NativeTestServer::start().await;
    let body = serde_json::json!({"method": "trust", "username": "unknown"});

    let result = nodedb::control::server::session_auth::authenticate(
        &server.shared,
        &AuthMode::Trust,
        &body,
        "127.0.0.1:1",
    )
    .await;

    assert!(
        matches!(result, Err(nodedb::Error::RejectedAuthz { .. })),
        "unknown trust username must fail closed: {result:?}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn native_handshake_version_mismatch_rejected() {
    use nodedb_types::protocol::handshake::PROTO_VERSION_MAX;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = NativeTestServer::start().await;

    // Send a HelloFrame with a version range that can't possibly match the server.
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let mut stream = TcpStream::connect(server.addr).await?;

        // Build a raw HelloFrame bytes with out-of-range versions.
        let proto_min = PROTO_VERSION_MAX.saturating_add(1);
        let proto_max = PROTO_VERSION_MAX.saturating_add(5);
        let caps: u64 = 0;
        let mut buf = [0u8; 16];
        buf[0..4].copy_from_slice(&nodedb_types::protocol::HELLO_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&proto_min.to_be_bytes());
        buf[6..8].copy_from_slice(&proto_max.to_be_bytes());
        buf[8..16].copy_from_slice(&caps.to_be_bytes());
        stream.write_all(&buf).await?;
        stream.flush().await?;

        // Expect a HelloErrorFrame.
        let mut magic_buf = [0u8; 4];
        stream.read_exact(&mut magic_buf).await?;
        let magic = u32::from_be_bytes(magic_buf);

        assert_eq!(
            magic,
            nodedb_types::protocol::HELLO_ERROR_MAGIC_U32,
            "expected HelloErrorFrame magic for out-of-range version"
        );

        // Read error code byte.
        let mut code_buf = [0u8; 1];
        stream.read_exact(&mut code_buf).await?;
        assert_eq!(code_buf[0], 1, "expected VersionMismatch error code (1)");

        Ok(())
    }
    .await;

    server.shutdown().await;
    assert!(result.is_ok(), "test setup failed: {result:?}");
}
