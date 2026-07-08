// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::Session;
use crate::bridge::dispatch::Dispatcher;
use crate::control::state::SharedState;
use crate::data::executor::core_loop::CoreLoop;
use crate::wal::WalManager;

/// End-to-end test: client -> session -> dispatcher -> core_loop -> response -> client.
#[tokio::test]
async fn full_request_response_roundtrip() {
    // Set up infrastructure.
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

    let (dispatcher, data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).unwrap();

    // Start a Data Plane core in a background thread.
    let data_side = data_sides.into_iter().next().unwrap();
    let core_dir = dir.path().to_path_buf();
    let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
    let core_handle = tokio::task::spawn_blocking(move || {
        let mut core = CoreLoop::open(
            0,
            data_side.request_rx,
            data_side.response_tx,
            &core_dir,
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .unwrap();
        while matches!(
            core_stop_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ) {
            core.tick();
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    // Start response poller.
    let shared_poller = Arc::clone(&shared);
    let (poller_stop_tx, mut poller_stop_rx) = tokio::sync::watch::channel(false);
    let poller_handle = tokio::spawn(async move {
        loop {
            shared_poller.poll_and_route_responses();
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                _ = poller_stop_rx.changed() => break,
            }
        }
    });

    // Bind a test listener.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn session handler.
    let shared_session = Arc::clone(&shared);
    let session_handle = tokio::spawn(async move {
        let (stream, peer_addr) = listener.accept().await.unwrap();
        let session = Session::new(
            stream,
            peer_addr,
            shared_session,
            crate::config::auth::AuthMode::Trust,
        );
        session.run().await
    });

    // Connect as a client and send a request.
    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

    let request_json =
        r#"{"op":"point_get","tenant_id":1,"collection":"users","document_id":"u1"}"#;
    let len = (request_json.len() as u32).to_be_bytes();
    client.write_all(&len).await.unwrap();
    client.write_all(request_json.as_bytes()).await.unwrap();

    // Read response.
    let mut resp_len_buf = [0u8; 4];
    client.read_exact(&mut resp_len_buf).await.unwrap();
    let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
    let mut resp_buf = vec![0u8; resp_len];
    client.read_exact(&mut resp_buf).await.unwrap();

    let resp_str = String::from_utf8(resp_buf).unwrap();
    // Document doesn't exist, so we get NotFound — but the roundtrip works.
    assert!(
        resp_str.contains(r#""status""#),
        "expected a valid response, got: {resp_str}"
    );
    assert!(
        resp_str.contains(r#""request_id""#),
        "expected request_id in response, got: {resp_str}"
    );

    // Clean up: signal background tasks to stop.
    drop(client);
    let _ = session_handle.await;
    let _ = poller_stop_tx.send(true);
    let _ = poller_handle.await;
    let _ = core_stop_tx.send(());
    let _ = core_handle.await;
}
