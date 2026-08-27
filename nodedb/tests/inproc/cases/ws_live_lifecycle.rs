// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: WS `LIVE SELECT` subscriptions must be reclaimed on
//! disconnect.
//!
//! Originally `ws_rpc::handle_request` fired a detached `tokio::spawn` per
//! LIVE subscription with no `JoinSet`, no abort handle, and no shutdown-bus
//! observation. A client that opened N subscriptions and RST-d the socket
//! left N tasks alive, each pinning a broadcast receiver and a
//! `Subscription` — `active_subscriptions` never decremented.
//!
//! The fix is a per-connection `LiveSubscriptionSet` (a `JoinSet` that
//! aborts on drop). This file covers it three ways:
//!   * unit-level: dropping the set tears down every spawned forwarder;
//!   * unit-level: explicit `abort_all()` works the same way;
//!   * **end-to-end**: a real axum HTTP server + tokio-tungstenite client
//!     opens LIVE subs, drops the socket, and `active_subscriptions`
//!     returns to 0 — proving the WS handler itself, not just the
//!     underlying type, is wired correctly.

use std::sync::Arc;

use nodedb::control::change_stream::{ChangeStream, LiveSubscriptionSet, Subscription};

#[tokio::test]
async fn dropping_live_set_aborts_spawned_tasks_and_drops_subscriptions() {
    let cs = Arc::new(ChangeStream::new(64));
    assert_eq!(cs.subscriber_count(), 0);

    {
        let mut set = LiveSubscriptionSet::new();
        for _ in 0..10 {
            let sub: Subscription = cs.subscribe(Some("orders".into()), None);
            // The set takes ownership of the Subscription and the spawned
            // forwarder task — aborting the task on drop releases the
            // Subscription, which decrements `active_subscriptions`.
            set.spawn_forwarder(sub, |_event| { /* forward to client */ });
        }
        assert_eq!(
            cs.subscriber_count(),
            10,
            "10 LIVE subscriptions registered while the connection is alive"
        );
    } // <- WS connection drops here.

    // Give the aborted tasks a tick to unwind their Subscription drops.
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert_eq!(
        cs.subscriber_count(),
        0,
        "dropping the per-connection LiveSubscriptionSet must abort every \
         spawned forwarder task so the Subscription's Drop runs and \
         active_subscriptions returns to 0"
    );
}

#[tokio::test]
async fn spawned_forwarder_observes_shutdown_abort() {
    // Secondary guard: even when the connection is still alive, an explicit
    // abort on the set must stop every forwarder — used during server
    // shutdown to drain all LIVE subscriptions cluster-wide.
    let cs = Arc::new(ChangeStream::new(64));
    let mut set = LiveSubscriptionSet::new();
    for _ in 0..5 {
        let sub: Subscription = cs.subscribe(None, None);
        set.spawn_forwarder(sub, |_| {});
    }
    assert_eq!(cs.subscriber_count(), 5);

    set.abort_all();
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert_eq!(
        cs.subscriber_count(),
        0,
        "abort_all must tear down every forwarder so no Subscription outlives shutdown"
    );
}

// ─── End-to-end: real axum WS server + tungstenite client ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_disconnect_drops_active_subscriptions() {
    use futures::{SinkExt, StreamExt};
    use nodedb::bridge::dispatch::Dispatcher;
    use nodedb::config::auth::AuthMode;
    use nodedb::control::state::SharedState;
    use nodedb::wal::WalManager;
    use tokio_tungstenite::tungstenite::Message;

    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("ws.wal")).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).unwrap();
    shared
        .credentials
        .bootstrap_trust_superuser("nodedb")
        .expect("bootstrap trust superuser");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_http = Arc::clone(&shared);
    let server_handle = tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_http,
            AuthMode::Trust,
            None,
            bus,
        )
        .await
        .ok();
    });

    // Give the server a tick to start accepting.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(shared.change_stream.subscriber_count(), 0);

    // Open a WS connection, register 5 LIVE subscriptions.
    let url = format!("ws://{local_addr}/v1/ws");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    for i in 0..5 {
        let sql = format!(r#"LIVE SELECT * FROM orders_{i}"#);
        let req = serde_json::json!({
            "id": i,
            "method": "live",
            "params": {"sql": sql},
        })
        .to_string();
        ws.send(Message::Text(req.into())).await.unwrap();
        // Read the ack so we know the server has registered the subscription.
        let _ack = ws.next().await.unwrap().unwrap();
    }

    // All 5 subscriptions registered on the server.
    assert_eq!(
        shared.change_stream.subscriber_count(),
        5,
        "server must have 5 active subscriptions while the WS client is connected"
    );

    // Abruptly drop the client. The server's `handle_ws_connection` drops
    // `LiveSubscriptionSet`, which aborts the 5 forwarder tasks, dropping
    // each `Subscription` — counter must return to 0.
    drop(ws);

    // Give the server a moment to observe the close and drop the set.
    for _ in 0..20 {
        if shared.change_stream.subscriber_count() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    assert_eq!(
        shared.change_stream.subscriber_count(),
        0,
        "disconnect must abort every LIVE forwarder and return active_subscriptions to 0 — \
         detached spawn would leak 5 subscriptions here"
    );

    server_handle.abort();
}
