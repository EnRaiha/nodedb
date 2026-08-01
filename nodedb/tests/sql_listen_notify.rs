// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for LISTEN / NOTIFY / UNLISTEN.
//!
//! Each test spins up a full NodeDB server via the pgwire harness and
//! exercises the PostgreSQL-compatible notification path end-to-end.
//! Two pgwire connections are used to cover cross-session delivery.

mod common;

use std::time::Duration;

use tokio_postgres::{AsyncMessage, NoTls};

use common::pgwire_harness::TestServer;

// ── Helper: listener connection ──────────────────────────────────────────────

/// A listener connection that retains the `Connection` future so we can poll
/// `AsyncMessage::Notification` messages directly.
struct ListenerConn {
    client: tokio_postgres::Client,
    /// Receives `AsyncMessage` items (including `Notification`).
    msg_rx: tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>,
    _task: tokio::task::JoinHandle<()>,
}

impl ListenerConn {
    async fn connect(port: u16) -> Self {
        let conn_str = format!("host=127.0.0.1 port={port} user=nodedb dbname=nodedb");
        let (client, mut connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .expect("listener connect");

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                match futures::future::poll_fn(|cx| connection.poll_message(cx)).await {
                    Some(Ok(AsyncMessage::Notification(n))) => {
                        let _ = tx.send(n);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        });

        Self {
            client,
            msg_rx: rx,
            _task: task,
        }
    }

    async fn exec(&self, sql: &str) {
        self.client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("exec({sql}) failed: {e}"));
    }

    /// Collect notifications arriving within `timeout`.
    async fn collect(&mut self, timeout: Duration) -> Vec<tokio_postgres::Notification> {
        // Ping to ensure the server has flushed the wire.
        let _ = self.client.simple_query("SELECT 1").await;
        let deadline = tokio::time::Instant::now() + timeout;
        let mut out = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, self.msg_rx.recv()).await {
                Ok(Some(n)) => out.push(n),
                _ => break,
            }
        }
        out
    }

    /// Try to receive at most one notification with a short timeout.
    async fn recv_one(&mut self) -> Option<tokio_postgres::Notification> {
        let _ = self.client.simple_query("SELECT 1").await;
        tokio::time::timeout(Duration::from_millis(200), self.msg_rx.recv())
            .await
            .ok()
            .flatten()
    }

    /// Assert no notification arrives within a short window.
    async fn assert_none(&mut self) {
        let _ = self.client.simple_query("SELECT 1").await;
        let result = tokio::time::timeout(Duration::from_millis(100), self.msg_rx.recv()).await;
        assert!(
            result.is_err() || matches!(result, Ok(None)),
            "expected no notification but received one"
        );
    }
}

/// Simple second client for NOTIFY only (no need to receive).
async fn notifier(port: u16) -> tokio_postgres::Client {
    let conn_str = format!("host=127.0.0.1 port={port} user=nodedb dbname=nodedb");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("notifier connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Basic round-trip: one session LISTENs, another NOTIFYs, delivery confirmed.
#[tokio::test]
async fn test_basic_listen_notify_roundtrip() {
    let server = TestServer::start().await;
    let port = server.pg_port;

    let mut listener = ListenerConn::connect(port).await;
    listener.exec("LISTEN orders").await;

    let sender = notifier(port).await;
    sender
        .simple_query("NOTIFY orders, 'order-created'")
        .await
        .unwrap();

    let n = listener.recv_one().await.expect("notification expected");
    assert_eq!(n.channel(), "orders");
    assert_eq!(n.payload(), "order-created");

    server.graceful_shutdown().await;
}

/// NOTIFY with no payload delivers an empty string payload.
#[tokio::test]
async fn test_notify_no_payload() {
    let server = TestServer::start().await;
    let port = server.pg_port;

    let mut listener = ListenerConn::connect(port).await;
    listener.exec("LISTEN events").await;

    let sender = notifier(port).await;
    sender.simple_query("NOTIFY events").await.unwrap();

    let n = listener.recv_one().await.expect("notification expected");
    assert_eq!(n.channel(), "events");
    assert_eq!(n.payload(), "");

    server.graceful_shutdown().await;
}

/// UNLISTEN stops delivery for the named channel.
#[tokio::test]
async fn test_unlisten_stops_delivery() {
    let server = TestServer::start().await;
    let port = server.pg_port;

    let mut listener = ListenerConn::connect(port).await;
    listener.exec("LISTEN orders").await;
    listener.exec("UNLISTEN orders").await;

    let sender = notifier(port).await;
    sender
        .simple_query("NOTIFY orders, 'late-delivery'")
        .await
        .unwrap();

    listener.assert_none().await;

    server.graceful_shutdown().await;
}

/// UNLISTEN * removes all subscriptions.
#[tokio::test]
async fn test_unlisten_star() {
    let server = TestServer::start().await;
    let port = server.pg_port;

    let mut listener = ListenerConn::connect(port).await;
    listener.exec("LISTEN ch1").await;
    listener.exec("LISTEN ch2").await;
    listener.exec("UNLISTEN *").await;

    let sender = notifier(port).await;
    sender.simple_query("NOTIFY ch1, 'a'").await.unwrap();
    sender.simple_query("NOTIFY ch2, 'b'").await.unwrap();

    listener.assert_none().await;

    server.graceful_shutdown().await;
}

/// Multi-channel: two channels each deliver correctly.
#[tokio::test]
async fn test_multi_channel() {
    let server = TestServer::start().await;
    let port = server.pg_port;

    let mut listener = ListenerConn::connect(port).await;
    listener.exec("LISTEN ch_a").await;
    listener.exec("LISTEN ch_b").await;

    let sender = notifier(port).await;
    sender.simple_query("NOTIFY ch_a, 'msg-a'").await.unwrap();
    sender.simple_query("NOTIFY ch_b, 'msg-b'").await.unwrap();

    let received = listener.collect(Duration::from_millis(300)).await;
    let channels: Vec<(&str, &str)> = received
        .iter()
        .map(|n| (n.channel(), n.payload()))
        .collect();

    assert!(
        channels.iter().any(|&(c, p)| c == "ch_a" && p == "msg-a"),
        "expected ch_a/msg-a in {channels:?}"
    );
    assert!(
        channels.iter().any(|&(c, p)| c == "ch_b" && p == "msg-b"),
        "expected ch_b/msg-b in {channels:?}"
    );

    server.graceful_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_listen_notify_isolated_across_databases() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE notify_other_database")
        .await
        .expect("create second notification database");

    let mut default_listener = ListenerConn::connect(server.pg_port).await;
    let mut other_listener = ListenerConn::connect(server.pg_port).await;
    default_listener.exec("LISTEN database_events").await;
    // This pre-switch handle must be unregistered by USE DATABASE; otherwise
    // the connection would retain authority to receive default-DB messages.
    other_listener.exec("LISTEN database_events").await;
    other_listener
        .exec("USE DATABASE notify_other_database")
        .await;
    other_listener.exec("LISTEN database_events").await;

    server
        .exec("NOTIFY database_events, 'default-only'")
        .await
        .expect("notify default database");
    let default_notification = default_listener
        .recv_one()
        .await
        .expect("default listener receives default notification");
    assert_eq!(default_notification.payload(), "default-only");
    other_listener.assert_none().await;

    other_listener
        .exec("NOTIFY database_events, 'other-only'")
        .await;
    let other_notification = other_listener
        .recv_one()
        .await
        .expect("other listener receives other-database notification");
    assert_eq!(other_notification.payload(), "other-only");
    default_listener.assert_none().await;

    server.graceful_shutdown().await;
}

/// Tenant isolation at the bus level: NOTIFY from tenant 1 does not arrive at
/// a session subscribed under tenant 2.
#[tokio::test]
async fn test_tenant_isolation_bus_level() {
    use nodedb::control::notify_bus::NotifyBus;
    use nodedb::types::{DatabaseId, TenantId};

    let bus = NotifyBus::new(64);
    let t1 = TenantId::new(1);
    let t2 = TenantId::new(2);

    let (_, mut rx1) = bus.listen(DatabaseId::DEFAULT, t1, "alerts");
    let (_, mut rx2) = bus.listen(DatabaseId::DEFAULT, t2, "alerts");

    bus.notify(DatabaseId::DEFAULT, t1, "alerts", "for-t1");

    assert!(rx1.try_recv().is_ok(), "t1 should receive");
    assert!(rx2.try_recv().is_err(), "t2 must not receive t1's notify");
}

/// Queue-full drop: when a session's queue is at capacity, the dropped counter
/// is incremented rather than blocking the sender.
#[tokio::test]
async fn test_queue_full_drop_metric() {
    use nodedb::control::notify_bus::NotifyBus;
    use nodedb::types::{DatabaseId, TenantId};

    let bus = NotifyBus::new(2); // tiny cap
    let t = TenantId::new(1);
    let (_, _rx) = bus.listen(DatabaseId::DEFAULT, t, "flood"); // deliberately not draining

    bus.notify(DatabaseId::DEFAULT, t, "flood", "a");
    bus.notify(DatabaseId::DEFAULT, t, "flood", "b"); // fills queue (cap=2)
    bus.notify(DatabaseId::DEFAULT, t, "flood", "c"); // queue full → drop
    bus.notify(DatabaseId::DEFAULT, t, "flood", "d"); // queue full → drop

    assert!(
        bus.total_dropped() >= 2,
        "expected ≥2 dropped, got {}",
        bus.total_dropped()
    );
}

/// Session disconnect cleanup: subsequent NOTIFYs after a listener disconnects
/// do not panic and complete normally.
#[tokio::test]
async fn test_session_disconnect_cleanup() {
    let server = TestServer::start().await;
    let port = server.pg_port;

    let listener = ListenerConn::connect(port).await;
    listener.exec("LISTEN disconnect_ch").await;

    // Drop the listener — simulates session disconnect.
    drop(listener);
    tokio::time::sleep(Duration::from_millis(30)).await;

    // NOTIFY must succeed without panicking.
    let sender = notifier(port).await;
    let result = sender
        .simple_query("NOTIFY disconnect_ch, 'after-disconnect'")
        .await;
    assert!(
        result.is_ok(),
        "NOTIFY after listener disconnect must succeed"
    );

    server.graceful_shutdown().await;
}

/// NOTIFY inside a transaction is buffered until COMMIT.
#[tokio::test]
async fn test_notify_buffered_on_transaction_commit() {
    let server = TestServer::start().await;
    let port = server.pg_port;

    let mut listener = ListenerConn::connect(port).await;
    listener.exec("LISTEN tx_ch").await;

    // NOTIFY inside a transaction.
    server.client.simple_query("BEGIN").await.unwrap();
    server
        .client
        .simple_query("NOTIFY tx_ch, 'committed-payload'")
        .await
        .unwrap();

    // Not yet committed — listener should not see it.
    listener.assert_none().await;

    // Commit fires the notification.
    server.client.simple_query("COMMIT").await.unwrap();
    let n = listener
        .recv_one()
        .await
        .expect("notification expected after COMMIT");
    assert_eq!(n.channel(), "tx_ch");
    assert_eq!(n.payload(), "committed-payload");

    server.graceful_shutdown().await;
}

/// NOTIFY inside a transaction is dropped on ROLLBACK.
#[tokio::test]
async fn test_notify_dropped_on_rollback() {
    let server = TestServer::start().await;
    let port = server.pg_port;

    let mut listener = ListenerConn::connect(port).await;
    listener.exec("LISTEN rollback_ch").await;

    server.client.simple_query("BEGIN").await.unwrap();
    server
        .client
        .simple_query("NOTIFY rollback_ch, 'dropped-payload'")
        .await
        .unwrap();
    server.client.simple_query("ROLLBACK").await.unwrap();

    listener.assert_none().await;

    server.graceful_shutdown().await;
}
