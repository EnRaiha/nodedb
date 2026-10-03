// SPDX-License-Identifier: BUSL-1.1

//! Steps the HTTP and WebSocket change-feed cases share: a one-node server
//! with an HTTP listener, `orders` writes, and a wait for their change
//! events.

use std::sync::Arc;
use std::time::Duration;

use nodedb::config::auth::AuthMode;
use nodedb::control::change_stream::ReplayStart;
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::identity::Role;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, TenantId};

/// A one-node cluster, with an HTTP listener in the auth mode under test.
/// Writes go through the node's pgwire client, so each one's change event
/// reaches the change stream through its replicated entry.
pub(super) struct TestServer {
    pub(super) local_addr: std::net::SocketAddr,
    pub(super) node: nodedb_test_support::pgwire_harness::TestServer,
    _server: tokio::task::JoinHandle<()>,
}

/// Start a node and serve its HTTP routes on an ephemeral port. The listener
/// is bound before this returns, so a client connects at once.
pub(super) async fn start_http(auth_mode: AuthMode) -> TestServer {
    let node = nodedb_test_support::pgwire_harness::TestServer::start().await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let local_addr = listener.local_addr().expect("local addr");

    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&node.shared.shutdown));
    let shared_http = Arc::clone(&node.shared);
    let handle = tokio::spawn(async move {
        nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_http,
            auth_mode,
            None,
            bus,
        )
        .await
        .ok();
    });

    TestServer {
        local_addr,
        node,
        _server: handle,
    }
}

/// Create the `orders` collection in the client's current database and
/// tenant.
pub(super) async fn create_orders(srv: &TestServer) {
    srv.node
        .exec("CREATE COLLECTION orders WITH (engine='document_schemaless')")
        .await
        .expect("create orders");
}

/// Insert one `orders` row per id, each its own replicated write.
pub(super) async fn insert_orders(srv: &TestServer, ids: &[&str]) {
    for id in ids {
        srv.node
            .exec(&format!("INSERT INTO orders {{ id: '{id}' }}"))
            .await
            .unwrap_or_else(|error| panic!("insert order {id}: {error}"));
    }
}

/// Wait until `tenant`'s `orders` feed in `database_id` holds `count`
/// events: the apply loop publishes a write's events once it settles the
/// write's entry.
pub(super) async fn await_orders_published(
    srv: &TestServer,
    tenant: TenantId,
    database_id: DatabaseId,
    count: usize,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let published = srv
            .node
            .shared
            .change_stream
            .query_changes_in_database(
                tenant,
                database_id,
                Some("orders"),
                ReplayStart::Timestamp(0),
                usize::MAX,
            )
            .expect("replay the orders feed")
            .events
            .len();
        if published >= count {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{published} of {count} order events were published"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Insert every id into tenant 1's default `orders` in one transaction, and
/// wait until the feed holds `total` events. The commit is one replicated
/// entry, so its events share one commit instant and one LSN.
pub(super) async fn commit_orders(srv: &TestServer, ids: &[&str], total: usize) {
    srv.node.exec("BEGIN").await.expect("begin");
    insert_orders(srv, ids).await;
    srv.node.exec("COMMIT").await.expect("commit");
    await_orders_published(srv, TenantId::new(1), DatabaseId::DEFAULT, total).await;
}

/// Insert one `orders` row per id into tenant 1's default database, and wait
/// until the feed holds `total` events.
pub(super) async fn publish_orders(srv: &TestServer, ids: &[&str], total: usize) {
    insert_orders(srv, ids).await;
    await_orders_published(srv, TenantId::new(1), DatabaseId::DEFAULT, total).await;
}

/// Create a database-scoped service account for tenant 1 with `roles` and an
/// API key for it, and return the key.
pub(super) fn create_api_key(shared: &SharedState, username: &str, roles: Vec<Role>) -> String {
    let user_id = shared
        .credentials
        .create_service_account(username, TenantId::new(1), roles, vec![DatabaseId::DEFAULT])
        .expect("create database-scoped service account");
    shared
        .api_keys
        .create_key(
            CreateKeyParams {
                username,
                user_id,
                tenant_id: TenantId::new(1),
                expires_secs: 0,
                scope: vec![],
                accessible_databases: vec![DatabaseId::DEFAULT],
            },
            Some(shared.credentials.catalog()),
        )
        .expect("create API key")
}
