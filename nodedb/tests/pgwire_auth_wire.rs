// SPDX-License-Identifier: BUSL-1.1

//! End-to-end TCP roundtrip: real pgwire connection executes DDL and
//! observes both state mutation and SHOW SESSION results.

mod common;

use std::sync::Arc;

use common::{pgwire_auth_helpers::make_state, pgwire_harness::TestServer};
use nodedb::config::auth::AuthMode;
use nodedb::control::security::identity::Role;
use nodedb::types::TenantId;
use nodedb::{ServerConfig, bootstrap};
use nodedb_types::DatabaseId;
use tokio_postgres::SimpleQueryMessage;

async fn connect_empty_store_trust(
    server: &TestServer,
    username: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(server.pg_port)
        .user(username)
        .dbname("default");
    let (client, connection) = config
        .connect(tokio_postgres::NoTls)
        .await
        .expect("trust mode must accept the stored configured identity");
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, connection_handle)
}

fn trust_config() -> ServerConfig {
    let mut config = ServerConfig::default();
    config.auth.mode = AuthMode::Trust;
    config.auth.superuser_name = "nodedb".to_owned();
    config
}

fn bootstrap_trust_superuser(server: &TestServer) {
    bootstrap::credentials::bootstrap_superuser(&server.shared, &trust_config())
        .expect("trust-mode superuser bootstrap must succeed");
}

async fn assert_configured_trust_superuser_survives(sql: &str) {
    let server = TestServer::start_empty_store_trust().await;
    bootstrap_trust_superuser(&server);
    let username = "nodedb";
    let (client, connection_handle) = connect_empty_store_trust(&server, username).await;

    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("trusted superuser DDL failed for {sql}: {error}"));

    let (reconnected, reconnect_handle) = connect_empty_store_trust(&server, username).await;
    let messages = reconnected
        .simple_query("SHOW SESSION")
        .await
        .expect("credential mutation must not revoke the configured trust identity");
    let session_username = messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    });
    assert_eq!(session_username, Some(username.to_owned()));

    drop(reconnected);
    reconnect_handle.abort();
    let _ = reconnect_handle.await;
    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    server.graceful_shutdown().await;
}

#[tokio::test]
async fn pgwire_ddl_roundtrip() {
    let state = make_state();
    bootstrap::credentials::bootstrap_superuser(&state, &trust_config())
        .expect("materialize configured trust superuser");

    let pg_listener =
        nodedb::control::server::pgwire::listener::PgListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let port = pg_listener.local_addr().port();

    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&state.shutdown));
    let shared_pg = Arc::clone(&state);
    let test_startup_gate = Arc::clone(&state.startup);
    let bus_pg = shutdown_bus.clone();
    let listener_handle = tokio::spawn(async move {
        pg_listener
            .run(
                shared_pg,
                nodedb::config::auth::AuthMode::Trust,
                None,
                Arc::new(tokio::sync::Semaphore::new(128)),
                test_startup_gate,
                bus_pg,
            )
            .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let conn_str = format!("host=127.0.0.1 port={port} user=nodedb dbname=nodedb");
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .simple_query("CREATE USER wire_test WITH PASSWORD 'pass'")
        .await
        .unwrap();

    let msgs = client.simple_query("SHOW SESSION").await.unwrap();
    let username = msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
        _ => None,
    });
    assert_eq!(username, Some("nodedb".to_string()));

    assert!(state.credentials.get_user("wire_test").is_some());

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    let _shutdown = shutdown_bus.initiate();
    listener_handle
        .await
        .expect("pgwire listener task must not panic")
        .expect("pgwire listener must shut down cleanly");
}

#[tokio::test]
async fn trust_bootstrap_materializes_configured_superuser() {
    let server = TestServer::start_empty_store_trust().await;
    bootstrap_trust_superuser(&server);

    let user = server
        .shared
        .credentials
        .get_user("nodedb")
        .expect("configured trust superuser must have a durable catalog identity");
    assert!(user.is_superuser);
    assert_eq!(user.tenant_id, TenantId::new(1));

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn trust_configured_superuser_survives_tenant_creation() {
    assert_configured_trust_superuser_survives("CREATE TENANT alpha").await;
}

#[tokio::test]
async fn trust_configured_superuser_survives_user_creation() {
    assert_configured_trust_superuser_survives(
        "CREATE USER alice WITH PASSWORD 'strong-secret' ROLE readonly",
    )
    .await;
}

#[tokio::test]
async fn trust_configured_superuser_survives_service_account_creation() {
    assert_configured_trust_superuser_survives("CREATE SERVICE ACCOUNT batch_processor").await;
}

#[tokio::test]
async fn trust_catalog_owners_remain_valid_across_restart() {
    let server = TestServer::start_empty_store_trust().await;
    bootstrap_trust_superuser(&server);
    let (client, connection_handle) = connect_empty_store_trust(&server, "nodedb").await;

    client
        .simple_query("CREATE COLLECTION trust_owned_records")
        .await
        .expect("trusted superuser must create an owned collection");
    client
        .simple_query("CREATE TENANT alpha")
        .await
        .expect("trusted superuser must create a tenant");

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    let (server, data_dir) = server.take_dir();
    server.graceful_shutdown().await;

    let (reopened, _data_dir) = TestServer::open_on_path_empty_store_trust(data_dir).await;
    bootstrap_trust_superuser(&reopened);
    let report = nodedb::control::cluster::recovery_check::verify_and_repair(&reopened.shared)
        .await
        .expect("catalog sanity check must complete");
    assert!(
        report.is_acceptable(),
        "configured trust ownership must remain startup-safe: {report}"
    );
    assert_eq!(
        report.integrity_repaired, 0,
        "valid configured-superuser ownership must not require startup repair"
    );

    let collection = reopened
        .shared
        .credentials
        .catalog()
        .get_collection(DatabaseId::DEFAULT, 1, "trust_owned_records")
        .expect("collection catalog lookup")
        .expect("owned collection must survive restart");
    assert_eq!(collection.owner, "nodedb");

    reopened.graceful_shutdown().await;
}

#[tokio::test]
async fn trust_mode_rejects_unmaterialized_identity() {
    let server = TestServer::start_empty_store_trust().await;

    let result = server
        .connect_as_database("unmaterialized_identity", "ignored", "default")
        .await;

    assert!(
        result.is_err(),
        "trust mode must skip password verification without fabricating an identity"
    );
    server.graceful_shutdown().await;
}

#[tokio::test]
async fn trust_superuser_identity_survives_password_mode_restart() {
    let server = TestServer::start_empty_store_trust().await;
    bootstrap_trust_superuser(&server);
    let original_user_id = server
        .shared
        .credentials
        .get_user("nodedb")
        .expect("trust superuser")
        .user_id;
    let (server, data_dir) = server.take_dir();
    server.graceful_shutdown().await;

    let (reopened, _data_dir) = TestServer::open_on_path_empty_store_password(data_dir).await;
    reopened
        .shared
        .credentials
        .bootstrap_superuser("nodedb", "operator-password")
        .expect("replace internal trust credential");
    let password_user = reopened
        .shared
        .credentials
        .get_user("nodedb")
        .expect("password superuser");
    assert_eq!(password_user.user_id, original_user_id);

    let (client, connection_handle) = reopened
        .connect_as_database("nodedb", "operator-password", "default")
        .await
        .expect("password bootstrap must authenticate the durable identity");
    client
        .simple_query("SELECT 1")
        .await
        .expect("password-authenticated query");

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    reopened.graceful_shutdown().await;
}

#[tokio::test]
async fn trust_configured_identity_survives_discard_all() {
    let server = TestServer::start_empty_store_trust().await;
    bootstrap_trust_superuser(&server);
    let username = "nodedb";
    let (client, connection_handle) = connect_empty_store_trust(&server, username).await;

    client
        .simple_query("SET nodedb.consistency = eventual")
        .await
        .expect("SET must establish mutable session state before DISCARD ALL");
    client
        .simple_query("SET TENANT = 99")
        .await
        .expect("SET TENANT must establish a temporary tenant overlay");
    client
        .simple_query("DISCARD ALL")
        .await
        .expect("DISCARD ALL must retain the authenticated trust identity");
    let messages = client
        .simple_query("SHOW SESSION")
        .await
        .expect("trusted connection must remain authenticated after DISCARD ALL");
    let session_username = messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    });

    assert_eq!(session_username, Some(username.to_owned()));

    let tenant_messages = client
        .simple_query("SHOW TENANT")
        .await
        .expect("DISCARD ALL must clear the tenant overlay");
    let effective_tenant = tenant_messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    });
    assert_eq!(effective_tenant, Some("1".to_owned()));

    let consistency_messages = client
        .simple_query("SHOW nodedb.consistency")
        .await
        .expect("DISCARD ALL must reset session parameters");
    let consistency = consistency_messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        });
    assert_eq!(consistency, Some("strong".to_owned()));
    assert!(
        server.shared.credentials.get_user(username).is_some(),
        "DISCARD ALL must retain the durable configured trust identity"
    );

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    server.graceful_shutdown().await;
}

#[tokio::test]
async fn trust_known_user_role_downgrade_takes_effect() {
    let server = TestServer::start().await;
    let username = "known_trust_role_downgrade";
    server
        .shared
        .credentials
        .create_user(
            username,
            "unused-in-trust-mode",
            TenantId::new(1),
            vec![Role::Superuser],
        )
        .expect("create known Trust user");

    let (client, connection_handle) = server
        .connect_as(username, "ignored")
        .await
        .expect("known Trust user must authenticate");
    client
        .simple_query("SHOW SESSION")
        .await
        .expect("known Trust user must issue an initial query");

    server
        .shared
        .credentials
        .update_roles(username, vec![Role::ReadOnly])
        .expect("downgrade known Trust user");
    let role_downgrade = client
        .simple_query("CREATE USER stale_trust_role_probe WITH PASSWORD 'x'")
        .await;
    assert!(
        role_downgrade.is_err(),
        "a known Trust connection must not retain a stale superuser identity after role removal"
    );
    assert!(
        server
            .shared
            .credentials
            .get_user("stale_trust_role_probe")
            .is_none(),
        "stale Trust roles must not authorize DDL"
    );

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    server.graceful_shutdown().await;
}

#[tokio::test]
async fn trust_known_user_drop_fails_closed() {
    let server = TestServer::start().await;
    let username = "known_trust_drop";
    server
        .shared
        .credentials
        .create_user(
            username,
            "unused-in-trust-mode",
            TenantId::new(1),
            vec![Role::Superuser],
        )
        .expect("create known Trust user");

    let (client, connection_handle) = server
        .connect_as(username, "ignored")
        .await
        .expect("known Trust user must authenticate");
    client
        .simple_query("SHOW SESSION")
        .await
        .expect("known Trust user must issue an initial query");

    server
        .shared
        .credentials
        .drop_user(username)
        .expect("drop known Trust user");
    assert!(
        client.simple_query("SHOW SESSION").await.is_err(),
        "a dropped known Trust user must fail closed on its next request"
    );

    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    server.graceful_shutdown().await;
}
