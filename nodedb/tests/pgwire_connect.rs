// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::wal::WalManager;

async fn shown_connection_ids(client: &tokio_postgres::Client) -> HashSet<u64> {
    client
        .simple_query("SHOW CONNECTIONS")
        .await
        .expect("SHOW CONNECTIONS")
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row
                .get("connection_id")
                .and_then(|value| value.parse::<u64>().ok()),
            _ => None,
        })
        .collect()
}

/// End-to-end test: psql-compatible client connects via pgwire,
/// sends a query, and gets a response.
#[tokio::test]
async fn pgwire_connect_and_query() {
    // Set up infrastructure (mirrors session::tests::full_request_response_roundtrip).
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

    let (dispatcher, data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).unwrap();
    shared
        .credentials
        .bootstrap_trust_superuser("nodedb")
        .expect("materialize configured trust superuser");

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

    // Bind pgwire listener on random port.
    let pg_listener = PgListener::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let pg_addr = pg_listener.local_addr();

    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_pg = Arc::clone(&shared);
    let test_startup_gate = Arc::clone(&shared.startup);
    let bus_pg = shutdown_bus.clone();
    let pg_handle = tokio::spawn(async move {
        pg_listener
            .run(
                shared_pg,
                AuthMode::Trust,
                None,
                Arc::new(tokio::sync::Semaphore::new(128)),
                test_startup_gate,
                bus_pg,
            )
            .await
            .unwrap();
    });

    // Give listener a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect using tokio-postgres (a real PostgreSQL client).
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
        pg_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("pgwire connect failed");

    // Spawn connection handler.
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // Test 1: Simple query that NodeDB handles (SET command).
    let result = client.simple_query("SET client_encoding = 'UTF8'").await;
    assert!(result.is_ok(), "SET command failed: {:?}", result.err());

    // Test 2: The connection is alive and responsive.
    // Send a query that will go through DataFusion planning.
    // This will likely error (no table registered), but proves the full path works.
    let result = client.simple_query("SELECT 1").await;
    // We expect this might error since DataFusion may not have a table, but the
    // pgwire protocol exchange should complete without a connection-level failure.
    match &result {
        Ok(msgs) => {
            println!("SELECT 1 returned {} messages", msgs.len());
            for msg in msgs {
                match msg {
                    tokio_postgres::SimpleQueryMessage::Row(row) => {
                        println!("  Row: {:?}", row.get(0));
                    }
                    tokio_postgres::SimpleQueryMessage::CommandComplete(n) => {
                        println!("  CommandComplete: {n}");
                    }
                    _ => {}
                }
            }
        }
        Err(e) => {
            // A SQL-level error returned via pgwire ErrorResponse is OK —
            // it means the protocol is working correctly.
            println!("SELECT 1 returned error (expected): {e}");
        }
    }

    // Test 3: Connection is still alive after the query (error didn't kill it).
    let result2 = client.simple_query("SET search_path = 'public'").await;
    assert!(
        result2.is_ok(),
        "Connection died after query: {:?}",
        result2.err()
    );

    // Connection administration is exact-ID: SHOW exposes the typed ID,
    // KILL closes only the selected connection, and the admin remains alive.
    let before_target = shown_connection_ids(&client).await;
    let (target_client, target_connection) =
        tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("target pgwire connect failed");
    let target_handle = tokio::spawn(target_connection);
    let after_target = shown_connection_ids(&client).await;
    let added: Vec<u64> = after_target.difference(&before_target).copied().collect();
    assert_eq!(added.len(), 1, "target must add exactly one connection ID");

    client
        .simple_query(&format!("KILL CONNECTION {}", added[0]))
        .await
        .expect("KILL CONNECTION");
    let _target_result = tokio::time::timeout(Duration::from_secs(5), target_handle)
        .await
        .expect("target connection did not close after KILL")
        .expect("target connection task join");
    assert!(
        target_client.simple_query("SELECT 1").await.is_err(),
        "killed client must no longer execute queries"
    );
    assert!(
        client
            .simple_query("SET search_path = 'public'")
            .await
            .is_ok(),
        "KILL must not close the administering connection"
    );

    client
        .simple_query("CREATE USER connection_viewer WITH PASSWORD 'x' ROLE readonly")
        .await
        .expect("create non-superuser connection viewer");
    let viewer_conn_str = format!(
        "host=127.0.0.1 port={} user=connection_viewer dbname=nodedb",
        pg_addr.port()
    );
    let (viewer, viewer_connection) =
        tokio_postgres::connect(&viewer_conn_str, tokio_postgres::NoTls)
            .await
            .expect("viewer pgwire connect failed");
    let viewer_handle = tokio::spawn(viewer_connection);
    assert!(viewer.simple_query("SHOW CONNECTIONS").await.is_err());
    assert!(
        viewer
            .simple_query(&format!("KILL CONNECTION {}", added[0]))
            .await
            .is_err()
    );
    drop(viewer);
    let _ = viewer_handle.await;

    // Clean up — signal all background tasks to stop.
    drop(client);
    let _ = conn_handle.await;
    shutdown_bus.initiate();
    let _ = pg_handle.await;
    let _ = poller_shutdown_tx.send(true);
    let _ = poller_handle.await;
    let _ = core_stop_tx.send(());
    let _ = core_handle.await;
}
