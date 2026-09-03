// SPDX-License-Identifier: BUSL-1.1

//! A database `max_connections` quota must gate pgwire connections.
//!
//! The cap is configured, replicated, and reported, so the only thing left to
//! prove is that the connection path honours it: the capped connection is
//! refused with `53400`, the slot comes back when a connection closes, an
//! uncapped database is never capped by accident, and `SHOW DATABASE USAGE`
//! reports the live count instead of a hardcoded zero.

use std::time::Duration;

use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::DatabaseId;

/// SQLSTATE both protocols report for an exhausted connection cap.
const QUOTA_EXCEEDED: &str = "53400";

/// A live pgwire connection: the client plus the task driving its socket.
struct Connection {
    client: tokio_postgres::Client,
    handle: tokio::task::JoinHandle<()>,
}

impl Connection {
    /// Close the socket and wait for the driver task to finish.
    async fn close(self) {
        drop(self.client);
        let _ = self.handle.await;
    }
}

/// Resolve a database name to its id.
fn db_id(server: &TestServer, name: &str) -> DatabaseId {
    server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(name)
        .expect("catalog lookup")
        .expect("the database exists")
}

/// Open one pgwire connection bound to `database`.
async fn connect(server: &TestServer, database: &str) -> Result<Connection, String> {
    server
        .connect_as_database("nodedb", "nodedb", database)
        .await
        .map(|(client, handle)| Connection { client, handle })
}

/// Open a connection that must be admitted.
async fn connect_ok(server: &TestServer, database: &str) -> Connection {
    match connect(server, database).await {
        Ok(connection) => connection,
        Err(error) => panic!("connection to '{database}' must be admitted, got: {error}"),
    }
}

/// Create a database and cap its connections.
async fn capped_database(server: &TestServer, name: &str, limit: u32) {
    server
        .exec(&format!("CREATE DATABASE {name}"))
        .await
        .expect("CREATE DATABASE");
    server
        .exec(&format!(
            "ALTER DATABASE {name} SET QUOTA (max_connections = {limit})"
        ))
        .await
        .expect("ALTER DATABASE SET QUOTA");
}

/// Wait until the registry reports `expected` live connections for `db`.
///
/// Teardown runs on a task the closing client does not await, so the released
/// slot lands shortly after the socket closes.
async fn await_live_connections(server: &TestServer, db: DatabaseId, expected: u32) {
    let registry = &server.shared.admission_registry;
    for _ in 0..100 {
        if registry.database_live_connections(db) == Some(expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "expected {expected} live connections, registry reports {:?}",
        registry.database_live_connections(db)
    );
}

/// A capped database refuses the connection past its cap, and releases the slot
/// when a connection closes.
///
/// The release half is the load-bearing one: a permit that is acquired but
/// never dropped caps the database permanently at its first N connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn database_connection_cap_refuses_then_releases() {
    let server = TestServer::start().await;
    capped_database(&server, "conn_cap_db", 2).await;
    let db = db_id(&server, "conn_cap_db");

    let first = connect_ok(&server, "conn_cap_db").await;
    let second = connect_ok(&server, "conn_cap_db").await;
    assert_eq!(
        server
            .shared
            .admission_registry
            .database_live_connections(db),
        Some(2),
        "both admitted connections must hold a slot"
    );

    let refusal = connect(&server, "conn_cap_db")
        .await
        .err()
        .expect("the third connection must be refused");
    assert!(
        refusal.contains(QUOTA_EXCEEDED),
        "the refusal must carry SQLSTATE {QUOTA_EXCEEDED}, got: {refusal}"
    );

    first.close().await;
    await_live_connections(&server, db, 1).await;

    let third = connect_ok(&server, "conn_cap_db").await;
    third
        .client
        .simple_query("SELECT 1")
        .await
        .expect("the readmitted connection must be usable");

    second.close().await;
    third.close().await;
}

/// A database with no `max_connections` quota admits every connection.
///
/// An admission path that treated "no cap" as a cap of zero would refuse the
/// first connection to every database on the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncapped_database_admits_every_connection() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE conn_uncapped_db")
        .await
        .expect("CREATE DATABASE");
    let db = db_id(&server, "conn_uncapped_db");

    let mut connections = Vec::new();
    for _ in 0..6 {
        connections.push(connect_ok(&server, "conn_uncapped_db").await);
    }

    assert_eq!(
        server
            .shared
            .admission_registry
            .database_live_connections(db),
        None,
        "an uncapped database must keep no connection entry"
    );

    for connection in connections {
        connection.close().await;
    }
}

/// `SHOW DATABASE USAGE` reports the live connection count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_database_usage_reports_live_connections() {
    let server = TestServer::start().await;
    capped_database(&server, "conn_usage_db", 4).await;

    let idle = usage_connections(&server, "conn_usage_db").await;
    assert_eq!(idle, "0", "no connection is open yet");

    let open = connect_ok(&server, "conn_usage_db").await;
    let held = usage_connections(&server, "conn_usage_db").await;
    assert_eq!(held, "1", "the open connection must be counted");

    open.close().await;
}

/// An uncapped scope reports `n/a`, never a `0` that reads as "none open".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_database_usage_reports_uncapped_connections_as_unmeasured() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE conn_usage_uncapped")
        .await
        .expect("CREATE DATABASE");

    let open = connect_ok(&server, "conn_usage_uncapped").await;
    let reported = usage_connections(&server, "conn_usage_uncapped").await;
    assert_eq!(
        reported, "n/a",
        "a database with no cap has no connection counter to report"
    );

    open.close().await;
}

/// Read the `current` column of the `max_connections` row of
/// `SHOW DATABASE USAGE FOR <name>`.
async fn usage_connections(server: &TestServer, name: &str) -> String {
    let rows = server
        .query_named_rows(&format!("SHOW DATABASE USAGE FOR {name}"))
        .await
        .expect("SHOW DATABASE USAGE");
    rows.into_iter()
        .find(|row| row.get("quota_name").map(String::as_str) == Some("max_connections"))
        .and_then(|row| row.get("current").cloned())
        .expect("the usage report must carry a max_connections row")
}

/// `DISCARD ALL` resets the session without releasing its admission slot.
///
/// The reset replaces the whole `ConnSession`. A reset that dropped the parked
/// permit would free a live connection's slot, so a client could hold the
/// socket open and let the next connection past a full cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discard_all_keeps_the_connection_slot() {
    let server = TestServer::start().await;
    capped_database(&server, "discard_cap_db", 1).await;
    let db = db_id(&server, "discard_cap_db");

    let held = connect_ok(&server, "discard_cap_db").await;
    held.client
        .simple_query("DISCARD ALL")
        .await
        .expect("DISCARD ALL must succeed");

    assert_eq!(
        server
            .shared
            .admission_registry
            .database_live_connections(db),
        Some(1),
        "the reset session must keep holding its slot"
    );

    let refusal = connect(&server, "discard_cap_db")
        .await
        .err()
        .expect("the cap must still refuse a second connection after DISCARD ALL");
    assert!(
        refusal.contains(QUOTA_EXCEEDED),
        "the refusal must carry SQLSTATE {QUOTA_EXCEEDED}, got: {refusal}"
    );

    held.close().await;
}

/// `USE DATABASE` into a full database is refused, and the session keeps the
/// slot it already holds.
///
/// Admission binds at startup, so a switch that moved the session without
/// re-acquiring would carry an uncapped connection into a capped database.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn use_database_into_a_full_database_is_refused() {
    let server = TestServer::start().await;
    capped_database(&server, "switch_target_db", 1).await;
    let target = db_id(&server, "switch_target_db");

    // One connection fills the target's only slot.
    let occupant = connect_ok(&server, "switch_target_db").await;
    await_live_connections(&server, target, 1).await;

    // A second connection lands elsewhere, then tries to switch in.
    let switcher = connect_ok(&server, "default").await;
    let refusal = switcher
        .client
        .simple_query("USE DATABASE switch_target_db")
        .await
        .err()
        .expect("switching into a full database must be refused");
    // `tokio_postgres::Error` displays as "db error"; the server's SQLSTATE
    // rides on the wrapped `DbError`.
    let code = refusal
        .as_db_error()
        .map(|e| e.code().code().to_string())
        .unwrap_or_else(|| refusal.to_string());
    assert_eq!(
        code, QUOTA_EXCEEDED,
        "the refused switch must carry SQLSTATE {QUOTA_EXCEEDED}, got: {refusal}"
    );

    assert_eq!(
        server
            .shared
            .admission_registry
            .database_live_connections(target),
        Some(1),
        "the refused switch must not take a slot in the target"
    );
    switcher
        .client
        .simple_query("SELECT 1")
        .await
        .expect("the refused switch must leave the session usable where it was");

    occupant.close().await;
    switcher.close().await;
}
