// SPDX-License-Identifier: BUSL-1.1

//! A client learns which timeseries lines were not stored, over pgwire and
//! native alike.
//!
//! - An ingest answers the rows it stored and reports the lines its resolve
//!   rejected as a notice: a pgwire `NoticeResponse`, a native warning.
//! - A statement staged inside `BEGIN` reports the lines its stage-time
//!   preview rejected the same way.
//! - When a concurrent ingest changes the collection schema between staging
//!   and COMMIT, COMMIT reports the lines it rejected beyond those its
//!   statements reported.
//!
//! Each collection declares only its `ts` time key. A timeseries collection
//! takes no undeclared column over SQL, so every other column comes from raw
//! ILP: the ILP listener evolves the live schema and projects each column's
//! type into the catalog. ILP ingest is Calvin-sequenced, so these cases run
//! against the real server.
//!
//! The COMMIT case runs over native only. It stages a line that names a
//! column the live schema does not hold yet. SQL names only catalog columns,
//! and every catalog column is already live, so no SQL statement stages such
//! a line. The native `TimeseriesIngest` opcode takes raw ILP lines, which can
//! name any column. The concurrent line goes through the same opcode on a
//! second connection: it changes the live schema only. Over the ILP listener
//! the line also projects the column into the catalog, and that catalog change
//! drains the descriptor lease the open transaction holds until COMMIT.

use std::time::{Duration, Instant};

use nodedb_test_support::ilp_client;
use nodedb_test_support::native_harness::{do_handshake, send_request, send_sql};
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, NativeResponse, OpCode};
use tokio::net::TcpStream;

use crate::harness::TestServer;
use crate::harness::raw_pgwire::{RawPgConn, command_tags, notice_messages};

/// How long an ILP line can take to read back over SQL. The ILP batch timer,
/// the Calvin commit and the catalog projection each finish well inside it.
const ARRIVAL_WAIT: Duration = Duration::from_secs(20);

/// The trust-mode user every connection runs as.
const USER: &str = "nodedb";

async fn native_session(server: &TestServer) -> TcpStream {
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), server.native_port);
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

async fn create_timeseries(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (ts BIGINT TIME_KEY) WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE {collection}: {e}"));
}

/// Poll `read` until its first column holds `expected`, or panic once
/// [`ARRIVAL_WAIT`] passes. `what` names the write the poll waits for.
async fn wait_for_row(server: &TestServer, read: &str, expected: &str, what: &str) {
    let deadline = Instant::now() + ARRIVAL_WAIT;
    loop {
        let outcome = server.query_text(read).await;
        if outcome
            .as_ref()
            .is_ok_and(|values| values.iter().any(|v| v == expected))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what} never read back as '{expected}' within {ARRIVAL_WAIT:?}; last read of \
             `{read}`: {outcome:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Give `collection` a text `value` column over the ILP listener, and wait
/// until SQL reads it back: the catalog knows the column, and the row landed.
async fn evolve_text_value(server: &TestServer, collection: &str) {
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), server.ilp_port);
    let mut ilp = ilp_client::connect_trust(addr, USER).await;
    let line = format!("{collection} value=\"a\" 1000000000");
    ilp_client::send_line(&mut ilp, &line).await;
    wait_for_row(
        server,
        &format!("SELECT value FROM {collection}"),
        "a",
        &format!("the ILP line `{line}`"),
    )
    .await;
    // Held open until the row reads back: closing it earlier races the
    // server's batch flush.
    drop(ilp);
}

/// Ingest the raw ILP `line` into `collection` through the native
/// `TimeseriesIngest` opcode.
async fn ingest_native(
    stream: &mut TcpStream,
    seq: u64,
    collection: &str,
    line: &str,
) -> NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::TimeseriesIngest,
        TextFields {
            collection: Some(collection.to_string()),
            payload: Some(line.as_bytes().to_vec()),
            format: Some("ilp".to_string()),
            ..Default::default()
        },
    )
    .await
}

/// Two rows. `value` holds text since the ILP line, so the second row's
/// integer conflicts with it: the planner passes an integer through to a text
/// column, and the ingest rejects the line.
fn conflicting_insert(collection: &str) -> String {
    format!("INSERT INTO {collection} (ts, value) VALUES (2000, 'b'), (3000, 5)")
}

fn assert_notice(notices: &[String], collection: &str, needle: &str) {
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains(collection) && notice.contains(needle)),
        "expected a notice naming '{collection}' with '{needle}', got {notices:?}"
    );
}

fn assert_native_ok(response: &NativeResponse, what: &str) {
    assert_ne!(
        response.status,
        ResponseStatus::Error,
        "{what} must succeed: {response:?}"
    );
}

// ── An ingest's reply ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_ingest_reports_its_rejected_line() {
    let server = TestServer::start().await;
    let collection = "ts_reject_pg_ingest";
    create_timeseries(&server, collection).await;
    evolve_text_value(&server, collection).await;

    let mut conn = RawPgConn::connect(server.pg_port, USER, "default").await;
    let messages = conn.simple_query(&conflicting_insert(collection)).await;

    assert_eq!(command_tags(&messages), vec!["INSERT 0 1".to_string()]);
    assert_notice(&notice_messages(&messages), collection, "1 line(s)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_ingest_reports_its_rejected_line() {
    let server = TestServer::start().await;
    let collection = "ts_reject_native_ingest";
    create_timeseries(&server, collection).await;
    evolve_text_value(&server, collection).await;

    let mut stream = native_session(&server).await;
    let response = send_sql(&mut stream, 1, &conflicting_insert(collection)).await;

    assert_native_ok(&response, "the ingest");
    assert_eq!(response.rows_affected, Some(1), "{response:?}");
    assert_notice(&response.warnings, collection, "1 line(s)");
}

// ── A staged statement ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_staged_statement_reports_its_rejected_line() {
    let server = TestServer::start().await;
    let collection = "ts_reject_pg_staged";
    create_timeseries(&server, collection).await;
    evolve_text_value(&server, collection).await;

    let mut conn = RawPgConn::connect(server.pg_port, USER, "default").await;
    conn.simple_query("BEGIN").await;
    let messages = conn.simple_query(&conflicting_insert(collection)).await;
    assert_eq!(command_tags(&messages), vec!["INSERT 0 1".to_string()]);
    assert_notice(&notice_messages(&messages), collection, "1 line(s)");

    let commit = conn.simple_query("COMMIT").await;
    assert!(
        notice_messages(&commit).is_empty(),
        "COMMIT rejected no line beyond the statement's: {:?}",
        notice_messages(&commit)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_staged_statement_reports_its_rejected_line() {
    let server = TestServer::start().await;
    let collection = "ts_reject_native_staged";
    create_timeseries(&server, collection).await;
    evolve_text_value(&server, collection).await;

    let mut stream = native_session(&server).await;
    assert_native_ok(&send_sql(&mut stream, 1, "BEGIN").await, "BEGIN");
    let staged = send_sql(&mut stream, 2, &conflicting_insert(collection)).await;
    assert_native_ok(&staged, "the staged INSERT");
    assert_eq!(staged.rows_affected, Some(1), "{staged:?}");
    assert_notice(&staged.warnings, collection, "1 line(s)");

    let commit = send_sql(&mut stream, 3, "COMMIT").await;
    assert_native_ok(&commit, "COMMIT");
    assert!(
        commit.warnings.is_empty(),
        "COMMIT rejected no line beyond the statement's: {:?}",
        commit.warnings
    );
}

// ── COMMIT after a concurrent schema change ─────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_commit_reports_lines_a_concurrent_schema_change_rejected() {
    let server = TestServer::start().await;
    let collection = "ts_reject_native_commit";
    create_timeseries(&server, collection).await;

    let mut stream = native_session(&server).await;
    assert_native_ok(&send_sql(&mut stream, 1, "BEGIN").await, "BEGIN");
    // The transaction's line gives `extra`, which no schema holds yet, a
    // float.
    let staged = ingest_native(
        &mut stream,
        2,
        collection,
        &format!("{collection} extra=1.5 2000000000"),
    )
    .await;
    assert_native_ok(&staged, "the staged ingest");
    assert!(
        staged.warnings.is_empty(),
        "the stage previews against the schema in force and rejects nothing: {:?}",
        staged.warnings
    );

    // A concurrent connection's line gives the live `extra` column text,
    // which the staged float conflicts with. Its row is the only one another
    // session can see before COMMIT.
    let mut concurrent = native_session(&server).await;
    let line = format!("{collection} extra=\"text\" 3000000000");
    let ingested = ingest_native(&mut concurrent, 1, collection, &line).await;
    assert_native_ok(&ingested, "the concurrent ingest");
    wait_for_row(
        &server,
        &format!("SELECT COUNT(*) FROM {collection}"),
        "1",
        &format!("the concurrent line `{line}`"),
    )
    .await;

    let commit = send_sql(&mut stream, 3, "COMMIT").await;
    assert_native_ok(&commit, "COMMIT");
    assert_notice(&commit.warnings, collection, "COMMIT rejected 1 line(s)");
}
