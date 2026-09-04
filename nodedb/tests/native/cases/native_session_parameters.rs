// SPDX-License-Identifier: BUSL-1.1

//! `SET` and `SHOW` over the native protocol enforce the same contract pgwire
//! enforces.
//!
//! The native MessagePack protocol is the canonical transport, so a name or a
//! value it stores while pgwire refuses it would give one client two different
//! servers. These tests drive the `Set` and `Show` opcodes directly — the form
//! a native client sends, and the one that used to store anything under any
//! name and answer `SET`.
//!
//! `statement_timeout` is the parameter with teeth: a stored value the parser
//! refuses reads back as "no session limit", so the statement silently runs on
//! the node default instead of the budget the client asked for.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::{OpCode, ResponseStatus};
use nodedb_types::protocol::text_fields::TextFields;

/// SQLSTATE `undefined_object` — an unrecognized configuration parameter.
const UNDEFINED_OBJECT: &str = "42704";
/// SQLSTATE `invalid_parameter_value` — a value the parameter refuses.
const INVALID_PARAMETER_VALUE: &str = "22023";
/// SQLSTATE `query_canceled` — a statement stopped at its deadline.
const QUERY_CANCELED: &str = "57014";

/// The smallest budget `statement_timeout` can express: one microsecond.
///
/// The deadline is pinned at the statement boundary, before the statement is
/// parsed, planned, authorized or dispatched. No machine completes that
/// prologue in a microsecond, so the budget is spent before the first row is
/// ever read — the statement is over its deadline BY CONSTRUCTION rather than
/// by winning a race against a row count. A budget picked to be "probably
/// slower than the query" fails in both directions: too generous and the
/// statement finishes, too tight and it still depends on load.
const UNMEETABLE_BUDGET: &str = "1us";

/// Rows seeded. Small: nothing here depends on the scan taking any particular
/// time, only on the budget being unmeetable.
const ROWS: usize = 8;

/// The bounded statement — an unfiltered full scan.
const SCAN_QUERY: &str = "SELECT id, payload FROM bounded_scan";

/// Send one `Set` opcode, the shape a native client uses.
async fn send_set(
    stream: &mut tokio::net::TcpStream,
    seq: u64,
    key: &str,
    value: &str,
) -> nodedb_types::protocol::NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::Set,
        TextFields {
            key: Some(key.into()),
            value: Some(value.into()),
            ..Default::default()
        },
    )
    .await
}

/// Send one `Show` opcode.
async fn send_show(
    stream: &mut tokio::net::TcpStream,
    seq: u64,
    key: &str,
) -> nodedb_types::protocol::NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::Show,
        TextFields {
            key: Some(key.into()),
            ..Default::default()
        },
    )
    .await
}

/// The SQLSTATE on an error response, or a panic naming the response that
/// carried none.
fn sqlstate_of(response: &nodedb_types::protocol::NativeResponse) -> &str {
    match &response.error {
        Some(error) => error.code.as_str(),
        None => panic!("expected an error frame, got {response:?}"),
    }
}

/// The single `setting` value a `SHOW` answered with.
fn setting_of(response: &nodedb_types::protocol::NativeResponse) -> &str {
    match response.rows.as_deref() {
        Some([row]) => match row.as_slice() {
            [nodedb_types::value::Value::String(value)] => value.as_str(),
            other => panic!("expected one string setting, got {other:?}"),
        },
        other => panic!("expected exactly one row, got {other:?}"),
    }
}

#[tokio::test]
async fn native_set_statement_timeout_refuses_a_junk_value() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let refused = send_set(&mut stream, 1, "statement_timeout", "whenever").await;
    let stored = send_show(&mut stream, 2, "statement_timeout").await;
    server.shutdown().await;

    assert_eq!(
        refused.status,
        ResponseStatus::Error,
        "a value the parser refuses must not answer SET: {refused:?}"
    );
    assert_eq!(sqlstate_of(&refused), INVALID_PARAMETER_VALUE);
    assert_eq!(
        setting_of(&stored),
        "0",
        "the refused SET must not have replaced the session's value"
    );
}

#[tokio::test]
async fn native_set_of_an_unknown_parameter_is_refused() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let refused = send_set(&mut stream, 1, "not_a_parameter", "1").await;
    let stored = send_show(&mut stream, 2, "not_a_parameter").await;
    server.shutdown().await;

    assert_eq!(
        refused.status,
        ResponseStatus::Error,
        "an unknown name must not answer SET: {refused:?}"
    );
    assert_eq!(sqlstate_of(&refused), UNDEFINED_OBJECT);
    assert!(
        refused
            .error
            .as_ref()
            .is_some_and(|error| error.message.contains("not_a_parameter")),
        "the message must name the parameter: {refused:?}"
    );
    assert_eq!(
        stored.status,
        ResponseStatus::Error,
        "the refused name must not have been stored: {stored:?}"
    );
}

#[tokio::test]
async fn native_show_of_an_unknown_parameter_reports_it_unknown() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let response = send_show(&mut stream, 1, "not_a_parameter").await;
    server.shutdown().await;

    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "an unknown name must not read back as an empty setting: {response:?}"
    );
    assert_eq!(sqlstate_of(&response), UNDEFINED_OBJECT);
}

/// The SQL form of `SET` reaches the same contract as the opcode form. The
/// Unicode key also pins the parser's byte offsets: the refusal names the key
/// exactly as sent, so the multi-byte name was sliced at the right boundary.
#[tokio::test]
async fn native_set_sql_form_refuses_an_unknown_unicode_key() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let refused = send_sql(&mut stream, 1, "SET custom.ﬀﬀ TO enabled").await;
    let show = send_sql(&mut stream, 2, "SHOW custom.ﬀﬀ").await;
    server.shutdown().await;

    assert_eq!(refused.status, ResponseStatus::Error, "{refused:?}");
    assert_eq!(sqlstate_of(&refused), UNDEFINED_OBJECT);
    assert!(
        refused
            .error
            .as_ref()
            .is_some_and(|error| error.message.contains("custom.ﬀﬀ")),
        "the message must carry the key verbatim: {refused:?}"
    );
    assert_eq!(show.status, ResponseStatus::Error, "{show:?}");
    assert_eq!(sqlstate_of(&show), UNDEFINED_OBJECT);
}

/// Identity keys reach their own dispatch branch. The native protocol binds
/// identity at connect time and reads none of them back, so each is refused
/// rather than stored as a switch that never happens.
#[tokio::test]
async fn native_set_of_an_identity_key_is_refused() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let mut seq = 0;
    for key in [
        "tenant",
        "nodedb.tenant_id",
        "role",
        "session_authorization",
        "nodedb.auth_session",
    ] {
        seq += 1;
        let refused = send_set(&mut stream, seq, key, "2").await;
        assert_eq!(
            refused.status,
            ResponseStatus::Error,
            "{key} must not be stored: {refused:?}"
        );
        assert_eq!(sqlstate_of(&refused), "0A000", "{key}");
    }
    server.shutdown().await;
}

/// The positive case: a valid `statement_timeout` is accepted, echoed, and
/// bounds the statement that follows.
#[tokio::test]
async fn native_set_statement_timeout_bounds_a_statement() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let create = send_sql(
        &mut stream,
        1,
        "CREATE COLLECTION bounded_scan \
         COLUMNS (id TEXT PRIMARY KEY, payload TEXT) \
         WITH (engine='document_strict')",
    )
    .await;
    assert_ne!(create.status, ResponseStatus::Error, "{create:?}");

    let mut insert = String::from("INSERT INTO bounded_scan (id, payload) VALUES ");
    for i in 0..ROWS {
        if i > 0 {
            insert.push(',');
        }
        insert.push_str(&format!("('{i:03}', 'row {i}')"));
    }
    let seeded = send_sql(&mut stream, 2, &insert).await;
    assert_ne!(seeded.status, ResponseStatus::Error, "{seeded:?}");

    let accepted = send_set(&mut stream, 3, "statement_timeout", UNMEETABLE_BUDGET).await;
    assert_eq!(
        accepted.status,
        ResponseStatus::Ok,
        "a valid value must be accepted: {accepted:?}"
    );
    assert_eq!(
        setting_of(&send_show(&mut stream, 4, "statement_timeout").await),
        UNMEETABLE_BUDGET,
        "SHOW must echo the value that was set"
    );

    let bounded = send_sql(&mut stream, 5, SCAN_QUERY).await;
    assert_eq!(
        bounded.status,
        ResponseStatus::Error,
        "a budget of {UNMEETABLE_BUDGET} is spent before the statement is planned, \
         so the scan must not complete (returned {} rows)",
        bounded.rows.as_ref().map_or(0, |rows| rows.len())
    );
    // Two halves can report the statement's own timeout: the Control-Plane
    // timer, and the shard refusing a task that is already past its deadline.
    // Which one wins depends on machine load, so both must answer this one
    // SQLSTATE.
    assert_eq!(sqlstate_of(&bounded), QUERY_CANCELED);

    // The same statement inside a generous budget returns its rows, so the
    // error above is the timeout doing the work.
    let relaxed = send_set(&mut stream, 6, "statement_timeout", "30s").await;
    assert_eq!(relaxed.status, ResponseStatus::Ok, "{relaxed:?}");
    let complete = send_sql(&mut stream, 7, SCAN_QUERY).await;
    server.shutdown().await;

    assert_ne!(
        complete.status,
        ResponseStatus::Error,
        "a statement inside its budget must return its rows"
    );
    assert_eq!(
        complete.rows.map(|rows| rows.len()),
        Some(ROWS),
        "every seeded row must come back"
    );
}

/// `RESET` restores the connection default rather than storing an empty
/// string over the parameter, and refuses a name `SET` cannot write.
#[tokio::test]
async fn native_reset_restores_the_connection_default() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let set = send_set(&mut stream, 1, "datestyle", "SQL, DMY").await;
    assert_eq!(set.status, ResponseStatus::Ok, "{set:?}");

    let reset = send_request(
        &mut stream,
        2,
        OpCode::Reset,
        TextFields {
            key: Some("datestyle".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(reset.status, ResponseStatus::Ok, "{reset:?}");
    let restored = send_show(&mut stream, 3, "datestyle").await;

    let refused = send_request(
        &mut stream,
        4,
        OpCode::Reset,
        TextFields {
            key: Some("not_a_parameter".into()),
            ..Default::default()
        },
    )
    .await;
    server.shutdown().await;

    assert_eq!(
        setting_of(&restored),
        "ISO, MDY",
        "RESET must restore the connection default"
    );
    assert_eq!(refused.status, ResponseStatus::Error, "{refused:?}");
    assert_eq!(sqlstate_of(&refused), UNDEFINED_OBJECT);
}
