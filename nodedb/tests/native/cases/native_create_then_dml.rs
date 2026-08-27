// SPDX-License-Identifier: BUSL-1.1

//! A collection created over the native protocol must be immediately visible
//! to native DML on the same connection.
//!
//! Both pgwire and the native protocol route DDL through the shared neutral
//! dispatch path into `propose_and_apply`, which synchronously commits to the
//! shared catalog before returning `Ok`. If that guarantee ever regresses, a
//! native `CREATE COLLECTION` would return success while a subsequent native
//! `INSERT` on the same connection fails with a "table not found" style
//! error — a native-DDL-not-visible-to-native-DML asymmetry. This test drives
//! CREATE -> INSERT -> SELECT entirely over one native connection, for every
//! non-array engine family, to guard against that regression.

mod common;

use common::native_harness::{NativeTestServer, do_handshake, send_sql};

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::value::Value;

/// Drive CREATE -> INSERT -> SELECT for one engine, all over a single native
/// connection, and assert each step succeeds with the expected row shape.
async fn assert_create_then_dml_visible(
    create_sql: &str,
    insert_sql: &str,
    select_sql: &str,
    expected_columns: &[&str],
    expected_row: &[Value],
) {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let create_resp = send_sql(&mut stream, 1, create_sql).await;
    assert_ne!(
        create_resp.status,
        ResponseStatus::Error,
        "native CREATE must succeed: {create_resp:?}"
    );

    // The key assertion: DDL committed on this native connection must be
    // immediately visible to DML on the SAME native connection — no
    // "table not found" / collection-not-found error.
    let insert_resp = send_sql(&mut stream, 2, insert_sql).await;
    assert_ne!(
        insert_resp.status,
        ResponseStatus::Error,
        "native INSERT immediately after native CREATE must succeed \
         (collection must be visible to DML on the same connection), got: {insert_resp:?}"
    );

    let select_resp = send_sql(&mut stream, 3, select_sql).await;
    server.shutdown().await;

    assert_ne!(
        select_resp.status,
        ResponseStatus::Error,
        "native SELECT after native CREATE+INSERT must succeed: {select_resp:?}"
    );
    let columns = select_resp.columns.expect("columns present");
    assert_eq!(
        columns,
        expected_columns
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>(),
        "SELECT must project the declared columns: got {columns:?}"
    );
    let rows = select_resp.rows.expect("rows present");
    assert_eq!(rows.len(), 1, "exactly one row expected: {rows:?}");
    assert_eq!(
        rows[0], expected_row,
        "row values must match what was inserted over native"
    );
}

/// A `document_strict` collection created over native must be immediately
/// insertable-into and selectable-from over native, on the same connection.
#[tokio::test]
async fn document_strict_create_then_insert_then_select_over_native() {
    assert_create_then_dml_visible(
        "CREATE COLLECTION c (id STRING PRIMARY KEY, name STRING) WITH (engine='document_strict')",
        "INSERT INTO c (id, name) VALUES ('a', 'alice')",
        "SELECT id, name FROM c WHERE id = 'a'",
        &["id", "name"],
        &[Value::String("a".into()), Value::String("alice".into())],
    )
    .await;
}

/// Sibling: the `kv` engine must show the same CREATE -> DML visibility
/// across engines, not just for document_strict.
#[tokio::test]
async fn kv_create_then_insert_then_select_over_native() {
    assert_create_then_dml_visible(
        "CREATE COLLECTION c (id STRING PRIMARY KEY, name STRING) WITH (engine='kv')",
        "INSERT INTO c (id, name) VALUES ('a', 'alice')",
        "SELECT id, name FROM c WHERE id = 'a'",
        &["id", "name"],
        &[Value::String("a".into()), Value::String("alice".into())],
    )
    .await;
}

/// Sibling: `document_schemaless` collections use `CREATE COLLECTION ...
/// WITH (engine=...)` DDL (no inline column list) and `{ }` insert syntax,
/// but must show the same CREATE -> DML visibility over native.
#[tokio::test]
async fn document_schemaless_create_then_insert_then_select_over_native() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let create_resp = send_sql(
        &mut stream,
        1,
        "CREATE COLLECTION c WITH (engine='document_schemaless')",
    )
    .await;
    assert_ne!(
        create_resp.status,
        ResponseStatus::Error,
        "native CREATE must succeed: {create_resp:?}"
    );

    let insert_resp = send_sql(&mut stream, 2, "INSERT INTO c { id: 'a', name: 'alice' }").await;
    assert_ne!(
        insert_resp.status,
        ResponseStatus::Error,
        "native INSERT immediately after native CREATE must succeed \
         (collection must be visible to DML on the same connection), got: {insert_resp:?}"
    );

    let select_resp = send_sql(&mut stream, 3, "SELECT id, name FROM c WHERE id = 'a'").await;
    server.shutdown().await;

    assert_ne!(
        select_resp.status,
        ResponseStatus::Error,
        "native SELECT after native CREATE+INSERT must succeed: {select_resp:?}"
    );
    let columns = select_resp.columns.expect("columns present");
    assert_eq!(
        columns,
        vec!["id".to_string(), "name".to_string()],
        "SELECT must project the declared columns: got {columns:?}"
    );
    let rows = select_resp.rows.expect("rows present");
    assert_eq!(rows.len(), 1, "exactly one row expected: {rows:?}");
    assert_eq!(rows[0][0], Value::String("a".into()), "id cell");
    assert_eq!(rows[0][1], Value::String("alice".into()), "name cell");
}
