// SPDX-License-Identifier: BUSL-1.1

//! `BACKUP DATABASE` and `RESTORE DATABASE` over the native protocol.
//!
//! The server writes the backup to a `file://` URI inside its backup root,
//! reads it back, and restores it over its own rows: the restore verifies
//! every row. A DRY RUN verifies nothing and reports no rows. A URI outside
//! the backup root is refused with SQLSTATE 22023.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_sql};
use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::NativeResponse;
use nodedb_types::protocol::opcodes::ResponseStatus;

fn assert_ok(response: &NativeResponse, what: &str) {
    assert_eq!(
        response.status,
        ResponseStatus::Ok,
        "{what}: {:?}",
        response.error
    );
}

async fn sql(stream: &mut tokio::net::TcpStream, seq: &mut u64, text: &str) -> NativeResponse {
    *seq += 1;
    send_sql(stream, *seq, text).await
}

#[tokio::test]
async fn native_backup_and_restore_database_round_trip() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    let mut seq = 0u64;

    for text in [
        "CREATE COLLECTION nbd_docs (id TEXT PRIMARY KEY, content TEXT) \
         WITH (engine='document_strict')",
        "INSERT INTO nbd_docs (id, content) VALUES ('a', 'alpha')",
        "INSERT INTO nbd_docs (id, content) VALUES ('b', 'beta')",
    ] {
        assert_ok(&sql(&mut stream, &mut seq, text).await, text);
    }

    let uri = server.backup_uri("native/default.ndbb");
    let backup = sql(
        &mut stream,
        &mut seq,
        &format!("BACKUP DATABASE default TO '{uri}'"),
    )
    .await;
    assert_ok(&backup, "BACKUP DATABASE");

    let dry = sql(
        &mut stream,
        &mut seq,
        &format!("RESTORE DATABASE default FROM '{uri}' DRY RUN"),
    )
    .await;
    assert_ok(&dry, "RESTORE DATABASE DRY RUN");
    assert_eq!(dry.rows_affected, Some(0), "a dry run verifies nothing");

    let restore = sql(
        &mut stream,
        &mut seq,
        &format!("RESTORE DATABASE default FROM '{uri}'"),
    )
    .await;
    assert_ok(&restore, "RESTORE DATABASE");
    assert!(
        restore.rows_affected.is_some_and(|rows| rows >= 2),
        "the restore verifies both rows: {restore:?}"
    );

    let bad = sql(
        &mut stream,
        &mut seq,
        "BACKUP DATABASE default TO 'file:///etc/nodedb-escape.ndbb'",
    )
    .await;
    server.shutdown().await;
    assert_eq!(bad.status, ResponseStatus::Error, "{bad:?}");
    let error = bad.error.expect("an error payload");
    assert!(
        error.code.contains("22023") || error.message.contains("local_root"),
        "a URI outside the backup root is a typed refusal: {error:?}"
    );
}
