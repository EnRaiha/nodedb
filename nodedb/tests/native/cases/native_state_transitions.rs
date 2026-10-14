// SPDX-License-Identifier: BUSL-1.1

//! A state constraint binds native updates as it binds pgwire ones.
//!
//! The collection declares `status TRANSITIONS ('draft' -> 'active')` on a
//! row in `draft`. Over one native connection, `draft -> closed` is refused
//! and leaves the row unchanged, and `draft -> active` is admitted. Both the
//! `DocumentUpdate` opcode and a native SQL UPDATE are covered.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};
use nodedb_types::Value;
use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::{OpCode, ResponseStatus};
use nodedb_types::protocol::text_fields::TextFields;
use tokio::net::TcpStream;

/// One native connection with its request sequence.
struct Session {
    stream: TcpStream,
    seq: u64,
}

impl Session {
    /// Open a connection and create `collection` with row `a` in `draft`,
    /// constrained by the `flow` rule.
    async fn with_flow(server: &NativeTestServer, collection: &str) -> Self {
        let (stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
            .await
            .expect("native handshake");
        let mut session = Self { stream, seq: 0 };
        for sql in [
            format!("CREATE COLLECTION {collection}"),
            format!("INSERT INTO {collection} (id, status) VALUES ('a', 'draft')"),
            format!(
                "ALTER COLLECTION {collection} ADD CONSTRAINT flow \
                 ON COLUMN status TRANSITIONS ('draft' -> 'active')"
            ),
        ] {
            assert_eq!(session.sql(&sql).await, ResponseStatus::Ok, "{sql}");
        }
        session
    }

    async fn sql(&mut self, sql: &str) -> ResponseStatus {
        self.seq += 1;
        send_sql(&mut self.stream, self.seq, sql).await.status
    }

    /// Set row `a`'s status with the `DocumentUpdate` opcode.
    async fn update_opcode(&mut self, collection: &str, status: &str) -> ResponseStatus {
        self.seq += 1;
        let packed = nodedb_types::value_to_msgpack(&Value::String(status.into())).expect("encode");
        send_request(
            &mut self.stream,
            self.seq,
            OpCode::DocumentUpdate,
            TextFields {
                collection: Some(collection.to_string()),
                document_id: Some("a".to_string()),
                updates: Some(vec![("status".to_string(), packed)]),
                ..Default::default()
            },
        )
        .await
        .status
    }

    async fn status(&mut self, collection: &str) -> Option<Vec<Vec<Value>>> {
        self.seq += 1;
        send_sql(
            &mut self.stream,
            self.seq,
            &format!("SELECT status FROM {collection} WHERE id = 'a'"),
        )
        .await
        .rows
    }
}

fn row(status: &str) -> Option<Vec<Vec<Value>>> {
    Some(vec![vec![Value::String(status.into())]])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_document_update_obeys_the_state_constraint() {
    let server = NativeTestServer::start().await;
    let collection = "nst_opcode";
    let mut session = Session::with_flow(&server, collection).await;

    assert_eq!(
        session.update_opcode(collection, "closed").await,
        ResponseStatus::Error,
        "draft -> closed must be refused"
    );
    assert_eq!(session.status(collection).await, row("draft"));
    assert_ne!(
        session.update_opcode(collection, "active").await,
        ResponseStatus::Error,
        "draft -> active must be admitted"
    );
    assert_eq!(session.status(collection).await, row("active"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_update_obeys_the_state_constraint() {
    let server = NativeTestServer::start().await;
    let collection = "nst_sql";
    let mut session = Session::with_flow(&server, collection).await;

    assert_eq!(
        session
            .sql(&format!(
                "UPDATE {collection} SET status = 'closed' WHERE id = 'a'"
            ))
            .await,
        ResponseStatus::Error,
        "draft -> closed must be refused"
    );
    assert_eq!(session.status(collection).await, row("draft"));
    assert_ne!(
        session
            .sql(&format!(
                "UPDATE {collection} SET status = 'active' WHERE id = 'a'"
            ))
            .await,
        ResponseStatus::Error,
        "draft -> active must be admitted"
    );
    assert_eq!(session.status(collection).await, row("active"));
}
