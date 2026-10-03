// SPDX-License-Identifier: BUSL-1.1

//! General CHECK constraints hold on every native SQL write, as they do over
//! pgwire: an INSERT or UPDATE whose row fails a constraint is refused, with
//! or without triggers on the collection.
//!
//! DDL and the reads run over pgwire. The writes under test run over one
//! native connection.

use nodedb_test_support::native_harness::{do_handshake, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{HelloFrame, NativeResponse};
use tokio::net::TcpStream;

/// One native connection with its request sequence.
struct Native {
    stream: TcpStream,
    seq: u64,
}

impl Native {
    async fn open(server: &TestServer) -> Self {
        let addr = format!("127.0.0.1:{}", server.native_port)
            .parse()
            .expect("native addr");
        let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
            .await
            .expect("native handshake");
        Self { stream, seq: 0 }
    }

    async fn sql(&mut self, sql: &str) -> NativeResponse {
        self.seq += 1;
        send_sql(&mut self.stream, self.seq, sql).await
    }

    async fn ok(&mut self, sql: &str) {
        let response = self.sql(sql).await;
        assert_eq!(
            response.status,
            ResponseStatus::Ok,
            "{sql}: {:?}",
            response.error
        );
    }

    /// Run `sql` and require a refusal whose message names `constraint`.
    async fn refused(&mut self, sql: &str, constraint: &str) {
        let response = self.sql(sql).await;
        assert_eq!(
            response.status,
            ResponseStatus::Error,
            "{sql} must be refused"
        );
        let error = response.error.expect("error payload expected");
        assert!(
            error.message.contains(constraint),
            "{sql}: the refusal must name {constraint}, got {}: {}",
            error.code,
            error.message
        );
    }
}

async fn items_with_check(server: &TestServer) {
    server.exec("CREATE COLLECTION items").await.unwrap();
    server
        .exec("ALTER COLLECTION items ADD CONSTRAINT valid_qty CHECK (NEW.qty >= 1)")
        .await
        .unwrap();
}

async fn rows(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// A native INSERT on a collection without triggers is checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_insert_is_checked_without_triggers() {
    let server = TestServer::start().await;
    items_with_check(&server).await;
    let mut native = Native::open(&server).await;

    native
        .ok("INSERT INTO items (id, qty) VALUES ('i1', 10)")
        .await;
    native
        .refused("INSERT INTO items (id, qty) VALUES ('i2', 0)", "valid_qty")
        .await;

    assert_eq!(
        rows(&server, "SELECT id FROM items ORDER BY id").await,
        vec!["i1".to_string()]
    );
}

/// A native UPDATE on a collection without triggers is checked against the
/// merged row, and a refused update leaves the row unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_update_is_checked_without_triggers() {
    let server = TestServer::start().await;
    items_with_check(&server).await;
    let mut native = Native::open(&server).await;

    native
        .ok("INSERT INTO items (id, qty) VALUES ('i1', 10)")
        .await;
    native
        .refused("UPDATE items SET qty = 0 WHERE id = 'i1'", "valid_qty")
        .await;
    native.ok("UPDATE items SET qty = 3 WHERE id = 'i1'").await;

    assert_eq!(
        rows(&server, "SELECT qty FROM items WHERE id = 'i1'").await,
        vec!["3".to_string()]
    );
}

/// A native UPDATE inside a block is checked against the row the block
/// itself wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_write_in_a_block_is_checked() {
    let server = TestServer::start().await;
    items_with_check(&server).await;
    let mut native = Native::open(&server).await;

    native.ok("BEGIN").await;
    native
        .ok("INSERT INTO items (id, qty) VALUES ('i1', 5)")
        .await;
    native
        .refused("UPDATE items SET qty = -1 WHERE id = 'i1'", "valid_qty")
        .await;
    native.ok("ROLLBACK").await;

    assert!(rows(&server, "SELECT id FROM items").await.is_empty());
}

/// On a collection with a BEFORE body, the check judges the row the body
/// left: a repaired value passes and a broken one is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_check_judges_the_post_trigger_row() {
    let server = TestServer::start().await;
    items_with_check(&server).await;
    server
        .exec(
            "CREATE TRIGGER flip BEFORE INSERT ON items FOR EACH ROW \
             BEGIN NEW.qty := 0 - NEW.qty; END",
        )
        .await
        .unwrap();
    let mut native = Native::open(&server).await;

    native
        .ok("INSERT INTO items (id, qty) VALUES ('repaired', -3)")
        .await;
    native
        .refused(
            "INSERT INTO items (id, qty) VALUES ('broken', 3)",
            "valid_qty",
        )
        .await;

    assert_eq!(
        rows(&server, "SELECT id FROM items ORDER BY id").await,
        vec!["repaired".to_string()]
    );
}
