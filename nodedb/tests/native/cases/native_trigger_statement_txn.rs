// SPDX-License-Identifier: BUSL-1.1

//! BEFORE, INSTEAD OF and SYNC AFTER bodies join their triggering statement's
//! transaction over the native protocol, as they do over pgwire: the
//! client's block, or an implicit one outside a block. The statement's write
//! and the bodies' writes commit together or not at all, and `RETURNING`
//! answers with the row the BEFORE body left.
//!
//! DDL and the checks run over pgwire. The statements under test run over
//! one native connection, since transaction state is keyed by the native
//! session.
//!
//! This harness runs no Calvin sequencer. A transaction whose writes span two
//! vShards commits only through Calvin, so the triggering collection and the
//! collection its bodies write share one vShard
//! ([`trigger_collections_are_co_resident`]).

use nodedb_test_support::native_harness::{do_handshake, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::value::Value;
use tokio::net::TcpStream;

const STRICT: &str = "(id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')";

/// The collection whose writes fire the BEFORE and SYNC AFTER bodies.
const ORDERS: &str = "reservations";
/// The collection those bodies write, on the same vShard as [`ORDERS`].
const AUDIT: &str = "reservations_activity";

/// The premise every test that commits [`ORDERS`] and [`AUDIT`] together rests
/// on: one vShard holds both, so the commit needs no Calvin sequencer.
#[test]
fn trigger_collections_are_co_resident() {
    let db = nodedb_types::DatabaseId::DEFAULT;
    assert_eq!(
        nodedb_types::CollectionKey::from_bare(db, ORDERS).vshard(),
        nodedb_types::CollectionKey::from_bare(db, AUDIT).vshard(),
        "rename the collections until the two hashes agree again"
    );
}

async fn native_session(server: &TestServer) -> TcpStream {
    let addr = format!("127.0.0.1:{}", server.native_port)
        .parse()
        .expect("native addr");
    let (stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    stream
}

/// One native connection with its request sequence.
struct Native {
    stream: TcpStream,
    seq: u64,
}

impl Native {
    async fn open(server: &TestServer) -> Self {
        Self {
            stream: native_session(server).await,
            seq: 0,
        }
    }

    async fn sql(&mut self, sql: &str) -> nodedb_types::protocol::NativeResponse {
        self.seq += 1;
        send_sql(&mut self.stream, self.seq, sql).await
    }

    async fn ok(&mut self, sql: &str) -> nodedb_types::protocol::NativeResponse {
        let response = self.sql(sql).await;
        assert_eq!(
            response.status,
            ResponseStatus::Ok,
            "{sql}: {:?}",
            response.error
        );
        response
    }
}

async fn create(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name} {STRICT}"))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

async fn ids(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT id FROM {collection} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

/// [`ORDERS`] with a BEFORE body that adds 10 to `v` and writes [`AUDIT`], and
/// a SYNC AFTER body that writes [`AUDIT`].
async fn orders_with_sync_triggers(server: &TestServer) {
    create(server, ORDERS).await;
    create(server, AUDIT).await;
    for ddl in [
        format!(
            "CREATE TRIGGER audit_before BEFORE INSERT ON {ORDERS} FOR EACH ROW \
             BEGIN NEW.v := NEW.v + 10; \
             INSERT INTO {AUDIT} (id, v) VALUES (NEW.id || '-b', NEW.v); END"
        ),
        format!(
            "CREATE SYNC TRIGGER audit_after AFTER INSERT ON {ORDERS} FOR EACH ROW \
             BEGIN INSERT INTO {AUDIT} (id, v) VALUES (NEW.id || '-a', NEW.v); END"
        ),
    ] {
        server
            .exec(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
}

/// Inside BEGIN..COMMIT the BEFORE and SYNC AFTER bodies fire, and their
/// writes commit with the statement's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sync_triggers_fire_inside_a_transaction_block() {
    let server = TestServer::start().await;
    orders_with_sync_triggers(&server).await;
    let mut native = Native::open(&server).await;

    native.ok("BEGIN").await;
    native
        .ok(&format!("INSERT INTO {ORDERS} (id, v) VALUES ('t1', 1)"))
        .await;
    native.ok("COMMIT").await;

    assert_eq!(ids(&server, ORDERS).await, vec!["t1".to_string()]);
    assert_eq!(
        ids(&server, AUDIT).await,
        vec!["t1-a".to_string(), "t1-b".to_string()]
    );
    assert_eq!(
        server
            .query_text(&format!("SELECT v FROM {ORDERS} WHERE id = 't1'"))
            .await
            .unwrap(),
        vec!["11".to_string()],
        "the BEFORE body's change committed"
    );
}

/// A ROLLBACK undoes the statement's write and its bodies' writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_rollback_undoes_trigger_writes() {
    let server = TestServer::start().await;
    orders_with_sync_triggers(&server).await;
    let mut native = Native::open(&server).await;

    native.ok("BEGIN").await;
    native
        .ok(&format!("INSERT INTO {ORDERS} (id, v) VALUES ('r1', 1)"))
        .await;
    native.ok("ROLLBACK").await;

    assert!(ids(&server, ORDERS).await.is_empty());
    assert!(ids(&server, AUDIT).await.is_empty());
}

/// An autocommit statement that fails after its bodies ran leaves none of
/// their writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_failed_statement_leaves_no_body_writes() {
    let server = TestServer::start().await;
    orders_with_sync_triggers(&server).await;
    let mut native = Native::open(&server).await;

    native
        .ok(&format!("INSERT INTO {ORDERS} (id, v) VALUES ('dup', 1)"))
        .await;
    let failed = native
        .sql(&format!(
            "INSERT INTO {ORDERS} (id, v) VALUES ('m1', 3), ('dup', 4)"
        ))
        .await;
    assert_eq!(
        failed.status,
        ResponseStatus::Error,
        "the second row's duplicate key fails the statement"
    );

    assert_eq!(ids(&server, ORDERS).await, vec!["dup".to_string()]);
    assert_eq!(
        ids(&server, AUDIT).await,
        vec!["dup-a".to_string(), "dup-b".to_string()],
        "only the first, committed insert left body writes"
    );
}

/// An autocommit `RETURNING` answers with the row the BEFORE body changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_returning_reflects_the_before_body() {
    let server = TestServer::start().await;
    orders_with_sync_triggers(&server).await;
    let mut native = Native::open(&server).await;

    let returned = native
        .ok(&format!(
            "INSERT INTO {ORDERS} (id, v) VALUES ('o1', 1) RETURNING v"
        ))
        .await;
    let rows = returned.rows.expect("RETURNING answers with rows");
    assert_eq!(
        rows.first().and_then(|row| row.first()).cloned(),
        Some(Value::Integer(11))
    );

    native.ok("BEGIN").await;
    let returned = native
        .ok(&format!(
            "INSERT INTO {ORDERS} (id, v) VALUES ('o2', 2) RETURNING v"
        ))
        .await;
    let rows = returned.rows.expect("RETURNING answers with rows");
    assert_eq!(
        rows.first().and_then(|row| row.first()).cloned(),
        Some(Value::Integer(12))
    );
    native.ok("COMMIT").await;
    assert_eq!(
        ids(&server, ORDERS).await,
        vec!["o1".to_string(), "o2".to_string()]
    );
}

/// An INSTEAD OF body replaces the write: the row never lands, and the
/// body's own write commits with the statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_instead_of_replaces_the_write() {
    let server = TestServer::start().await;
    create(&server, "requests").await;
    create(&server, "audit").await;
    server
        .exec(
            "CREATE TRIGGER redirect INSTEAD OF INSERT ON requests FOR EACH ROW \
             BEGIN INSERT INTO audit (id, v) VALUES (NEW.id || '-redirected', NEW.v); END",
        )
        .await
        .unwrap();
    let mut native = Native::open(&server).await;

    native
        .ok("INSERT INTO requests (id, v) VALUES ('q1', 1)")
        .await;
    native.ok("BEGIN").await;
    native
        .ok("INSERT INTO requests (id, v) VALUES ('q2', 2)")
        .await;
    native.ok("COMMIT").await;

    assert!(ids(&server, "requests").await.is_empty());
    assert_eq!(
        ids(&server, "audit").await,
        vec!["q1-redirected".to_string(), "q2-redirected".to_string()]
    );
}
