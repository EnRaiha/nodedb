// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` on a statement whose collection fires BEFORE and SYNC AFTER
//! bodies. The statement runs in a transaction with its bodies, an implicit
//! one outside a block, and answers with the row as the BEFORE body left it.

use crate::harness::TestServer;

const STRICT: &str = "(id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')";

async fn create(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name} {STRICT}"))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

async fn rows(server: &TestServer, sql: &str) -> Vec<Vec<String>> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// `orders` whose BEFORE bodies add 10 on insert and 100 on update, with SYNC
/// AFTER bodies logging each insert, update and delete to `audit`.
async fn orders_with_triggers(server: &TestServer) {
    create(server, "orders").await;
    create(server, "audit").await;
    for ddl in [
        "CREATE TRIGGER bump_insert BEFORE INSERT ON orders FOR EACH ROW \
         BEGIN NEW.v := NEW.v + 10; END",
        "CREATE TRIGGER bump_update BEFORE UPDATE ON orders FOR EACH ROW \
         BEGIN NEW.v := NEW.v + 100; END",
        "CREATE SYNC TRIGGER log_insert AFTER INSERT ON orders FOR EACH ROW \
         BEGIN INSERT INTO audit (id, v) VALUES (NEW.id || '-i', NEW.v); END",
        "CREATE SYNC TRIGGER log_update AFTER UPDATE ON orders FOR EACH ROW \
         BEGIN INSERT INTO audit (id, v) VALUES (NEW.id || '-u', NEW.v); END",
        "CREATE SYNC TRIGGER log_delete AFTER DELETE ON orders FOR EACH ROW \
         BEGIN INSERT INTO audit (id, v) VALUES (OLD.id || '-d', OLD.v); END",
    ] {
        server
            .exec(ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
}

fn row(id: &str, v: &str) -> Vec<String> {
    vec![id.to_string(), v.to_string()]
}

/// An autocommit INSERT returns the row the BEFORE body changed, and the SYNC
/// AFTER body sees the same row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_insert_returns_the_post_trigger_row() {
    let server = TestServer::start().await;
    orders_with_triggers(&server).await;

    let returned = rows(
        &server,
        "INSERT INTO orders (id, v) VALUES ('o1', 1) RETURNING id, v",
    )
    .await;
    assert_eq!(returned, vec![row("o1", "11")]);
    assert_eq!(
        rows(&server, "SELECT id, v FROM orders").await,
        vec![row("o1", "11")]
    );
    assert_eq!(
        rows(&server, "SELECT id, v FROM audit").await,
        vec![row("o1-i", "11")]
    );
}

/// Inside BEGIN..COMMIT, INSERT, UPDATE and DELETE each return their row, and
/// COMMIT keeps every write with its bodies' writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_inside_a_block_reflects_the_bodies() {
    let server = TestServer::start().await;
    orders_with_triggers(&server).await;
    server
        .exec("INSERT INTO orders (id, v) VALUES ('gone', 5)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        rows(
            &server,
            "INSERT INTO orders (id, v) VALUES ('o2', 2) RETURNING id, v"
        )
        .await,
        vec![row("o2", "12")]
    );
    assert_eq!(
        rows(
            &server,
            "UPDATE orders SET v = 3 WHERE id = 'o2' RETURNING id, v"
        )
        .await,
        vec![row("o2", "103")]
    );
    assert_eq!(
        rows(
            &server,
            "DELETE FROM orders WHERE id = 'gone' RETURNING id, v"
        )
        .await,
        vec![row("gone", "15")]
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        rows(&server, "SELECT id, v FROM orders").await,
        vec![row("o2", "103")]
    );
    assert_eq!(
        rows(&server, "SELECT id, v FROM audit ORDER BY id").await,
        vec![
            row("gone-d", "15"),
            row("gone-i", "15"),
            row("o2-i", "12"),
            row("o2-u", "103"),
        ]
    );
}

/// A ROLLBACK after a RETURNING write undoes the write and its bodies'
/// writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_undoes_a_returning_write() {
    let server = TestServer::start().await;
    orders_with_triggers(&server).await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        rows(
            &server,
            "INSERT INTO orders (id, v) VALUES ('r1', 1) RETURNING id, v"
        )
        .await,
        vec![row("r1", "11")]
    );
    server.exec("ROLLBACK").await.unwrap();

    assert!(rows(&server, "SELECT id FROM orders").await.is_empty());
    assert!(rows(&server, "SELECT id FROM audit").await.is_empty());
}
