// SPDX-License-Identifier: BUSL-1.1

//! A predicate `UPDATE ... WHERE` or `DELETE ... WHERE` on a collection with
//! BEFORE and SYNC AFTER row triggers. Each row the predicate matches fires
//! its bodies, including a row the same transaction inserted, and the
//! statement's writes commit with the bodies' writes.

use crate::harness::TestServer;

const STRICT: &str = "(id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')";

async fn rows(server: &TestServer, sql: &str) -> Vec<Vec<String>> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn row(id: &str, v: &str) -> Vec<String> {
    vec![id.to_string(), v.to_string()]
}

/// `orders` (created by `create_orders`) with a BEFORE UPDATE body adding
/// 100, and SYNC AFTER bodies logging each update and delete to `audit`.
async fn triggers_on_orders(server: &TestServer) {
    server
        .exec(&format!("CREATE COLLECTION audit {STRICT}"))
        .await
        .unwrap();
    for ddl in [
        "CREATE TRIGGER bump BEFORE UPDATE ON orders FOR EACH ROW \
         BEGIN NEW.v := NEW.v + 100; END",
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

/// Run the scenario on `orders`: three committed rows, then a block that
/// inserts a fourth, updates by predicate and deletes by predicate.
async fn predicate_writes_fire_row_bodies(server: &TestServer) {
    triggers_on_orders(server).await;
    for sql in [
        "INSERT INTO orders (id, v) VALUES ('a', 1)",
        "INSERT INTO orders (id, v) VALUES ('b', 2)",
        "INSERT INTO orders (id, v) VALUES ('c', 3)",
    ] {
        server.exec(sql).await.unwrap();
    }

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO orders (id, v) VALUES ('d', 4)")
        .await
        .unwrap();
    server
        .exec("UPDATE orders SET v = 10 WHERE v >= 2")
        .await
        .unwrap();
    server
        .exec("DELETE FROM orders WHERE id = 'd' OR v < 2")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        rows(server, "SELECT id, v FROM orders ORDER BY id").await,
        vec![row("b", "110"), row("c", "110")],
        "every matched row took the BEFORE body's change"
    );
    assert_eq!(
        rows(server, "SELECT id, v FROM audit ORDER BY id").await,
        vec![
            row("a-d", "1"),
            row("b-u", "110"),
            row("c-u", "110"),
            row("d-d", "110"),
            row("d-u", "110"),
        ],
        "each matched row fired its bodies, the row the block inserted too"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_predicate_writes_fire_row_bodies() {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION orders {STRICT}"))
        .await
        .unwrap();
    predicate_writes_fire_row_bodies(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_predicate_writes_fire_row_bodies() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION orders").await.unwrap();
    predicate_writes_fire_row_bodies(&server).await;
}

/// A ROLLBACK after a predicate write undoes every matched row's write and
/// every body's write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_undoes_a_predicate_write_and_its_bodies() {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION orders {STRICT}"))
        .await
        .unwrap();
    triggers_on_orders(&server).await;
    server
        .exec("INSERT INTO orders (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE orders SET v = 5 WHERE v = 1")
        .await
        .unwrap();
    server.exec("DELETE FROM orders WHERE v > 0").await.unwrap();
    server.exec("ROLLBACK").await.unwrap();

    assert_eq!(
        rows(&server, "SELECT id, v FROM orders").await,
        vec![row("a", "1")]
    );
    assert!(rows(&server, "SELECT id FROM audit").await.is_empty());
}
