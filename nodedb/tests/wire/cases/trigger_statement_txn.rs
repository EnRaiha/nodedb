// SPDX-License-Identifier: BUSL-1.1

//! BEFORE and SYNC AFTER bodies join their triggering statement's
//! transaction: the client's block, or an implicit one outside a block. The
//! statement's write and the bodies' writes commit together or not at all,
//! the bodies' rows fire no trigger, and an autocommit client row fires its
//! ASYNC triggers, not its DEFERRED ones.

use std::time::Duration;

use crate::harness::TestServer;

const STRICT: &str = "(id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')";

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

/// Wait until `collection` holds `id`, the ASYNC trigger path's positive
/// control.
async fn wait_for(server: &TestServer, collection: &str, id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !ids(server, collection).await.iter().any(|row| row == id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{collection} never received {id}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// `orders` with a BEFORE and a SYNC AFTER trigger, each writing `audit`.
async fn orders_with_sync_triggers(server: &TestServer) {
    create(server, "orders").await;
    create(server, "audit").await;
    server
        .exec(
            "CREATE TRIGGER audit_before BEFORE INSERT ON orders FOR EACH ROW \
             BEGIN INSERT INTO audit (id, v) VALUES (NEW.id || '-b', NEW.v); END",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE SYNC TRIGGER audit_after AFTER INSERT ON orders FOR EACH ROW \
             BEGIN INSERT INTO audit (id, v) VALUES (NEW.id || '-a', NEW.v); END",
        )
        .await
        .unwrap();
}

/// Inside BEGIN..COMMIT the BEFORE and SYNC AFTER bodies fire, and their
/// writes commit with the statement's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_triggers_fire_inside_a_transaction_block() {
    let server = TestServer::start().await;
    orders_with_sync_triggers(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO orders (id, v) VALUES ('t1', 1)")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    assert_eq!(ids(&server, "orders").await, vec!["t1".to_string()]);
    assert_eq!(
        ids(&server, "audit").await,
        vec!["t1-a".to_string(), "t1-b".to_string()]
    );
}

/// A ROLLBACK undoes the statement's write and its bodies' writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_undoes_trigger_writes() {
    let server = TestServer::start().await;
    orders_with_sync_triggers(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO orders (id, v) VALUES ('r1', 1)")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    assert!(ids(&server, "orders").await.is_empty());
    assert!(ids(&server, "audit").await.is_empty());
}

/// An autocommit statement that fails after its bodies ran leaves none of
/// their writes: the BEFORE body of a duplicate insert, and the SYNC AFTER
/// body of a multi-row insert whose second row fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_statement_leaves_no_body_writes() {
    let server = TestServer::start().await;
    orders_with_sync_triggers(&server).await;
    server
        .exec("INSERT INTO orders (id, v) VALUES ('dup', 1)")
        .await
        .unwrap();

    assert!(
        server
            .exec("INSERT INTO orders (id, v) VALUES ('dup', 2)")
            .await
            .is_err(),
        "a duplicate primary key fails the statement"
    );
    assert!(
        server
            .exec("INSERT INTO orders (id, v) VALUES ('m1', 3), ('dup', 4)")
            .await
            .is_err(),
        "the second row's duplicate key fails the statement"
    );

    assert_eq!(ids(&server, "orders").await, vec!["dup".to_string()]);
    assert_eq!(
        ids(&server, "audit").await,
        vec!["dup-a".to_string(), "dup-b".to_string()],
        "only the first, committed insert left body writes"
    );
}

/// A body's writes fire no trigger: `audit` has an ASYNC trigger, which
/// fires for a client's insert and never for the BEFORE body's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn body_writes_never_refire_triggers() {
    let server = TestServer::start().await;
    orders_with_sync_triggers(&server).await;
    create(&server, "echo").await;
    server
        .exec(
            "CREATE TRIGGER audit_echo AFTER INSERT ON audit FOR EACH ROW \
             BEGIN INSERT INTO echo (id, v) VALUES (NEW.id, NEW.v); END",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO orders (id, v) VALUES ('o1', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO audit (id, v) VALUES ('direct', 1)")
        .await
        .unwrap();

    wait_for(&server, "echo", "direct").await;
    assert_eq!(
        ids(&server, "echo").await,
        vec!["direct".to_string()],
        "the bodies' audit rows fired no trigger"
    );
}

/// A body that writes its own triggering collection: the client's row fires
/// the collection's ASYNC trigger, and the body's row in the same collection
/// does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_body_writing_its_own_collection_does_not_refire() {
    let server = TestServer::start().await;
    create(&server, "orders").await;
    create(&server, "echo").await;
    server
        .exec(
            "CREATE TRIGGER shadow BEFORE INSERT ON orders FOR EACH ROW \
             BEGIN INSERT INTO orders (id, v) VALUES (NEW.id || '-shadow', 0); END",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE TRIGGER orders_echo AFTER INSERT ON orders FOR EACH ROW \
             BEGIN INSERT INTO echo (id, v) VALUES (NEW.id, NEW.v); END",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO orders (id, v) VALUES ('o1', 1)")
        .await
        .unwrap();

    assert_eq!(
        ids(&server, "orders").await,
        vec!["o1".to_string(), "o1-shadow".to_string()]
    );
    wait_for(&server, "echo", "o1").await;
    assert_eq!(
        ids(&server, "echo").await,
        vec!["o1".to_string()],
        "the body's row in the same collection fired no trigger"
    );
}

/// An autocommit client row committed in an implicit transaction fires its
/// ASYNC triggers, as any autocommit write does, and no DEFERRED trigger.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_client_rows_fire_async_not_deferred() {
    let server = TestServer::start().await;
    orders_with_sync_triggers(&server).await;
    create(&server, "async_log").await;
    create(&server, "deferred_log").await;
    server
        .exec(
            "CREATE TRIGGER orders_async AFTER INSERT ON orders FOR EACH ROW \
             BEGIN INSERT INTO async_log (id, v) VALUES (NEW.id, NEW.v); END",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE DEFERRED TRIGGER orders_deferred AFTER INSERT ON orders FOR EACH ROW \
             BEGIN INSERT INTO deferred_log (id, v) VALUES (NEW.id, NEW.v); END",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO orders (id, v) VALUES ('o1', 1)")
        .await
        .unwrap();

    wait_for(&server, "async_log", "o1").await;
    // The DEFERRED dispatcher runs on the same Event Plane pass that fired
    // the ASYNC trigger: once that one landed, a DEFERRED fire has landed too.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        ids(&server, "deferred_log").await.is_empty(),
        "an autocommit client row fires no DEFERRED trigger"
    );
}

/// A CHECK constraint judges the row the BEFORE body left, not the row the
/// client sent: a body that repairs a value lets the write through, and a
/// body that breaks one fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_constraints_judge_the_post_trigger_row() {
    let server = TestServer::start().await;
    create(&server, "orders").await;
    server
        .exec("ALTER COLLECTION orders ADD CONSTRAINT positive_v CHECK (NEW.v > 0)")
        .await
        .unwrap();
    server
        .exec(
            "CREATE TRIGGER flip BEFORE INSERT ON orders FOR EACH ROW \
             BEGIN NEW.v := 0 - NEW.v; END",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO orders (id, v) VALUES ('repaired', -3)")
        .await
        .unwrap();
    server
        .expect_error(
            "INSERT INTO orders (id, v) VALUES ('broken', 3)",
            "positive_v",
        )
        .await;

    assert_eq!(ids(&server, "orders").await, vec!["repaired".to_string()]);
}

/// An UPSERT fires the family of the write it makes: the INSERT family's
/// BEFORE body for a new row, the UPDATE family's for an existing one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upsert_fires_the_family_of_the_write_it_makes() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION docs").await.unwrap();
    create(&server, "audit").await;
    server
        .exec(
            "CREATE TRIGGER docs_before_insert BEFORE INSERT ON docs FOR EACH ROW \
             BEGIN INSERT INTO audit (id, v) VALUES (NEW.id || '-bi', 1); END",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE TRIGGER docs_before_update BEFORE UPDATE ON docs FOR EACH ROW \
             BEGIN INSERT INTO audit (id, v) VALUES (NEW.id || '-bu', 1); END",
        )
        .await
        .unwrap();

    server
        .exec("UPSERT INTO docs (id, v) VALUES ('d1', 1)")
        .await
        .unwrap();
    server
        .exec("UPSERT INTO docs (id, v) VALUES ('d1', 2)")
        .await
        .unwrap();

    assert_eq!(
        ids(&server, "audit").await,
        vec!["d1-bi".to_string(), "d1-bu".to_string()]
    );
}

/// The DSL insert `INSERT INTO c { ... }` runs its write and its bodies in one
/// transaction: a SYNC AFTER body that fails leaves neither the write nor the
/// BEFORE body's write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dsl_insert_commits_with_its_bodies() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION docs").await.unwrap();
    create(&server, "audit").await;
    server
        .exec(
            "CREATE TRIGGER docs_before BEFORE INSERT ON docs FOR EACH ROW \
             BEGIN INSERT INTO audit (id, v) VALUES (NEW.id || '-b', 1); END",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE SYNC TRIGGER docs_after AFTER INSERT ON docs FOR EACH ROW \
             BEGIN RAISE EXCEPTION 'stop'; END",
        )
        .await
        .unwrap();

    server
        .expect_error("INSERT INTO docs { id: 'd1', v: 1 }", "stop")
        .await;

    assert!(
        ids(&server, "docs").await.is_empty(),
        "the write rolled back"
    );
    assert!(
        ids(&server, "audit").await.is_empty(),
        "the BEFORE body's write rolled back"
    );
}
