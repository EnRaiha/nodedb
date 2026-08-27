// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for SQL transaction behavior.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_persists_buffered_writes() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION txn_test (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO txn_test (id, val) VALUES ('t1', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO txn_test (id, val) VALUES ('t2', 20)")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    let rows = server
        .query_text("SELECT id FROM txn_test WHERE id = 't1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_discards_buffered_write_and_missing_row_is_empty() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION txn_test (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO txn_test (id, val) VALUES ('t3', 30)")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    let rows = server
        .query_text("SELECT id FROM txn_test WHERE id = 't3'")
        .await
        .unwrap();
    assert!(rows.is_empty(), "rolled-back row should not be visible");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_table_add_column_refreshes_strict_schema() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION alter_test (id TEXT PRIMARY KEY, name TEXT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO alter_test (id, name) VALUES ('a1', 'Alice')")
        .await
        .unwrap();

    server
        .exec("ALTER TABLE alter_test ADD COLUMN score INT DEFAULT 0")
        .await
        .unwrap();
    server
        .exec("INSERT INTO alter_test (id, name, score) VALUES ('a3', 'New', 100)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM alter_test WHERE id = 'a3'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("a3"),
        "expected row to include inserted id"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_add_column_refreshes_strict_schema() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION memories (\
                id TEXT PRIMARY KEY, \
                name TEXT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO memories (id, name) VALUES ('m1', 'first')")
        .await
        .unwrap();

    // `ALTER COLLECTION ... ADD COLUMN` must reach the catalog-generic
    // add-column handler — the same path exercised by `ALTER TABLE` above.
    server
        .exec("ALTER COLLECTION memories ADD COLUMN is_latest BOOL DEFAULT true")
        .await
        .unwrap();

    server
        .exec("INSERT INTO memories (id, name, is_latest) VALUES ('m2', 'second', false)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM memories WHERE id = 'm2'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("m2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_drop_column() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION memories (\
                id TEXT PRIMARY KEY, \
                name TEXT NOT NULL, \
                scratch TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO memories (id, name, scratch) VALUES ('m1', 'first', 'temp')")
        .await
        .unwrap();

    server
        .exec("ALTER COLLECTION memories DROP COLUMN scratch")
        .await
        .unwrap();

    // New inserts without the dropped column still succeed, and old data reads.
    server
        .exec("INSERT INTO memories (id, name) VALUES ('m2', 'second')")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT id FROM memories WHERE id = 'm2'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_rename_column() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION memories (\
                id TEXT PRIMARY KEY, \
                name TEXT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO memories (id, name) VALUES ('m1', 'first')")
        .await
        .unwrap();

    server
        .exec("ALTER COLLECTION memories RENAME COLUMN name TO title")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT title FROM memories WHERE id = 'm1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0], "first",
        "expected renamed column 'title' = 'first', got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_alter_column_type() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION measurements (\
                id TEXT PRIMARY KEY, \
                value INT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO measurements (id, value) VALUES ('m1', 42)")
        .await
        .unwrap();

    server
        .exec("ALTER COLLECTION measurements ALTER COLUMN value TYPE BIGINT")
        .await
        .unwrap();

    // Re-insert using the widened type.
    server
        .exec("INSERT INTO measurements (id, value) VALUES ('m2', 9999999999)")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT id FROM measurements WHERE id = 'm2'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

// ── Pre-ALTER row survival tests ──────────────────────────────────────
//
// Every test below verifies that rows written BEFORE a schema-altering DDL
// remain readable with correct values AFTER the DDL. The bug class is:
// catalog schema mutated without row migration or read-time compat shim.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_column_preserves_pre_alter_row_existing_columns() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION ac_preserve (id TEXT PRIMARY KEY, name TEXT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ac_preserve (id, name) VALUES ('a', 'alice')")
        .await
        .unwrap();

    server
        .exec("ALTER TABLE ac_preserve ADD COLUMN note TEXT DEFAULT 'n/a'")
        .await
        .unwrap();

    // Pre-ALTER row must still return correct values for original columns.
    let rows = server
        .query_rows("SELECT id, name FROM ac_preserve WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "pre-ALTER row must be visible");
    // row[0]=id, row[1]=name
    assert_eq!(
        rows[0][1], "alice",
        "original column 'name' must retain its value, got {:?}",
        rows[0]
    );
    // Regression guard: must NOT return null.
    assert_ne!(
        rows[0][1], "null",
        "pre-ALTER row must not have null-everywhere corruption, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_column_returns_default_for_pre_alter_row() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION ac_default (id TEXT PRIMARY KEY, name TEXT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ac_default (id, name) VALUES ('a', 'alice')")
        .await
        .unwrap();

    server
        .exec("ALTER TABLE ac_default ADD COLUMN note TEXT DEFAULT 'n/a'")
        .await
        .unwrap();

    // The new column should virtual-fill with its DEFAULT for pre-ALTER rows.
    let rows = server
        .query_rows("SELECT id, name, note FROM ac_default WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "pre-ALTER row must be visible");
    // row[0]=id, row[1]=name, row[2]=note
    assert_eq!(
        rows[0][2], "n/a",
        "new column must return DEFAULT value for pre-ALTER rows, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_column_then_update_pre_alter_row() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION ac_update (id TEXT PRIMARY KEY, name TEXT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ac_update (id, name) VALUES ('a', 'alice')")
        .await
        .unwrap();

    server
        .exec("ALTER TABLE ac_update ADD COLUMN note TEXT DEFAULT 'n/a'")
        .await
        .unwrap();

    // Updating a pre-ALTER row must succeed and preserve all columns.
    server
        .exec("UPDATE ac_update SET note = 'updated' WHERE id = 'a'")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, name, note FROM ac_update WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // row[0]=id, row[1]=name, row[2]=note
    assert_eq!(
        rows[0][1], "alice",
        "original column must survive update, got {:?}",
        rows[0]
    );
    assert_eq!(
        rows[0][2], "updated",
        "updated column must reflect new value, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_add_columns_preserves_pre_alter_row() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION ac_multi (id TEXT PRIMARY KEY, name TEXT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ac_multi (id, name) VALUES ('a', 'alice')")
        .await
        .unwrap();

    server
        .exec("ALTER TABLE ac_multi ADD COLUMN col1 INT DEFAULT 0")
        .await
        .unwrap();
    server
        .exec("ALTER TABLE ac_multi ADD COLUMN col2 TEXT DEFAULT 'x'")
        .await
        .unwrap();

    // Two sequential ADD COLUMNs compound the schema drift — pre-ALTER row
    // must still be readable with correct values and defaults.
    let rows = server
        .query_rows("SELECT id, name, col1, col2 FROM ac_multi WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "pre-ALTER row must be visible after two ADD COLUMNs"
    );
    // row[0]=id, row[1]=name, row[2]=col1, row[3]=col2
    assert_eq!(
        rows[0][1], "alice",
        "original column must retain value, got {:?}",
        rows[0]
    );
    // Regression guard: null-everywhere means total schema-data offset corruption.
    assert_ne!(
        rows[0][1], "null",
        "must not exhibit null-everywhere corruption, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_column_preserves_pre_alter_row_remaining_columns() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION dc_preserve (\
                id TEXT PRIMARY KEY, \
                name TEXT NOT NULL, \
                scratch TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO dc_preserve (id, name, scratch) VALUES ('a', 'alice', 'temp')")
        .await
        .unwrap();

    server
        .exec("ALTER COLLECTION dc_preserve DROP COLUMN scratch")
        .await
        .unwrap();

    // Remaining columns of the pre-ALTER row must read correctly.
    let rows = server
        .query_rows("SELECT id, name FROM dc_preserve WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "pre-ALTER row must be visible after DROP COLUMN"
    );
    // row[0]=id, row[1]=name
    assert_eq!(
        rows[0][1], "alice",
        "remaining column 'name' must retain its value after DROP COLUMN, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rename_column_preserves_pre_alter_row_value() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION rc_preserve (\
                id TEXT PRIMARY KEY, \
                name TEXT NOT NULL, \
                score INT DEFAULT 0) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO rc_preserve (id, name, score) VALUES ('a', 'alice', 42)")
        .await
        .unwrap();

    server
        .exec("ALTER COLLECTION rc_preserve RENAME COLUMN score TO points")
        .await
        .unwrap();

    // Pre-ALTER row must be readable under the new column name with correct value.
    let rows = server
        .query_rows("SELECT id, name, points FROM rc_preserve WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // row[0]=id, row[1]=name, row[2]=points
    assert_eq!(
        rows[0][2], "42",
        "renamed column must retain pre-ALTER value, got {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_column_type_preserves_pre_alter_row_value() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION at_preserve (\
                id TEXT PRIMARY KEY, \
                value INT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO at_preserve (id, value) VALUES ('a', 42)")
        .await
        .unwrap();

    server
        .exec("ALTER COLLECTION at_preserve ALTER COLUMN value TYPE BIGINT")
        .await
        .unwrap();

    // Pre-ALTER row must still read correctly after type widening.
    let rows = server
        .query_rows("SELECT id, value FROM at_preserve WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // row[0]=id, row[1]=value
    assert_eq!(
        rows[0][1], "42",
        "value must survive ALTER COLUMN TYPE, got {:?}",
        rows[0]
    );
}
