// SPDX-License-Identifier: BUSL-1.1

//! `CONVERT COLLECTION` carries typeguards and the source key onto the
//! converted schema.
//!
//! - A guard `VALUE` becomes a generated column and keeps overwriting.
//! - A constant guard `DEFAULT` becomes a column `DEFAULT`.
//! - A cross-field guard `DEFAULT` is refused, naming field and clause.
//! - The converted schema keeps the source primary key.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_carries_a_cross_field_guard_value_as_a_generated_column() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cc_value (id TEXT PRIMARY KEY, status TEXT, lowered TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE TYPEGUARD ON cc_value (lowered STRING VALUE LOWER(status))")
        .await
        .unwrap();
    server
        .exec(
            "CONVERT COLLECTION cc_value TO document_strict \
             (id TEXT, status TEXT, lowered TEXT)",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO cc_value (id, status) VALUES ('k1', 'ACTIVE')")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT lowered FROM cc_value WHERE id = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the row must land: {rows:?}");
    assert!(rows[0].contains("active"), "VALUE must compute: {rows:?}");

    // The column is generated: a write that supplies it is refused.
    server
        .expect_error(
            "INSERT INTO cc_value (id, status, lowered) VALUES ('k2', 'B', 'zzz')",
            "428C9",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_refuses_a_cross_field_guard_default() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cc_default (id TEXT PRIMARY KEY, status TEXT, lowered TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE TYPEGUARD ON cc_default (lowered STRING DEFAULT LOWER(status))")
        .await
        .unwrap();

    server
        .expect_error(
            "CONVERT COLLECTION cc_default TO document_strict \
             (id TEXT, status TEXT, lowered TEXT)",
            "0A000",
        )
        .await;
    server
        .expect_error(
            "CONVERT COLLECTION cc_default TO document_strict \
             (id TEXT, status TEXT, lowered TEXT)",
            "lowered",
        )
        .await;

    // A refused conversion leaves the collection schemaless, guards intact.
    let guards = server
        .query_text("SHOW TYPEGUARD ON cc_default")
        .await
        .unwrap();
    assert_eq!(guards.len(), 1, "the guard must survive: {guards:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_refuses_a_guard_value_reading_a_field_outside_the_schema() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION cc_missing").await.unwrap();
    server
        .exec("CREATE TYPEGUARD ON cc_missing (lowered STRING VALUE LOWER(status))")
        .await
        .unwrap();

    // Derived from the guards alone, the schema has no `status` column.
    server
        .expect_error("CONVERT COLLECTION cc_missing TO document_strict", "42703")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_carries_a_constant_guard_value_as_always_overwrite() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cc_const (id TEXT PRIMARY KEY, origin TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE TYPEGUARD ON cc_const (origin STRING VALUE 'server')")
        .await
        .unwrap();
    server
        .exec("CONVERT COLLECTION cc_const TO document_strict (id TEXT, origin TEXT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO cc_const (id) VALUES ('c1')")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT origin FROM cc_const WHERE id = 'c1'")
        .await
        .unwrap();
    assert!(rows[0].contains("server"), "VALUE must fill: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_keeps_the_source_primary_key() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cc_key (sku TEXT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO cc_key (sku, name) VALUES ('s1', 'a')")
        .await
        .unwrap();
    server
        .exec("CONVERT COLLECTION cc_key TO document_strict (sku TEXT, name TEXT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO cc_key (sku, name) VALUES ('s2', 'b')")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT name FROM cc_key WHERE sku = 's2'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the keyed row must be found: {rows:?}");

    // The key is still the key: a duplicate is refused.
    server
        .expect_error("INSERT INTO cc_key (sku, name) VALUES ('s2', 'c')", "23505")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_refuses_a_column_list_without_the_source_key() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cc_nokey (sku TEXT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    server
        .expect_error(
            "CONVERT COLLECTION cc_nokey TO document_strict (name TEXT)",
            "42601",
        )
        .await;
    server
        .expect_error(
            "CONVERT COLLECTION cc_nokey TO document_strict (name TEXT)",
            "sku",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_refuses_a_column_list_that_rekeys() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cc_rekey (sku TEXT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    server
        .expect_error(
            "CONVERT COLLECTION cc_rekey TO document_strict \
             (sku TEXT, name TEXT PRIMARY KEY)",
            "42601",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_add_column_works_after_convert() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cc_alter (id TEXT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    server
        .exec("CONVERT COLLECTION cc_alter TO document_strict (id TEXT, name TEXT)")
        .await
        .unwrap();
    server
        .exec("ALTER COLLECTION cc_alter ADD COLUMN extra TEXT")
        .await
        .unwrap();
    server
        .exec("INSERT INTO cc_alter (id, name, extra) VALUES ('a1', 'n', 'x')")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT extra FROM cc_alter WHERE id = 'a1'")
        .await
        .unwrap();
    assert!(
        rows[0].contains('x'),
        "the added column must hold: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_reflects_the_converted_schema() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cc_desc (id TEXT PRIMARY KEY, status TEXT, lowered TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE TYPEGUARD ON cc_desc (lowered STRING VALUE LOWER(status))")
        .await
        .unwrap();
    server
        .exec(
            "CONVERT COLLECTION cc_desc TO document_strict \
             (id TEXT, status TEXT, lowered TEXT)",
        )
        .await
        .unwrap();

    let rows = server.query_text_joined("DESCRIBE cc_desc").await.unwrap();
    assert!(
        rows.iter()
            .any(|row| row.contains("lowered") && row.contains("GENERATED")),
        "the generated column must be described: {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row.contains("id") && row.contains("PRIMARY KEY")),
        "the key must be described: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_strict_without_a_key_to_strict_keeps_its_rows() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cc_rowid (name TEXT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO cc_rowid (name) VALUES ('a')")
        .await
        .unwrap();
    server
        .exec("CONVERT COLLECTION cc_rowid TO document_strict (name TEXT, extra TEXT)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT name FROM cc_rowid")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the row must survive: {rows:?}");
    assert!(rows[0].contains('a'), "{rows:?}");
    server
        .exec("INSERT INTO cc_rowid (name) VALUES ('b')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_kv_carries_guards_and_clears_them() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION cc_kv").await.unwrap();
    server
        .exec(
            "CREATE TYPEGUARD ON cc_kv (\
                 status STRING DEFAULT 'new', \
                 qty INT CHECK (qty >= 0)\
             )",
        )
        .await
        .unwrap();
    server
        .exec("CONVERT COLLECTION cc_kv TO kv (id TEXT, status TEXT, qty INT)")
        .await
        .unwrap();

    let guards = server.query_text("SHOW TYPEGUARD ON cc_kv").await.unwrap();
    assert_eq!(guards.len(), 0, "the guards must be cleared: {guards:?}");
    let constraints = server
        .query_text("SHOW CONSTRAINTS ON cc_kv")
        .await
        .unwrap();
    assert!(
        constraints.iter().any(|row| row.contains("_guard_qty")),
        "the CHECK must carry: {constraints:?}"
    );

    server
        .exec("INSERT INTO cc_kv (id, qty) VALUES ('k1', 1)")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT status FROM cc_kv WHERE id = 'k1'")
        .await
        .unwrap();
    assert!(rows[0].contains("new"), "the DEFAULT must carry: {rows:?}");
    server
        .expect_error("INSERT INTO cc_kv (id, qty) VALUES ('k2', -1)", "23514")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_kv_refuses_a_guard_value() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION cc_kv_value").await.unwrap();
    server
        .exec("CREATE TYPEGUARD ON cc_kv_value (origin STRING VALUE 'server')")
        .await
        .unwrap();
    server
        .expect_error(
            "CONVERT COLLECTION cc_kv_value TO kv (id TEXT, origin TEXT)",
            "0A000",
        )
        .await;
}
