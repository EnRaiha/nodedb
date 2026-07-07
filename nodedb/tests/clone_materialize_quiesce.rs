// SPDX-License-Identifier: BUSL-1.1

//! Source-database quiesce during clone materialization.
//!
//! Tests that:
//! 1. A write against a source database while it is frozen returns a
//!    `serialization_failure` (SQLSTATE 40001) error.
//! 2. A write against the source AFTER the freeze is released succeeds.
//! 3. The freeze does not interfere with reads on the same database.
//!
//! Uses the direct `MaterializeFreezeRegistry` API instead of a live
//! materializer sweep to make the freeze window deterministic without
//! relying on goroutine/timing races.

mod common;

use common::pgwire_harness::TestServer;
use nodedb_types::id::DatabaseId;

#[tokio::test(flavor = "multi_thread")]
async fn freeze_blocks_writes_and_allows_reads() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Set up source database with a KV collection and a few rows.
    client
        .simple_query("CREATE DATABASE quiesce_src")
        .await
        .expect("CREATE DATABASE quiesce_src");
    client
        .simple_query("USE DATABASE quiesce_src")
        .await
        .expect("USE quiesce_src");
    client
        .simple_query(
            "CREATE COLLECTION items \
             (key TEXT PRIMARY KEY) \
             WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION items");
    for i in 0..10u32 {
        client
            .simple_query(&format!(
                "INSERT INTO items (key, value) VALUES ('k{i}', 'v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT k{i}: {e:#?}"));
    }

    // Resolve the DatabaseId for quiesce_src.
    let db_id: DatabaseId = {
        let catalog = server.shared.credentials.catalog();
        let databases = catalog.list_databases().expect("list_databases");
        databases
            .into_iter()
            .find(|d| d.name == "quiesce_src")
            .expect("quiesce_src database descriptor")
            .id
    };

    // -----------------------------------------------------------------------
    // 1. While the database is frozen, writes must fail with SQLSTATE 40001.
    // -----------------------------------------------------------------------
    let freeze_reg = std::sync::Arc::clone(&server.shared.materialize_freeze);
    let _guard = freeze_reg.freeze(db_id);

    // The write should be rejected.
    let write_err = client
        .simple_query("INSERT INTO items (key, value) VALUES ('blocked', 'blocked')")
        .await
        .expect_err("write must fail while source is frozen");

    // tokio-postgres wraps the pgwire error. Use `as_db_error()` to extract it.
    use tokio_postgres::error::SqlState;
    let db_err = write_err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected a Db error, got: {write_err:#?}"));
    assert_eq!(
        db_err.code(),
        &SqlState::T_R_SERIALIZATION_FAILURE,
        "expected SQLSTATE 40001 (serialization_failure), got {:?}: {}",
        db_err.code(),
        db_err.message(),
    );

    // -----------------------------------------------------------------------
    // 2. Reads must still succeed while the database is frozen.
    // -----------------------------------------------------------------------
    let rows = client
        .simple_query("SELECT key FROM items LIMIT 1")
        .await
        .expect("reads must succeed while source is frozen");
    let row_count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        row_count, 1,
        "read must return a row while source is frozen"
    );

    // -----------------------------------------------------------------------
    // 3. After the freeze guard drops, writes succeed again.
    // -----------------------------------------------------------------------
    drop(_guard);

    client
        .simple_query("INSERT INTO items (key, value) VALUES ('after_freeze', 'ok')")
        .await
        .expect("write must succeed after freeze is released");

    // Verify the post-freeze insert is visible.
    let rows = client
        .simple_query("SELECT key FROM items WHERE key = 'after_freeze'")
        .await
        .expect("SELECT after freeze");
    let count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(count, 1, "post-freeze insert must be visible");
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_freeze_releases_on_last_drop() {
    let server = TestServer::start().await;

    let registry = std::sync::Arc::clone(&server.shared.materialize_freeze);
    let db_id = DatabaseId::new(99_999);

    assert!(!registry.is_frozen(db_id), "should not be frozen initially");

    let g1 = registry.freeze(db_id);
    let g2 = registry.freeze(db_id);
    assert!(
        registry.is_frozen(db_id),
        "should be frozen after two freeze() calls"
    );

    drop(g1);
    assert!(
        registry.is_frozen(db_id),
        "should still be frozen after dropping first guard"
    );

    drop(g2);
    assert!(
        !registry.is_frozen(db_id),
        "should be unfrozen after dropping last guard"
    );
}
