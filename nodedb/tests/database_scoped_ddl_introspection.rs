// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: DDL introspection (`DESCRIBE`,
//! `SHOW COLLECTIONS`, `SHOW INDEXES`) must resolve against the session's
//! current database, not hardcoded `DatabaseId::DEFAULT`.
//!
//! Before the fix, the string-recognized introspection router
//! (`string_introspection::try_string`) accepted the session's
//! `database_id` but discarded it (bound to `_database_id`), so every
//! `DESCRIBE` / `SHOW COLLECTIONS` / `SHOW INDEXES` / `UNDROP COLLECTION`
//! resolved against the DEFAULT database regardless of an active
//! `USE DATABASE <name>` — collections created in a non-default database
//! were invisible to introspection even though DML (`SELECT` / `INSERT`)
//! against them worked correctly via the already-fixed planner path.

mod common;

use common::pgwire_harness::TestServer;

async fn create_database(server: &TestServer, name: &str) {
    server
        .client
        .simple_query(&format!("CREATE DATABASE {name}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE {name} failed: {e}"));
}

async fn query_ok(server: &TestServer, sql: &str) {
    server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nsql: {sql}"));
}

fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// `DESCRIBE <collection>` must find a collection created in the session's
/// current non-default database.
///
/// Pre-fix: `describe_collection` looked up `DatabaseId::DEFAULT` instead of
/// the session's `mydb`, so the collection was reported "not found" even
/// though it demonstrably exists (DML against it succeeds).
#[tokio::test]
async fn describe_collection_finds_it_in_non_default_database() {
    let server = TestServer::start().await;
    create_database(&server, "mydb").await;
    query_ok(&server, "USE DATABASE mydb").await;

    query_ok(
        &server,
        "CREATE COLLECTION t (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')",
    )
    .await;
    query_ok(&server, "INSERT INTO t { id: 'row1', v: 'hello' }").await;

    // DML confirms the collection genuinely exists in `mydb` (already-fixed
    // planner path) — this must succeed both before and after the fix.
    let dml_rows = server
        .client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT against the collection just created must succeed");
    assert_eq!(
        row_count(&dml_rows),
        1,
        "expected exactly the one inserted row"
    );

    // DESCRIBE must find it too — this is the introspection path that was
    // hardcoded to DatabaseId::DEFAULT pre-fix.
    let describe_rows = server
        .client
        .simple_query("DESCRIBE t")
        .await
        .unwrap_or_else(|e| panic!("DESCRIBE t must succeed in mydb: {e}"));
    assert!(
        row_count(&describe_rows) >= 1,
        "DESCRIBE t must return at least one field row in mydb, got: {describe_rows:?}"
    );
}

/// Regression: `DESCRIBE` on a strict collection created with an
/// explicit `id ... PRIMARY KEY` listed the `id` field twice — once from the
/// unconditional synthetic `id` row and once from iterating the declared
/// `coll.fields` — with contradictory nullability. The synthetic row is now
/// emitted only when the collection does not already declare an `id`, and each
/// declared field's nullability is derived from its type modifiers.
#[tokio::test]
async fn describe_strict_does_not_duplicate_explicit_id() {
    let server = TestServer::start().await;

    query_ok(
        &server,
        "CREATE COLLECTION desc_repro (id TEXT PRIMARY KEY, label TEXT) \
         WITH (engine='document_strict')",
    )
    .await;

    let describe_rows = server
        .client
        .simple_query("DESCRIBE desc_repro")
        .await
        .unwrap_or_else(|e| panic!("DESCRIBE desc_repro must succeed: {e}"));

    let id_rows: Vec<&tokio_postgres::SimpleQueryRow> = describe_rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) if row.get("field") == Some("id") => {
                Some(row)
            }
            _ => None,
        })
        .collect();

    assert_eq!(
        id_rows.len(),
        1,
        "DESCRIBE must list the explicit `id` PK field exactly once (pre-fix: twice), \
         got: {describe_rows:?}"
    );
    assert_eq!(
        id_rows[0].get("nullable"),
        Some("false"),
        "the `id` PRIMARY KEY field must be non-nullable, got: {describe_rows:?}"
    );
}

/// `SHOW COLLECTIONS` in a non-default database must list collections
/// created in that database.
///
/// Pre-fix: `show_collections` called `load_all_collections` /
/// `load_collections_for_tenant` with `DatabaseId::DEFAULT`, so a
/// freshly-created `mydb` session saw an empty (or DEFAULT-only) list.
#[tokio::test]
async fn show_collections_lists_it_in_non_default_database() {
    let server = TestServer::start().await;
    create_database(&server, "mydb2").await;
    query_ok(&server, "USE DATABASE mydb2").await;

    query_ok(
        &server,
        "CREATE COLLECTION t2 (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')",
    )
    .await;
    query_ok(&server, "INSERT INTO t2 { id: 'row1', v: 'hello' }").await;

    let rows = server
        .client
        .simple_query("SHOW COLLECTIONS")
        .await
        .unwrap_or_else(|e| panic!("SHOW COLLECTIONS must succeed in mydb2: {e}"));

    let found = rows.iter().any(|m| {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
            row.get("name") == Some("t2")
        } else {
            false
        }
    });
    assert!(
        found,
        "SHOW COLLECTIONS in mydb2 must list 't2', got: {rows:?}"
    );
}

/// `SHOW INDEXES` in a non-default database must list that database's
/// indexes, and only those: index records are filed under the database of
/// the collection they index.
#[tokio::test]
async fn show_indexes_works_in_non_default_database() {
    let server = TestServer::start().await;
    create_database(&server, "mydb3").await;
    query_ok(&server, "USE DATABASE mydb3").await;

    query_ok(
        &server,
        "CREATE COLLECTION t3 (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')",
    )
    .await;
    query_ok(&server, "CREATE INDEX idx_t3_v ON t3 (v)").await;

    let rows = server
        .client
        .simple_query("SHOW INDEXES")
        .await
        .unwrap_or_else(|e| panic!("SHOW INDEXES must succeed in mydb3: {e}"));
    let listed = |rows: &[tokio_postgres::SimpleQueryMessage], name: &str| {
        rows.iter().any(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get("index_name") == Some(name),
            _ => false,
        })
    };
    assert!(
        listed(&rows, "idx_t3_v"),
        "SHOW INDEXES in mydb3 must list mydb3's index, got: {rows:?}"
    );

    // The same index must not leak into the DEFAULT database's listing.
    query_ok(&server, "USE DATABASE default").await;
    let default_rows = server
        .client
        .simple_query("SHOW INDEXES")
        .await
        .unwrap_or_else(|e| panic!("SHOW INDEXES must succeed in the default database: {e}"));
    assert!(
        !listed(&default_rows, "idx_t3_v"),
        "an index of mydb3 must not appear in the default database, got: {default_rows:?}"
    );
}

/// A collection created in a non-default database must NOT be visible to
/// `DESCRIBE` / `SHOW COLLECTIONS` from the DEFAULT database — isolation
/// must hold in both directions.
#[tokio::test]
async fn introspection_isolation_holds_from_default_database() {
    let server = TestServer::start().await;
    create_database(&server, "mydb4").await;
    query_ok(&server, "USE DATABASE mydb4").await;

    query_ok(
        &server,
        "CREATE COLLECTION only_in_mydb4 (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')",
    )
    .await;
    query_ok(
        &server,
        "INSERT INTO only_in_mydb4 { id: 'row1', v: 'hello' }",
    )
    .await;

    // Switch back to the default database.
    query_ok(&server, "USE DATABASE default").await;

    // DESCRIBE from `default` must not find the mydb4-only collection.
    server
        .expect_error("DESCRIBE only_in_mydb4", "not found")
        .await;

    // SHOW COLLECTIONS from `default` must not list it either.
    let rows = server
        .client
        .simple_query("SHOW COLLECTIONS")
        .await
        .expect("SHOW COLLECTIONS in default must succeed");
    let leaked = rows.iter().any(|m| {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
            row.get("name") == Some("only_in_mydb4")
        } else {
            false
        }
    });
    assert!(
        !leaked,
        "SHOW COLLECTIONS in default must not see mydb4's collection: {rows:?}"
    );
}
