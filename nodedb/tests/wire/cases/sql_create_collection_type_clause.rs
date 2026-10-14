// SPDX-License-Identifier: BUSL-1.1

//! `CREATE COLLECTION … TYPE <kind>` creates the engine the kind names.
//!
//! The clause was skipped as an unclaimed word, so `TYPE STRICT (…)`,
//! `TYPE DOCUMENT STRICT (…)`, `STRICT (…)` and `TYPE KV` all created a
//! schemaless collection. `DESCRIBE` reports every collection's engine in
//! its `__storage` row.

use crate::harness::TestServer;

/// The engine `DESCRIBE` reports for `collection`: the `__storage` row's
/// type.
async fn storage(server: &TestServer, collection: &str) -> Option<String> {
    let messages = server
        .client
        .simple_query(&format!("DESCRIBE {collection}"))
        .await
        .unwrap_or_else(|e| panic!("DESCRIBE {collection}: {e}"));
    messages.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(row) if row.get("field") == Some("__storage") => {
            row.get("type").map(str::to_string)
        }
        _ => None,
    })
}

async fn create(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_spellings_create_a_strict_collection() {
    let server = TestServer::start().await;
    for (name, sql) in [
        (
            "ty_strict",
            "CREATE COLLECTION ty_strict TYPE strict (id STRING PRIMARY KEY, v INT)",
        ),
        (
            "ty_doc_strict",
            "CREATE COLLECTION ty_doc_strict TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, v INT)",
        ),
        (
            "ty_bare_strict",
            "CREATE COLLECTION ty_bare_strict STRICT (id STRING PRIMARY KEY, v INT)",
        ),
    ] {
        create(&server, sql).await;
        assert_eq!(
            storage(&server, name).await.as_deref(),
            Some("document_strict"),
            "{sql}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_spellings_create_a_schemaless_collection() {
    let server = TestServer::start().await;
    for (name, sql) in [
        ("ty_doc", "CREATE COLLECTION ty_doc TYPE document"),
        (
            "ty_doc_schemaless",
            "CREATE COLLECTION ty_doc_schemaless TYPE DOCUMENT SCHEMALESS",
        ),
    ] {
        create(&server, sql).await;
        assert_eq!(
            storage(&server, name).await.as_deref(),
            Some("document_schemaless"),
            "{sql}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_spelling_creates_a_vector_primary_collection() {
    let server = TestServer::start().await;
    for (name, sql) in [
        (
            "ty_vector",
            "CREATE COLLECTION ty_vector TYPE vector (id STRING PRIMARY KEY, vec VECTOR(3)) \
             WITH (primary='vector', vector_field='vec', dim=3)",
        ),
        (
            "ty_vector_engine",
            "CREATE COLLECTION ty_vector_engine (id STRING PRIMARY KEY, vec VECTOR(3)) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3)",
        ),
    ] {
        create(&server, sql).await;
        assert_eq!(
            storage(&server, name).await.as_deref(),
            Some("vector"),
            "{sql}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_spellings_create_a_kv_collection() {
    let server = TestServer::start().await;
    for (name, sql) in [
        (
            "ty_kv",
            "CREATE COLLECTION ty_kv TYPE kv (k TEXT PRIMARY KEY, v TEXT)",
        ),
        (
            "ty_key_value",
            "CREATE COLLECTION ty_key_value TYPE KEY_VALUE (k TEXT PRIMARY KEY, v TEXT)",
        ),
    ] {
        create(&server, sql).await;
        assert_eq!(storage(&server, name).await.as_deref(), Some("kv"), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_family_spellings_create_their_engine() {
    let server = TestServer::start().await;
    for (name, sql, engine) in [
        (
            "ty_columnar",
            "CREATE COLLECTION ty_columnar TYPE columnar (id INT PRIMARY KEY, v INT)",
            "columnar",
        ),
        (
            "ty_timeseries",
            "CREATE COLLECTION ty_timeseries TYPE timeseries (ts BIGINT TIME_KEY, v FLOAT)",
            "timeseries",
        ),
        (
            "ty_spatial",
            "CREATE COLLECTION ty_spatial TYPE spatial (id TEXT, loc GEOMETRY)",
            "spatial",
        ),
    ] {
        create(&server, sql).await;
        assert_eq!(
            storage(&server, name).await.as_deref(),
            Some(engine),
            "{sql}"
        );
    }
}

/// `engine='vector'` with no `primary` key implies `primary='vector'`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_engine_implies_vector_primary() {
    let server = TestServer::start().await;
    for (name, sql) in [
        (
            "ve_implied",
            "CREATE COLLECTION ve_implied (id STRING PRIMARY KEY, vec VECTOR(3)) \
             WITH (engine='vector', vector_field='vec', dim=3)",
        ),
        (
            "ve_type_implied",
            "CREATE COLLECTION ve_type_implied TYPE vector (id STRING PRIMARY KEY, vec VECTOR(3)) \
             WITH (vector_field='vec', dim=3)",
        ),
    ] {
        create(&server, sql).await;
        assert_eq!(
            storage(&server, name).await.as_deref(),
            Some("vector"),
            "{sql}"
        );
    }
    create(
        &server,
        "INSERT INTO ve_implied (id, vec) VALUES ('a', ARRAY[1.0, 0.0, 0.0])",
    )
    .await;
    let rows = server
        .query_rows("SELECT id FROM ve_implied")
        .await
        .unwrap_or_else(|e| panic!("read ve_implied: {e}"));
    assert_eq!(rows, vec![vec!["a".to_string()]]);
}

/// `engine='vector'` beside another `primary` is a contradiction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_engine_with_another_primary_is_refused() {
    let server = TestServer::start().await;
    for primary in ["document_schemaless", "document_strict"] {
        let sql = format!(
            "CREATE COLLECTION ve_contra (id STRING PRIMARY KEY, vec VECTOR(3)) \
             WITH (engine='vector', primary='{primary}', vector_field='vec', dim=3)"
        );
        server.expect_error(&sql, "42601").await;
        server.expect_error(&sql, "primary='").await;
    }
    server.expect_error("DESCRIBE ve_contra", "ve_contra").await;
}

/// A missing `vector_field` or `dim` is refused with the full form, never
/// created as a document collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_engine_without_its_options_is_refused() {
    let server = TestServer::start().await;
    for (sql, missing) in [
        (
            "CREATE COLLECTION ve_bare WITH (engine='vector')",
            "vector_field and dim",
        ),
        (
            "CREATE COLLECTION ve_bare (id STRING PRIMARY KEY, vec VECTOR(3)) \
             WITH (engine='vector', dim=3)",
            "vector_field",
        ),
        (
            "CREATE COLLECTION ve_bare (id STRING PRIMARY KEY, vec VECTOR(3)) \
             WITH (engine='vector', vector_field='vec')",
            "dim",
        ),
        (
            "CREATE COLLECTION ve_bare TYPE vector",
            "vector_field and dim",
        ),
    ] {
        server.expect_error(sql, "42601").await;
        server.expect_error(sql, missing).await;
        server.expect_error(sql, "Full form").await;
    }
    server.expect_error("DESCRIBE ve_bare", "ve_bare").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_kind_is_refused_and_creates_nothing() {
    let server = TestServer::start().await;
    server
        .expect_error("CREATE COLLECTION ty_graph TYPE graph (id TEXT)", "42601")
        .await;
    server
        .expect_error(
            "CREATE COLLECTION ty_graph TYPE graph (id TEXT)",
            "engine='<name>'",
        )
        .await;
    server.expect_error("DESCRIBE ty_graph", "ty_graph").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn type_and_engine_naming_different_engines_are_refused() {
    let server = TestServer::start().await;
    server
        .expect_error(
            "CREATE COLLECTION ty_conflict TYPE strict (id STRING PRIMARY KEY) \
             WITH (engine='kv')",
            "TYPE selects engine",
        )
        .await;
    create(
        &server,
        "CREATE COLLECTION ty_agree TYPE strict (id STRING PRIMARY KEY) \
         WITH (engine='document_strict')",
    )
    .await;
    assert_eq!(
        storage(&server, "ty_agree").await.as_deref(),
        Some("document_strict")
    );
}
