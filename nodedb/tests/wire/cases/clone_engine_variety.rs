// SPDX-License-Identifier: BUSL-1.1

//! Clone correctness across engine types.
//!
//! A cloned database must read rows from the source across all supported
//! non-array engine types: `kv`, `document_strict`, and `document_schemaless`.
//! Each engine is exercised in its own test function so failures are isolated.

use crate::harness::TestServer;

fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

/// `kv` engine: clone reads source row.
#[tokio::test]
async fn clone_kv_engine_reads_source_row() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cev_kv_src")
        .await
        .expect("CREATE DATABASE cev_kv_src");
    client
        .simple_query("USE DATABASE cev_kv_src")
        .await
        .expect("USE cev_kv_src");
    client
        .simple_query(
            "CREATE COLLECTION kv_items (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION kv_items");
    client
        .simple_query("INSERT INTO kv_items (k, v) VALUES ('key1', 'val1')")
        .await
        .expect("INSERT key1");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cev_kv_clone FROM cev_kv_src")
        .await
        .expect("CLONE kv");

    client
        .simple_query("USE DATABASE cev_kv_clone")
        .await
        .expect("USE cev_kv_clone");
    let msgs = client
        .simple_query("SELECT v FROM kv_items WHERE k = 'key1'")
        .await
        .expect("SELECT from kv clone");
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("val1"),
        "kv clone must read source row"
    );
}

/// `document_strict` engine: clone reads source row.
#[tokio::test]
async fn clone_document_strict_engine_reads_source_row() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cev_strict_src")
        .await
        .expect("CREATE DATABASE cev_strict_src");
    client
        .simple_query("USE DATABASE cev_strict_src")
        .await
        .expect("USE cev_strict_src");
    client
        .simple_query(
            "CREATE COLLECTION products \
             (id STRING PRIMARY KEY, name STRING NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION products");
    client
        .simple_query("INSERT INTO products (id, name) VALUES ('p1', 'anvil')")
        .await
        .expect("INSERT p1");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cev_strict_clone FROM cev_strict_src")
        .await
        .expect("CLONE strict");

    client
        .simple_query("USE DATABASE cev_strict_clone")
        .await
        .expect("USE cev_strict_clone");
    let msgs = client
        .simple_query("SELECT name FROM products WHERE id = 'p1'")
        .await
        .expect("SELECT from strict clone");
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("anvil"),
        "document_strict clone must read source row"
    );
}

/// `document_schemaless` engine: clone reads source row.
#[tokio::test]
async fn clone_document_schemaless_engine_reads_source_row() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cev_schema_src")
        .await
        .expect("CREATE DATABASE cev_schema_src");
    client
        .simple_query("USE DATABASE cev_schema_src")
        .await
        .expect("USE cev_schema_src");
    client
        .simple_query(
            "CREATE COLLECTION notes (id STRING PRIMARY KEY) WITH (engine='document_schemaless')",
        )
        .await
        .expect("CREATE COLLECTION notes");
    client
        .simple_query("INSERT INTO notes (id) VALUES ('n1')")
        .await
        .expect("INSERT n1");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cev_schema_clone FROM cev_schema_src")
        .await
        .expect("CLONE schemaless");

    client
        .simple_query("USE DATABASE cev_schema_clone")
        .await
        .expect("USE cev_schema_clone");
    let msgs = client
        .simple_query("SELECT id FROM notes WHERE id = 'n1'")
        .await
        .expect("SELECT from schemaless clone");
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("n1"),
        "document_schemaless clone must read source row"
    );
}
