// SPDX-License-Identifier: BUSL-1.1

//! Clone correctness across engine types: a cloned database must read rows
//! from the source across `kv`, `document_strict`, `document_schemaless`,
//! `columnar`, and `spatial`, each exercised in its own test function.

use crate::harness::TestServer;

/// First column of the first row, or `None`. `None` also means "column 0 was
/// SQL NULL", so any test whose point is that rows came back asserts
/// [`row_count`] as well.
fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
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
    assert_eq!(row_count(&msgs), 1, "kv clone must return exactly one row");
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
    assert_eq!(
        row_count(&msgs),
        1,
        "document_strict clone must return exactly one row"
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
    assert_eq!(
        row_count(&msgs),
        1,
        "document_schemaless clone must return exactly one row"
    );
}

/// `columnar` engine: clone reads source rows. The resolver rewrites
/// `ColumnarOp::Scan` against a shadowed clone into a source scan the same
/// way it rewrites `KvOp::Scan` — nothing branches on engine type.
#[tokio::test]
async fn clone_columnar_engine_reads_source_rows() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cev_col_src")
        .await
        .expect("CREATE DATABASE cev_col_src");
    client
        .simple_query("USE DATABASE cev_col_src")
        .await
        .expect("USE cev_col_src");
    client
        .simple_query("CREATE COLLECTION col_items (id TEXT, v TEXT) WITH (engine='columnar')")
        .await
        .expect("CREATE COLLECTION col_items");
    for i in 0..5 {
        client
            .simple_query(&format!(
                "INSERT INTO col_items (id, v) VALUES ('c{i}', 'val{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT c{i}: {e}"));
    }

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cev_col_clone FROM cev_col_src")
        .await
        .expect("CLONE columnar");

    client
        .simple_query("USE DATABASE cev_col_clone")
        .await
        .expect("USE cev_col_clone");
    let msgs = client
        .simple_query("SELECT v FROM col_items WHERE id = 'c3'")
        .await
        .expect("SELECT from columnar clone");
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("val3"),
        "columnar clone must read source rows"
    );
    assert_eq!(
        row_count(&msgs),
        1,
        "columnar clone must return the matching source row, not an empty result"
    );
}

/// `document_schemaless` engine, full scan: clone reads every source row. A
/// bare `SELECT` lowers to `DocumentOp::Scan` wrapped in `Exchange{Gather}`,
/// the shape the clone resolver must see through.
#[tokio::test]
async fn clone_document_scan_reads_all_source_rows() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cev_scan_src")
        .await
        .expect("CREATE DATABASE cev_scan_src");
    client
        .simple_query("USE DATABASE cev_scan_src")
        .await
        .expect("USE cev_scan_src");
    client
        .simple_query(
            "CREATE COLLECTION notes (id STRING PRIMARY KEY) WITH (engine='document_schemaless')",
        )
        .await
        .expect("CREATE COLLECTION notes");
    for i in 0..3 {
        client
            .simple_query(&format!("INSERT INTO notes (id) VALUES ('n{i}')"))
            .await
            .unwrap_or_else(|e| panic!("INSERT n{i}: {e}"));
    }

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cev_scan_clone FROM cev_scan_src")
        .await
        .expect("CLONE schemaless scan");

    client
        .simple_query("USE DATABASE cev_scan_clone")
        .await
        .expect("USE cev_scan_clone");
    let msgs = client
        .simple_query("SELECT id FROM notes")
        .await
        .expect("SELECT * from schemaless clone");
    assert_eq!(
        row_count(&msgs),
        3,
        "document scan on a shadowed clone must read every source row"
    );
}

/// `spatial` engine: clone reads source rows.
///
/// A spatial collection stores its rows in the columnar core, so a plain
/// `SELECT` over it lowers to `ColumnarOp::Scan` wrapped in `Exchange{Gather}`.
#[tokio::test]
async fn clone_spatial_engine_reads_source_rows() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cev_sp_src")
        .await
        .expect("CREATE DATABASE cev_sp_src");
    client
        .simple_query("USE DATABASE cev_sp_src")
        .await
        .expect("USE cev_sp_src");
    client
        .simple_query(
            "CREATE COLLECTION places \
             COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .expect("CREATE COLLECTION places");
    let points = [
        ("p1", -122.4, 37.8, "SF"),
        ("p2", -118.2, 34.0, "LA"),
        ("p3", -87.6, 41.9, "Chicago"),
        ("p4", -73.9, 40.7, "NYC"),
        ("p5", -95.4, 29.8, "Houston"),
    ];
    for (id, lon, lat, name) in points {
        client
            .simple_query(&format!(
                "INSERT INTO places (id, location, name) \
                 VALUES ('{id}', ST_Point({lon}, {lat}), '{name}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT {id}: {e}"));
    }

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cev_sp_clone FROM cev_sp_src")
        .await
        .expect("CLONE spatial");

    client
        .simple_query("USE DATABASE cev_sp_clone")
        .await
        .expect("USE cev_sp_clone");
    let msgs = client
        .simple_query("SELECT id FROM places")
        .await
        .expect("SELECT from spatial clone");
    assert_eq!(
        row_count(&msgs),
        5,
        "spatial clone must read every source row"
    );
}

/// An aggregate over a shadowed clone is refused, and works once
/// materialized. The read path concatenates target and source payloads,
/// which is not the aggregate over their union, so the resolver refuses.
#[tokio::test]
async fn clone_aggregate_refused_until_materialized() {
    let server = TestServer::start().await;

    server
        .exec("CREATE DATABASE cev_agg_src")
        .await
        .expect("CREATE DATABASE cev_agg_src");
    server
        .exec("USE DATABASE cev_agg_src")
        .await
        .expect("USE cev_agg_src");
    server
        .exec("CREATE COLLECTION agg_items (id TEXT, v TEXT) WITH (engine='columnar')")
        .await
        .expect("CREATE COLLECTION agg_items");
    for i in 0..5 {
        server
            .exec(&format!(
                "INSERT INTO agg_items (id, v) VALUES ('a{i}', 'val{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT a{i}: {e}"));
    }

    server
        .exec("USE DATABASE default")
        .await
        .expect("USE default");
    server
        .exec("CLONE DATABASE cev_agg_clone FROM cev_agg_src")
        .await
        .expect("CLONE aggregate");

    server
        .exec("USE DATABASE cev_agg_clone")
        .await
        .expect("USE cev_agg_clone");
    server
        .expect_error(
            "SELECT COUNT(*) FROM agg_items",
            "cannot be read through an unmaterialized clone",
        )
        .await;

    server
        .exec("USE DATABASE default")
        .await
        .expect("USE default");
    server
        .exec("ALTER DATABASE cev_agg_clone MATERIALIZE")
        .await
        .expect("MATERIALIZE aggregate clone");
    server
        .exec("USE DATABASE cev_agg_clone")
        .await
        .expect("USE cev_agg_clone");
    let rows = server
        .query_rows("SELECT COUNT(*) FROM agg_items")
        .await
        .expect("COUNT(*) on materialized clone");
    assert_eq!(rows.len(), 1, "COUNT(*) must return one row: {rows:?}");
    assert_eq!(
        rows[0].first().map(String::as_str),
        Some("5"),
        "materialized clone must count every source row: {rows:?}"
    );
}
