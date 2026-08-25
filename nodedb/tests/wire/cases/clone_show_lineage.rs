// SPDX-License-Identifier: BUSL-1.1

//! `SHOW DATABASE LINEAGE FOR <name>` test.
//!
//! Creates a 3-deep clone chain (A → B → C) and verifies that
//! `SHOW DATABASE LINEAGE FOR C` returns rows for C, B, and A in
//! ancestor order.

use crate::harness::TestServer;

/// A 3-deep clone chain A→B→C: `SHOW DATABASE LINEAGE FOR C` must return
/// at least 3 rows (C itself, B, A) in order from newest to oldest.
#[tokio::test]
async fn show_lineage_three_deep_chain() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // ── A: root database ──────────────────────────────────────────────────────
    client
        .simple_query("CREATE DATABASE lineage_a")
        .await
        .expect("CREATE DATABASE lineage_a");
    client
        .simple_query("USE DATABASE lineage_a")
        .await
        .expect("USE lineage_a");
    client
        .simple_query(
            "CREATE COLLECTION nodes (id STRING PRIMARY KEY, label STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION nodes");
    client
        .simple_query("INSERT INTO nodes (id, label) VALUES ('n1', 'root')")
        .await
        .expect("INSERT n1");

    // ── B: clone of A ─────────────────────────────────────────────────────────
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE lineage_b FROM lineage_a")
        .await
        .expect("CLONE lineage_b FROM lineage_a");

    // ── C: clone of B ─────────────────────────────────────────────────────────
    client
        .simple_query("CLONE DATABASE lineage_c FROM lineage_b")
        .await
        .expect("CLONE lineage_c FROM lineage_b");

    // ── SHOW DATABASE LINEAGE FOR C ───────────────────────────────────────────
    let rows = client
        .simple_query("SHOW DATABASE LINEAGE FOR lineage_c")
        .await
        .expect("SHOW DATABASE LINEAGE FOR lineage_c");

    let data_rows: Vec<String> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r.get("name").unwrap_or("").to_string())
            } else {
                None
            }
        })
        .collect();

    // Must contain lineage_c, lineage_b, and lineage_a.
    assert!(
        data_rows.contains(&"lineage_c".to_string()),
        "lineage_c missing from lineage output: {data_rows:?}"
    );
    assert!(
        data_rows.contains(&"lineage_b".to_string()),
        "lineage_b missing from lineage output: {data_rows:?}"
    );
    assert!(
        data_rows.contains(&"lineage_a".to_string()),
        "lineage_a missing from lineage output: {data_rows:?}"
    );

    // First row must be lineage_c (the queried database itself).
    assert_eq!(
        data_rows.first().map(String::as_str),
        Some("lineage_c"),
        "first row must be the queried database (lineage_c), got: {data_rows:?}"
    );

    // Last row must be lineage_a (the root).
    assert_eq!(
        data_rows.last().map(String::as_str),
        Some("lineage_a"),
        "last row must be the root (lineage_a), got: {data_rows:?}"
    );
}

/// A non-cloned (root) database returns exactly one row for itself.
#[tokio::test]
async fn show_lineage_root_database_returns_one_row() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE lineage_root_only")
        .await
        .expect("CREATE DATABASE lineage_root_only");

    let rows = client
        .simple_query("SHOW DATABASE LINEAGE FOR lineage_root_only")
        .await
        .expect("SHOW DATABASE LINEAGE FOR lineage_root_only");

    let data_rows: Vec<String> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r.get("name").unwrap_or("").to_string())
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        data_rows.len(),
        1,
        "root-only database must return exactly 1 row, got: {data_rows:?}"
    );
    assert_eq!(data_rows[0], "lineage_root_only");
}
