// SPDX-License-Identifier: BUSL-1.1

//! `MOVE TENANT <name> FROM <src_db> TO <tgt_db>` round-trip test.
//!
//! After a successful move:
//! - The tenant's collections exist in the target database.
//! - The tenant's collections no longer exist in the source database.
//! - Queries routed against the target database see the original data.

mod common;

use common::pgwire_harness::TestServer;

/// Helper: extract the first column of the first `Row` message.
fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

/// Count `Row` messages in a result set.
fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// Verify that a `MOVE TENANT` command transfers a KV collection from one
/// database to another and that data is accessible in the target.
#[tokio::test]
async fn move_tenant_transfers_collection_to_target() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // ── Setup: source database ────────────────────────────────────────────────
    client
        .simple_query("CREATE DATABASE mt_src")
        .await
        .expect("CREATE DATABASE mt_src");
    client
        .simple_query("USE DATABASE mt_src")
        .await
        .expect("USE mt_src");

    // Create a tenant and a collection owned by it.
    client
        .simple_query("CREATE TENANT acme_mt ID 10")
        .await
        .expect("CREATE TENANT acme_mt");
    client
        .simple_query(
            "CREATE COLLECTION orders \
             (order_id STRING PRIMARY KEY, amount STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION orders in src");
    client
        .simple_query("INSERT INTO orders (order_id, amount) VALUES ('o1', '100')")
        .await
        .expect("INSERT o1");

    // ── Setup: target database with matching schema ───────────────────────────
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CREATE DATABASE mt_tgt")
        .await
        .expect("CREATE DATABASE mt_tgt");
    client
        .simple_query("USE DATABASE mt_tgt")
        .await
        .expect("USE mt_tgt");
    client
        .simple_query(
            "CREATE COLLECTION orders \
             (order_id STRING PRIMARY KEY, amount STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION orders in tgt");

    // ── Execute MOVE TENANT ───────────────────────────────────────────────────
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("MOVE TENANT acme_mt FROM mt_src TO mt_tgt")
        .await
        .expect("MOVE TENANT acme_mt FROM mt_src TO mt_tgt");

    // ── Verify: collection exists in target ───────────────────────────────────
    client
        .simple_query("USE DATABASE mt_tgt")
        .await
        .expect("USE mt_tgt after move");

    // The collection must be accessible in the target database.
    let msgs = client
        .simple_query("SELECT amount FROM orders WHERE order_id = 'o1'")
        .await
        .expect("SELECT from target orders after move");

    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("100"),
        "data should be accessible in target after MOVE TENANT"
    );

    // ── Verify: collection is gone from source ────────────────────────────────
    client
        .simple_query("USE DATABASE mt_src")
        .await
        .expect("USE mt_src after move");

    let source_rows = client
        .simple_query("SELECT amount FROM orders WHERE order_id = 'o1'")
        .await
        .unwrap_or_default();

    assert_eq!(
        row_count(&source_rows),
        0,
        "data should NOT be present in source after MOVE TENANT"
    );
}

/// Verify that `MOVE TENANT` moves a strict-document collection correctly.
#[tokio::test]
async fn move_tenant_strict_document_engine() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source database.
    client
        .simple_query("CREATE DATABASE mt_strict_src")
        .await
        .expect("CREATE DATABASE mt_strict_src");
    client
        .simple_query("USE DATABASE mt_strict_src")
        .await
        .expect("USE mt_strict_src");
    client
        .simple_query("CREATE TENANT biz_mt ID 20")
        .await
        .expect("CREATE TENANT biz_mt");
    client
        .simple_query(
            "CREATE COLLECTION catalog \
             (sku STRING PRIMARY KEY, title STRING NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION catalog");
    client
        .simple_query("INSERT INTO catalog (sku, title) VALUES ('s1', 'widget')")
        .await
        .expect("INSERT s1");

    // Target database.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CREATE DATABASE mt_strict_tgt")
        .await
        .expect("CREATE DATABASE mt_strict_tgt");
    client
        .simple_query("USE DATABASE mt_strict_tgt")
        .await
        .expect("USE mt_strict_tgt");
    client
        .simple_query(
            "CREATE COLLECTION catalog \
             (sku STRING PRIMARY KEY, title STRING NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION catalog in tgt");

    // Execute.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("MOVE TENANT biz_mt FROM mt_strict_src TO mt_strict_tgt")
        .await
        .expect("MOVE TENANT biz_mt");

    // Confirm data lives in target.
    client
        .simple_query("USE DATABASE mt_strict_tgt")
        .await
        .expect("USE mt_strict_tgt");
    let msgs = client
        .simple_query("SELECT title FROM catalog WHERE sku = 's1'")
        .await
        .expect("SELECT after move");
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("widget"),
        "strict-document data must survive MOVE TENANT"
    );
}
