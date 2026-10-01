// SPDX-License-Identifier: BUSL-1.1

//! `MOVE TENANT <name> FROM <src_db> TO <tgt_db>` round-trip test.
//!
//! After a successful move:
//! - The tenant's collections exist in the target database.
//! - The tenant's collections no longer exist in the source database.
//! - Queries routed against the target database see the original data.

use crate::harness::TestServer;

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

/// A collection's home vShard hashes its database, so a moved collection
/// homes to another core. With four cores and six collections, the rows must
/// travel across cores: every row reads back through the target, and the
/// source holds none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_tenant_moves_rows_across_cores() {
    const COLLECTIONS: [&str; 6] = ["mc_a", "mc_b", "mc_c", "mc_d", "mc_e", "mc_f"];
    const ROWS: usize = 4;

    let server = TestServer::start_multicores(4).await;
    let client = &*server.client;

    for (database, fill) in [("mc_src", true), ("mc_tgt", false)] {
        client
            .simple_query("USE DATABASE default")
            .await
            .expect("USE default");
        client
            .simple_query(&format!("CREATE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DATABASE {database}: {e}"));
        client
            .simple_query(&format!("USE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("USE {database}: {e}"));
        for name in COLLECTIONS {
            client
                .simple_query(&format!(
                    "CREATE COLLECTION {name} (id STRING PRIMARY KEY, body STRING) \
                     WITH (engine='document_strict')"
                ))
                .await
                .unwrap_or_else(|e| panic!("CREATE COLLECTION {name} in {database}: {e}"));
            if !fill {
                continue;
            }
            for i in 0..ROWS {
                client
                    .simple_query(&format!(
                        "INSERT INTO {name} (id, body) VALUES ('k{i}', '{name}-{i}')"
                    ))
                    .await
                    .unwrap_or_else(|e| panic!("INSERT k{i} into {name}: {e}"));
            }
        }
    }

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CREATE TENANT mc_owner ID 30")
        .await
        .expect("CREATE TENANT mc_owner");
    client
        .simple_query("MOVE TENANT mc_owner FROM mc_src TO mc_tgt")
        .await
        .expect("MOVE TENANT mc_owner FROM mc_src TO mc_tgt");

    client
        .simple_query("USE DATABASE mc_tgt")
        .await
        .expect("USE mc_tgt");
    for name in COLLECTIONS {
        let all = client
            .simple_query(&format!("SELECT id FROM {name}"))
            .await
            .unwrap_or_else(|e| panic!("scan {name} in target: {e}"));
        assert_eq!(row_count(&all), ROWS, "mc_tgt.{name} must hold every row");
        for i in 0..ROWS {
            let msgs = client
                .simple_query(&format!("SELECT body FROM {name} WHERE id = 'k{i}'"))
                .await
                .unwrap_or_else(|e| panic!("point read k{i} of {name}: {e}"));
            assert_eq!(
                first_value(&msgs),
                Some(format!("{name}-{i}")),
                "mc_tgt.{name} row k{i}"
            );
        }
    }

    client
        .simple_query("USE DATABASE mc_src")
        .await
        .expect("USE mc_src");
    for name in COLLECTIONS {
        let rows = client
            .simple_query(&format!("SELECT id FROM {name}"))
            .await
            .unwrap_or_default();
        assert_eq!(row_count(&rows), 0, "mc_src.{name} must hold no rows");
    }
}

/// `MOVE TENANT` cannot run inside a transaction block: a ROLLBACK cannot
/// undo its re-issued rows. The refusal is SQLSTATE 25001, and neither
/// database changes.
#[tokio::test]
async fn move_tenant_inside_a_transaction_block_is_refused() {
    let server = TestServer::start().await;
    let client = &*server.client;

    for database in ["tx_src", "tx_tgt"] {
        client
            .simple_query("USE DATABASE default")
            .await
            .expect("USE default");
        client
            .simple_query(&format!("CREATE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DATABASE {database}: {e}"));
        client
            .simple_query(&format!("USE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("USE {database}: {e}"));
        client
            .simple_query(
                "CREATE COLLECTION tx_orders (id STRING PRIMARY KEY, amount STRING) \
                 WITH (engine='kv')",
            )
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION in {database}: {e}"));
    }
    client
        .simple_query("USE DATABASE tx_src")
        .await
        .expect("USE tx_src");
    client
        .simple_query("INSERT INTO tx_orders (id, amount) VALUES ('o1', '7')")
        .await
        .expect("INSERT o1");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CREATE TENANT tx_owner ID 40")
        .await
        .expect("CREATE TENANT tx_owner");
    client.simple_query("BEGIN").await.expect("BEGIN");
    let refused = client
        .simple_query("MOVE TENANT tx_owner FROM tx_src TO tx_tgt")
        .await
        .expect_err("MOVE TENANT inside BEGIN must be refused");
    let sqlstate = refused
        .as_db_error()
        .map(|db_error| db_error.code().code().to_string());
    assert_eq!(sqlstate.as_deref(), Some("25001"), "refusal: {refused}");
    client.simple_query("ROLLBACK").await.expect("ROLLBACK");

    client
        .simple_query("USE DATABASE tx_src")
        .await
        .expect("USE tx_src");
    let source = client
        .simple_query("SELECT amount FROM tx_orders WHERE id = 'o1'")
        .await
        .expect("read the source");
    assert_eq!(
        first_value(&source).as_deref(),
        Some("7"),
        "the source keeps its row"
    );

    client
        .simple_query("USE DATABASE tx_tgt")
        .await
        .expect("USE tx_tgt");
    let target = client
        .simple_query("SELECT id FROM tx_orders")
        .await
        .expect("read the target");
    assert_eq!(row_count(&target), 0, "the target gains no row");
}
