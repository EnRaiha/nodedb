// SPDX-License-Identifier: BUSL-1.1

//! In-transaction MATCH pattern reads observe the transaction's own staged
//! graph edge writes/deletes (read-your-own-writes).
//!
//! `GRAPH NEIGHBORS`/`Hop`/traversal already merged the per-transaction
//! `GraphTxnOverlay` for RYOW, but the MATCH pattern engine read committed CSR
//! only: a staged `GRAPH INSERT EDGE` was invisible to a MATCH in the same
//! transaction, and a staged `GRAPH DELETE EDGE` was still traversed. This
//! suite locks in the MATCH-side merge: a staged edge PUT appears in an in-txn
//! MATCH and disappears on ROLLBACK; a staged edge DELETE disappears from an
//! in-txn MATCH and reappears on ROLLBACK; and an autocommit MATCH (no
//! transaction) is unchanged (committed-CSR-only).
//!
//! Every staged edge here is a SELF-LOOP (`FROM == TO == node`), exactly as the
//! sibling `sql_transactions_graph_overlay` / `sql_transactions_graph_edge_delete_overlay`
//! suites do: inside an explicit `BEGIN..COMMIT` only a SINGLE-HOME
//! (both-endpoints-on-one-vShard) edge write stages through the per-task gate;
//! a distinct-endpoint (dual-home) edge write in a transaction is rejected as
//! cross-shard. A self-loop `n -[:l]-> n` still yields a MATCH row `{x:n, y:n}`,
//! which is sufficient to exercise the staged-PUT-visible and
//! staged-DELETE-hidden merge on the MATCH read path.
//!
//! NOTE on multi-hop through a STAGED-ONLY intermediate node: driving that at
//! the SQL layer needs distinct-endpoint staged edges (`a->b`, `b->c` with `b`
//! brand-new), which the single-home staging constraint above rejects inside a
//! transaction. That path (a MATCH walking through a staged-only node with no
//! durable CSR id) is covered by the `overlay_expand` unit test
//! `staged_only_node_expands_as_source` instead, where the overlay is exercised
//! without the SQL staging gate.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn create_collection(server: &TestServer) {
    server.exec("CREATE COLLECTION g_match").await.unwrap();
}

/// `GRAPH INSERT EDGE` self-loop `node -[:label]-> node` in collection `g_match`.
async fn insert_self_loop(server: &TestServer, node: &str, label: &str) -> Result<(), String> {
    server
        .client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'g_match' FROM '{node}' TO '{node}' TYPE '{label}'"
        ))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// `GRAPH DELETE EDGE` self-loop `node -[:label]-> node` in collection `g_match`.
async fn delete_self_loop(server: &TestServer, node: &str, label: &str) -> Result<(), String> {
    server
        .client
        .simple_query(&format!(
            "GRAPH DELETE EDGE IN 'g_match' FROM '{node}' TO '{node}' TYPE '{label}'"
        ))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Run `MATCH (x)-[:l]->(y) RETURN x, y` and return the set of `x` node ids
/// (self-loops satisfy `x == y`).
async fn match_sources(server: &TestServer, label: &str) -> Vec<String> {
    let msgs = server
        .client
        .simple_query(&format!("MATCH (x)-[:{label}]->(y) RETURN x, y"))
        .await
        .expect("MATCH should succeed");
    let mut out = Vec::new();
    for msg in msgs {
        if let SimpleQueryMessage::Row(row) = msg
            && let Some(x) = row.get(0)
        {
            out.push(x.to_string());
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_match_sees_staged_edge_put_and_rollback_discards() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    // Committed edge, before any transaction.
    insert_self_loop(&server, "base_put", "l").await.unwrap();

    server.exec("BEGIN").await.unwrap();
    insert_self_loop(&server, "staged_put", "l")
        .await
        .expect("in-tx GRAPH INSERT EDGE should stage at statement time");

    // Read-your-own-writes: the in-tx MATCH must include the staged edge.
    // Pre-fix, MATCH read committed CSR only and this was absent.
    let in_tx = match_sources(&server, "l").await;
    assert!(
        in_tx.contains(&"staged_put".to_string()),
        "in-tx MATCH must observe the transaction's own staged edge PUT \
         (read-your-own-writes), got: {in_tx:?}"
    );
    assert!(
        in_tx.contains(&"base_put".to_string()),
        "a pre-existing committed edge must remain visible in-tx, got: {in_tx:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after = match_sources(&server, "l").await;
    assert!(
        !after.contains(&"staged_put".to_string()),
        "a rolled-back staged edge must not appear in MATCH, got: {after:?}"
    );
    assert!(
        after.contains(&"base_put".to_string()),
        "the committed base edge must survive the rollback, got: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_match_hides_staged_edge_delete_and_rollback_restores() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    // Committed edge, before any transaction.
    insert_self_loop(&server, "del_target", "l").await.unwrap();
    assert!(
        match_sources(&server, "l")
            .await
            .contains(&"del_target".to_string()),
        "the committed edge must be visible before the transaction"
    );

    server.exec("BEGIN").await.unwrap();
    delete_self_loop(&server, "del_target", "l")
        .await
        .expect("in-tx GRAPH DELETE EDGE should stage at statement time");

    // Read-your-own-writes: the staged tombstone must hide the edge from an
    // in-tx MATCH. Pre-fix, MATCH still traversed the durable edge.
    let in_tx = match_sources(&server, "l").await;
    assert!(
        !in_tx.contains(&"del_target".to_string()),
        "in-tx MATCH must NOT observe an edge deleted in the same transaction \
         (read-your-own-writes), got: {in_tx:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    // The delete was staged, never applied — ROLLBACK restores the edge.
    let after = match_sources(&server, "l").await;
    assert!(
        after.contains(&"del_target".to_string()),
        "a rolled-back edge delete must leave the edge visible to MATCH, got: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_match_is_committed_only_unchanged() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    insert_self_loop(&server, "auto_edge", "l").await.unwrap();

    // No transaction: MATCH resolves the active TxnId to None, so behaviour is
    // committed-CSR-only exactly as before the overlay merge.
    let rows = match_sources(&server, "l").await;
    assert!(
        rows.contains(&"auto_edge".to_string()),
        "an autocommit MATCH must see the committed edge, got: {rows:?}"
    );
}
