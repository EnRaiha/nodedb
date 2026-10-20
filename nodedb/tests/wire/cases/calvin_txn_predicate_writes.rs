// SPDX-License-Identifier: BUSL-1.1

//! A predicate `UPDATE` or `DELETE` inside a transaction commits through
//! Calvin.
//!
//! A delete on an edge-bearing collection carries one node guard per deleted
//! row, homed on the row's key vShard. The COMMIT then spans two vShards and
//! takes the Calvin path. The predicate write carries the rows its statement
//! read, so the Calvin stage writes exactly those rows. A statement inside a
//! transaction block, a trigger body and a procedure body all buffer their
//! writes the same way.

use crate::harness::TestServer;

/// Four rows and one edge `d0 -> d1` in the edge-bearing collection `g`.
async fn seed_edge_collection(server: &TestServer) {
    for sql in [
        "CREATE COLLECTION g",
        "UPSERT INTO g { id: 'd0', title: 't0' }",
        "UPSERT INTO g { id: 'd1', title: 't1' }",
        "UPSERT INTO g { id: 'd2', title: 't2' }",
        "UPSERT INTO g { id: 'd3', title: 't3' }",
        "GRAPH INSERT EDGE IN 'g' FROM 'd0' TO 'd1' TYPE 'l'",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// The ids of the rows of `g` the predicate `filter` matches.
async fn ids_where(server: &TestServer, filter: &str) -> Vec<String> {
    let mut ids: Vec<String> = server
        .query_rows(&format!("SELECT id FROM g WHERE {filter}"))
        .await
        .expect("select")
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect();
    ids.sort();
    ids
}

/// The destination nodes of `node`'s outgoing `l` edges in `g`.
async fn neighbors_of(server: &TestServer, node: &str) -> Vec<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN 'g' OF '{node}' LABEL 'l' DIRECTION out"
        ))
        .await
        .expect("neighbors");
    let mut out = Vec::new();
    for row in rows {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&row).unwrap_or_default();
        for entry in parsed {
            if let Some(n) = entry.get("node").and_then(|v| v.as_str()) {
                out.push(n.to_string());
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_block_delete_on_edge_bearing_collection_commits() {
    let server = TestServer::start().await;
    seed_edge_collection(&server).await;

    server.exec("BEGIN").await.unwrap();
    server.exec("DELETE FROM g WHERE id = 'd3'").await.unwrap();
    server
        .exec("COMMIT")
        .await
        .expect("a Calvin COMMIT of a buffered predicate delete must succeed");

    assert!(ids_where(&server, "id = 'd3'").await.is_empty());
    assert_eq!(neighbors_of(&server, "d0").await, vec!["d1".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_block_update_and_delete_on_edge_bearing_collection_commits() {
    let server = TestServer::start().await;
    seed_edge_collection(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE g SET title = 'x' WHERE id = 'd2'")
        .await
        .unwrap();
    server.exec("DELETE FROM g WHERE id = 'd3'").await.unwrap();
    server.exec("COMMIT").await.expect("COMMIT must succeed");

    assert_eq!(
        ids_where(&server, "title = 'x'").await,
        vec!["d2".to_string()]
    );
    assert!(ids_where(&server, "id = 'd3'").await.is_empty());
}

/// The delete's prediction is read as the transaction sees its rows, so a
/// row the same block inserted is one it deletes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_block_delete_matches_its_own_earlier_insert() {
    let server = TestServer::start().await;
    seed_edge_collection(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPSERT INTO g { id: 'd9', title: 'mine' }")
        .await
        .unwrap();
    server
        .exec("DELETE FROM g WHERE title = 'mine'")
        .await
        .unwrap();
    server.exec("COMMIT").await.expect("COMMIT must succeed");

    assert!(ids_where(&server, "title = 'mine'").await.is_empty());
}

/// A SYNC AFTER body joins its statement's implicit transaction. Its
/// predicate delete on an edge-bearing collection commits with the insert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trigger_body_predicate_delete_commits_with_its_insert() {
    let server = TestServer::start().await;
    seed_edge_collection(&server).await;
    for ddl in [
        "CREATE COLLECTION orders",
        "CREATE SYNC TRIGGER drop_title AFTER INSERT ON orders FOR EACH ROW \
         BEGIN DELETE FROM g WHERE title = NEW.title; END",
    ] {
        server
            .exec(ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }

    server
        .exec("INSERT INTO orders { id: 'o1', title: 't3' }")
        .await
        .expect("the insert and its body's predicate delete must commit");

    assert!(ids_where(&server, "id = 'd3'").await.is_empty());
    let orders = server
        .query_rows("SELECT id FROM orders WHERE id = 'o1'")
        .await
        .expect("select orders");
    assert_eq!(orders.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedure_body_predicate_delete_on_edge_bearing_collection_commits() {
    let server = TestServer::start().await;
    seed_edge_collection(&server).await;
    server
        .exec(
            "CREATE PROCEDURE prune() AS \
             BEGIN \
               UPDATE g SET title = 'x' WHERE id = 'd2'; \
               DELETE FROM g WHERE id = 'd3'; \
             END",
        )
        .await
        .expect("CREATE PROCEDURE");

    server.exec("CALL prune()").await.expect("CALL must commit");

    assert_eq!(
        ids_where(&server, "title = 'x'").await,
        vec!["d2".to_string()]
    );
    assert!(ids_where(&server, "id = 'd3'").await.is_empty());
}

/// A row that joins the predicate after the statement read it moves the
/// prediction. COMMIT refuses the stale set with a serialization conflict
/// and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_block_commit_after_a_concurrent_match_change_aborts_40001() {
    const PASSWORD: &str = "calvin-txn-predicate-7";
    let server = TestServer::start().await;
    seed_edge_collection(&server).await;
    server
        .exec(&format!("CREATE USER writer PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    let (writer, handle) = server
        .connect_as("writer", PASSWORD)
        .await
        .expect("second connection");

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DELETE FROM g WHERE title = 't3'")
        .await
        .unwrap();
    writer
        .simple_query("UPSERT INTO g { id: 'n1', title: 't3' }")
        .await
        .expect("the concurrent write commits");
    drop(writer);
    handle.abort();

    let commit = server.client.simple_query("COMMIT").await;
    let error = commit.expect_err("a stale prediction must abort the COMMIT");
    let code = error.as_db_error().map(|e| e.code().code().to_string());
    assert_eq!(code.as_deref(), Some("40001"), "{error:?}");

    let mut survivors = ids_where(&server, "title = 't3'").await;
    survivors.sort();
    assert_eq!(survivors, vec!["d3".to_string(), "n1".to_string()]);
}
