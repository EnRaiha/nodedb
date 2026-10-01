// SPDX-License-Identifier: BUSL-1.1

//! A CRDT document delete tombstones its node's edges only when it removes a
//! stored document. A delete that matches no document leaves every edge of
//! the node live.

use crate::harness::TestServer;

const COLL: &str = "crdt_edge_nodes";

async fn create(srv: &TestServer) {
    srv.exec(&format!(
        "CREATE TABLE {COLL} (id TEXT PRIMARY KEY, name TEXT) WITH (crdt='true')"
    ))
    .await
    .expect("create crdt collection");
}

async fn insert_edge(srv: &TestServer, src: &str, dst: &str) {
    srv.exec(&format!(
        "GRAPH INSERT EDGE IN '{COLL}' FROM '{src}' TO '{dst}' TYPE 'knows'"
    ))
    .await
    .expect("insert edge");
}

/// Every live `knows` edge of the collection, as `src\tdst` rows.
async fn live_edges(srv: &TestServer) -> Vec<String> {
    srv.query_text_joined(&format!("MATCH (x)-[:knows]->(y) IN '{COLL}' RETURN x, y"))
        .await
        .expect("match edges")
}

fn has_edge(edges: &[String], src: &str, dst: &str) -> bool {
    edges
        .iter()
        .any(|row| row.contains(src) && row.contains(dst))
}

/// A delete of a stored document tombstones the node's edges. A second
/// delete of the same key finds no document, so an edge written between the
/// two deletes stays live.
#[tokio::test]
async fn deleting_a_missing_crdt_document_keeps_its_node_edges() {
    let srv = TestServer::start().await;
    create(&srv).await;
    srv.exec(&format!("INSERT INTO {COLL} (id, name) VALUES ('a', 'A')"))
        .await
        .expect("insert document");
    insert_edge(&srv, "a", "b").await;
    assert!(
        has_edge(&live_edges(&srv).await, "a", "b"),
        "the edge is live before the delete"
    );

    srv.exec(&format!("DELETE FROM {COLL} WHERE id = 'a'"))
        .await
        .expect("delete stored document");
    assert!(
        !has_edge(&live_edges(&srv).await, "a", "b"),
        "a delete that removes the document tombstones its node's edges"
    );

    insert_edge(&srv, "a", "c").await;
    srv.exec(&format!("DELETE FROM {COLL} WHERE id = 'a'"))
        .await
        .expect("delete missing document");
    let edges = live_edges(&srv).await;
    assert!(
        has_edge(&edges, "a", "c"),
        "a delete that removes no document keeps the node's edges: {edges:?}"
    );
}

/// A delete of an id no write ever bound names it absent in its presence
/// guard, removes nothing, and leaves other nodes' edges live. A document
/// stored under that id later is deleted with its node's edges.
#[tokio::test]
async fn deleting_an_unbound_crdt_document_removes_nothing_until_it_is_stored() {
    let srv = TestServer::start().await;
    create(&srv).await;
    insert_edge(&srv, "p", "q").await;

    srv.exec(&format!("DELETE FROM {COLL} WHERE id = 'unbound'"))
        .await
        .expect("delete an id no write bound");
    assert!(
        has_edge(&live_edges(&srv).await, "p", "q"),
        "a delete that removes no document keeps every edge"
    );

    srv.exec(&format!(
        "INSERT INTO {COLL} (id, name) VALUES ('unbound', 'U')"
    ))
    .await
    .expect("store the document");
    insert_edge(&srv, "unbound", "r").await;
    srv.exec(&format!("DELETE FROM {COLL} WHERE id = 'unbound'"))
        .await
        .expect("delete the stored document");
    let edges = live_edges(&srv).await;
    assert!(
        !has_edge(&edges, "unbound", "r"),
        "the delete of the stored document tombstones its node's edges: {edges:?}"
    );
    assert!(has_edge(&edges, "p", "q"), "other nodes' edges stay live");
}

/// A delete keyed by a node that only an edge names, with no document ever
/// stored, removes nothing and keeps the edge live.
#[tokio::test]
async fn deleting_a_never_stored_crdt_document_keeps_its_node_edges() {
    let srv = TestServer::start().await;
    create(&srv).await;
    insert_edge(&srv, "ghost", "d").await;

    srv.exec(&format!("DELETE FROM {COLL} WHERE id = 'ghost'"))
        .await
        .expect("delete never-stored document");
    let edges = live_edges(&srv).await;
    assert!(
        has_edge(&edges, "ghost", "d"),
        "a delete that removes no document keeps the node's edges: {edges:?}"
    );
}
