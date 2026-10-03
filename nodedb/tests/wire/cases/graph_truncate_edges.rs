// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE of an edge-bearing collection runs as one Calvin transaction:
//! the rows' truncate and a cut share on every vShard. Rows and edges go
//! together, and an edge sequenced before the TRUNCATE never survives it.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::harness::TestServer;

/// The nodes `GRAPH NEIGHBORS` returns for `node` in `collection`.
async fn neighbors(
    client: &tokio_postgres::Client,
    collection: &str,
    node: &str,
    direction: &str,
) -> Vec<String> {
    let messages = client
        .simple_query(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{node}' DIRECTION {direction}"
        ))
        .await
        .expect("GRAPH NEIGHBORS");
    let mut out = Vec::new();
    for message in messages {
        let tokio_postgres::SimpleQueryMessage::Row(row) = message else {
            continue;
        };
        let Some(text) = row.get(0) else {
            continue;
        };
        let parsed: Vec<serde_json::Value> = serde_json::from_str(text).unwrap_or_default();
        out.extend(
            parsed
                .iter()
                .filter_map(|entry| entry.get("node").and_then(|v| v.as_str()))
                .map(str::to_owned),
        );
    }
    out
}

fn edge_sql(collection: &str, i: usize) -> String {
    format!("INSERT INTO {collection} {{ id: 'e_{i}', _from: 'a_{i}', _to: 'b_{i}', _type: 'l' }}")
}

fn dsl_edge_sql(collection: &str, i: usize) -> String {
    format!("GRAPH INSERT EDGE IN '{collection}' FROM 'a_{i}' TO 'b_{i}' TYPE 'l'")
}

/// `GRAPH INSERT EDGE` writes race a TRUNCATE. Every edge write runs as a
/// Calvin transaction, so the TRUNCATE's cut orders it by sequence position,
/// never by a node's clock. Every edge acknowledged before the TRUNCATE was
/// sent is gone, and an edge written after it stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_graph_dsl_edge_acknowledged_before_the_truncate_never_survives_it() {
    const INSERTS: usize = 120;
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tr_dsl WITH (engine='document_schemaless')")
        .await
        .unwrap();
    server.exec(&dsl_edge_sql("tr_dsl", 0)).await.unwrap();

    let (writer, _writer_task) = server.connect_as("nodedb", "nodedb").await.unwrap();
    let acked = Arc::new(AtomicUsize::new(1));
    let progress = Arc::clone(&acked);
    let inserter = tokio::spawn(async move {
        for i in 1..INSERTS {
            writer
                .simple_query(&dsl_edge_sql("tr_dsl", i))
                .await
                .expect("edge insert");
            progress.store(i + 1, Ordering::SeqCst);
        }
    });

    while acked.load(Ordering::SeqCst) < INSERTS / 3 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let acked_before = acked.load(Ordering::SeqCst);
    server
        .exec("TRUNCATE tr_dsl")
        .await
        .expect("TRUNCATE commits");
    inserter.await.expect("inserter");

    for i in 0..acked_before {
        assert!(
            neighbors(&server.client, "tr_dsl", &format!("a_{i}"), "out")
                .await
                .is_empty(),
            "edge {i} was acknowledged before the TRUNCATE and survived"
        );
    }
    server.exec(&dsl_edge_sql("tr_dsl", INSERTS)).await.unwrap();
    assert_eq!(
        neighbors(&server.client, "tr_dsl", &format!("a_{INSERTS}"), "out").await,
        [format!("b_{INSERTS}")]
    );
}

/// A TRUNCATE removes every row and every edge of the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_removes_rows_and_edges_together() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tr_g WITH (engine='document_schemaless')")
        .await
        .unwrap();
    for i in 0..40 {
        server.exec(&edge_sql("tr_g", i)).await.unwrap();
    }
    assert_eq!(
        neighbors(&server.client, "tr_g", "a_7", "out").await,
        ["b_7"]
    );

    server
        .exec("TRUNCATE tr_g")
        .await
        .expect("TRUNCATE commits");

    let rows = server.query_text("SELECT id FROM tr_g").await.unwrap();
    assert!(rows.is_empty(), "rows remain: {rows:?}");
    for i in 0..40 {
        let from = format!("a_{i}");
        let to = format!("b_{i}");
        assert!(
            neighbors(&server.client, "tr_g", &from, "out")
                .await
                .is_empty()
        );
        assert!(
            neighbors(&server.client, "tr_g", &to, "in")
                .await
                .is_empty()
        );
    }

    // The collection takes new edges after the TRUNCATE.
    server.exec(&edge_sql("tr_g", 99)).await.unwrap();
    assert_eq!(
        neighbors(&server.client, "tr_g", "a_99", "out").await,
        ["b_99"]
    );
}

/// Edge inserts race a TRUNCATE. Every insert acknowledged before the
/// TRUNCATE was sent is gone, row and edge. Every insert that survives
/// keeps both its row and its edge: no edge outlives its row, and no row
/// loses its edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_sequenced_before_the_truncate_never_survives_it() {
    const INSERTS: usize = 150;
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tr_race WITH (engine='document_schemaless')")
        .await
        .unwrap();
    server.exec(&edge_sql("tr_race", 0)).await.unwrap();

    let (writer, _writer_task) = server.connect_as("nodedb", "nodedb").await.unwrap();
    let acked = Arc::new(AtomicUsize::new(1));
    let progress = Arc::clone(&acked);
    let inserter = tokio::spawn(async move {
        for i in 1..INSERTS {
            writer
                .simple_query(&edge_sql("tr_race", i))
                .await
                .expect("edge insert");
            progress.store(i + 1, Ordering::SeqCst);
        }
    });

    while acked.load(Ordering::SeqCst) < INSERTS / 3 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let acked_before = acked.load(Ordering::SeqCst);
    server
        .exec("TRUNCATE tr_race")
        .await
        .expect("TRUNCATE commits");
    inserter.await.expect("inserter");

    let rows: BTreeSet<String> = server
        .query_text("SELECT id FROM tr_race")
        .await
        .unwrap()
        .into_iter()
        .collect();
    for i in 0..INSERTS {
        let row = rows.contains(&format!("e_{i}"));
        let edge = !neighbors(&server.client, "tr_race", &format!("a_{i}"), "out")
            .await
            .is_empty();
        assert_eq!(
            row, edge,
            "insert {i}: row present {row}, edge present {edge}"
        );
        if i < acked_before {
            assert!(
                !edge,
                "insert {i} was acknowledged before the TRUNCATE and survived"
            );
        }
    }
}
