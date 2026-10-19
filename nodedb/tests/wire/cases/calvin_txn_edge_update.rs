// SPDX-License-Identifier: BUSL-1.1

//! Predicate writes inside a transaction that need more than the row set:
//!
//! - An UPDATE of an edge document's `_to` moves its mirrored graph edge in
//!   the same transaction. The edge tasks derive from the statement's own
//!   read, so the COMMIT takes the Calvin path with them.
//! - A predicate DELETE on a `bitemporal=true` collection reads and checks
//!   the rows' current versions, so its Calvin COMMIT removes them.

use crate::harness::TestServer;

/// The destination nodes of `node`'s outgoing `l` edges in `collection`.
async fn neighbors_of(server: &TestServer, collection: &str, node: &str) -> Vec<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{node}' LABEL 'l' DIRECTION out"
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
async fn txn_block_update_of_edge_target_moves_the_graph_edge() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION ge").await.unwrap();
    server
        .exec("INSERT INTO ge { id: 'e1', _from: 'a', _to: 'b', _type: 'l' }")
        .await
        .unwrap();
    assert_eq!(
        neighbors_of(&server, "ge", "a").await,
        vec!["b".to_string()]
    );

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE ge SET _to = 'c' WHERE id = 'e1'")
        .await
        .unwrap();
    server.exec("COMMIT").await.expect("COMMIT must succeed");

    let after = neighbors_of(&server, "ge", "a").await;
    assert_eq!(
        after,
        vec!["c".to_string()],
        "the traversal follows the moved edge, not the old one"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitemporal_predicate_delete_commits_through_calvin() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE COLLECTION bt (id STRING PRIMARY KEY, region STRING) \
         WITH (engine='document_schemaless', bitemporal=true)",
        "INSERT INTO bt (id, region) VALUES ('r0', 'us')",
        "INSERT INTO bt (id, region) VALUES ('r1', 'eu')",
        "INSERT INTO bt (id, region) VALUES ('r2', 'eu')",
        // An edge makes the collection edge-bearing, so a delete carries
        // node guards on the rows' key vShards and COMMIT takes Calvin.
        "GRAPH INSERT EDGE IN 'bt' FROM 'r0' TO 'r1' TYPE 'l'",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DELETE FROM bt WHERE region = 'eu'")
        .await
        .unwrap();
    server.exec("COMMIT").await.expect("COMMIT must succeed");

    let mut ids: Vec<String> = server
        .query_rows("SELECT id FROM bt")
        .await
        .expect("select")
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["r0".to_string()]);
    assert!(
        neighbors_of(&server, "bt", "r0").await.is_empty(),
        "the deleted node's incident edge goes with it"
    );
}
