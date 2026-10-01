// SPDX-License-Identifier: BUSL-1.1

//! A strict collection with no declared primary key keeps each row's identity
//! in its `_rowid` system column. A clone copies a row under a new surrogate,
//! by copy-up or by materialization, and the row must keep the id it showed
//! in the source.

use std::collections::BTreeMap;

use crate::harness::TestServer;

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// `name -> (visible id, _rowid)` for every row of `items`.
async fn identities(server: &TestServer, stage: &str) -> BTreeMap<String, (String, String)> {
    server
        .query_named_rows("SELECT * FROM items")
        .await
        .unwrap_or_else(|e| panic!("{stage}: read items: {e}"))
        .into_iter()
        .map(|row| {
            let name = row.get("name").cloned().unwrap_or_default();
            let rowid = row.get("_rowid").cloned().unwrap_or_default();
            let id = row.get("id").cloned().unwrap_or_else(|| rowid.clone());
            (name, (id, rowid))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_no_pk_row_keeps_its_id_through_copy_up_and_materialize() {
    let server = TestServer::start().await;
    exec(&server, "CREATE DATABASE srid_src").await;
    exec(&server, "USE DATABASE srid_src").await;
    exec(
        &server,
        "CREATE COLLECTION items (name STRING, qty INT) WITH (engine='document_strict')",
    )
    .await;
    for (name, qty) in [("a", 1), ("b", 2), ("c", 3)] {
        exec(
            &server,
            &format!("INSERT INTO items (name, qty) VALUES ('{name}', {qty})"),
        )
        .await;
    }
    let source = identities(&server, "source").await;
    assert_eq!(source.len(), 3, "source rows: {source:?}");

    exec(&server, "USE DATABASE default").await;
    exec(&server, "CLONE DATABASE srid_clone FROM srid_src LATEST").await;
    exec(&server, "USE DATABASE srid_clone").await;
    assert_eq!(identities(&server, "shadowed clone").await, source);

    // Copy-up: the updated row lands under a new target surrogate.
    let (_, b_rowid) = &source["b"];
    exec(
        &server,
        &format!("UPDATE items SET qty = 20 WHERE _rowid = {b_rowid}"),
    )
    .await;
    assert_eq!(
        identities(&server, "after copy-up").await,
        source,
        "a copied-up row keeps its id"
    );

    // Materialization copies every remaining row under a new surrogate.
    exec(&server, "ALTER DATABASE srid_clone MATERIALIZE").await;
    assert_eq!(
        identities(&server, "after materialize").await,
        source,
        "a materialized row keeps its id"
    );
    let qty_b = server
        .query_rows("SELECT qty FROM items WHERE name = 'b'")
        .await
        .unwrap_or_else(|e| panic!("read b: {e}"));
    assert_eq!(qty_b, vec![vec!["20".to_string()]], "the update survives");
}
