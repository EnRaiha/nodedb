// SPDX-License-Identifier: BUSL-1.1

//! A cloned database carries its synonym groups and custom types.
//!
//! Both are database-scoped, so a clone missing them answers differently from
//! its source: a text query expands fewer terms, and a typed column resolves
//! against nothing.
//!
//! Each object needs three effects, and the clone handler proposes a catalog
//! entry per row precisely to get all three: the catalog write, the in-memory
//! registry `SHOW` reads, and — for a synonym group — the FTS backend on every
//! node. This asserts through `SHOW` and through expansion rather than through
//! the catalog row, because a catalog-only copy passes a row assertion while
//! both live effects are still missing.

use crate::harness::TestServer;

fn row_values(msgs: &[tokio_postgres::SimpleQueryMessage], column: usize) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(column).map(|v| v.to_string()),
            _ => None,
        })
        .collect()
}

/// The clone lists the source's group and type, and its own FTS backend
/// expands the copied group over rows written into the clone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clone_carries_its_synonym_groups_and_custom_types() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE syn_clone_src")
        .await
        .expect("CREATE DATABASE syn_clone_src");
    client
        .simple_query("USE DATABASE syn_clone_src")
        .await
        .expect("USE syn_clone_src");

    client
        .simple_query("CREATE COLLECTION clone_docs WITH (engine='document_schemaless')")
        .await
        .expect("CREATE COLLECTION clone_docs");
    client
        .simple_query("CREATE SEARCH INDEX idx_clone_docs ON clone_docs FIELDS body")
        .await
        .expect("CREATE SEARCH INDEX idx_clone_docs");
    client
        .simple_query("CREATE SYNONYM GROUP vehicles AS ('automobile', 'car')")
        .await
        .expect("CREATE SYNONYM GROUP vehicles");
    client
        .simple_query("CREATE TYPE mood AS ENUM ('joy', 'anger')")
        .await
        .expect("CREATE TYPE mood");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE syn_clone_dst FROM syn_clone_src")
        .await
        .expect("CLONE DATABASE syn_clone_dst");
    client
        .simple_query("USE DATABASE syn_clone_dst")
        .await
        .expect("USE syn_clone_dst");

    // The registry effect. `SHOW` reads the in-memory registry, not the
    // catalog, so a catalog-only copy lists nothing here until restart.
    let groups = client
        .simple_query("SHOW SYNONYM GROUPS")
        .await
        .expect("SHOW SYNONYM GROUPS in the clone");
    assert!(
        row_values(&groups, 0).iter().any(|n| n == "vehicles"),
        "the clone must list the source's synonym group: {:?}",
        row_values(&groups, 0)
    );

    let types = client
        .simple_query("SHOW TYPES")
        .await
        .expect("SHOW TYPES in the clone");
    assert!(
        row_values(&types, 0).iter().any(|n| n == "mood"),
        "the clone must list the source's custom type: {:?}",
        row_values(&types, 0)
    );

    // A shadowed clone refuses every Text engine query shape, so the clone
    // materializes before the expansion assertion.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("ALTER DATABASE syn_clone_dst MATERIALIZE")
        .await
        .expect("ALTER DATABASE syn_clone_dst MATERIALIZE");
    client
        .simple_query("USE DATABASE syn_clone_dst")
        .await
        .expect("USE syn_clone_dst");

    // The FTS effect, asserted over rows the clone writes itself so the result
    // depends on the clone's own backend rather than on copy-up delegation.
    client
        .simple_query("INSERT INTO clone_docs { id: 'f1', body: 'automobile engine repair' }")
        .await
        .expect("insert f1");
    client
        .simple_query("INSERT INTO clone_docs { id: 'f2', body: 'car maintenance guide' }")
        .await
        .expect("insert f2");

    let matched = client
        .simple_query("SELECT id FROM clone_docs WHERE text_match(body, 'automobile') ORDER BY id")
        .await
        .expect("text_match in the clone");
    let ids = row_values(&matched, 0);
    assert!(
        ids.iter().any(|id| id == "f1"),
        "f1 must match the query term: {ids:?}"
    );
    assert!(
        ids.iter().any(|id| id == "f2"),
        "f2 must match through the copied group, so the clone's FTS backend \
         received it: {ids:?}"
    );
}
