// SPDX-License-Identifier: BUSL-1.1

//! A read issued on a node that does not replicate the collection's home
//! group reads the rows from the group's owner.
//!
//! With a replication factor of 1, each data group lives on one node. The
//! other nodes hold no rows of the collection. `CREATE GRAPH INDEX` scans the
//! collection, and `COPY ... TO` reads it through the internal dispatch
//! funnel. Both run on a node outside the group and must see every row.

use std::time::Duration;

use crate::common;
use common::cluster_harness::TestCluster;
use common::cluster_harness::shared_steps::db_detail;
use common::cluster_harness::wait::wait_for;

const COLLECTION: &str = "owner_read_docs";
const INDEX: &str = "owner_read_reports";
const ROOT: &str = "root";
const CHILDREN: [&str; 3] = ["c0", "c1", "c2"];

/// The first row's `column` value from a simple query.
async fn first_value(client: &tokio_postgres::Client, sql: &str, column: &str) -> String {
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {}", db_detail(&e)));
    messages
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(column).map(str::to_owned),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no row with column `{column}`"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_on_a_node_outside_the_home_group_see_every_row() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION");
    let group_id = cluster.nodes[0]
        .group_id_for_collection(COLLECTION)
        .expect("the collection's data group");
    // Placement convergence removes the nodes outside the group's placement,
    // so exactly one node ends up replicating it.
    wait_for(
        "exactly one node replicates the collection's group",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .filter(|node| node.replicates_data_group(group_id))
                .count()
                == 1
        },
    )
    .await;
    let reader = cluster
        .nodes
        .iter()
        .find(|node| !node.replicates_data_group(group_id))
        .expect("a node that does not replicate the group");

    reader
        .client
        .simple_query(&format!(
            "INSERT INTO {COLLECTION} (id, parent) VALUES ('{ROOT}', '')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {ROOT}: {}", db_detail(&e)));
    for child in CHILDREN {
        reader
            .client
            .simple_query(&format!(
                "INSERT INTO {COLLECTION} (id, parent) VALUES ('{child}', '{ROOT}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {child}: {}", db_detail(&e)));
    }

    // The index scan reads the collection's home group from its owner. A
    // local-only scan on this node finds no rows and creates no edges.
    let edges_created = first_value(
        &reader.client,
        &format!("CREATE GRAPH INDEX {INDEX} ON {COLLECTION} (parent -> id)"),
        "edges_created",
    )
    .await;
    assert_eq!(
        edges_created,
        CHILDREN.len().to_string(),
        "CREATE GRAPH INDEX on a node outside the home group must index every row"
    );

    // COPY TO reads through the internal dispatch funnel on this node.
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("owner_read.ndjson");
    let path_text = path.to_str().expect("UTF-8 temporary path");
    reader
        .client
        .simple_query(&format!(
            "COPY {COLLECTION} TO '{path_text}' WITH (FORMAT ndjson)"
        ))
        .await
        .unwrap_or_else(|e| panic!("COPY TO: {}", db_detail(&e)));
    let exported = std::fs::read_to_string(&path).expect("read the exported file");
    let rows = exported
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
    assert_eq!(
        rows,
        CHILDREN.len() + 1,
        "a funnel read on a node outside the home group must return every row, got: {exported}"
    );

    cluster.shutdown().await;
}
