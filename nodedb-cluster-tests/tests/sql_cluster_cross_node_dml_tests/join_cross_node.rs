// SPDX-License-Identifier: BUSL-1.1

//! Regression test for the "cross-node JOIN build-side gather" correctness bug.
//!
//! A cross-node `SELECT ... FROM fact JOIN dim ON fact.fk = dim.id` joins two
//! single-vShard-homed collections that live on DIFFERENT vShards (and so,
//! potentially, different nodes). The HashJoin task routes to the LEFT (probe)
//! collection's owning vShard, where the LEFT side is scanned locally. Before
//! the fix, the RIGHT (build) collection was scanned BY NAME from that same
//! node — but the build collection is homed elsewhere, so the by-name scan
//! returned nothing and the join silently dropped all matching rows.
//!
//! After the fix, `resolve_exchange` (cluster mode) gathers the build
//! collection across all vShards on the coordinator and inlines it as a
//! `ProviderScan`, so the HashJoin shipped to the probe node is self-contained.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Helper: run a simple query and return the number of data rows returned.
async fn count_rows(client: &tokio_postgres::Client, sql: &str) -> usize {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    msgs.into_iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// A cross-node inner join between two single-vShard-homed collections must
/// return every matching row, regardless of which node the SELECT is issued
/// from. Before the build-side gather fix this returned 0 rows when the build
/// collection was homed on a different node than the probe collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_join_returns_all_matches() {
    // Pick two collection names that hash to DIFFERENT vShards so the probe and
    // build sides are genuinely homed on different routes (and likely different
    // nodes). Assert the divergence up front — if these ever collide, the test
    // would no longer exercise the cross-node path.
    use nodedb_cluster::routing::vshard_for_collection;
    use nodedb_types::DatabaseId;
    const FACT: &str = "fact";
    const DIM: &str = "dim";
    assert_ne!(
        vshard_for_collection(DatabaseId::DEFAULT, FACT),
        vshard_for_collection(DatabaseId::DEFAULT, DIM),
        "test collections must hash to different vShards to exercise cross-node join"
    );

    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION dim \
             (id TEXT PRIMARY KEY, label TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION dim");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION fact \
             (id TEXT PRIMARY KEY, fk TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION fact");

    // Wait for all nodes to see both new collections before inserting.
    wait_for(
        "all 3 nodes see both collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 2)
        },
    )
    .await;

    // 5 dimension rows: c1..c5.
    cluster.nodes[0]
        .client
        .simple_query(
            "INSERT INTO dim (id, label) VALUES \
             ('c1', 'one'), ('c2', 'two'), ('c3', 'three'), \
             ('c4', 'four'), ('c5', 'five')",
        )
        .await
        .expect("insert dim rows");

    // 8 fact rows: 6 with fk in c1..c3 (matching), 2 with non-matching fk.
    cluster.nodes[0]
        .client
        .simple_query(
            "INSERT INTO fact (id, fk) VALUES \
             ('f1', 'c1'), ('f2', 'c1'), ('f3', 'c2'), \
             ('f4', 'c2'), ('f5', 'c3'), ('f6', 'c3'), \
             ('f7', 'zz'), ('f8', 'yy')",
        )
        .await
        .expect("insert fact rows");

    // Wait until every node sees the full local row counts so replication has
    // completed before we assert the cross-node join.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all dim rows"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM dim"))
                });
                n >= 5
            },
        )
        .await;
        wait_for(
            &format!("node {idx} sees all fact rows"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM fact"))
                });
                n >= 8
            },
        )
        .await;
    }

    // The inner join must return exactly the 6 matching fact rows, regardless of
    // which node issues the SELECT. Run from node 0 and node 1.
    let join_sql = "SELECT f.id FROM fact f JOIN dim d ON f.fk = d.id";

    let from_node_0 = count_rows(&cluster.nodes[0].client, join_sql).await;
    assert_eq!(
        from_node_0, 6,
        "cross-node join from node 0 must return all 6 matches; got {from_node_0}"
    );

    let from_node_1 = count_rows(&cluster.nodes[1].client, join_sql).await;
    assert_eq!(
        from_node_1, 6,
        "cross-node join from node 1 must return all 6 matches; got {from_node_1}"
    );

    cluster.shutdown().await;
}
