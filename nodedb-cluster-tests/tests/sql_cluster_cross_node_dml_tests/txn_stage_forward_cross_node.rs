// SPDX-License-Identifier: BUSL-1.1

//! An in-transaction write forwarded to a remote multi-core leader runs on the
//! one core that owns its vShard.
//!
//! A coordinator that does not lead a collection's group forwards each staged
//! write to the leader as a `StageWrite`. The leader runs 2 cores, and only
//! the core owning the collection's vShard holds its rows. Each collection
//! here homes to core 1, so a forward fanned to every core answers with core
//! 0's result: a bulk UPDATE matches no rows there, and `KV_INCR` counts from
//! an absent key. The forward must return the owning core's answer.

use std::time::Duration;

use nodedb_types::CollectionKey;
use nodedb_types::id::DatabaseId;

use crate::common::cluster_harness::{TestCluster, wait_for};

const CORES_PER_NODE: usize = 2;
/// The core every test collection homes to on its leader.
const OWNING_CORE: u32 = 1;
const MATCHING_ROWS: usize = 5;

/// The first `{prefix}_{i}` whose vShard homes to [`OWNING_CORE`].
fn collection_on_owning_core(prefix: &str) -> String {
    (0u32..)
        .map(|i| format!("{prefix}_{i}"))
        .find(|name| {
            let vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, name).vshard();
            vshard.as_u32() % CORES_PER_NODE as u32 == OWNING_CORE
        })
        .unwrap_or_default()
}

/// The index of a node that does not lead `collection`'s group.
fn non_leader_for(cluster: &TestCluster, collection: &str) -> usize {
    let vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, collection)
        .vshard()
        .as_u32();
    let group = cluster.nodes[0]
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(vshard)
        .expect("group for the collection's vShard");
    let leader = cluster.nodes[0]
        .all_group_leaders()
        .into_iter()
        .find(|(g, _)| *g == group)
        .map(|(_, l)| l)
        .expect("leader for the collection's group");
    cluster
        .nodes
        .iter()
        .position(|n| n.node_id != leader)
        .expect("a node that does not lead the collection's group")
}

/// The row count a statement's `CommandComplete` reports.
async fn affected(client: &tokio_postgres::Client, sql: &str) -> u64 {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no CommandComplete"))
}

/// The JSON text column a `SELECT KV_*(...)` returns.
async fn kv_result(client: &tokio_postgres::Client, sql: &str) -> serde_json::Value {
    let msgs = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let text = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no row"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{sql}: {text}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn forwarded_staged_writes_answer_from_the_owning_core() {
    let cluster = TestCluster::spawn_three_with_cores(CORES_PER_NODE)
        .await
        .expect("3-node 2-core cluster");

    let docs = collection_on_owning_core("fwd_docs");
    let kv = collection_on_owning_core("fwd_kv");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {docs}"))
        .await
        .expect("CREATE COLLECTION docs");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {kv} (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')"
        ))
        .await
        .expect("CREATE COLLECTION kv");

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
    wait_for(
        "all groups have a stable leader",
        Duration::from_secs(15),
        Duration::from_millis(100),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.all_group_leaders().iter().all(|(_, l)| *l != 0))
        },
    )
    .await;

    for i in 0..MATCHING_ROWS {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {docs} (id, grp, n) VALUES ('a{i}', 'a', {i})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert a{i}: {e}"));
    }
    for i in 0..2 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {docs} (id, grp, n) VALUES ('b{i}', 'b', {i})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert b{i}: {e}"));
    }
    cluster.nodes[0]
        .client
        .simple_query(&format!("INSERT INTO {kv} (key, n) VALUES ('ctr', 5)"))
        .await
        .expect("insert ctr");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Bulk UPDATE: the owning core matches every 'a' row. Core 0 matches none.
    let coord = non_leader_for(&cluster, &docs);
    let client = &cluster.nodes[coord].client;
    client.simple_query("BEGIN").await.expect("BEGIN");
    let updated = affected(client, &format!("UPDATE {docs} SET n = 99 WHERE grp = 'a'")).await;
    client.simple_query("ROLLBACK").await.expect("ROLLBACK");
    assert_eq!(
        updated, MATCHING_ROWS as u64,
        "node {}: a forwarded bulk UPDATE must report the owning core's count",
        cluster.nodes[coord].node_id
    );

    // KV_INCR: the owning core counts from the stored 5. Core 0 holds no row.
    let coord = non_leader_for(&cluster, &kv);
    let client = &cluster.nodes[coord].client;
    client.simple_query("BEGIN").await.expect("BEGIN");
    let incr = kv_result(client, &format!("SELECT KV_INCR('{kv}', 'ctr', 3)")).await;
    let chained = kv_result(client, &format!("SELECT KV_INCR('{kv}', 'ctr', 2)")).await;
    client.simple_query("ROLLBACK").await.expect("ROLLBACK");
    assert_eq!(
        incr["value"], 8,
        "node {}: a forwarded KV_INCR must count from the owning core's row",
        cluster.nodes[coord].node_id
    );
    assert_eq!(
        chained["value"], 10,
        "node {}: a second forwarded KV_INCR must chain off the first staged value",
        cluster.nodes[coord].node_id
    );

    cluster.shutdown().await;
}
