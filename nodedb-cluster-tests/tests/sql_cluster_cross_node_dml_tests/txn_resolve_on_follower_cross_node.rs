// SPDX-License-Identifier: BUSL-1.1

//! A `MERGE` or `UPDATE ... FROM` inside a transaction, issued on a node that
//! does not lead the target's group, resolves against the rows the same
//! transaction staged.
//!
//! The transaction's staging overlay lives on the target group's leader. The
//! resolve pass is a read (`DocumentOp::ResolveWrite`), so it is routed to the
//! leader with the transaction id, and folds the overlay in there. A resolve on
//! the follower's own replica misses the staged row: `MERGE` inserts
//! a duplicate, and `UPDATE ... FROM` leaves the staged row unchanged.

use std::collections::BTreeMap;
use std::time::Duration;

use nodedb_types::CollectionKey;
use nodedb_types::id::DatabaseId;

use crate::common::cluster_harness::{TestCluster, wait_for};

const SOURCE: &str = "txr_source";
const MERGE_TARGET: &str = "txr_merge_target";
const UPDATE_TARGET: &str = "txr_update_target";

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

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// `id → (sku, qty)` for every row of `collection`.
async fn rows_by_id(
    client: &tokio_postgres::Client,
    collection: &str,
) -> BTreeMap<String, (String, String)> {
    let sql = format!("SELECT id, sku, qty FROM {collection}");
    let msgs = client
        .simple_query(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some((
                r.get("id").unwrap_or("").to_string(),
                (
                    r.get("sku").unwrap_or("").to_string(),
                    r.get("qty").unwrap_or("").to_string(),
                ),
            )),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn in_transaction_resolve_on_a_follower_sees_the_staged_rows() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    for collection in [SOURCE, MERGE_TARGET, UPDATE_TARGET] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {collection} TYPE document"))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {collection}: {e}"));
    }
    wait_for(
        "all 3 nodes see the collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 3)
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

    exec(
        &cluster.nodes[0].client,
        &format!("INSERT INTO {SOURCE} (id, sku, qty) VALUES ('s_a', 'a', 5), ('s_b', 'b', 7)"),
    )
    .await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // MERGE: the staged 'a' row must match, so only 'b' is inserted.
    let coord = non_leader_for(&cluster, MERGE_TARGET);
    let client = &cluster.nodes[coord].client;
    exec(client, "BEGIN").await;
    exec(
        client,
        &format!("INSERT INTO {MERGE_TARGET} (id, sku, qty) VALUES ('t_a', 'a', 1)"),
    )
    .await;
    exec(
        client,
        &format!(
            "MERGE INTO {MERGE_TARGET} t USING {SOURCE} s ON t.sku = s.sku \
             WHEN MATCHED THEN UPDATE SET qty = s.qty \
             WHEN NOT MATCHED THEN INSERT (id, sku, qty) VALUES (s.id, s.sku, s.qty)"
        ),
    )
    .await;
    exec(client, "COMMIT").await;

    // UPDATE ... FROM: the staged 'a' row must be updated.
    let coord = non_leader_for(&cluster, UPDATE_TARGET);
    let client = &cluster.nodes[coord].client;
    exec(client, "BEGIN").await;
    exec(
        client,
        &format!("INSERT INTO {UPDATE_TARGET} (id, sku, qty) VALUES ('u_a', 'a', 1)"),
    )
    .await;
    exec(
        client,
        &format!(
            "UPDATE {UPDATE_TARGET} SET qty = s.qty FROM {SOURCE} s \
             WHERE {UPDATE_TARGET}.sku = s.sku"
        ),
    )
    .await;
    exec(client, "COMMIT").await;

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let merged = rows_by_id(&cluster.nodes[0].client, MERGE_TARGET).await;
    assert_eq!(
        merged,
        BTreeMap::from([
            ("t_a".to_string(), ("a".to_string(), "5".to_string())),
            ("s_b".to_string(), ("b".to_string(), "7".to_string())),
        ]),
        "MERGE on a follower must match the row its transaction staged"
    );

    let updated = rows_by_id(&cluster.nodes[0].client, UPDATE_TARGET).await;
    assert_eq!(
        updated,
        BTreeMap::from([("u_a".to_string(), ("a".to_string(), "5".to_string()))]),
        "UPDATE ... FROM on a follower must update the row its transaction staged"
    );

    cluster.shutdown().await;
}
