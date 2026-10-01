// SPDX-License-Identifier: BUSL-1.1

//! A transaction that commits through Calvin reads its own timeseries rows.
//!
//! The transaction inserts a timeseries row and a document on another
//! vShard, so its COMMIT is sequenced through the single-node Calvin stack.
//! Before COMMIT, a `SELECT` in the same transaction reads the timeseries
//! row. After COMMIT, which resolves the ingest to its rows before it is
//! sequenced and stages those rows on the scheduler, the row reads back
//! committed, with the value the transaction wrote.

use crate::common;

use std::time::Duration;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_types::DatabaseId;

use common::cluster_harness::shared_steps::sequencer_admitted;
use common::cluster_harness::{TestClusterNode, wait_for};

const TIMESERIES: &str = "sncalvin_ts_ryw";

fn sequencer_leader(node: &TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

fn vshard_of(collection: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection)
        .vshard()
        .as_u32()
}

/// A document collection name whose vShard differs from the timeseries
/// collection's, so a transaction writing both is cross-shard.
fn other_vshard_collection() -> String {
    let home = vshard_of(TIMESERIES);
    (0u32..4096)
        .map(|i| format!("sncalvin_ryw_doc_{i}"))
        .find(|name| vshard_of(name) != home)
        .expect("a collection name on another vShard within 4096 tries")
}

/// The `(id, value)` cells of every row `sql` returns.
async fn id_value_rows(node: &TestClusterNode, sql: &str) -> Vec<(String, String)> {
    let messages = node
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get("id").unwrap_or_default().to_string(),
                row.get("value").unwrap_or_default().to_string(),
            )),
            _ => None,
        })
        .collect()
}

async fn exec(node: &TestClusterNode, sql: &str) {
    node.client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_calvin_committed_transaction_reads_its_own_timeseries_row() {
    let node = TestClusterNode::spawn_single_node_calvin(4)
        .await
        .expect("spawn standalone single-node-calvin server");
    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    let documents = other_vshard_collection();
    exec(
        &node,
        &format!(
            "CREATE COLLECTION {TIMESERIES} \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
             WITH (engine='timeseries')"
        ),
    )
    .await;
    exec(&node, &format!("CREATE COLLECTION {documents}")).await;
    wait_for(
        "both collections visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 2,
    )
    .await;

    let select = format!("SELECT id, value FROM {TIMESERIES}");
    let expected = vec![("r-1".to_string(), "2.5".to_string())];
    let admitted_before = sequencer_admitted(&node);

    exec(&node, "BEGIN").await;
    exec(
        &node,
        &format!("INSERT INTO {TIMESERIES} (id, ts, value) VALUES ('r-1', 1000, 2.5)"),
    )
    .await;
    exec(
        &node,
        &format!("INSERT INTO {documents} {{ id: 'd-1', n: 1 }}"),
    )
    .await;
    assert_eq!(
        id_value_rows(&node, &select).await,
        expected,
        "the transaction reads its own timeseries row before COMMIT"
    );
    exec(&node, "COMMIT").await;

    assert!(
        sequencer_admitted(&node) > admitted_before,
        "the cross-shard COMMIT is sequenced through Calvin"
    );
    assert_eq!(
        id_value_rows(&node, &select).await,
        expected,
        "the committed row reads back with the value the transaction wrote"
    );

    node.shutdown().await;
}
