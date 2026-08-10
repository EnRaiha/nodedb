// SPDX-License-Identifier: BUSL-1.1

//! A MATERIALIZED SUM whose source and target home apart on a SINGLE node with
//! several Data-Plane cores.
//!
//! # Why one node is not the easy case
//!
//! Every core opens its own document store — `sparse/core-{core_id}.redb` — so a
//! balance folded inside the source write's transaction lands in the SOURCE
//! core's store. A collection homes to one vShard and a vShard homes to one
//! core, so a target that hashes to a different vShard is served by a different
//! store even when both live in the same process. Reading the target then finds
//! the total exactly where it was before the write, and the statement reports
//! success.
//!
//! That makes this the CHEAPER and more likely-hit failure than the multi-node
//! one: no cluster, no network, just more than one core. It is also why the
//! co-residency predicate is a question about vShards rather than about nodes —
//! "same node" is not "same store".
//!
//! The node runs the single-node Calvin stack, because the two collections'
//! tasks classify as multi-shard and commit through the sequencer's Vote/Verdict
//! barrier exactly as they do across nodes.

mod common;
use common::cluster_harness::{TestClusterNode, wait_for};

use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;

const SOURCE: &str = "xc_entries";
const TARGET: &str = "xc_accounts";

/// Enough cores that two independently-named collections land on different ones
/// with high probability, and that the round-robin vShard→core map has somewhere
/// to send them.
const CORES: usize = 4;

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

/// The premise, asserted first: the two collections do NOT share a vShard, so
/// the balance cannot ride the source write's own transaction.
#[test]
fn source_and_target_home_to_different_vshards() {
    assert_ne!(
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, SOURCE),
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, TARGET),
        "this file tests the CROSS-SHARD path; '{SOURCE}' and '{TARGET}' must not be co-resident"
    );
}

/// Observed sequencer-group leader id, or `0` while none is known.
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

async fn exec(node: &TestClusterNode, sql: &str) {
    node.client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {}", pg_detail(&e)));
}

async fn balance(node: &TestClusterNode) -> String {
    node.client
        .simple_query(&format!("SELECT balance FROM {TARGET} WHERE id = 'acc-1'"))
        .await
        .unwrap_or_else(|e| panic!("read balance: {}", pg_detail(&e)))
        .into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .unwrap_or_else(|| panic!("target row acc-1 must exist"))
}

/// On one node with several cores, every non-insert shape still totals
/// correctly: the balance travels to the core that actually owns the target
/// row instead of being folded into the source core's own store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_write_shape_totals_across_cores_on_one_node() {
    let node = TestClusterNode::spawn_single_node_calvin(CORES)
        .await
        .expect("spawn single-node Calvin server");
    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    exec(
        &node,
        &format!(
            "CREATE COLLECTION {TARGET} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &node,
        &format!(
            "CREATE COLLECTION {SOURCE} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &node,
        &format!(
            "ALTER COLLECTION {TARGET} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {SOURCE} \
             ON {SOURCE}.account_id = {TARGET}.id VALUE {SOURCE}.amount"
        ),
    )
    .await;

    exec(
        &node,
        &format!("INSERT INTO {TARGET} (id, owner, balance) VALUES ('acc-1', 'alice', '100')"),
    )
    .await;
    exec(
        &node,
        &format!("INSERT INTO {SOURCE} (id, account_id, amount) VALUES ('e1', 'acc-1', '25')"),
    )
    .await;
    assert_eq!(balance(&node).await, "125", "the INSERT must cross cores");

    exec(
        &node,
        &format!("UPDATE {SOURCE} SET amount = '40' WHERE id = 'e1'"),
    )
    .await;
    assert_eq!(
        balance(&node).await,
        "140",
        "an UPDATE owes only its difference, on the core that owns the target"
    );

    exec(&node, &format!("DELETE FROM {SOURCE} WHERE id = 'e1'")).await;
    assert_eq!(
        balance(&node).await,
        "100",
        "a DELETE must take the row's whole contribution back off"
    );
}
