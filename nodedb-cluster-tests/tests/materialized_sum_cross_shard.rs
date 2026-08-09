// SPDX-License-Identifier: BUSL-1.1
//! 3-node cluster test for a MATERIALIZED SUM whose SOURCE and TARGET
//! collections home to DIFFERENT vShards.
//!
//! A collection homes to one vShard, so this is the ordinary case, not the
//! exotic one: two collections named independently almost never collide. The
//! balance therefore cannot ride the source write's transaction — that
//! transaction belongs to the source's core, which owns none of the target's
//! rows. The Control Plane appends an `ApplyBalanceDelta` task homed on the
//! target instead, the pair classifies as multi-shard, and Calvin commits both
//! or neither.
//!
//! The test asserts the homing premise FIRST. Without that assertion a change
//! that happened to make the two collections co-resident would leave every
//! balance assertion below passing while testing the co-resident path.

mod common;
use common::cluster_harness::TestCluster;

use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};

/// Source and target, chosen for readability rather than for their hashes — the
/// homing assertion below is what makes the choice meaningful.
const SOURCE: &str = "xs_entries";
const TARGET: &str = "xs_accounts";

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

/// The single balance column of `TARGET`'s one row, as read through `client`.
async fn balance(client: &tokio_postgres::Client) -> String {
    let rows = client
        .simple_query(&format!("SELECT balance FROM {TARGET} WHERE id = 'acc-1'"))
        .await
        .unwrap_or_else(|e| panic!("read balance: {}", pg_detail(&e)));
    rows.into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .unwrap_or_else(|| panic!("target row acc-1 must exist"))
}

/// Create the two collections and declare the binding.
async fn declare_binding(cluster: &TestCluster) {
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {TARGET} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the target collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {SOURCE} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the source collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "ALTER COLLECTION {TARGET} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {SOURCE} \
             ON {SOURCE}.account_id = {TARGET}.id VALUE {SOURCE}.amount"
        ))
        .await
        .expect("declare materialized sum");
}

/// The premise the whole file rests on: the two collections do NOT share a
/// vShard, so every balance below travels on its own task.
#[test]
fn source_and_target_home_to_different_vshards() {
    let source = VShardId::from_collection_in_database(DatabaseId::DEFAULT, SOURCE);
    let target = VShardId::from_collection_in_database(DatabaseId::DEFAULT, TARGET);
    assert_ne!(
        source, target,
        "this file tests the CROSS-SHARD path; '{SOURCE}' and '{TARGET}' must not be co-resident"
    );
}

/// A single INSERT into the source credits the target's balance, across shards.
///
/// The failure this guards is silent: before the balance travelled as its own
/// task, the derived write was applied inside the source's transaction, on the
/// source's core — a store no reader of the target collection ever consults. The
/// statement succeeded and the total never moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_shard_insert_credits_the_target_balance() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {TARGET} (id, owner, balance) VALUES ('acc-1', 'alice', '100')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed account: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {SOURCE} (id, account_id, amount) VALUES ('e1', 'acc-1', '25')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert entry: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    assert_eq!(
        balance(&cluster.nodes[0].client).await,
        "125",
        "100 + 25 must land on the target row that lives on another vShard"
    );
}

/// Several inserts against the same account accumulate, and the column the sum
/// does not touch survives every read-modify-write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_cross_shard_inserts_accumulate() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {TARGET} (id, owner, balance) VALUES ('acc-1', 'alice', '0')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed account: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for (id, amount) in [("e1", "10"), ("e2", "20"), ("e3", "30.5")] {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {SOURCE} (id, account_id, amount) VALUES \
                 ('{id}', 'acc-1', '{amount}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {}", pg_detail(&e)));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    assert_eq!(
        balance(&cluster.nodes[0].client).await,
        "60.5",
        "every cross-shard entry must be counted exactly once"
    );

    let rows = cluster.nodes[0]
        .client
        .simple_query(&format!("SELECT owner FROM {TARGET} WHERE id = 'acc-1'"))
        .await
        .unwrap_or_else(|e| panic!("read owner: {}", pg_detail(&e)));
    let owner = rows.into_iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
        _ => None,
    });
    assert_eq!(
        owner.as_deref(),
        Some("alice"),
        "columns the sum does not touch must survive the write-back"
    );
}
