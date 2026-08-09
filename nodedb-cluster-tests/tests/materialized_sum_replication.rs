// SPDX-License-Identifier: BUSL-1.1
//! A follower reaches the same materialized-sum total as the leader.
//!
//! Two paths, one assertion each, because they replicate differently:
//!
//! * **Co-resident** target — nothing about the balance is on the wire. The
//!   record replicates the SOURCE row, the follower re-executes the plan, and
//!   the follower's own enforcement derives the delta from the images it just
//!   produced. Nothing in `decode/document.rs` carries a resolution: every
//!   document decode arm sets `resolved_sum_targets: Vec::new()`.
//! * **Cross-shard** target — the balance is a task of its own, so it
//!   replicates as `ReplicatedWrite::ApplyBalanceDelta`, a DELTA on the wire
//!   modelled on `KvIncr`. Every replica applies it exactly once, in log order,
//!   on top of whatever balance that replica had already committed.
//!
//! Both are read the same way: the follower's balance is read from the
//! follower's own client, never from the leader's.

mod common;
use common::cluster_harness::TestCluster;

use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};

const XS_SOURCE: &str = "rep_entries";
const XS_TARGET: &str = "rep_accounts";

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn balance_on(client: &tokio_postgres::Client, target: &str) -> Option<String> {
    let rows = client
        .simple_query(&format!("SELECT balance FROM {target} WHERE id = 'acc-1'"))
        .await
        .unwrap_or_else(|e| panic!("read balance: {}", pg_detail(&e)));
    rows.into_iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
        _ => None,
    })
}

/// The cross-shard premise, asserted rather than assumed — see the sibling
/// cross-shard test's rationale.
#[test]
fn replication_fixture_is_cross_shard() {
    assert_ne!(
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, XS_SOURCE),
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, XS_TARGET),
        "this fixture must exercise the replicated cross-shard balance write"
    );
}

/// Every replica ends up with the same total, read from its own store.
///
/// The failure this guards is a replica that never applies the balance at all:
/// the source row replicates on its own record, so a follower that received no
/// balance entry would serve a total short by every entry ever inserted — and
/// would look perfectly healthy doing it, because its source rows are all
/// present.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_replica_reaches_the_same_cross_shard_total() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {XS_TARGET} (id TEXT PRIMARY KEY) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the target collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {XS_SOURCE} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the source collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "ALTER COLLECTION {XS_TARGET} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {XS_SOURCE} \
             ON {XS_SOURCE}.account_id = {XS_TARGET}.id VALUE {XS_SOURCE}.amount"
        ))
        .await
        .expect("declare materialized sum");

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {XS_TARGET} (id, balance) VALUES ('acc-1', '0')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed account: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for (id, amount) in [("e1", "40"), ("e2", "60")] {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {XS_SOURCE} (id, account_id, amount) VALUES \
                 ('{id}', 'acc-1', '{amount}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {}", pg_detail(&e)));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for (index, node) in cluster.nodes.iter().enumerate() {
        assert_eq!(
            balance_on(&node.client, XS_TARGET).await.as_deref(),
            Some("100"),
            "node {index} must reach the same total as the leader; a replica that \
             applied the source rows but not the balance looks healthy and serves a \
             total short by every entry"
        );
    }
}
