// SPDX-License-Identifier: BUSL-1.1
//! Every replica reaches the same materialized-sum total as the leader.
//!
//! Two paths, one test each, because they replicate differently:
//!
//! * **Co-resident** target — no balance is on the wire at all. The record
//!   replicates the SOURCE row, every node re-executes the plan, and each node's
//!   own enforcement folds the delta from the images it just produced. What the
//!   record does carry is the leader's RESOLUTION: the join-key value → target
//!   row surrogate table, which no replica can derive (the primary-key →
//!   surrogate binding lives in the catalog of the vShard that owns the target's
//!   key) and which the fold cannot proceed without.
//! * **Cross-shard** target — the balance is a task of its own, so it replicates
//!   as `ApplyBalanceDelta`, a DELTA on the wire modelled on `KvIncr`. Every
//!   replica applies it exactly once, in log order, on top of whatever balance
//!   that replica had already committed. The source write names that target on
//!   its deferral list so the co-resident fold above skips it — the list travels
//!   with the record for the same reason the resolution does, and a replica that
//!   lost it would count the same amount twice.
//!
//! Both are read the same way: each node's balance is read from that node's own
//! client, never from the leader's.

mod common;
use common::cluster_harness::TestCluster;

use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};

/// Cross-shard fixture: source and target hash to different vShards.
const XS_SOURCE: &str = "rep_entries";
const XS_TARGET: &str = "rep_accounts";

/// Co-resident fixture. The names are not decorative: they are chosen so both
/// collections hash to ONE vShard, which is what puts the fold on the source
/// write's own transaction instead of on a deferred sibling task.
const CO_SOURCE: &str = "coresident_entries";
const CO_TARGET: &str = "sameshard_accounts";

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn balance_on(
    client: &tokio_postgres::Client,
    target: &str,
    account: &str,
) -> Option<String> {
    let rows = client
        .simple_query(&format!(
            "SELECT balance FROM {target} WHERE id = '{account}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("read balance: {}", pg_detail(&e)));
    rows.into_iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
        _ => None,
    })
}

/// How many source rows a node holds for one account.
///
/// Read alongside the balance because the two failures look nothing alike. A
/// replica that never applied the balance serves a short total over a complete
/// set of source rows; a replica whose write was REFUSED — which is what an
/// unresolvable fold does to the whole statement — is missing the source rows
/// too. Asserting only the total cannot tell those apart.
async fn source_rows_on(client: &tokio_postgres::Client, source: &str, account: &str) -> usize {
    let rows = client
        .simple_query(&format!(
            "SELECT id FROM {source} WHERE account_id = '{account}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("count source rows: {}", pg_detail(&e)));
    rows.into_iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// Create the pair and declare the sum over it.
async fn declare_fixture(cluster: &TestCluster, source: &str, target: &str) {
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {target} (id TEXT PRIMARY KEY) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the target collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {source} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the source collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "ALTER COLLECTION {target} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {source} \
             ON {source}.account_id = {target}.id VALUE {source}.amount"
        ))
        .await
        .expect("declare materialized sum");
}

/// Seed both accounts at zero and let every node see them.
///
/// The seeds must be applied everywhere before the first source row is written:
/// the resolution the leader ships names these rows' surrogates, and each
/// replica has to already hold the rows those surrogates address.
async fn seed_accounts(cluster: &TestCluster, target: &str) {
    for account in ["acc-1", "acc-2"] {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {target} (id, balance) VALUES ('{account}', '0')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {account}: {}", pg_detail(&e)));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
}

/// The mixed INSERT workload both paths are measured on.
///
/// Deliberately not one shape repeated: single-row inserts land as `PointInsert`
/// records, the multi-row `VALUES` list lands as a `DocBatchInsert`, and the two
/// accounts prove the resolution is a table keyed by join value rather than one
/// remembered surrogate. Returns the total expected per account.
async fn run_mixed_inserts(
    cluster: &TestCluster,
    source: &str,
) -> [(&'static str, &'static str); 2] {
    for (id, account, amount) in [
        ("e1", "acc-1", "40"),
        ("e2", "acc-1", "60"),
        ("e3", "acc-2", "7"),
    ] {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {source} (id, account_id, amount) VALUES \
                 ('{id}', '{account}', '{amount}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {}", pg_detail(&e)));
    }

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {source} (id, account_id, amount) VALUES \
             ('e4', 'acc-1', '5'), ('e5', 'acc-2', '3')"
        ))
        .await
        .unwrap_or_else(|e| panic!("multi-row insert: {}", pg_detail(&e)));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    [("acc-1", "105"), ("acc-2", "10")]
}

/// Assert every node agrees, reading each node from its own client.
async fn assert_every_replica_agrees(
    cluster: &TestCluster,
    source: &str,
    target: &str,
    expected: &[(&str, &str)],
    expected_rows: &[(&str, usize)],
) {
    for (index, node) in cluster.nodes.iter().enumerate() {
        for (account, total) in expected {
            assert_eq!(
                balance_on(&node.client, target, account).await.as_deref(),
                Some(*total),
                "node {index} must reach the leader's total for {account}; a replica that \
                 applied the source rows but not the balance looks healthy and serves a \
                 total short by every entry"
            );
        }
        for (account, count) in expected_rows {
            assert_eq!(
                source_rows_on(&node.client, source, account).await,
                *count,
                "node {index} must hold every source row for {account}; a replica that \
                 could not resolve the sum target fails the whole write, losing the source \
                 row as well as the balance"
            );
        }
    }
}

/// The co-resident premise, asserted rather than assumed: if these two ever
/// stopped sharing a vShard the test would silently become a second copy of the
/// cross-shard one.
#[test]
fn coresident_fixture_shares_one_vshard() {
    assert_eq!(
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, CO_SOURCE),
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, CO_TARGET),
        "this fixture must exercise the fold that runs inside the source write's own \
         transaction"
    );
}

/// The cross-shard premise, asserted for the same reason.
#[test]
fn replication_fixture_is_cross_shard() {
    assert_ne!(
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, XS_SOURCE),
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, XS_TARGET),
        "this fixture must exercise the replicated cross-shard balance write"
    );
}

/// Every replica reaches the same total when the target is CO-RESIDENT — the
/// case where nothing about the balance is on the wire and each node derives it.
///
/// This is the path that cannot work unless the leader's resolution travels with
/// the record. A replica re-executing the write folds a delta for join value
/// `acc-1`, looks for the target row that value names, and — with an empty
/// resolution — has nowhere to look: the primary-key → surrogate binding is
/// catalog state owned by the target key's vShard, not something a Data-Plane
/// fold can derive. The write then fails on the replica while succeeding on the
/// leader, which is why this test reads the source rows as well as the total.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_replica_reaches_the_same_coresident_total() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    declare_fixture(&cluster, CO_SOURCE, CO_TARGET).await;
    seed_accounts(&cluster, CO_TARGET).await;
    let expected = run_mixed_inserts(&cluster, CO_SOURCE).await;

    assert_every_replica_agrees(
        &cluster,
        CO_SOURCE,
        CO_TARGET,
        &expected,
        &[("acc-1", 3), ("acc-2", 2)],
    )
    .await;
}

/// Every replica reaches the same total when the target is CROSS-SHARD — the
/// case where the balance replicates as its own delta entry.
///
/// The source write still travels to this shard's replicas, and it still names
/// the target on its deferral list. A replica that folded anyway would apply the
/// delta twice: once locally and once as the sibling entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_replica_reaches_the_same_cross_shard_total() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    declare_fixture(&cluster, XS_SOURCE, XS_TARGET).await;
    seed_accounts(&cluster, XS_TARGET).await;
    let expected = run_mixed_inserts(&cluster, XS_SOURCE).await;

    assert_every_replica_agrees(
        &cluster,
        XS_SOURCE,
        XS_TARGET,
        &expected,
        &[("acc-1", 3), ("acc-2", 2)],
    )
    .await;
}
