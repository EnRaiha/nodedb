// SPDX-License-Identifier: BUSL-1.1
//! A trigger write shipped to another node commits with every write it
//! derives, on every vShard, or not at all.
//!
//! An AFTER trigger on a source collection led by the firing node inserts
//! into `entries`, a collection led by another node. `entries` is the source
//! of a materialized sum whose target, `accounts`, homes to a third vShard.
//! The insert therefore derives a balance write on the `accounts` vShard. The
//! firing node ships the statement whole, and the receiving node commits the
//! entry and the balance as one Calvin transaction.
//!
//! The test:
//!
//!  1. forces the shipped insert to fail on a duplicate primary key, and
//!     checks neither the entry nor the balance change landed;
//!  2. fires the trigger for a fresh row, and checks both landed once.

use crate::common;

use std::time::Duration;

use common::cluster_harness::shared_steps::{group_of, leader_of, name_where, row_count};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};

const SRC: &str = "ship_src";

fn vshard_of(collection: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(nodedb_types::DatabaseId::DEFAULT, collection)
        .vshard()
        .as_u32()
}

async fn balance(node: &TestClusterNode, accounts: &str) -> Option<String> {
    let rows = node
        .client
        .simple_query(&format!(
            "SELECT balance FROM {accounts} WHERE id = 'acc-1'"
        ))
        .await
        .ok()?;
    rows.into_iter().find_map(|msg| match msg {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
        _ => None,
    })
}

async fn has_entry(node: &TestClusterNode, entries: &str, id: &str) -> bool {
    let Ok(rows) = node
        .client
        .simple_query(&format!("SELECT id FROM {entries} WHERE id = '{id}'"))
        .await
    else {
        return false;
    };
    rows.iter()
        .any(|msg| matches!(msg, tokio_postgres::SimpleQueryMessage::Row(_)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_shipped_write_commits_with_its_derived_writes_or_not_at_all() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    let probe = &cluster.nodes[0];

    wait_for(
        "every data group has a leader",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            probe
                .all_group_leaders()
                .iter()
                .all(|(_, leader)| *leader != 0)
        },
    )
    .await;

    let src_group = group_of(probe, SRC);
    let firing_node = leader_of(probe, src_group);
    let reader = cluster
        .nodes
        .iter()
        .find(|n| n.node_id != firing_node)
        .expect("a second node");
    let entries = name_where("ship_entries", |name| {
        let group = group_of(probe, name);
        group != src_group && leader_of(probe, group) != firing_node
    });
    let entries_group = group_of(probe, &entries);
    // The harness runs two data groups, so the third participant is a third
    // vShard, not a third group.
    let accounts = name_where("ship_accounts", |name| {
        let vshard = vshard_of(name);
        vshard != vshard_of(&entries) && vshard != vshard_of(SRC)
    });

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {SRC} (id TEXT PRIMARY KEY, val BIGINT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create source");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {accounts} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create accounts");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {entries} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create entries");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "ALTER COLLECTION {accounts} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {entries} \
             ON {entries}.account_id = {accounts}.id VALUE {entries}.amount"
        ))
        .await
        .expect("declare materialized sum");

    reader
        .exec(&format!(
            "INSERT INTO {accounts} (id, owner, balance) VALUES ('acc-1', 'alice', '100')"
        ))
        .await
        .expect("seed account");
    // A materialized sum requires its target row, so the account the blocker
    // credits exists too.
    reader
        .exec(&format!(
            "INSERT INTO {accounts} (id, owner, balance) VALUES ('acc-2', 'bob', '0')"
        ))
        .await
        .expect("seed the blocker's account");
    // The blocker holds the id the first shipped insert writes. It credits
    // another account, so acc-1's balance counts only the shipped entries.
    reader
        .exec(&format!(
            "INSERT INTO {entries} (id, account_id, amount) VALUES ('o1', 'acc-2', '0')"
        ))
        .await
        .expect("insert blocker");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE TRIGGER ship_trig AFTER INSERT ON {SRC} FOR EACH ROW \
             BEGIN \
                 INSERT INTO {entries} (id, account_id, amount) VALUES (NEW.id, 'acc-1', '5'); \
             END"
        ))
        .await
        .expect("create trigger");
    wait_for(
        "trigger visible on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_trigger(1, "ship_trig")),
    )
    .await;
    assert_ne!(
        leader_of(probe, entries_group),
        firing_node,
        "the entries group must stay led by another node"
    );

    // The shipped insert collides with the blocker on every attempt.
    reader
        .exec(&format!("INSERT INTO {SRC} (id, val) VALUES ('o1', 1)"))
        .await
        .expect("insert failing source row");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        row_count(reader, &entries).await,
        1,
        "a failed shipped insert leaves only the blocker"
    );
    assert_eq!(
        balance(reader, &accounts).await.as_deref(),
        Some("100"),
        "a failed shipped insert commits none of its derived writes"
    );

    // A fresh row ships an insert that commits with its balance write.
    reader
        .exec(&format!("INSERT INTO {SRC} (id, val) VALUES ('o2', 2)"))
        .await
        .expect("insert source row");
    wait_for_async(
        "the shipped insert and its balance land",
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            has_entry(reader, &entries, "o2").await
                && balance(reader, &accounts).await.as_deref() == Some("105")
        },
    )
    .await;

    // Let any duplicate delivery or retry that lands arrive first.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(row_count(reader, &entries).await, 2, "the entry lands once");
    assert_eq!(
        balance(reader, &accounts).await.as_deref(),
        Some("105"),
        "the balance counts the entry once"
    );

    cluster.shutdown().await;
}
