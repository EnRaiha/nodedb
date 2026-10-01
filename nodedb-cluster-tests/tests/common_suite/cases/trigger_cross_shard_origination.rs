// SPDX-License-Identifier: BUSL-1.1
//! A trigger body whose writes all home to other nodes sends them only when
//! the body commits, and each statement lands once.
//!
//! An AFTER trigger on a source collection led by the firing node writes two
//! collections led by other nodes. The second collection does not exist when
//! the trigger first fires, so the body fails at its second statement. The
//! test:
//!
//!  1. checks the failed body sent neither write, so the first collection
//!     stays empty;
//!  2. creates the second collection, so the queued retry succeeds;
//!  3. checks each collection holds the body's row exactly once.
//!
//! The body with a local write that fails its commit is covered by
//! `trigger_body_atomic_cross_node`.

use crate::common;

use std::time::Duration;

use common::cluster_harness::shared_steps::{group_of, leader_of, row_count};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};

const SRC: &str = "origin_src";

/// First `{prefix}_<i>` whose group is led by a node other than
/// `firing_node`.
fn remote_name(probe: &TestClusterNode, prefix: &str, firing_node: u64) -> String {
    (0..4096u32)
        .map(|i| format!("{prefix}_{i}"))
        .find(|name| leader_of(probe, group_of(probe, name)) != firing_node)
        .unwrap_or_else(|| {
            panic!(
                "no data group is led by a node other than {firing_node}: leaders {:?}",
                probe.all_group_leaders()
            )
        })
}

async fn has_row(node: &TestClusterNode, collection: &str, id: &str) -> bool {
    let Ok(rows) = node
        .client
        .simple_query(&format!("SELECT id FROM {collection} WHERE id = '{id}'"))
        .await
    else {
        return false;
    };
    rows.iter()
        .any(|msg| matches!(msg, tokio_postgres::SimpleQueryMessage::Row(_)))
}

async fn create_strict(cluster: &TestCluster, name: &str) {
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, val BIGINT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_remote_only_body_sends_its_writes_at_commit_and_each_lands_once() {
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

    let firing_node = leader_of(probe, group_of(probe, SRC));
    let reader = cluster
        .nodes
        .iter()
        .find(|n| n.node_id != firing_node)
        .expect("a second node");
    let first = remote_name(probe, "origin_first", firing_node);
    let second = remote_name(probe, "origin_second", firing_node);

    create_strict(&cluster, SRC).await;
    create_strict(&cluster, &first).await;

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE TRIGGER origin_trig AFTER INSERT ON {SRC} FOR EACH ROW \
             BEGIN \
                 INSERT INTO {first} (id, val) VALUES (NEW.id || '-a', 1); \
                 INSERT INTO {second} (id, val) VALUES (NEW.id || '-b', 2); \
             END"
        ))
        .await
        .expect("create trigger");
    wait_for(
        "trigger visible on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_trigger(1, "origin_trig"))
        },
    )
    .await;

    reader
        .exec(&format!("INSERT INTO {SRC} (id, val) VALUES ('o1', 1)"))
        .await
        .expect("insert source row");

    // The body fails at its second statement. Its first write is held until
    // the body commits, so nothing reaches the other node.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        row_count(reader, &first).await,
        0,
        "a failed body must not send a write it held for another node"
    );

    create_strict(&cluster, &second).await;

    wait_for_async(
        "the retried body lands both rows",
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            has_row(reader, &first, "o1-a").await && has_row(reader, &second, "o1-b").await
        },
    )
    .await;

    // Let any duplicate delivery or retry that lands arrive first.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        row_count(reader, &first).await,
        1,
        "the first write lands once"
    );
    assert_eq!(
        row_count(reader, &second).await,
        1,
        "the second write lands once"
    );

    cluster.shutdown().await;
}
