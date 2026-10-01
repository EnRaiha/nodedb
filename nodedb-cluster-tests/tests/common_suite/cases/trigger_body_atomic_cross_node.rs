// SPDX-License-Identifier: BUSL-1.1
//! An AFTER trigger body runs as one transaction across a local and a
//! cross-node write.
//!
//! The body first writes a collection led by another node, then a
//! collection led by the firing node. The local write collides with a
//! blocker row: a `document_strict` duplicate primary key is refused with
//! 23505 at the statement, inside a transaction as outside one. The first
//! attempt therefore fails. The test then:
//!
//!  1. checks the failed attempt sent nothing to the other node;
//!  2. removes the blocker so the queued retry succeeds;
//!  3. checks each row landed exactly once on both collections.
//!
//! The firing node is the leader of the source collection's group. The
//! cross-node collection is chosen from candidates by reading the routing
//! table and the group leaders: its group is led by another node.

use crate::common;

use std::time::Duration;

use common::cluster_harness::shared_steps::{group_of, leader_of, name_where, row_count};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};

const SRC: &str = "atom_src";

async fn has_row(node: &TestClusterNode, collection: &str, id: &str, val: &str) -> bool {
    let Ok(rows) = node
        .client
        .simple_query(&format!("SELECT val FROM {collection} WHERE id = '{id}'"))
        .await
    else {
        return false;
    };
    rows.iter().any(|msg| match msg {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0) == Some(val),
        _ => false,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn trigger_body_retry_lands_local_and_cross_node_rows_once() {
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
    let local = name_where("atom_local", |name| group_of(probe, name) == src_group);
    let remote = (0..4096u32)
        .map(|i| format!("atom_remote_{i}"))
        .find(|name| {
            let group = group_of(probe, name);
            group != src_group && leader_of(probe, group) != firing_node
        })
        .unwrap_or_else(|| {
            panic!(
                "no data group is led by a node other than {firing_node}: leaders {:?}",
                probe.all_group_leaders()
            )
        });
    let remote_group = group_of(probe, &remote);

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {SRC} (id TEXT PRIMARY KEY, val BIGINT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create source");
    for name in [&local, &remote] {
        cluster
            .exec_ddl_on_any_leader(&format!(
                "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, val BIGINT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .expect("create target");
    }

    // The blocker makes the body's local write collide on its first attempt.
    reader
        .exec(&format!(
            "INSERT INTO {local} (id, val) VALUES ('o1-log', 0)"
        ))
        .await
        .expect("insert blocker");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE TRIGGER atom_trig AFTER INSERT ON {SRC} FOR EACH ROW \
             BEGIN \
                 INSERT INTO {remote} (id, val) VALUES (NEW.id || '-r', 1); \
                 INSERT INTO {local} (id, val) VALUES (NEW.id || '-log', 1); \
             END"
        ))
        .await
        .expect("create trigger");
    wait_for(
        "trigger visible on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_trigger(1, "atom_trig")),
    )
    .await;

    assert_ne!(
        leader_of(probe, remote_group),
        firing_node,
        "the cross-node collection's group must stay led by another node"
    );
    reader
        .exec(&format!("INSERT INTO {SRC} (id, val) VALUES ('o1', 1)"))
        .await
        .expect("insert source row");

    // The first attempt collides on the blocker. It must send nothing.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        row_count(reader, &remote).await,
        0,
        "a failed trigger body must not deliver its cross-node write"
    );
    assert!(
        has_row(reader, &local, "o1-log", "0").await,
        "a failed trigger body must not overwrite the blocker"
    );

    reader
        .exec(&format!("DELETE FROM {local} WHERE id = 'o1-log'"))
        .await
        .expect("delete blocker");

    wait_for_async(
        "the retried body lands both rows",
        Duration::from_secs(20),
        Duration::from_millis(100),
        || async {
            has_row(reader, &local, "o1-log", "1").await
                && has_row(reader, &remote, "o1-r", "1").await
        },
    )
    .await;

    // Let any duplicate delivery or retry that lands arrive first.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(row_count(reader, &local).await, 1, "local row lands once");
    assert_eq!(
        row_count(reader, &remote).await,
        1,
        "cross-node row lands once"
    );

    cluster.shutdown().await;
}
