// SPDX-License-Identifier: BUSL-1.1

//! A topic keeps every message across a change of its home leader.
//!
//! `PUBLISH TO` proposes a `TopicPublish` entry to the data group of the
//! topic's home vShard. Every replica appends the message at apply, at the
//! entry's log position, and `COMMIT OFFSET` is a replicated catalog entry.
//! So a consumer that reads part of the topic from the home leader, commits,
//! and resumes on another node after the leader dies receives every message
//! exactly once, in order.

use crate::common;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use nodedb::event::cdc::CdcOffset;
use nodedb::event::cdc::consume::{ConsumeError, ConsumeParams, consume_local};
use nodedb::event::topic::publish::topic_vshard;
use nodedb_types::DatabaseId;

const TOPIC: &str = "topic_home_failover";
const GROUP: &str = "topic_home_readers";
const TENANT: u64 = 1;

/// Messages published before the home leader dies.
const BEFORE: usize = 6;
/// Messages published after the home leader dies.
const AFTER: usize = 3;
/// Messages the consumer reads and commits before the leader dies.
const FIRST_READ: usize = 3;

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// The buffer and consumer-group key of [`TOPIC`].
fn stream() -> String {
    format!("topic:{TOPIC}")
}

/// One consumed message: its position and its topic sequence.
type Seen = (CdcOffset, u64);

/// The messages `node` serves after the group's committed offset.
fn read(node: &TestClusterNode, limit: usize) -> Vec<Seen> {
    let stream = stream();
    let params = ConsumeParams {
        database_id: DatabaseId::DEFAULT,
        tenant_id: TENANT,
        stream_name: &stream,
        group_name: GROUP,
        partition: None,
        limit,
    };
    match consume_local(&node.shared, &params) {
        Ok(result) => result
            .events
            .iter()
            .map(|event| (event.position(), event.sequence))
            .collect(),
        Err(ConsumeError::BufferEmpty(_)) => Vec::new(),
        Err(error) => panic!("node {}: consume failed: {error}", node.node_id),
    }
}

/// Publish message `n` through `node`, retrying while the home group elects
/// a leader.
async fn publish(node: &TestClusterNode, n: usize) {
    let sql = format!("PUBLISH TO {TOPIC} 'message-{n}'");
    let deadline = Instant::now() + CONVERGE;
    loop {
        match node.client.simple_query(&sql).await {
            Ok(_) => return,
            Err(error) if Instant::now() < deadline => {
                tracing::debug!(n, %error, "publish not accepted yet; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("publish message-{n}: {error}"),
        }
    }
}

/// The topic's home data group and its leader, as `node` sees them.
fn home_leader(node: &TestClusterNode) -> (u64, u64) {
    let group = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(topic_vshard(DatabaseId::DEFAULT, TOPIC))
        .expect("the home vShard maps to a data group");
    let leader = node
        .all_group_leaders()
        .into_iter()
        .find_map(|(id, leader)| (id == group).then_some(leader))
        .unwrap_or(0);
    (group, leader)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topic_consumer_receives_every_message_once_after_the_home_leader_dies() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE TOPIC {TOPIC}"))
        .await
        .expect("create topic");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE CONSUMER GROUP {GROUP} ON {TOPIC}"))
        .await
        .expect("create consumer group");
    let stream = stream();
    wait_for(
        "every node registers the topic and the group",
        CONVERGE,
        STEP,
        || {
            cluster.nodes.iter().all(|node| {
                node.shared
                    .ep_topic_registry
                    .get(DatabaseId::DEFAULT, TENANT, TOPIC)
                    .is_some()
                    && node
                        .shared
                        .group_registry
                        .get(DatabaseId::DEFAULT, TENANT, &stream, GROUP)
                        .is_some()
            })
        },
    )
    .await;

    for n in 0..BEFORE {
        publish(&cluster.nodes[0], n).await;
    }
    wait_for("every replica holds every message", CONVERGE, STEP, || {
        cluster
            .nodes
            .iter()
            .all(|node| read(node, 1_000).len() == BEFORE)
    })
    .await;

    // Every replica serves the same messages at the same positions.
    let reference = read(&cluster.nodes[0], 1_000);
    for node in &cluster.nodes {
        assert_eq!(
            read(node, 1_000),
            reference,
            "node {} serves a different message sequence",
            node.node_id
        );
    }

    // Consume part of the topic from its home leader, and commit.
    let (group, leader) = home_leader(&cluster.nodes[0]);
    let leader_idx = cluster
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .unwrap_or_else(|| panic!("no live node leads the home group {group}"));
    let first = read(&cluster.nodes[leader_idx], FIRST_READ);
    assert_eq!(first, reference[..FIRST_READ].to_vec());
    let (committed, _) = *first.last().expect("a first read");
    cluster.nodes[leader_idx]
        .client
        .simple_query(&format!(
            "COMMIT OFFSET PARTITION 0 AT {committed} ON {TOPIC} CONSUMER GROUP {GROUP}"
        ))
        .await
        .unwrap_or_else(|e| panic!("commit offset {committed}: {e}"));
    wait_for(
        "every node holds the committed offset",
        CONVERGE,
        STEP,
        || {
            cluster.nodes.iter().all(|node| {
                node.shared
                    .offset_store
                    .get_offset(DatabaseId::DEFAULT, TENANT, &stream, GROUP, 0)
                    == committed
            })
        },
    )
    .await;

    // Kill the home leader.
    let dead = cluster.nodes.remove(leader_idx);
    let dead_id = dead.node_id;
    dead.shutdown().await;
    wait_for(
        "the survivors elect a new home leader",
        CONVERGE,
        STEP,
        || {
            cluster.nodes.iter().all(|node| {
                node.all_group_leaders()
                    .into_iter()
                    .any(|(id, leader)| id == group && leader != 0 && leader != dead_id)
            })
        },
    )
    .await;

    for n in BEFORE..BEFORE + AFTER {
        publish(&cluster.nodes[0], n).await;
    }
    let remaining = BEFORE - FIRST_READ + AFTER;
    wait_for(
        "every survivor holds the new messages",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|node| read(node, 1_000).len() == remaining)
        },
    )
    .await;

    // Resume on a survivor. Both survivors serve the same continuation.
    let resumed = read(&cluster.nodes[0], 1_000);
    for node in &cluster.nodes {
        assert_eq!(
            read(node, 1_000),
            resumed,
            "survivor {} resumes with a different sequence",
            node.node_id
        );
    }

    // Every message arrives exactly once, in order.
    let delivered: Vec<Seen> = first.iter().chain(resumed.iter()).copied().collect();
    let sequences: Vec<u64> = delivered.iter().map(|(_, sequence)| *sequence).collect();
    let distinct: BTreeSet<u64> = sequences.iter().copied().collect();
    assert_eq!(
        sequences.len(),
        BEFORE + AFTER,
        "duplicate or missing messages: {sequences:?}"
    );
    assert_eq!(
        distinct,
        (1..=(BEFORE + AFTER) as u64).collect::<BTreeSet<_>>(),
        "missing messages: {sequences:?}"
    );
    assert!(
        delivered.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "positions must rise with publication order: {delivered:?}"
    );

    cluster.shutdown().await;
}
