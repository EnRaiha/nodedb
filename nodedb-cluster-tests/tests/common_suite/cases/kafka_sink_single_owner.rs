// SPDX-License-Identifier: BUSL-1.1

//! A Kafka sink publishes every event once, from one node, across a
//! failover.
//!
//! The broker is librdkafka's in-process mock cluster (`rdkafka::mocking`).
//! Every node runs a producer task for the stream, but only the node that
//! holds the leader lease of the stream's owning group publishes, and it
//! commits the group's offsets through the metadata log. So:
//!
//! - every event reaches the topic exactly once while the cluster is stable;
//! - after the publishing node dies, the next lease holder resumes from the
//!   committed offsets: every later event arrives once, and no earlier one
//!   arrives again;
//! - every record carries the owner's fencing token, and the next owner's
//!   token is higher.

use crate::common;
use common::cluster_harness::{TestCluster, wait_for};

use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use rdkafka::mocking::MockCluster;

use nodedb::event::cdc::sink_owner::owning_group;
use nodedb_types::DatabaseId;

use super::webhook_sink_single_owner::{Deliveries, FencingTokens, distinct, duplicates};

const COLLECTION: &str = "kafka_owner_rows";
const STREAM: &str = "kafka_owner_feed";
const TOPIC: &str = "kafka_owner_topic";
const TENANT: u64 = 1;
const BEFORE: usize = 5;
const AFTER: usize = 4;
const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// Read `TOPIC` from the start, counting each record's deliveries by key and
/// recording each record's fencing token.
async fn consume(consumer: StreamConsumer, deliveries: Deliveries, tokens: FencingTokens) {
    loop {
        let Ok(message) = consumer.recv().await else {
            continue;
        };
        if let Some(token) = message.headers().and_then(|headers| {
            headers
                .iter()
                .find(|header| header.key == "fencing-token")
                .and_then(|header| header.value)
                .and_then(|value| std::str::from_utf8(value).ok())
                .and_then(|value| value.parse::<u64>().ok())
        }) {
            tokens.lock().unwrap_or_else(|p| p.into_inner()).push(token);
        }
        if let Some(key) = message.key().and_then(|key| std::str::from_utf8(key).ok()) {
            *deliveries
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .entry(key.to_owned())
                .or_default() += 1;
        }
    }
}

async fn insert(cluster: &TestCluster, row: usize) {
    let sql = format!("INSERT INTO {COLLECTION} {{ id: 'row-{row}', n: {row} }}");
    let deadline = Instant::now() + CONVERGE;
    loop {
        match cluster.nodes[0].client.simple_query(&sql).await {
            Ok(_) => return,
            Err(error) if Instant::now() < deadline => {
                tracing::debug!(row, %error, "insert not accepted yet; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("insert row-{row}: {error}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kafka_sink_publishes_every_event_once_across_a_failover() {
    let broker = MockCluster::new(1).expect("start the in-process Kafka mock broker");
    broker
        .create_topic(TOPIC, 1, 1)
        .expect("create the mock topic");
    let bootstrap = broker.bootstrap_servers();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "kafka_owner_test")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("create the test consumer");
    consumer
        .subscribe(&[TOPIC])
        .expect("subscribe to the topic");
    let deliveries: Deliveries = Arc::default();
    let tokens: FencingTokens = Arc::default();
    let reader = tokio::spawn(consume(
        consumer,
        Arc::clone(&deliveries),
        Arc::clone(&tokens),
    ));

    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("create collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE CHANGE STREAM {STREAM} ON {COLLECTION} \
             WITH (DELIVERY = 'kafka', BROKERS = '{bootstrap}', TOPIC = '{TOPIC}')"
        ))
        .await
        .expect("create change stream");
    wait_for("every node registers the stream", CONVERGE, STEP, || {
        cluster
            .nodes
            .iter()
            .all(|node| node.has_change_stream(DatabaseId::DEFAULT, TENANT, STREAM))
    })
    .await;

    for row in 0..BEFORE {
        insert(&cluster, row).await;
    }
    wait_for("the topic receives every event", CONVERGE, STEP, || {
        distinct(&deliveries) == BEFORE
    })
    .await;
    // Let a stray second publish surface before counting.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        duplicates(&deliveries).is_empty(),
        "an event was published twice while the cluster was stable: {:?}",
        duplicates(&deliveries)
    );
    let first_owner_tokens = tokens.lock().unwrap_or_else(|p| p.into_inner()).clone();
    assert_eq!(
        first_owner_tokens.len(),
        BEFORE,
        "every record carries a fencing token"
    );

    // Kill the node that publishes: the leader of the stream's owning group.
    let group = owning_group(&cluster.nodes[0].shared, DatabaseId::DEFAULT, STREAM)
        .expect("the stream's name maps to a data group");
    let leader = cluster.nodes[0]
        .all_group_leaders()
        .into_iter()
        .find_map(|(id, leader)| (id == group).then_some(leader))
        .unwrap_or(0);
    let owner = cluster
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .unwrap_or_else(|| panic!("no live node leads the owning group {group}"));
    let dead = cluster.nodes.remove(owner);
    let dead_id = dead.node_id;
    dead.shutdown().await;
    wait_for("the survivors elect a new owner", CONVERGE, STEP, || {
        cluster.nodes.iter().all(|node| {
            node.all_group_leaders()
                .into_iter()
                .any(|(id, leader)| id == group && leader != 0 && leader != dead_id)
        })
    })
    .await;

    for row in BEFORE..BEFORE + AFTER {
        insert(&cluster, row).await;
    }
    wait_for(
        "the new owner publishes the later events",
        CONVERGE,
        STEP,
        || distinct(&deliveries) == BEFORE + AFTER,
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        duplicates(&deliveries).is_empty(),
        "the failover published an event twice: {:?}",
        duplicates(&deliveries)
    );
    let all_tokens = tokens.lock().unwrap_or_else(|p| p.into_inner()).clone();
    let first_owner_max = first_owner_tokens.iter().copied().max().unwrap_or(0);
    assert!(
        all_tokens[first_owner_tokens.len()..]
            .iter()
            .all(|token| *token > first_owner_max),
        "the new owner's fencing token rises above the old owner's: {all_tokens:?}"
    );

    reader.abort();
    cluster.shutdown().await;
    drop(broker);
}
