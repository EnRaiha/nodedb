// SPDX-License-Identifier: BUSL-1.1
//! Cross-node replication of topic and consumer-group DDL.
//!
//! `CREATE` / `DROP TOPIC` and `CREATE` / `DROP CONSUMER GROUP` propose
//! `CatalogEntry::CreateTopicIfAbsent`, `DeleteTopicWithConsumerGroups`,
//! `PutConsumerGroupIfAbsent`, and `DeleteConsumerGroup` through the metadata
//! raft group. Every node writes the `topics_ep` / `consumer_groups` row and
//! installs the definition into its own `EpTopicRegistry` / `GroupRegistry`.
//! Publication, subscription, and offset resolution read those registries, so
//! a definition that reached only the executing node is invisible on every
//! follower. Each test asserts the follower's durable row AND its live
//! registry entry separately.
//!
//! Message publication is out of scope: it is a node-local data path.

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb::event::cdc::ConsumerGroupDef;
use nodedb::event::topic::TopicDef;
use nodedb_types::DatabaseId;

/// Topic every test proposes.
const TOPIC: &str = "tcg_cross_topic";
/// Consumer group every test proposes on [`TOPIC`].
const GROUP: &str = "tcg_cross_group";
/// Database the harness pgwire client is connected to.
const DATABASE: DatabaseId = DatabaseId::DEFAULT;
/// Tenant of the harness superuser.
const TENANT: u64 = 1;
/// Retention requested by CREATE. Distinct from the 3600s topic default.
const RETENTION_SECS: u64 = 7_200;

/// Durable stream identity of a consumer group attached to [`TOPIC`].
fn canonical_stream() -> String {
    format!("topic:{TOPIC}")
}

/// Index of the metadata-group leader.
fn pick_leader_index(cluster: &TestCluster) -> usize {
    let leader_id = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .position(|n| n.node_id == leader_id)
        .expect("the metadata leader must be one of the spawned nodes")
}

/// Index of a node that is not the metadata-group leader.
fn pick_follower_index(cluster: &TestCluster) -> usize {
    let leader_id = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .position(|n| n.node_id != leader_id)
        .expect("a 3-node cluster must have a follower")
}

/// Node id every node agrees is the metadata-group leader.
fn leader_id(cluster: &TestCluster) -> u64 {
    cluster
        .nodes
        .iter()
        .map(|n| n.metadata_group_leader())
        .find(|&id| id != 0)
        .expect("at least one node must report a non-zero leader id")
}

/// The durable `topics_ep` row this node persisted for [`TOPIC`].
fn stored_topic(node: &TestClusterNode) -> Option<TopicDef> {
    node.shared
        .credentials
        .catalog()
        .load_all_ep_topics()
        .expect("load topics")
        .into_iter()
        .find(|t| t.name == TOPIC)
}

/// The live registry entry this node holds for [`TOPIC`].
/// Publication and subscription read this map, not the catalog row.
fn registered_topic(node: &TestClusterNode) -> Option<TopicDef> {
    node.shared.ep_topic_registry.get(DATABASE, TENANT, TOPIC)
}

/// The durable `consumer_groups` row this node persisted for [`GROUP`].
fn stored_group(node: &TestClusterNode) -> Option<ConsumerGroupDef> {
    let stream = canonical_stream();
    node.shared
        .credentials
        .catalog()
        .load_all_consumer_groups()
        .expect("load consumer groups")
        .into_iter()
        .find(|g| g.name == GROUP && g.stream_name == stream)
}

/// The live registry entry this node holds for [`GROUP`].
/// Offset resolution and consumption read this map, not the catalog row.
fn registered_group(node: &TestClusterNode) -> Option<ConsumerGroupDef> {
    node.shared
        .group_registry
        .get(DATABASE, TENANT, &canonical_stream(), GROUP)
}

/// Consumer-group names the catalog reports as attached to [`TOPIC`].
fn attached_group_rows(node: &TestClusterNode) -> Vec<String> {
    node.shared
        .credentials
        .catalog()
        .topic_consumer_group_names(DATABASE, TENANT, TOPIC)
        .expect("enumerate topic consumer groups")
}

/// Run `CREATE TOPIC` on this node.
async fn create_topic_on(node: &TestClusterNode) {
    node.client
        .simple_query(&format!(
            "CREATE TOPIC {TOPIC} WITH (RETENTION = '2 hours')"
        ))
        .await
        .expect("CREATE TOPIC on the metadata leader");
}

/// Run `CREATE CONSUMER GROUP` on this node.
async fn create_group_on(node: &TestClusterNode) {
    node.client
        .simple_query(&format!("CREATE CONSUMER GROUP {GROUP} ON {TOPIC}"))
        .await
        .expect("CREATE CONSUMER GROUP on the metadata leader");
}

/// Topic names reported by `SHOW TOPICS` on this node.
/// The handler reads the registry, so this is a SQL view of the live map.
async fn shown_topics(node: &TestClusterNode) -> Vec<String> {
    let messages = node
        .client
        .simple_query("SHOW TOPICS")
        .await
        .expect("SHOW TOPICS");
    messages
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get("name").map(str::to_string),
            _ => None,
        })
        .collect()
}

/// Bring up a cluster whose leader already holds the replicated topic.
/// The leader validates against its own registry, so wait for it too.
async fn cluster_with_topic() -> TestCluster {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    create_topic_on(&cluster.nodes[leader_index]).await;

    wait_for(
        "leader registers its own topic",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_topic(&cluster.nodes[leader_index]).is_some(),
    )
    .await;
    wait_for(
        "follower registers the replicated topic",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_topic(&cluster.nodes[follower_index]).is_some(),
    )
    .await;
    cluster
}

/// Bring up a cluster whose leader holds the topic and one attached group.
async fn cluster_with_topic_and_group() -> TestCluster {
    let cluster = cluster_with_topic().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    create_group_on(&cluster.nodes[leader_index]).await;

    wait_for(
        "leader registers its own consumer group",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_group(&cluster.nodes[leader_index]).is_some(),
    )
    .await;
    wait_for(
        "follower registers the replicated consumer group",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_group(&cluster.nodes[follower_index]).is_some(),
    )
    .await;
    cluster
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn topic_created_on_leader_reaches_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    // Baseline: the follower carries neither the row nor the registry entry.
    assert!(stored_topic(follower).is_none());
    assert!(registered_topic(follower).is_none());
    assert!(!shown_topics(follower).await.contains(&TOPIC.to_string()));

    create_topic_on(&cluster.nodes[leader_index]).await;

    // The registry install follows the row write, so waiting on it
    // covers both.
    wait_for(
        "follower registers the replicated topic",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_topic(&cluster.nodes[follower_index]).is_some(),
    )
    .await;

    let record = stored_topic(follower)
        .expect("CreateTopicIfAbsent must write the row on the follower, not only the leader");
    assert_eq!(record.database_id, DATABASE);
    assert_eq!(record.tenant_id, TENANT);
    assert_eq!(record.retention.max_age_secs, RETENTION_SECS);

    let live = registered_topic(follower)
        .expect("post-apply must install the definition in the follower topic registry");
    assert_eq!(live.name, TOPIC);
    assert_eq!(live.database_id, DATABASE);
    assert_eq!(live.tenant_id, TENANT);
    assert_eq!(live.retention.max_age_secs, RETENTION_SECS);

    assert!(
        shown_topics(follower).await.contains(&TOPIC.to_string()),
        "SHOW TOPICS on follower node {} must report the leader's topic",
        follower.node_id,
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn consumer_group_created_on_leader_reaches_follower() {
    let cluster = cluster_with_topic().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    // Baseline: the topic is replicated, the group is not.
    assert!(registered_topic(follower).is_some());
    assert!(stored_group(follower).is_none());
    assert!(registered_group(follower).is_none());
    assert!(attached_group_rows(follower).is_empty());

    create_group_on(&cluster.nodes[leader_index]).await;

    wait_for(
        "follower registers the replicated consumer group",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_group(&cluster.nodes[follower_index]).is_some(),
    )
    .await;

    let record = stored_group(follower)
        .expect("PutConsumerGroupIfAbsent must write the row on the follower, not only the leader");
    assert_eq!(record.database_id, DATABASE);
    assert_eq!(record.tenant_id, TENANT);
    assert_eq!(record.stream_name, canonical_stream());
    assert_eq!(attached_group_rows(follower), vec![GROUP.to_string()]);

    let live = registered_group(follower)
        .expect("post-apply must install the definition in the follower group registry");
    assert_eq!(live.name, GROUP);
    assert_eq!(live.database_id, DATABASE);
    assert_eq!(live.tenant_id, TENANT);
    assert_eq!(live.stream_name, canonical_stream());

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn topic_dropped_on_leader_reaches_follower() {
    let cluster = cluster_with_topic_and_group().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    // Baseline: the follower holds the topic, the group, and both rows.
    assert!(stored_topic(follower).is_some());
    assert!(registered_topic(follower).is_some());
    assert!(stored_group(follower).is_some());
    assert!(registered_group(follower).is_some());

    cluster.nodes[leader_index]
        .client
        .simple_query(&format!("DROP TOPIC {TOPIC}"))
        .await
        .expect("DROP TOPIC on the metadata leader");

    wait_for(
        "follower drops the replicated topic",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_topic(&cluster.nodes[follower_index]).is_none(),
    )
    .await;

    assert!(
        stored_topic(follower).is_none(),
        "DeleteTopicWithConsumerGroups must remove the topic row on follower node {}",
        follower.node_id,
    );
    assert!(
        registered_topic(follower).is_none(),
        "the drop must reach the topic registry on follower node {}",
        follower.node_id,
    );
    assert!(
        !shown_topics(follower).await.contains(&TOPIC.to_string()),
        "SHOW TOPICS on follower node {} must no longer report the topic",
        follower.node_id,
    );

    // The attached group goes with the topic, row and registry alike.
    wait_for(
        "follower drops the attached consumer group",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_group(&cluster.nodes[follower_index]).is_none(),
    )
    .await;
    assert!(
        stored_group(follower).is_none(),
        "the entry must remove the attached group row on follower node {}",
        follower.node_id,
    );
    assert!(
        attached_group_rows(follower).is_empty(),
        "no group row may stay attached to the dropped topic on follower node {}",
        follower.node_id,
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn consumer_group_dropped_on_leader_reaches_follower() {
    let cluster = cluster_with_topic_and_group().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    // Baseline: the follower holds both the row and the registry entry.
    assert!(stored_group(follower).is_some());
    assert!(registered_group(follower).is_some());

    cluster.nodes[leader_index]
        .client
        .simple_query(&format!("DROP CONSUMER GROUP {GROUP} ON {TOPIC}"))
        .await
        .expect("DROP CONSUMER GROUP on the metadata leader");

    wait_for(
        "follower drops the replicated consumer group",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_group(&cluster.nodes[follower_index]).is_none(),
    )
    .await;

    assert!(
        stored_group(follower).is_none(),
        "DeleteConsumerGroup must remove the row on follower node {}",
        follower.node_id,
    );
    assert!(
        attached_group_rows(follower).is_empty(),
        "the dropped group must leave no attachment row on follower node {}",
        follower.node_id,
    );
    assert!(
        registered_group(follower).is_none(),
        "the drop must reach the group registry on follower node {}",
        follower.node_id,
    );
    // The topic itself survives the group drop.
    assert!(registered_topic(follower).is_some());
    assert!(stored_topic(follower).is_some());

    cluster.shutdown().await;
}
