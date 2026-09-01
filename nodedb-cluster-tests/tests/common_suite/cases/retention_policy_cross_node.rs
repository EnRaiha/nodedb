// SPDX-License-Identifier: BUSL-1.1
//! Cross-node replication of `RETENTION POLICY` DDL.
//!
//! `CREATE` / `ALTER` / `DROP RETENTION POLICY` propose
//! `CatalogEntry::PutRetentionPolicy` / `DeleteRetentionPolicy` through the
//! metadata raft group. Every node writes the `retention_policies` row and
//! installs the definition into its own `RetentionPolicyRegistry`. The
//! enforcement loop and the auto-tier planner read that registry, so a policy
//! that reached only the executing node leaves each follower enforcing
//! nothing. Each test asserts the follower's durable row AND its live
//! registry entry.

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb::engine::timeseries::retention_policy::RetentionPolicyDef;

/// Timeseries collection every test targets.
const COLLECTION: &str = "rp_cross_metrics";
/// Policy name every test proposes.
const POLICY: &str = "rp_cross_policy";
/// Evaluation interval requested by CREATE. Distinct from the 1h default.
const EVAL_INTERVAL_MS: u64 = 900_000;

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

/// The durable `retention_policies` row this node persisted for [`POLICY`].
fn stored_policy(node: &TestClusterNode) -> Option<RetentionPolicyDef> {
    node.shared
        .credentials
        .catalog()
        .load_all_retention_policies()
        .expect("load retention policies")
        .into_iter()
        .find(|p| p.name == POLICY)
}

/// The live registry entry this node holds for [`POLICY`].
/// Enforcement and auto-tier routing read this map, not the catalog row.
fn registered_policy(node: &TestClusterNode) -> Option<RetentionPolicyDef> {
    node.shared
        .retention_policy_registry
        .list_all()
        .into_iter()
        .find(|p| p.name == POLICY)
}

/// Create the timeseries collection every policy targets.
async fn create_collection(cluster: &TestCluster) {
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .expect("CREATE COLLECTION for the retention policy target");
}

/// Run `CREATE RETENTION POLICY` on this node.
async fn create_policy_on(node: &TestClusterNode) {
    node.client
        .simple_query(&format!(
            "CREATE RETENTION POLICY {POLICY} ON {COLLECTION} (RAW RETAIN '7d') \
             WITH (EVAL_INTERVAL = '15m')"
        ))
        .await
        .expect("CREATE RETENTION POLICY on the metadata leader");
}

/// Policy names reported by `SHOW RETENTION POLICIES` on this node.
async fn shown_policies(node: &TestClusterNode) -> Vec<String> {
    let messages = node
        .client
        .simple_query("SHOW RETENTION POLICIES")
        .await
        .expect("SHOW RETENTION POLICIES");
    messages
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                row.get("policy_name").map(str::to_string)
            }
            _ => None,
        })
        .collect()
}

/// Bring up a cluster whose leader already holds the replicated policy.
/// Every node carries it, so ALTER and DROP validate on the leader.
async fn cluster_with_policy() -> TestCluster {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    create_collection(&cluster).await;
    let leader = &cluster.nodes[pick_leader_index(&cluster)];
    create_policy_on(leader).await;

    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    wait_for(
        "leader registers its own retention policy",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_policy(&cluster.nodes[leader_index]).is_some(),
    )
    .await;
    wait_for(
        "follower registers the replicated retention policy",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_policy(&cluster.nodes[follower_index]).is_some(),
    )
    .await;
    cluster
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn retention_policy_created_on_leader_reaches_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    create_collection(&cluster).await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    // Baseline: the follower carries neither the row nor the registry entry.
    assert!(stored_policy(follower).is_none());
    assert!(registered_policy(follower).is_none());
    assert!(!shown_policies(follower).await.contains(&POLICY.to_string()));

    create_policy_on(&cluster.nodes[leader_index]).await;

    // The registry install follows the row write, so waiting on it
    // covers both.
    wait_for(
        "follower registers the replicated retention policy",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_policy(&cluster.nodes[follower_index]).is_some(),
    )
    .await;

    let record = stored_policy(follower)
        .expect("PutRetentionPolicy must write the row on the follower, not only the leader");
    assert_eq!(record.collection, COLLECTION);
    assert_eq!(record.eval_interval_ms, EVAL_INTERVAL_MS);
    assert!(record.enabled);
    assert!(!record.auto_tier);
    assert_eq!(record.tiers.len(), 1);

    let live = registered_policy(follower)
        .expect("post-apply must install the definition in follower enforcement");
    assert_eq!(live.collection, COLLECTION);
    assert_eq!(live.eval_interval_ms, EVAL_INTERVAL_MS);
    assert!(live.enabled);
    assert!(!live.auto_tier);
    assert_eq!(live.tiers.len(), 1);

    assert!(
        shown_policies(follower).await.contains(&POLICY.to_string()),
        "SHOW RETENTION POLICIES on follower node {} must report the leader's policy",
        follower.node_id,
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn retention_policy_altered_on_leader_reaches_follower() {
    let cluster = cluster_with_policy().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    // Baseline: CREATE leaves auto-tier routing off on the follower.
    assert!(
        !registered_policy(follower)
            .expect("policy is registered")
            .auto_tier
    );

    cluster.nodes[leader_index]
        .client
        .simple_query(&format!(
            "ALTER RETENTION POLICY {POLICY} ON {COLLECTION} SET AUTO_TIER = TRUE"
        ))
        .await
        .expect("ALTER RETENTION POLICY on the metadata leader");

    wait_for(
        "follower registers the altered retention policy",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_policy(&cluster.nodes[follower_index]).is_some_and(|p| p.auto_tier),
    )
    .await;

    let record = stored_policy(follower).expect("the altered row must reach the follower");
    assert!(
        record.auto_tier,
        "ALTER must rewrite the row on follower node {}",
        follower.node_id,
    );

    let live = registered_policy(follower).expect("the policy stays registered after the alter");
    assert!(
        live.auto_tier,
        "ALTER must reach the auto-tier planner on follower node {}, not only its row",
        follower.node_id,
    );
    assert_eq!(live.collection, COLLECTION);
    assert_eq!(live.eval_interval_ms, EVAL_INTERVAL_MS);

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn retention_policy_dropped_on_leader_reaches_follower() {
    let cluster = cluster_with_policy().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    assert!(stored_policy(follower).is_some());

    cluster.nodes[leader_index]
        .client
        .simple_query(&format!("DROP RETENTION POLICY {POLICY}"))
        .await
        .expect("DROP RETENTION POLICY on the metadata leader");

    wait_for(
        "follower drops the replicated retention policy",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_policy(&cluster.nodes[follower_index]).is_none(),
    )
    .await;

    assert!(
        stored_policy(follower).is_none(),
        "DeleteRetentionPolicy must remove the row on follower node {}",
        follower.node_id,
    );
    assert!(
        !shown_policies(follower).await.contains(&POLICY.to_string()),
        "SHOW RETENTION POLICIES on follower node {} must no longer report the policy",
        follower.node_id,
    );

    cluster.shutdown().await;
}
