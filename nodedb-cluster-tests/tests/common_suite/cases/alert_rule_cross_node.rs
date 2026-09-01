// SPDX-License-Identifier: BUSL-1.1
//! Cross-node replication of `ALERT` DDL.
//!
//! `CREATE` / `ALTER` / `DROP ALERT` propose `CatalogEntry::PutAlertRule` /
//! `DeleteAlertRule` through the metadata raft group. Every node writes the
//! `alert_rules` row and installs the definition into its own
//! `AlertRegistry`. The alert eval loop reads that registry, so a rule that
//! reached only the executing node never fires on a follower. Each test
//! asserts the follower's durable row AND its live registry entry.

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb::event::alert::types::AlertDef;

/// Timeseries collection every alert targets.
const COLLECTION: &str = "alert_cross_metrics";
/// Alert name every test proposes.
const ALERT: &str = "alert_cross_high_temp";
/// Window requested by CREATE, in milliseconds.
const WINDOW_MS: u64 = 300_000;
/// Consecutive windows before firing. Distinct from the default of 1.
const FIRE_AFTER: u32 = 3;

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

/// The durable `alert_rules` row this node persisted for [`ALERT`].
fn stored_alert(node: &TestClusterNode) -> Option<AlertDef> {
    node.shared
        .credentials
        .catalog()
        .load_all_alert_rules()
        .expect("load alert rules")
        .into_iter()
        .find(|a| a.name == ALERT)
}

/// The live registry entry this node holds for [`ALERT`].
/// The eval loop reads this map, not the catalog row.
fn registered_alert(node: &TestClusterNode) -> Option<AlertDef> {
    node.shared
        .alert_registry
        .list_all()
        .into_iter()
        .find(|a| a.name == ALERT)
}

/// Create the timeseries collection every alert targets.
async fn create_collection(cluster: &TestCluster) {
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .expect("CREATE COLLECTION for the alert target");
}

/// Run `CREATE ALERT` on this node.
async fn create_alert_on(node: &TestClusterNode) {
    node.client
        .simple_query(&format!(
            "CREATE ALERT {ALERT} ON {COLLECTION} \
             CONDITION AVG(value) > 90.0 \
             GROUP BY metric \
             WINDOW '5 minutes' \
             FOR '3 consecutive windows' \
             SEVERITY 'critical' \
             NOTIFY TOPIC 'alert_cross_topic'"
        ))
        .await
        .expect("CREATE ALERT on the metadata leader");
}

/// Alert names reported by `SHOW ALERTS` on this node.
async fn shown_alerts(node: &TestClusterNode) -> Vec<String> {
    let messages = node
        .client
        .simple_query("SHOW ALERTS")
        .await
        .expect("SHOW ALERTS");
    messages
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get("name").map(str::to_string),
            _ => None,
        })
        .collect()
}

/// Bring up a cluster whose leader already holds the replicated alert.
/// Every node carries it, so ALTER and DROP validate on the leader.
async fn cluster_with_alert() -> TestCluster {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    create_collection(&cluster).await;
    let leader_index = pick_leader_index(&cluster);
    create_alert_on(&cluster.nodes[leader_index]).await;

    let follower_index = pick_follower_index(&cluster);
    wait_for(
        "leader registers its own alert rule",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_alert(&cluster.nodes[leader_index]).is_some(),
    )
    .await;
    wait_for(
        "follower registers the replicated alert rule",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_alert(&cluster.nodes[follower_index]).is_some(),
    )
    .await;
    cluster
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn alert_rule_created_on_leader_reaches_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    create_collection(&cluster).await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    // Baseline: the follower carries neither the row nor the registry entry.
    assert!(stored_alert(follower).is_none());
    assert!(registered_alert(follower).is_none());
    assert!(!shown_alerts(follower).await.contains(&ALERT.to_string()));

    create_alert_on(&cluster.nodes[leader_index]).await;

    // The registry install follows the row write, so waiting on it
    // covers both.
    wait_for(
        "follower registers the replicated alert rule",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_alert(&cluster.nodes[follower_index]).is_some(),
    )
    .await;

    let record = stored_alert(follower)
        .expect("PutAlertRule must write the row on the follower, not only the leader");
    assert_eq!(record.collection, COLLECTION);
    assert_eq!(record.window_ms, WINDOW_MS);
    assert_eq!(record.fire_after, FIRE_AFTER);
    assert_eq!(record.severity, "critical");
    assert_eq!(record.group_by, vec!["metric".to_string()]);
    assert_eq!(record.condition.agg_func, "avg");
    assert_eq!(record.condition.column, "value");
    assert!((record.condition.threshold - 90.0).abs() < f64::EPSILON);
    assert!(record.enabled);

    let live = registered_alert(follower)
        .expect("post-apply must install the definition in the follower eval loop");
    assert_eq!(live.collection, COLLECTION);
    assert_eq!(live.window_ms, WINDOW_MS);
    assert_eq!(live.fire_after, FIRE_AFTER);
    assert_eq!(live.severity, "critical");
    assert_eq!(live.condition.agg_func, "avg");
    assert!((live.condition.threshold - 90.0).abs() < f64::EPSILON);
    assert!(live.enabled);
    assert_eq!(live.notify_targets.len(), 1);

    assert!(
        shown_alerts(follower).await.contains(&ALERT.to_string()),
        "SHOW ALERTS on follower node {} must report the leader's alert",
        follower.node_id,
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn alert_rule_altered_on_leader_reaches_follower() {
    let cluster = cluster_with_alert().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    // Baseline: CREATE leaves the rule enabled on the follower.
    assert!(
        registered_alert(follower)
            .expect("alert is registered")
            .enabled
    );

    cluster.nodes[leader_index]
        .client
        .simple_query(&format!("ALTER ALERT {ALERT} DISABLE"))
        .await
        .expect("ALTER ALERT on the metadata leader");

    wait_for(
        "follower registers the disabled alert rule",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_alert(&cluster.nodes[follower_index]).is_some_and(|a| !a.enabled),
    )
    .await;

    let record = stored_alert(follower).expect("the altered row must reach the follower");
    assert!(
        !record.enabled,
        "ALTER must rewrite the row on follower node {}",
        follower.node_id,
    );

    let live = registered_alert(follower).expect("the alert stays registered after the alter");
    assert!(
        !live.enabled,
        "ALTER must reach the eval loop on follower node {}, not only its row",
        follower.node_id,
    );
    assert_eq!(live.collection, COLLECTION);
    assert_eq!(live.window_ms, WINDOW_MS);

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn alert_rule_dropped_on_leader_reaches_follower() {
    let cluster = cluster_with_alert().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];

    assert!(stored_alert(follower).is_some());

    cluster.nodes[leader_index]
        .client
        .simple_query(&format!("DROP ALERT {ALERT}"))
        .await
        .expect("DROP ALERT on the metadata leader");

    wait_for(
        "follower drops the replicated alert rule",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || registered_alert(&cluster.nodes[follower_index]).is_none(),
    )
    .await;

    assert!(
        stored_alert(follower).is_none(),
        "DeleteAlertRule must remove the row on follower node {}",
        follower.node_id,
    );
    assert!(
        !shown_alerts(follower).await.contains(&ALERT.to_string()),
        "SHOW ALERTS on follower node {} must no longer report the alert",
        follower.node_id,
    );

    cluster.shutdown().await;
}
