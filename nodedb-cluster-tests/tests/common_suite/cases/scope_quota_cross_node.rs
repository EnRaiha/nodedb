// SPDX-License-Identifier: BUSL-1.1
//! Cross-node replication of `DEFINE QUOTA ON SCOPE` DDL.
//!
//! `DEFINE QUOTA ON SCOPE` and `DROP QUOTA ON SCOPE` propose
//! `CatalogEntry::PutScopeQuota` / `DeleteScopeQuota` through the metadata
//! raft group. Every node writes the `scope_quotas` row and installs the
//! definition into its own `QuotaManager`. A definition that reached only
//! the executing node leaves each follower enforcing nothing while its
//! catalog reports no quota, so each test asserts the follower's durable
//! row AND its live cap.

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb::control::security::catalog::auth_types::StoredScopeQuota;
use nodedb::control::security::metering::quota::QuotaEnforcement;

/// Scope named by both tests. Defined by no bootstrap path.
const SCOPE: &str = "ops:cluster";

/// Token budget proposed by each test. Distinct from every default.
const MAX_TOKENS: u64 = 5_000;
/// Quota period, in seconds.
const PERIOD_SECS: u64 = 60;
/// Warning fraction. Distinct from the 0.8 default.
const WARN_AT: f64 = 0.5;

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

/// The durable `scope_quotas` row this node persisted for [`SCOPE`].
fn stored_quota(node: &TestClusterNode) -> Option<StoredScopeQuota> {
    node.shared
        .credentials
        .catalog()
        .load_all_scope_quotas()
        .expect("load scope quotas")
        .into_iter()
        .find(|q| q.scope_name == SCOPE)
}

/// `true` once this node holds a live cap for [`SCOPE`] in `QuotaManager`.
/// Admission checks read that map, not the catalog row.
fn live_cap_installed(node: &TestClusterNode) -> bool {
    node.shared.quota_manager.has_quota(SCOPE)
}

/// Run `DEFINE QUOTA ON SCOPE` for [`SCOPE`] on this node.
async fn define_on(node: &TestClusterNode) {
    node.client
        .simple_query(&format!(
            "DEFINE QUOTA ON SCOPE '{SCOPE}' MAX {MAX_TOKENS} TOKENS \
             PER {PERIOD_SECS} SECONDS ENFORCEMENT HARD WARN AT {WARN_AT}"
        ))
        .await
        .expect("DEFINE QUOTA ON SCOPE on the metadata leader");
}

/// Scope names reported by `SHOW QUOTAS` on this node.
async fn shown_scopes(node: &TestClusterNode) -> Vec<String> {
    let messages = node
        .client
        .simple_query("SHOW QUOTAS")
        .await
        .expect("SHOW QUOTAS");
    messages
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get("scope").map(str::to_string),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn scope_quota_defined_on_leader_reaches_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let leader = &cluster.nodes[pick_leader_index(&cluster)];
    let follower = &cluster.nodes[pick_follower_index(&cluster)];

    // Baseline: the follower carries neither the row nor the live cap.
    assert!(stored_quota(follower).is_none());
    assert!(!live_cap_installed(follower));
    assert!(!shown_scopes(follower).await.contains(&SCOPE.to_string()));

    define_on(leader).await;

    // The live cap is installed after the row write, so waiting on it
    // covers both.
    wait_for(
        "follower installs the replicated scope quota",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || live_cap_installed(follower),
    )
    .await;

    let record = stored_quota(follower)
        .expect("PutScopeQuota must write the row on the follower, not only the leader");
    assert_eq!(record.max_tokens, MAX_TOKENS);
    assert_eq!(record.period_secs, PERIOD_SECS);
    assert_eq!(record.enforcement, "hard");
    assert_eq!(record.warning_threshold, WARN_AT);

    let live = follower
        .shared
        .quota_manager
        .list_quotas()
        .into_iter()
        .find(|q| q.scope_name == SCOPE)
        .expect("post-apply must install the definition in follower enforcement");
    assert_eq!(live.max_tokens, MAX_TOKENS);
    assert_eq!(live.period_secs, PERIOD_SECS);
    assert_eq!(live.enforcement, QuotaEnforcement::Hard);
    assert_eq!(live.warning_threshold, WARN_AT);

    assert!(
        shown_scopes(follower).await.contains(&SCOPE.to_string()),
        "SHOW QUOTAS on follower node {} must report the leader's quota",
        follower.node_id,
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn scope_quota_dropped_on_leader_reaches_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let leader = &cluster.nodes[pick_leader_index(&cluster)];
    let follower = &cluster.nodes[pick_follower_index(&cluster)];

    define_on(leader).await;
    wait_for(
        "follower installs the replicated scope quota",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || live_cap_installed(follower),
    )
    .await;
    assert!(stored_quota(follower).is_some());

    // The leader refuses a drop it does not see, so wait for its own cap.
    wait_for(
        "leader installs its own scope quota",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || live_cap_installed(leader),
    )
    .await;

    leader
        .client
        .simple_query(&format!("DROP QUOTA ON SCOPE '{SCOPE}'"))
        .await
        .expect("DROP QUOTA ON SCOPE on the metadata leader");

    wait_for(
        "follower removes the replicated scope quota",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || !live_cap_installed(follower),
    )
    .await;

    assert!(
        stored_quota(follower).is_none(),
        "DeleteScopeQuota must remove the row on follower node {}",
        follower.node_id,
    );
    assert!(
        !shown_scopes(follower).await.contains(&SCOPE.to_string()),
        "SHOW QUOTAS on follower node {} must no longer report the quota",
        follower.node_id,
    );

    cluster.shutdown().await;
}
