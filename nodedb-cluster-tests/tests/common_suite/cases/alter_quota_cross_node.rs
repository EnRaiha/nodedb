// SPDX-License-Identifier: BUSL-1.1
//! Cross-node replication of `SET QUOTA` DDL.
//!
//! `ALTER DATABASE … SET QUOTA` and `ALTER TENANT … IN DATABASE … SET
//! QUOTA` propose `CatalogEntry::PutDatabaseQuota` / `PutTenantQuota`
//! through the metadata raft group. Every node writes the quota row and
//! installs the record into its live enforcement components from the
//! apply path. A quota that reached only the executing node leaves each
//! follower enforcing nothing while its catalog reports defaults, so
//! each test asserts the follower's row AND its live caps.

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb_types::{DatabaseId, QuotaRecord, TenantId};

/// Bootstrap database every node carries.
const DATABASE: &str = "default";
/// Tenant used by the tenant-scope test.
const TENANT_NAME: &str = "quota_tenant";
const TENANT_ID: u64 = 7401;

/// Connection cap proposed by each test. Distinct from every default.
const MAX_CONNECTIONS: u32 = 250;
/// Maintenance CPU cap, in percent. Yields a 0.6s per-window budget.
const MAINTENANCE_PCT: u8 = 1;
/// Maintenance lease longer than the capped window, shorter than none.
const OVER_BUDGET_LEASE_SECS: f64 = 5.0;

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

/// The database quota row this node persisted.
fn stored_database_quota(node: &TestClusterNode) -> Option<QuotaRecord> {
    node.shared
        .credentials
        .catalog()
        .get_database_quota(DatabaseId::DEFAULT)
        .expect("read database quota")
}

/// The tenant quota row this node persisted.
fn stored_tenant_quota(node: &TestClusterNode) -> Option<QuotaRecord> {
    node.shared
        .credentials
        .catalog()
        .get_tenant_quota(DatabaseId::DEFAULT, TenantId::new(TENANT_ID))
        .expect("read tenant quota")
}

/// `true` once this node holds a live connection cap for the database.
/// The registry keeps an entry only for a capped scope.
fn database_cap_installed(node: &TestClusterNode) -> bool {
    node.shared
        .admission_registry
        .database_live_connections(DatabaseId::DEFAULT)
        .is_some()
}

/// `true` once this node holds a live connection cap for the tenant.
fn tenant_cap_installed(node: &TestClusterNode) -> bool {
    node.shared
        .admission_registry
        .tenant_live_connections(DatabaseId::DEFAULT, TenantId::new(TENANT_ID))
        .is_some()
}

/// `true` while this node grants an over-budget maintenance lease.
/// An installed `maintenance_cpu_pct` cap refuses it.
fn maintenance_lease_granted(node: &TestClusterNode) -> bool {
    node.shared
        .maintenance_budget
        .try_acquire(DatabaseId::DEFAULT, OVER_BUDGET_LEASE_SECS)
        .is_some()
}

/// Read one quota dimension out of a `SHOW … QUOTA` result.
async fn shown_limit(node: &TestClusterNode, sql: &str, dimension: &str) -> Option<String> {
    let messages = node
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    messages.into_iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(row)
            if row.get("quota_name") == Some(dimension) =>
        {
            row.get("limit").map(str::to_string)
        }
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn database_quota_set_on_leader_reaches_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let leader = &cluster.nodes[pick_leader_index(&cluster)];
    let follower = &cluster.nodes[pick_follower_index(&cluster)];
    let show = format!("SHOW DATABASE QUOTA FOR {DATABASE}");

    // Baseline: the follower carries no row and no cap.
    assert!(stored_database_quota(follower).is_none());
    assert!(!database_cap_installed(follower));
    assert!(maintenance_lease_granted(follower));
    assert_eq!(
        shown_limit(follower, &show, "max_connections").await,
        Some("unlimited".to_string()),
    );

    leader
        .client
        .simple_query(&format!(
            "ALTER DATABASE {DATABASE} SET QUOTA \
             (max_connections = {MAX_CONNECTIONS}, maintenance_cpu_pct = {MAINTENANCE_PCT})"
        ))
        .await
        .expect("ALTER DATABASE SET QUOTA on the metadata leader");

    // The connection cap is installed last, so waiting on it also
    // covers the row write and the maintenance cap.
    wait_for(
        "follower installs the replicated database connection cap",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || database_cap_installed(follower),
    )
    .await;

    let record = stored_database_quota(follower)
        .expect("PutDatabaseQuota must write the row on the follower, not only the leader");
    assert_eq!(record.max_connections, MAX_CONNECTIONS);
    assert_eq!(record.maintenance_cpu_pct, MAINTENANCE_PCT);

    assert_eq!(
        shown_limit(follower, &show, "max_connections").await,
        Some(MAX_CONNECTIONS.to_string()),
        "SHOW DATABASE QUOTA on follower node {} must report the leader's quota",
        follower.node_id,
    );

    assert!(
        !maintenance_lease_granted(follower),
        "follower node {} must enforce the replicated maintenance cap, \
         not merely store the row",
        follower.node_id,
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tenant_quota_set_on_leader_reaches_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE TENANT {TENANT_NAME} ID {TENANT_ID}"))
        .await
        .expect("create tenant");

    let leader = &cluster.nodes[pick_leader_index(&cluster)];
    let follower = &cluster.nodes[pick_follower_index(&cluster)];
    let show = format!("SHOW TENANT QUOTA FOR {TENANT_NAME} IN DATABASE {DATABASE}");

    // Baseline: the follower carries no row and no cap.
    assert!(stored_tenant_quota(follower).is_none());
    assert!(!tenant_cap_installed(follower));
    assert_eq!(
        shown_limit(follower, &show, "max_connections").await,
        Some("unlimited".to_string()),
    );

    leader
        .client
        .simple_query(&format!(
            "ALTER TENANT {TENANT_NAME} IN DATABASE {DATABASE} SET QUOTA \
             (max_connections = {MAX_CONNECTIONS})"
        ))
        .await
        .expect("ALTER TENANT SET QUOTA on the metadata leader");

    wait_for(
        "follower installs the replicated tenant connection cap",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || tenant_cap_installed(follower),
    )
    .await;

    let record = stored_tenant_quota(follower)
        .expect("PutTenantQuota must write the row on the follower, not only the leader");
    assert_eq!(record.max_connections, MAX_CONNECTIONS);

    assert_eq!(
        shown_limit(follower, &show, "max_connections").await,
        Some(MAX_CONNECTIONS.to_string()),
        "SHOW TENANT QUOTA on follower node {} must report the leader's quota",
        follower.node_id,
    );

    cluster.shutdown().await;
}
