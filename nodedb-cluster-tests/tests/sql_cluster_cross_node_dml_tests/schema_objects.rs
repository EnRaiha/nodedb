// SPDX-License-Identifier: BUSL-1.1

//! Schema object replication tests: functions, tenants, RLS, grants, ownership, materialized views.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn function_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE FUNCTION add_one(x INT) RETURNS INT AS BEGIN RETURN x + 1; END",
        )
        .await
        .expect("create function");

    wait_for(
        "all 3 nodes see the function in local SystemCatalog redb",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_function(1, "add_one")),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP FUNCTION add_one")
        .await
        .expect("drop function");

    wait_for(
        "all 3 nodes no longer see the function",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_function(1, "add_one")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tenant_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE TENANT acme ID 4242")
        .await
        .expect("create tenant");

    wait_for(
        "all 3 nodes see tenant 4242",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_tenant(4242)),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP TENANT 4242")
        .await
        .expect("drop tenant");

    wait_for(
        "all 3 nodes no longer see tenant 4242",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_tenant(4242)),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rls_policy_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION accounts (id BIGINT PRIMARY KEY, owner TEXT)")
        .await
        .expect("create source collection");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE RLS POLICY owner_only ON accounts FOR READ USING (owner = 'alice')",
        )
        .await
        .expect("create rls policy");

    wait_for(
        "all 3 nodes see the RLS policy",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_rls_policy(1, "accounts", "owner_only"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP RLS POLICY owner_only ON accounts")
        .await
        .expect("drop rls policy");

    wait_for(
        "all 3 nodes no longer see the RLS policy",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_rls_policy(1, "accounts", "owner_only"))
        },
    )
    .await;

    cluster.shutdown().await;
}

/// `GRANT <perm> ON <collection> TO <grantee>` replicates
/// to every node's `PermissionStore` in-memory grants set.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn grant_permission_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION documents (id BIGINT PRIMARY KEY, body TEXT)")
        .await
        .expect("create collection");

    cluster
        .exec_ddl_on_any_leader("CREATE USER analyst WITH PASSWORD 'secret123'")
        .await
        .expect("create user");

    cluster
        .exec_ddl_on_any_leader("GRANT read ON documents TO analyst")
        .await
        .expect("grant read");

    let target = "collection:1:documents";
    wait_for(
        "all 3 nodes see the grant",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_grant(target, "user:analyst", "read"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("REVOKE read ON documents FROM analyst")
        .await
        .expect("revoke read");

    wait_for(
        "all 3 nodes no longer see the grant",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_grant(target, "user:analyst", "read"))
        },
    )
    .await;

    cluster.shutdown().await;
}

/// `GRANT ROLE x TO user` replicates the updated `StoredUser`
/// (via `CatalogEntry::PutUser`) to every node's credentials cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn grant_role_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE USER ops_user WITH PASSWORD 'ops_pass1'")
        .await
        .expect("create user");

    cluster
        .exec_ddl_on_any_leader("GRANT ROLE monitor TO ops_user")
        .await
        .expect("grant role");

    wait_for(
        "all 3 nodes see ops_user has monitor role",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.user_has_role("ops_user", "monitor"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("REVOKE ROLE monitor FROM ops_user")
        .await
        .expect("revoke role");

    wait_for(
        "all 3 nodes see ops_user no longer has monitor role",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.user_has_role("ops_user", "monitor"))
        },
    )
    .await;

    cluster.shutdown().await;
}

/// `ALTER COLLECTION owner OWNER TO new_owner` replicates
/// the updated `StoredCollection` to every node's `PermissionStore`.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ownership_transfer_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION assets (id BIGINT PRIMARY KEY, label TEXT)")
        .await
        .expect("create collection");

    cluster
        .exec_ddl_on_any_leader("CREATE USER new_owner_user WITH PASSWORD 'pass4567'")
        .await
        .expect("create new owner user");

    wait_for(
        "all 3 nodes observe new_owner_user",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_active_user("new_owner_user"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("ALTER COLLECTION assets OWNER TO new_owner_user")
        .await
        .expect("transfer ownership");

    wait_for(
        "all 3 nodes see new_owner_user as owner of assets",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|n| {
                n.owner_of(
                    "collection",
                    nodedb_types::DatabaseId::DEFAULT.as_u64(),
                    1,
                    "assets",
                )
                .as_deref()
                    == Some("new_owner_user")
            })
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn materialized_view_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION orders (id BIGINT PRIMARY KEY, amount BIGINT)")
        .await
        .expect("create source collection");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE MATERIALIZED VIEW sales_total ON orders AS SELECT SUM(amount) FROM orders",
        )
        .await
        .expect("create materialized view");

    wait_for(
        "all 3 nodes see the materialized view",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_materialized_view(1, "sales_total"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP MATERIALIZED VIEW sales_total")
        .await
        .expect("drop materialized view");

    wait_for(
        "all 3 nodes no longer see the materialized view",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_materialized_view(1, "sales_total"))
        },
    )
    .await;

    cluster.shutdown().await;
}
