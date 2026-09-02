// SPDX-License-Identifier: BUSL-1.1
//! Cross-node replication of `ALTER VECTOR INDEX ... SET (...)`.
//!
//! A node-local dispatch leaves the altered build parameters on the executing
//! node alone, while every other node keeps the CREATE-time row and rebuilds
//! its index from that row at the next boot. The statement therefore proposes
//! the whole post-ALTER `StoredVectorIndexParams` row as a catalog entry, and
//! apply writes it on every node. This test reads the follower's own catalog
//! and rules out two divergences: the altered field never reaching the
//! follower, and a merge that reaches it having wiped the fields the statement
//! never named.

use crate::common;

use std::time::Duration;

use common::cluster_harness::node::lifecycle::HARNESS_SUPERUSER;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb_types::{DatabaseId, StoredVectorIndexParams};

/// Schemaless collection carrying the indexed embedding field.
const COLLECTION: &str = "vec_alter_docs";
/// Indexed document field.
const FIELD: &str = "embedding";
/// Index the test creates, then alters.
const INDEX: &str = "vec_alter_idx";

/// CREATE-time build parameters. None match the engine defaults (`m = 16`,
/// `ef_construction = 200`), so a row carrying them came from the statement.
const DIM: usize = 8;
const M: usize = 32;
const EF_CONSTRUCTION: usize = 400;
/// CREATE defaults the metric to `cosine` and the index type to `hnsw`.
const METRIC: &str = "cosine";
const INDEX_TYPE: &str = "hnsw";

/// The single value `ALTER` sets. It matches neither the CREATE-time value nor
/// the engine default, so the follower's row carrying it came from the ALTER.
const ALTERED_EF_CONSTRUCTION: usize = 512;

/// Node id every node agrees is the metadata-group leader.
fn leader_id(cluster: &TestCluster) -> u64 {
    cluster
        .nodes
        .iter()
        .map(|n| n.metadata_group_leader())
        .find(|&id| id != 0)
        .expect("at least one node must report a non-zero leader id")
}

/// The node that runs the DDL.
fn leader(cluster: &TestCluster) -> &TestClusterNode {
    let id = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .find(|n| n.node_id == id)
        .expect("the metadata leader must be one of the spawned nodes")
}

/// A node that does not run the DDL.
fn follower(cluster: &TestCluster) -> &TestClusterNode {
    let id = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .find(|n| n.node_id != id)
        .expect("a 3-node cluster must have a follower")
}

/// Tenant the harness pgwire client authenticates as.
///
/// The catalog row is keyed by the executing identity's tenant, so a catalog
/// read must use the same tenant the harness client carries.
fn harness_tenant_id(node: &TestClusterNode) -> u64 {
    node.shared
        .credentials
        .get_user(HARNESS_SUPERUSER)
        .expect("the harness superuser must exist on every node")
        .tenant_id
        .as_u64()
}

/// The `vector_index_params` row this node holds for the indexed field.
fn stored_params(node: &TestClusterNode) -> Option<StoredVectorIndexParams> {
    node.shared
        .credentials
        .catalog()
        .get_vector_index_params(
            DatabaseId::DEFAULT.as_u64(),
            harness_tenant_id(node),
            COLLECTION,
            FIELD,
        )
        .expect("read the vector index params row")
}

/// The row this node holds, or a panic naming the node that holds none.
fn require_params(node: &TestClusterNode) -> StoredVectorIndexParams {
    stored_params(node)
        .unwrap_or_else(|| panic!("node {} holds no vector index params row", node.node_id))
}

/// `ALTER` must change the field it names on every node, and keep every field
/// it does not name at its CREATE-time value.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn alter_vector_index_params_reach_the_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE VECTOR INDEX {INDEX} ON {COLLECTION} ({FIELD}) \
             METRIC {METRIC} DIM {DIM} M {M} EF_CONSTRUCTION {EF_CONSTRUCTION}"
        ))
        .await
        .expect("CREATE VECTOR INDEX");

    let follower = follower(&cluster);
    let follower_id = follower.node_id;

    wait_for(
        "follower persists the created vector index params",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || stored_params(follower).is_some(),
    )
    .await;

    // Baseline: the follower's own row carries the declared numbers, so a
    // later change to them is the ALTER's doing.
    let before = require_params(follower);
    assert_eq!(
        (before.dim, before.m, before.ef_construction),
        (DIM, M, EF_CONSTRUCTION),
        "node {follower_id} must hold the declared build parameters before the ALTER",
    );
    assert_eq!(
        (before.metric.as_str(), before.index_type.as_str()),
        (METRIC, INDEX_TYPE),
        "node {follower_id} must hold the declared metric and index type before the ALTER",
    );
    assert_eq!(
        (before.pq_m, before.ivf_cells, before.ivf_nprobe),
        (0, 0, 0),
        "a plain HNSW index declares no quantization parameters",
    );

    leader(&cluster)
        .client
        .simple_query(&format!(
            "ALTER VECTOR INDEX ON {COLLECTION}.{FIELD} \
             SET (ef_construction = {ALTERED_EF_CONSTRUCTION})"
        ))
        .await
        .expect("ALTER VECTOR INDEX on the metadata leader");

    wait_for(
        "follower persists the altered ef_construction",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            stored_params(follower)
                .is_some_and(|row| row.ef_construction == ALTERED_EF_CONSTRUCTION)
        },
    )
    .await;

    let after = require_params(follower);
    assert_eq!(
        after.ef_construction, ALTERED_EF_CONSTRUCTION,
        "node {follower_id} must hold the altered ef_construction, not the \
         CREATE-time {EF_CONSTRUCTION}",
    );
    assert_eq!(
        after.m, M,
        "the SET clause names no m, so node {follower_id} must keep the \
         CREATE-time value rather than a merged-away default",
    );
    assert_eq!(
        after.dim, DIM,
        "the statement never redeclares the dimension, so a zero here clears \
         the width node {follower_id} enforces",
    );
    assert_eq!(
        after.metric, METRIC,
        "the SET clause names no metric, so node {follower_id} must keep the \
         CREATE-time one",
    );
    assert_eq!(
        after.index_type, INDEX_TYPE,
        "the SET clause names no index_type, so node {follower_id} must keep \
         the CREATE-time one",
    );
    assert_eq!(
        (after.pq_m, after.ivf_cells, after.ivf_nprobe),
        (0, 0, 0),
        "the SET clause names no quantization parameter, so node \
         {follower_id} must hold none",
    );
    assert_eq!(
        (
            after.database_id,
            after.tenant_id,
            after.collection.as_str(),
            after.field_name.as_str(),
        ),
        (
            DatabaseId::DEFAULT.as_u64(),
            harness_tenant_id(follower),
            COLLECTION,
            FIELD,
        ),
        "the identity fields key the row, so a merge that moved them landed \
         the altered parameters on a different index",
    );

    cluster.shutdown().await;
}
