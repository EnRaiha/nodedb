// SPDX-License-Identifier: BUSL-1.1
//! `ALTER DATABASE ... MATERIALIZE` across a 3-node cluster.
//!
//! The materialization runs on one node. Afterwards every node's catalog
//! holds the clone's collections as `Materialized` with no clone origin, and
//! the clone answers from its own storage on every node once its source is
//! dropped.

use crate::common;

use std::time::Duration;

use common::cluster_harness::shared_steps::{database_id, use_database};
use common::cluster_harness::{TestCluster, TestClusterNode};
use nodedb_types::{CloneStatus, DatabaseId};

const SOURCE: &str = "cm_src";
const CLONE: &str = "cm_clone";
const ROWS: usize = 5;

/// Every collection of `db` on `node` that is not a finished materialization.
fn unmaterialized(node: &TestClusterNode, db: DatabaseId) -> Vec<String> {
    node.shared
        .credentials
        .catalog()
        .load_all_collections(db)
        .expect("load collections")
        .into_iter()
        .filter(|c| c.cloned_from.is_some() || c.clone_status != CloneStatus::Materialized)
        .map(|c| c.name)
        .collect()
}

async fn row_count(node: &TestClusterNode, sql: &str) -> usize {
    node.client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} on node {}: {e}", node.node_id))
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialize_on_one_node_matches_every_catalog_and_replica() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {SOURCE}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE: {e}"));
    use_database(&cluster, SOURCE).await;
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cm_records (key STRING PRIMARY KEY, data STRING) \
             WITH (engine='kv')",
        )
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION: {e}"));
    for i in 0..ROWS {
        cluster.nodes[0]
            .exec(&format!(
                "INSERT INTO cm_records (key, data) VALUES ('k{i}', 'v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT k{i}: {e}"));
    }
    use_database(&cluster, "default").await;

    cluster
        .exec_ddl_on_any_leader(&format!("CLONE DATABASE {CLONE} FROM {SOURCE}"))
        .await
        .unwrap_or_else(|e| panic!("CLONE DATABASE: {e}"));
    cluster
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {CLONE} MATERIALIZE"))
        .await
        .unwrap_or_else(|e| panic!("ALTER DATABASE MATERIALIZE: {e}"));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    for node in &cluster.nodes {
        let clone_id = database_id(node, CLONE);
        let pending = unmaterialized(node, clone_id);
        assert!(
            pending.is_empty(),
            "node {} still holds unmaterialized clone collections: {pending:?}",
            node.node_id
        );
    }

    // With the source gone, only the clone's own replicated storage answers.
    cluster
        .exec_ddl_on_any_leader(&format!("DROP DATABASE {SOURCE} CASCADE"))
        .await
        .unwrap_or_else(|e| panic!("DROP DATABASE source: {e}"));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    use_database(&cluster, CLONE).await;
    for node in &cluster.nodes {
        assert_eq!(
            row_count(node, "SELECT key FROM cm_records").await,
            ROWS,
            "node {} must read every materialized row from the clone",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

/// Every `(key, value)` row `sql` returns on `node`, sorted.
async fn rows(node: &TestClusterNode, sql: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = node
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} on node {}: {e}", node.node_id))
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap_or_default().to_owned(),
                row.get(1).unwrap_or_default().to_owned(),
            )),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// An UPDATE on a shadowed clone copies the source row up through one node.
/// Every node then holds the copy-up mapping, and every node reads the
/// updated row once, never next to the stale source copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_up_through_one_node_is_readable_on_every_node() {
    const SRC: &str = "cu_src";
    const CLN: &str = "cu_clone";
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {SRC}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE: {e}"));
    use_database(&cluster, SRC).await;
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cu_docs (id TEXT PRIMARY KEY, content TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION: {e}"));
    for i in 0..3 {
        cluster.nodes[0]
            .exec(&format!(
                "INSERT INTO cu_docs (id, content) VALUES ('d{i}', 'old{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT d{i}: {e}"));
    }
    use_database(&cluster, "default").await;
    cluster
        .exec_ddl_on_any_leader(&format!("CLONE DATABASE {CLN} FROM {SRC}"))
        .await
        .unwrap_or_else(|e| panic!("CLONE DATABASE: {e}"));
    use_database(&cluster, CLN).await;

    cluster.nodes[1]
        .exec("UPDATE cu_docs SET content = 'new1' WHERE id = 'd1'")
        .await
        .unwrap_or_else(|e| panic!("UPDATE on the clone: {e}"));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let expected = vec![
        ("d0".to_string(), "old0".to_string()),
        ("d1".to_string(), "new1".to_string()),
        ("d2".to_string(), "old2".to_string()),
    ];
    for node in &cluster.nodes {
        let clone_id = database_id(node, CLN);
        let key =
            nodedb::control::planner::sql_plan_convert::convert::db_qualified(clone_id, "cu_docs");
        let mappings = node
            .shared
            .credentials
            .catalog()
            .list_clone_copyups(&key)
            .expect("list copy-ups");
        assert_eq!(
            mappings.len(),
            1,
            "node {} must hold the replicated copy-up mapping",
            node.node_id
        );
        assert_eq!(
            rows(node, "SELECT id, content FROM cu_docs").await,
            expected,
            "node {} must read the copied-up row once, updated",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
