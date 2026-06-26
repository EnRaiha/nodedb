// SPDX-License-Identifier: BUSL-1.1
//! Data Raft groups must be genuinely multi-replica.
//!
//! ## What this guards
//!
//! A cluster bootstraps with one founding node and others join. Data groups
//! (ids 1..N, vshard-partitioned) must become real RF-way replicas: every
//! node a voter of every group (for an N <= RF cluster), every node locally
//! applying the group's committed writes, and the shared routing table the
//! data plane reads converging to that membership on every node.
//!
//! Two historical bugs this pins down:
//!   1. A bootstrap clamp forced the stored replication factor to 1, so data
//!      groups stayed single-voter and joiners never replicated their data.
//!   2. Every node held two separate routing tables — the Raft coordinator's
//!      private copy (updated by conf-changes) and the `Arc<RwLock>` the data
//!      plane reads (frozen at the join-time snapshot). Committed
//!      AddLearner/PromoteLearner changes never reached the data-plane view,
//!      so membership was permanently frozen and divergent across nodes.
//!
//! ## Shape
//!
//!  1. Spawn a 3-node cluster (RF defaults to 3), create a `document_strict`
//!     collection, insert rows via one node, converge.
//!  2. Assert the collection's data group has all three nodes as VOTERS on
//!     EVERY node's routing view (no learners left, no divergence).
//!  3. Kill the data group's LEADER and assert both survivors still serve the
//!     full row set — the only confound-free proof that the followers locally
//!     replicated the data rather than routing reads to a single owner.

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb_types::DatabaseId;

const COLL: &str = "mr_data_group";
const ROW_COUNT: u32 = 5;

fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// `SELECT COUNT(*)`, retrying transient catch-up errors until `timeout`.
async fn count_rows(client: &tokio_postgres::Client, timeout: Duration) -> Result<usize, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT COUNT(*) FROM {COLL}"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
                        && let Some(s) = r.get(0)
                    {
                        return Ok(s.parse::<usize>().expect("COUNT(*) parse"));
                    }
                }
                return Err("COUNT(*) returned no rows".to_string());
            }
            Err(ref e) => {
                if Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    continue;
                }
                return Err(pg_detail(e));
            }
        }
    }
}

/// Sorted voter list for `group_id` as seen by `node`'s shared routing table.
fn voters_seen_by(node: &common::cluster_harness::TestClusterNode, group_id: u64) -> Vec<u64> {
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let mut v = routing
        .group_info(group_id)
        .map(|i| i.members.clone())
        .unwrap_or_default();
    v.sort_unstable();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_group_is_multi_replica_and_survives_leader_loss() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION");

    for i in 0..ROW_COUNT {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {COLL} (id, payload) VALUES ('row-{i}', 'payload-{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert row-{i}: {}", pg_detail(&e)));
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Resolve the collection's data group.
    let vshard = nodedb_cluster::routing::vshard_for_collection(DatabaseId::DEFAULT, COLL);
    let group_id = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        routing
            .group_for_vshard(vshard)
            .expect("collection vshard mapped to a group")
    };
    assert!(
        group_id != 0,
        "collection must map to a data group, not metadata"
    );

    // Every node's routing view must converge to all three nodes as voters
    // (no learners left, no divergence). Bounded poll for the promotion
    // conf-changes to commit + apply through the shared routing table.
    let deadline = Instant::now() + Duration::from_secs(20);
    let all_voters = loop {
        let converged = cluster
            .nodes
            .iter()
            .all(|n| voters_seen_by(n, group_id) == vec![1, 2, 3]);
        if converged {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert!(
        all_voters,
        "data group {group_id} did not converge to [1,2,3] voters on every node; \
         views: {:?}",
        cluster
            .nodes
            .iter()
            .map(|n| (n.node_id, voters_seen_by(n, group_id)))
            .collect::<Vec<_>>()
    );

    // Kill the data group's LEADER. Reading from a survivor afterward is the
    // only confound-free proof of local replication: had the data lived only
    // on a single owner, killing the leader would lose it.
    let group_leader = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        routing.group_info(group_id).map(|i| i.leader).unwrap_or(0)
    };
    assert!(group_leader != 0, "data group {group_id} has no leader");

    let mut nodes = cluster.nodes;
    let leader_idx = nodes
        .iter()
        .position(|n| n.node_id == group_leader)
        .expect("leader node present");
    nodes.remove(leader_idx).shutdown().await;

    // Survivors re-elect a new leader; give the group a moment to settle.
    tokio::time::sleep(Duration::from_secs(3)).await;

    for node in &nodes {
        let n = count_rows(&node.client, Duration::from_secs(20))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "survivor node {} could not serve rows after leader death: {e} \
                     => data group was NOT multi-replica",
                    node.node_id
                )
            });
        assert_eq!(
            n, ROW_COUNT as usize,
            "survivor node {} served {n} rows, expected {ROW_COUNT}",
            node.node_id
        );
    }

    for node in nodes {
        node.shutdown().await;
    }
}
