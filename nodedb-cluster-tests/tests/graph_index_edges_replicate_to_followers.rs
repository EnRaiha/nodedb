// SPDX-License-Identifier: BUSL-1.1
//! `CREATE GRAPH INDEX` must replicate its materialized edges via Raft, not
//! land local-only.
//!
//! ## What this guards
//!
//! `CREATE GRAPH INDEX` scans a collection and materializes one CSR edge per
//! `parent → child` relation, dispatched per destination vShard as a
//! `GraphOp::EdgePutBatch` (and rolled back via `EdgeDeleteBatch`). Those batch
//! ops had no `ReplicatedWrite` encoder arm, and the DDL dispatched them
//! through the LOCAL-ONLY Data-Plane path: under replication factor > 1 every
//! follower was missing the index edges, and if the receiving node was the
//! group leader the whole index was lost on failover when a former follower
//! took over — silent write-loss, the same class of bug as the point-write,
//! `crdt_apply`, and node-label paths.
//!
//! ## Shape
//!
//!  1. Spawn a 3-node cluster (RF=3), create a collection, insert documents
//!     that all parent onto a single `root` id (so every index edge homes on
//!     `from_key("root")`), build the index, and converge.
//!  2. Resolve the data group that owns `root`'s home vShard and kill that
//!     group's LEADER.
//!  3. After re-election, run a `MATCH (a)-[:<index>]->(b)` from a SURVIVING
//!     node and assert the index edges come back. Without the encoder arm and
//!     the Raft-proposing dispatch the edges never reached the survivors, so
//!     the index match resolves to nothing.

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb_types::id::VShardId;

const COLL: &str = "gidx_repl";
const INDEX: &str = "reports";
const PARENT: &str = "root";
const CHILDREN: [&str; 3] = ["c0", "c1", "c2"];

fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// All `(a, b)` pairs from `MATCH (a)-[:reports]->(b)` over pgwire
/// simple-query (columns `a`/`b`). Retries transient catch-up errors until
/// `timeout`.
async fn index_match_rows(
    client: &tokio_postgres::Client,
    sql: &str,
    timeout: Duration,
) -> Result<Vec<(String, String)>, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client.simple_query(sql).await {
            Ok(msgs) => {
                let mut out = Vec::new();
                for m in &msgs {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                        let a = r.get("a").unwrap_or("").to_string();
                        let b = r.get("b").unwrap_or("").to_string();
                        out.push((a, b));
                    }
                }
                return Ok(out);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_index_edges_replicate_and_survive_leader_loss() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION");

    // Insert the root plus three children, all parented onto `root`. Every
    // materialized edge therefore homes on `from_key("root")`, so killing that
    // single group's leader is a confound-free probe of replication.
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {COLL} (id, parent) VALUES ('{PARENT}', '')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert root: {}", pg_detail(&e)));
    for child in CHILDREN {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {COLL} (id, parent) VALUES ('{child}', '{PARENT}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {child}: {}", pg_detail(&e)));
    }

    // Build the index. With the encoder arm + Raft-proposing dispatch this
    // proposes each `EdgePutBatch` through the owning data group's Raft log;
    // without it the edges would only ever exist on the receiving node.
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "CREATE GRAPH INDEX {INDEX} ON {COLL} (parent -> id)"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE GRAPH INDEX: {}", pg_detail(&e)));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let match_sql = format!("MATCH (a)-[:{INDEX}]->(b) RETURN a, b");
    let expected: Vec<(String, String)> = CHILDREN
        .iter()
        .map(|c| (PARENT.to_string(), c.to_string()))
        .collect();

    // Sanity: the index edges are readable before the failover.
    let landed = index_match_rows(
        &cluster.nodes[0].client,
        &match_sql,
        Duration::from_secs(10),
    )
    .await
    .expect("index MATCH on node 0");
    for pair in &expected {
        assert!(
            landed.contains(pair),
            "index MATCH missing {pair:?} before failover; got {landed:?}"
        );
    }

    // Resolve the data group owning `root`'s home vShard (edges are partitioned
    // by `from_key(parent)`) and its leader.
    let vshard = VShardId::from_key(PARENT.as_bytes()).as_u32();
    let (group_id, group_leader) = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let gid = routing
            .group_for_vshard(vshard)
            .expect("root vshard mapped to a group");
        let leader = routing.group_info(gid).map(|i| i.leader).unwrap_or(0);
        (gid, leader)
    };
    assert!(
        group_id != 0,
        "index edge must map to a data group, not metadata"
    );
    assert!(group_leader != 0, "data group {group_id} has no leader");

    // Kill the data group's LEADER. Reading the index from a survivor afterward
    // is the confound-free proof of replication: had the edges lived only on the
    // receiving node (the pre-fix local-only dispatch), killing the leader that
    // held them would lose them, and the index match on a survivor would be empty.
    let mut nodes = cluster.nodes;
    let leader_idx = nodes
        .iter()
        .position(|n| n.node_id == group_leader)
        .expect("leader node present");
    nodes.remove(leader_idx).shutdown().await;

    // Survivors re-elect a new leader; give the group a moment to settle.
    tokio::time::sleep(Duration::from_secs(3)).await;

    for node in &nodes {
        let rows = index_match_rows(&node.client, &match_sql, Duration::from_secs(20))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "survivor node {} could not run index MATCH after leader death: {e}",
                    node.node_id
                )
            });
        for pair in &expected {
            assert!(
                rows.contains(pair),
                "BUG: survivor node {} missing index edge {pair:?} after the data-group \
                 leader was killed — CREATE GRAPH INDEX dispatched its EdgePutBatch LOCAL-ONLY \
                 and never proposed it through Raft, so the index was lost on failover (silent \
                 write-loss under RF>1); got {rows:?}",
                node.node_id
            );
        }
    }

    for node in nodes {
        node.shutdown().await;
    }
}
