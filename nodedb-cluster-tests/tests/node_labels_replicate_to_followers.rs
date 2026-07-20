// SPDX-License-Identifier: BUSL-1.1
//! A `GRAPH LABEL` node-label write must replicate via Raft, not land local-only.
//!
//! ## What this guards
//!
//! `GraphOp::SetNodeLabels` / `RemoveNodeLabels` are dispatched through
//! `dispatch_sync_response`, which proposes a write through the data group's
//! Raft log ONLY when the plan maps to a `ReplicatedWrite` via the encoder.
//! The node-label ops had no encoder arm, so `to_replicated_entry` returned
//! `None` and the write fell through to local-only Data-Plane dispatch: the
//! label landed solely on the receiving node. Under replication factor > 1
//! every follower was missing it, and if the receiving node was the group
//! leader the label was lost entirely on failover when a former follower took
//! over — silent write-loss, the same class of bug as the point-write and
//! `crdt_apply` paths.
//!
//! ## Shape
//!
//!  1. Spawn a 3-node cluster (RF=3), create a collection, insert an edge
//!     `alice -> bob` (implicitly creating both graph nodes), and label
//!     `alice` as `Person` through node 0's pgwire gateway, then converge.
//!  2. Resolve the data group that owns `alice`'s home vShard and kill that
//!     group's LEADER.
//!  3. After re-election, run a label-filtered `MATCH (a:Person)-[:knows]->(b)`
//!     from a SURVIVING node and assert the labeled path comes back. Without
//!     the encoder arm the label never reached the survivors, so the
//!     label-filtered match resolves to nothing.

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb_types::id::VShardId;

const COLL: &str = "glabel_repl";

fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// All `(a, b)` pairs from a label-filtered `MATCH (a:Person)-[:knows]->(b)`
/// over pgwire simple-query (columns `a`/`b`). Retries transient catch-up
/// errors until `timeout`.
async fn labeled_match_rows(
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

/// Poll a label-filtered MATCH until `expected` appears, or `timeout` elapses.
///
/// This is the confound-free survivor read after a leader kill. A survivor can
/// answer the MATCH *successfully* while its data group is still catching up
/// post-failover — returning an empty (stale) result before the replicated
/// label/edge state has been applied. `labeled_match_rows` returns on the first
/// `Ok`, so reading it once races that catch-up window and can observe empty
/// even though the write replicated durably. Polling until `expected` is
/// present (retrying BOTH transient errors and empty results) closes that race:
/// only a genuine failure to replicate — the label truly absent for the entire
/// window — survives to the caller as a still-missing row. Returns the last
/// observed rows so a real loss produces an informative assertion.
async fn wait_for_labeled_match(
    client: &tokio_postgres::Client,
    sql: &str,
    expected: &(String, String),
    timeout: Duration,
) -> Vec<(String, String)> {
    let deadline = Instant::now() + timeout;
    let mut last: Vec<(String, String)> = Vec::new();
    loop {
        // Each probe gets a short read budget for transient post-failover
        // errors; the outer loop owns the catch-up budget for empty results.
        match labeled_match_rows(client, sql, Duration::from_millis(500)).await {
            Ok(rows) => {
                if rows.contains(expected) {
                    return rows;
                }
                last = rows;
            }
            Err(_) if Instant::now() < deadline => {}
            Err(_) => return last,
        }
        if Instant::now() >= deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_labels_replicate_and_survive_leader_loss() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION");

    // Edge alice -> bob implicitly creates both graph nodes. Edge replication
    // already works; the label is the only variable under test.
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN '{COLL}' FROM 'alice' TO 'bob' TYPE 'knows'"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert alice -> bob: {}", pg_detail(&e)));

    // Label `alice` through node 0's pgwire gateway. With the encoder arm this
    // proposes through the data group's Raft log; without it the label would
    // only ever exist on node 0.
    cluster.nodes[0]
        .client
        .simple_query("GRAPH LABEL 'alice' AS 'Person'")
        .await
        .unwrap_or_else(|e| panic!("GRAPH LABEL alice: {}", pg_detail(&e)));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    const MATCH_LABELED: &str = "MATCH (a:Person)-[:knows]->(b) RETURN a, b";
    let expected = ("alice".to_string(), "bob".to_string());

    // Hard precondition: the labeled path must be readable on EVERY node before
    // the failover. `wait_for_full_apply_convergence` falls through silently on
    // timeout, so it does not by itself guarantee the "all three replicas have
    // applied the label" state this test depends on. Proving the label is
    // present on all three replicas here is what makes the post-kill assertion
    // meaningful: with every replica holding it before the kill, Raft election
    // safety guarantees a survivor keeps it, so a still-empty survivor read
    // afterward is genuine loss — not a lagging replica that never converged
    // pre-kill racing the post-failover catch-up window.
    for node in &cluster.nodes {
        let rows = wait_for_labeled_match(
            &node.client,
            MATCH_LABELED,
            &expected,
            Duration::from_secs(15),
        )
        .await;
        assert!(
            rows.contains(&expected),
            "labeled path did not replicate to node {} before failover — the label \
             never converged to every replica, so the leader-loss assertion below would \
             be testing a non-converged cluster; got {rows:?}",
            node.node_id,
        );
    }

    // Resolve the data group owning `alice`'s home vShard (node labels are
    // single-keyed on the node id, homed at `from_key(node_id)`) and its leader.
    let vshard = VShardId::from_key(b"alice").as_u32();
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
            .expect("alice vshard mapped to a group");
        let leader = routing.group_info(gid).map(|i| i.leader).unwrap_or(0);
        (gid, leader)
    };
    assert!(
        group_id != 0,
        "node label must map to a data group, not metadata"
    );
    assert!(group_leader != 0, "data group {group_id} has no leader");

    // Kill the data group's LEADER. Reading the label from a survivor afterward
    // is the confound-free proof of replication: had the label lived only on the
    // receiving node (the pre-fix local-only dispatch), killing the leader that
    // held it would lose it, and the labeled match on a survivor would be empty.
    let mut nodes = cluster.nodes;
    let leader_idx = nodes
        .iter()
        .position(|n| n.node_id == group_leader)
        .expect("leader node present");
    nodes.remove(leader_idx).shutdown().await;

    // Survivors re-elect a new leader and apply the replicated label; the poll
    // below owns the settle + catch-up window, so no fixed pre-sleep is needed.
    for node in &nodes {
        let rows = wait_for_labeled_match(
            &node.client,
            MATCH_LABELED,
            &expected,
            Duration::from_secs(20),
        )
        .await;
        assert!(
            rows.contains(&expected),
            "survivor node {} still had no 'Person' label {:?} after the data-group leader \
             was killed and a full post-failover catch-up window elapsed — the labeled path \
             never reached this survivor, so the SetNodeLabels write did not replicate durably \
             via Raft (silent write-loss under RF>1); got {rows:?}",
            node.node_id,
            expected,
        );
    }

    for node in nodes {
        node.shutdown().await;
    }
}
