// SPDX-License-Identifier: BUSL-1.1

//! Single-node Calvin is always on: a server with no `[cluster]` runs the full
//! Calvin stack — sequencer Raft group + per-vShard schedulers — so a
//! cross-core (cross-vShard) transaction traverses the SAME deterministic
//! Calvin path as in a multi-node cluster. The server synthesizes a
//! one-node cluster (self-seeded, replication factor 1) via
//! `init_single_node_calvin`.
//!
//! The test proves that `cluster_transport` and `sequencer_inbox` are both
//! wired, and that an AUTOCOMMIT cross-shard write — a `GRAPH INSERT EDGE`
//! whose endpoints home to DISTINCT vShards — is admitted to a Calvin epoch
//! (the sequencer's `admitted_total` advances) and dual-homes atomically (a
//! reverse/IN traversal from the destination reaches the source). That is the
//! sequencer→scheduler path, not the single-home fast path.

use crate::common;

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_types::id::VShardId;

use common::cluster_harness::{TestClusterNode, wait_for};

/// Observed sequencer-group leader id from a node's local Raft status, or `0`
/// if no leader is known yet.
fn sequencer_leader(node: &TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

/// Count of transactions the single-node sequencer has admitted to an epoch, or
/// `0` if the sequencer metrics handle is not installed yet.
fn sequencer_admitted(node: &TestClusterNode) -> u64 {
    node.shared
        .sequencer_metrics
        .get()
        .map(|m| m.admitted_total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// A `(src, dst)` pair of graph node keys whose home vShards differ, so an edge
/// between them is genuinely cross-shard (`VShardId::from_key` is how
/// `insert_edge` homes each endpoint). Deterministic: the mapping is a pure
/// function of the key bytes.
fn distinct_vshard_node_keys() -> (String, String) {
    let dst = "sncalvin_hub".to_string();
    let vdst = VShardId::from_key(dst.as_bytes()).as_u32();
    for i in 0u32..4096 {
        let src = format!("sncalvin_src_{i}");
        if VShardId::from_key(src.as_bytes()).as_u32() != vdst {
            return (src, dst);
        }
    }
    panic!("could not find a node key on a distinct vShard from the hub in 4096 tries");
}

/// Run a `GRAPH TRAVERSE` query that yields a single `result` JSON row.
async fn query_graph_json(client: &tokio_postgres::Client, sql: &str) -> serde_json::Value {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    let row = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("graph query returned no result row");
    let raw = row.get("result").expect("result column present");
    serde_json::from_str(raw).expect("result column is valid JSON")
}

/// The set of reached node ids from a `GRAPH TRAVERSE` result JSON.
fn traversed_node_ids(v: &serde_json::Value) -> HashSet<String> {
    v.get("nodes")
        .and_then(|n| n.as_array())
        .expect("traverse result has a nodes array")
        .iter()
        .map(|n| {
            n.get("id")
                .and_then(|id| id.as_str())
                .expect("node has string id")
                .to_string()
        })
        .collect()
}

/// A server with no `[cluster]` runs the Calvin stack and routes an autocommit
/// cross-shard edge insert through the single-node sequencer→scheduler path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_calvin_routes_cross_shard_write_through_sequencer() {
    // 4 Data-Plane cores so distinct vShards land on distinct cores — a genuine
    // cross-core transaction.
    let node = TestClusterNode::spawn_single_node_calvin(4)
        .await
        .expect("spawn standalone single-node-calvin server");

    // The lone sequencer voter self-elects; wait for it before submitting.
    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    assert!(
        node.shared.cluster_transport.is_some(),
        "single-node calvin must install cluster_transport"
    );
    assert!(
        node.shared.sequencer_inbox.get().is_some(),
        "single-node calvin must install sequencer_inbox"
    );

    // A graph-capable collection.
    node.client
        .simple_query("CREATE COLLECTION sncalvin_graph")
        .await
        .expect("CREATE COLLECTION sncalvin_graph");
    wait_for(
        "collection visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 1,
    )
    .await;

    let (src, dst) = distinct_vshard_node_keys();
    let admitted_before = sequencer_admitted(&node);

    // AUTOCOMMIT cross-shard edge insert. Because the endpoints home to
    // distinct vShards, `insert_edge` dual-homes the edge
    // atomically through Calvin (`submit_calvin_routed`) instead of taking the
    // single-home fast path. If the single-node sequencer stack were not
    // operational this would error (`SequencerUnavailable`) or time out.
    node.client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'sncalvin_graph' FROM '{src}' TO '{dst}' TYPE 'l'"
        ))
        .await
        .expect("cross-shard edge insert must commit via the single-node Calvin path");

    // Proof it traversed the sequencer→scheduler path: the transaction was
    // admitted to a Calvin epoch.
    let admitted_after = sequencer_admitted(&node);
    assert!(
        admitted_after > admitted_before,
        "cross-shard edge must be admitted to a Calvin epoch (before={admitted_before}, \
         after={admitted_after})"
    );

    // Functional proof the edge dual-homed atomically: a reverse (IN) traversal
    // from the destination reaches the source, so the REVERSE_EDGES row landed
    // on the destination's home vShard.
    let v = query_graph_json(
        &node.client,
        &format!("GRAPH TRAVERSE IN 'sncalvin_graph' FROM '{dst}' DEPTH 1 LABEL 'l' DIRECTION in"),
    )
    .await;
    let reached = traversed_node_ids(&v);
    assert!(
        reached.contains(&src),
        "reverse traversal from '{dst}' must reach '{src}' (edge dual-homed via Calvin); \
         got {reached:?}"
    );

    node.shutdown().await;
}
