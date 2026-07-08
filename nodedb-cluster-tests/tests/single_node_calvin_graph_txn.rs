// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard (dual-home) graph edge STAGING inside an explicit transaction on
//! a single-node-calvin server.
//!
//! Before this coverage, a `GRAPH INSERT EDGE` whose endpoints home to DISTINCT
//! vShards, issued inside a `BEGIN..COMMIT` block, was REJECTED with
//! `CrossShardInExplicitTransaction`: only single-home edges could stage. A
//! cross-shard edge lives on BOTH endpoints (forward row on `from_key(src)`,
//! reverse row on `from_key(dst)`), and a read scatters to the home vShard of
//! the node it starts from — so each Data-Plane core merges only its OWN
//! per-transaction overlay. To make read-your-own-writes work from either
//! endpoint the edge must be staged into BOTH overlays.
//!
//! `dual_home_edge_stages_both_overlays_and_rollback_tears_down` proves the new
//! behavior end to end on a `single_node_calvin` server (where `calvin_available`
//! is true so a cross-shard edge is genuinely dual-home, not forced single-home):
//!
//! 1. `BEGIN`; `GRAPH INSERT EDGE` across two distinct vShards is ACCEPTED (no
//!    `CrossShardInExplicitTransaction`) and staged.
//! 2. In-txn RYOW from BOTH endpoints: `GRAPH NEIGHBORS ... DIRECTION out` from
//!    the SRC endpoint sees the edge (forward overlay on `vsrc`), and
//!    `GRAPH NEIGHBORS ... DIRECTION in` from the DST endpoint ALSO sees it
//!    (reverse overlay on `vdst`) — proving both overlays hold the staged edge.
//! 3. `ROLLBACK` fans `DropTxnOverlay` to both touched vShards, so a post-rollback
//!    read from each endpoint sees the edge gone.

mod common;

use std::time::Duration;

use nodedb_types::id::VShardId;

use common::cluster_harness::{TestClusterNode, wait_for};

/// The lone sequencer voter's observed leader id, or `0` if none known yet.
/// Mirrors the wait used by the sibling `single_node_calvin` suite so the
/// Calvin stack is up before the transaction runs.
fn sequencer_leader(node: &TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == nodedb_cluster::calvin::SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

/// A `(src, dst)` pair of graph node keys whose home vShards differ, so an edge
/// between them is genuinely cross-shard. Deterministic: `VShardId::from_key` is
/// a pure function of the key bytes, and it is how `insert_edge` homes each
/// endpoint. Same key-picking approach as the sibling `single_node_calvin` suite.
fn distinct_vshard_node_keys() -> (String, String) {
    let dst = "sncgtx_hub".to_string();
    let vdst = VShardId::from_key(dst.as_bytes()).as_u32();
    for i in 0u32..4096 {
        let src = format!("sncgtx_src_{i}");
        if VShardId::from_key(src.as_bytes()).as_u32() != vdst {
            return (src, dst);
        }
    }
    panic!("could not find a node key on a distinct vShard from the hub in 4096 tries");
}

/// Run `GRAPH NEIGHBORS OF '<node>' LABEL '<label>' DIRECTION <dir>` and return
/// the neighbor node ids from the single-row `[{"label":..,"node":..}, ...]`
/// JSON payload in the first column.
async fn neighbors(
    client: &tokio_postgres::Client,
    node: &str,
    label: &str,
    dir: &str,
) -> Vec<String> {
    let sql = format!("GRAPH NEIGHBORS OF '{node}' LABEL '{label}' DIRECTION {dir}");
    let msgs = client.simple_query(&sql).await.expect("GRAPH NEIGHBORS");
    let mut out = Vec::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let raw = row.get(0).unwrap_or("");
            let parsed: Vec<serde_json::Value> = serde_json::from_str(raw).unwrap_or_default();
            for entry in parsed {
                if let Some(n) = entry.get("node").and_then(|v| v.as_str()) {
                    out.push(n.to_string());
                }
            }
        }
    }
    out
}

/// A cross-shard edge inserted inside a transaction stages into BOTH endpoint
/// overlays (RYOW from either direction) and ROLLBACK tears both down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dual_home_edge_stages_both_overlays_and_rollback_tears_down() {
    // 4 Data-Plane cores so distinct vShards land on distinct cores — a genuine
    // cross-core (dual-home) edge.
    let node = TestClusterNode::spawn_single_node_calvin(4)
        .await
        .expect("spawn standalone single-node-calvin server");

    // The lone sequencer voter self-elects; wait for it so `calvin_available` is
    // genuinely operational (a cross-shard edge is dual-home, not forced
    // single-home).
    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;
    assert!(
        node.shared.cluster_transport.is_some() && node.shared.sequencer_inbox.get().is_some(),
        "single-node calvin must wire calvin_available (cluster_transport + sequencer_inbox)"
    );

    node.client
        .simple_query("CREATE COLLECTION sncgtx_graph")
        .await
        .expect("CREATE COLLECTION sncgtx_graph");
    wait_for(
        "collection visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 1,
    )
    .await;

    let (src, dst) = distinct_vshard_node_keys();
    let label = "l";

    node.client.simple_query("BEGIN").await.expect("BEGIN");

    // Cross-shard edge insert INSIDE the transaction. Pre-change this returned
    // `CrossShardInExplicitTransaction`; now it stages into both endpoint
    // overlays.
    node.client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'sncgtx_graph' FROM '{src}' TO '{dst}' TYPE '{label}'"
        ))
        .await
        .expect(
            "cross-shard edge insert inside a transaction must STAGE (dual-home), \
             not reject with CrossShardInExplicitTransaction",
        );

    // RYOW from the SRC endpoint: forward overlay on `vsrc` holds the edge.
    let out_src = neighbors(&node.client, &src, label, "out").await;
    assert!(
        out_src.contains(&dst),
        "in-tx forward NEIGHBORS from src '{src}' must observe the staged edge to \
         '{dst}' (vsrc overlay), got: {out_src:?}"
    );

    // RYOW from the DST endpoint: reverse overlay on `vdst` ALSO holds the edge.
    // This is the dual-home proof — a single-home stage would leave `vdst` empty.
    let in_dst = neighbors(&node.client, &dst, label, "in").await;
    assert!(
        in_dst.contains(&src),
        "in-tx reverse NEIGHBORS from dst '{dst}' must observe the staged edge from \
         '{src}' (vdst overlay) — proves BOTH overlays hold it, got: {in_dst:?}"
    );

    node.client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK");

    // Post-rollback: the edge is gone from BOTH endpoints (DropTxnOverlay fanned
    // to both touched vShards).
    let out_src_after = neighbors(&node.client, &src, label, "out").await;
    assert!(
        !out_src_after.contains(&dst),
        "after ROLLBACK the staged edge must be gone from src '{src}' (vsrc \
         overlay dropped), got: {out_src_after:?}"
    );
    let in_dst_after = neighbors(&node.client, &dst, label, "in").await;
    assert!(
        !in_dst_after.contains(&src),
        "after ROLLBACK the staged edge must be gone from dst '{dst}' (vdst \
         overlay dropped), got: {in_dst_after:?}"
    );

    node.shutdown().await;
}
