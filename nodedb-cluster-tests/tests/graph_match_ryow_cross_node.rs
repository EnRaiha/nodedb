// SPDX-License-Identifier: BUSL-1.1

//! Cross-CORE read-your-own-writes for a variable-length graph MATCH inside an
//! explicit transaction on a single-node-calvin server.
//!
//! A graph edge STAGED inside `BEGIN..ROLLBACK` lives only in the per-vShard
//! transaction overlay until commit. A variable-length pattern
//! `MATCH (a)-[:K*1..k]->(b)` scatters across Data-Plane cores and continues
//! across shard boundaries; for RYOW to hold, a staged edge whose endpoints home
//! to DISTINCT vShards (distinct cores) must be observed by the SAME
//! transaction's expansion AND the traversal must continue THROUGH it onto a
//! durable node homed on yet another shard.
//!
//! The graph is a durable chain plus one durable onward hop that is unreachable
//! from the anchor until the staged edge bridges the gap:
//!
//! ```text
//!   durable:  c0 -> c1 -> c2            c3 -> c4   (c3 unreachable from c0)
//!   staged :                 c2 -> c3              (the only bridge)
//! ```
//!
//! Consecutive chain nodes home to distinct vShards, so the staged `c2 -> c3`
//! edge is a genuine dual-home cross-core edge and the expansion must cross a
//! core boundary to continue past it. The test proves: (1) before `BEGIN`, the
//! anchored expansion reaches only `{c1, c2}`; (2) inside the txn after staging,
//! it reaches `{c1, c2, c3, c4}` — the staged edge is traversed AND the durable
//! `c3 -> c4` hop beyond it is reached THROUGH the overlay; (3) after `ROLLBACK`
//! the overlay is dropped and the expansion reaches only `{c1, c2}` again.

mod common;

use std::collections::HashSet;
use std::time::Duration;

use nodedb_types::id::VShardId;

use common::cluster_harness::{TestClusterNode, wait_for, wait_for_async};

/// Length of the anchored chain `c0 -> c1 -> c2 -> c3 -> c4` (5 nodes).
const CHAIN_LEN: usize = 5;

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

/// Build `len` distinct graph-node keys whose CONSECUTIVE entries home to
/// DISTINCT vShards. `VShardId::from_key` is a pure function of the key bytes
/// and is exactly how edges home each endpoint, so every hop of the resulting
/// chain crosses a shard boundary — and with enough cores, a core boundary.
fn spread_chain(prefix: &str, len: usize) -> Vec<String> {
    let mut chain: Vec<String> = Vec::with_capacity(len);
    let mut prev_vshard: Option<u32> = None;
    let mut i = 0u32;
    while chain.len() < len {
        let key = format!("{prefix}_{i}");
        let vshard = VShardId::from_key(key.as_bytes()).as_u32();
        if prev_vshard != Some(vshard) {
            prev_vshard = Some(vshard);
            chain.push(key);
        }
        i += 1;
        assert!(
            i < 100_000,
            "could not build a spread chain of length {len} for prefix '{prefix}'"
        );
    }
    chain
}

/// All `b` bindings from a `MATCH (a)-[:K*1..k]->(b) ... RETURN b` result.
async fn varlen_b_bindings(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let msgs = client
        .simple_query(sql)
        .await
        .expect("simple_query varlen MATCH");
    let mut out = Vec::new();
    for m in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            out.push(r.get("b").unwrap_or("").to_string());
        }
    }
    out
}

/// A cross-core graph edge staged inside a transaction is read-your-own-writes
/// visible to a same-transaction variable-length `MATCH (a)-[:K*1..k]->(b)`:
/// the expansion traverses the staged edge across a core boundary and continues
/// onto a durable node reachable only through it; `ROLLBACK` drops the overlay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_varlen_edge_is_read_your_own_writes_across_cores() {
    // 4 Data-Plane cores so distinct vShards land on distinct cores — the staged
    // edge is then a genuine cross-core (dual-home) edge.
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
        .simple_query("CREATE COLLECTION rgraph")
        .await
        .expect("CREATE COLLECTION rgraph");
    wait_for(
        "collection visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 1,
    )
    .await;

    let chain = spread_chain("ryow", CHAIN_LEN);

    // Durable prefix c0 -> c1 -> c2 (reachable from the anchor c0 immediately).
    for i in 0..2 {
        node.client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'rgraph' FROM '{}' TO '{}' TYPE 'K'",
                chain[i],
                chain[i + 1]
            ))
            .await
            .unwrap_or_else(|e| panic!("durable insert {} -> {}: {e}", chain[i], chain[i + 1]));
    }
    // Durable onward hop c3 -> c4 — c3 (and thus c4) is UNREACHABLE from the
    // anchor c0 until the staged c2 -> c3 edge bridges the gap.
    node.client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'rgraph' FROM '{}' TO '{}' TYPE 'K'",
            chain[3], chain[4]
        ))
        .await
        .unwrap_or_else(|e| panic!("durable onward insert {} -> {}: {e}", chain[3], chain[4]));

    let anchor = &chain[0];
    // Depth 5 covers all four hops of the fully-bridged chain while staying well
    // within the default `max_graph_depth` tenant quota (10).
    let match_sql = format!("MATCH (a)-[:K*1..5]->(b) WHERE a = '{anchor}' RETURN b");

    let durable_only: HashSet<String> = [chain[1].clone(), chain[2].clone()].into_iter().collect();
    let full_set: HashSet<String> = (1..CHAIN_LEN).map(|i| chain[i].clone()).collect();

    // (1) Before BEGIN the staged bridge does not exist: only the durable prefix
    // {c1, c2} is reachable; c3/c4 are absent. Direct assertion (no waiting for
    // absence) once the durable prefix has converged.
    wait_for_async(
        "durable prefix reachable from anchor before staging",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            let set: HashSet<String> = varlen_b_bindings(&node.client, &match_sql)
                .await
                .into_iter()
                .collect();
            set == durable_only
        },
    )
    .await;
    let before: HashSet<String> = varlen_b_bindings(&node.client, &match_sql)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        before, durable_only,
        "before staging, anchored varlen MATCH must reach only the durable prefix \
         {{{}, {}}}; got {before:?}",
        chain[1], chain[2]
    );

    node.client.simple_query("BEGIN").await.expect("BEGIN");

    // Stage the ONLY bridge from the durable prefix onto c3 (and, via the durable
    // c3 -> c4 hop, c4). c2 and c3 home to distinct vShards, so this is a
    // dual-home cross-core edge staged into both endpoint overlays.
    node.client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'rgraph' FROM '{}' TO '{}' TYPE 'K'",
            chain[2], chain[3]
        ))
        .await
        .expect("staged bridge edge c2 -> c3 must be accepted inside the transaction");

    // (2) Inside the txn the staged edge is RYOW-visible AND the expansion
    // continues across the core boundary through it onto the durable c4 — so the
    // full set {c1, c2, c3, c4} is reachable.
    wait_for_async(
        "in-txn anchored varlen MATCH drains to the full bridged set",
        Duration::from_secs(15),
        Duration::from_millis(100),
        || async {
            let set: HashSet<String> = varlen_b_bindings(&node.client, &match_sql)
                .await
                .into_iter()
                .collect();
            set == full_set
        },
    )
    .await;
    let in_txn: HashSet<String> = varlen_b_bindings(&node.client, &match_sql)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        in_txn, full_set,
        "inside the txn, the anchored varlen MATCH must traverse the staged bridge \
         {} -> {} AND continue onto the durable {} reachable only through it; \
         expected {full_set:?}, got {in_txn:?}",
        chain[2], chain[3], chain[4]
    );

    node.client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK");

    // (3) After ROLLBACK the overlay is dropped from both touched vShards: the
    // bridge is gone and the expansion reaches only the durable prefix again.
    let after: HashSet<String> = varlen_b_bindings(&node.client, &match_sql)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        after, durable_only,
        "after ROLLBACK the staged bridge must be gone; anchored varlen MATCH must \
         reach only {{{}, {}}} again; got {after:?}",
        chain[1], chain[2]
    );

    node.shutdown().await;
}
