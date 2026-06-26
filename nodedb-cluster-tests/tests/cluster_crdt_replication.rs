// SPDX-License-Identifier: BUSL-1.1
//! A `crdt_apply` through any gateway must replicate via Raft, not land local-only.
//!
//! ## What this guards
//!
//! The SQL `crdt_apply('coll','doc','<hex>')` entry point used to dispatch the
//! built `CrdtOp::Apply` plan straight to the local SPSC bridge — never proposing
//! it through the data group's Raft log. Under replication factor > 1 the delta
//! then lands ONLY on the receiving node: every follower is missing it, and if the
//! receiving node was the leader, the delta is lost entirely on failover when a
//! former follower takes over.
//!
//! The `ReplicatedWrite::CrdtApply` variant and the follower-apply machinery
//! already existed (the Lite-sync path uses them); the gateway entry points just
//! skipped the propose step. This is the same class of silent write-loss as the
//! already-fixed point-write path.
//!
//! ## Shape
//!
//!  1. Spawn a 3-node cluster (RF=3), create a CRDT collection, apply ONE delta
//!     through node 0's pgwire `crdt_apply`, converge.
//!  2. Resolve the collection's data group, kill that group's LEADER.
//!  3. After re-election, read `crdt_state` from a SURVIVING node and assert the
//!     CRDT field is present. Without the fix the new leader (a former follower)
//!     never received the delta and this read comes back empty.

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb_types::DatabaseId;

const COLL: &str = "crdt_repl";
const DOC: &str = "doc1";

fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// Build a real Loro snapshot delta for collection=`COLL`, row=`DOC`,
/// field `name=alice`, exactly as the single-node CRDT snapshot test does:
/// collection = root map keyed by name, row = a Map container under it, the
/// row's fields inserted on that map. Returns the hex encoding `crdt_apply`
/// hex-decodes before merging into the tenant doc.
fn build_delta_hex() -> String {
    let doc = loro::LoroDoc::new();
    let coll = doc.get_map(COLL);
    let row = coll
        .insert_container(DOC, loro::LoroMap::new())
        .expect("row container");
    row.insert("name", "alice").expect("field");
    doc.commit();
    let delta = doc
        .export(loro::ExportMode::Snapshot)
        .expect("export loro snapshot");
    hex::encode(delta)
}

/// Read `crdt_state('coll','doc')` on `client`, returning the (possibly empty)
/// text payload. Retries transient catch-up errors until `timeout`.
async fn read_crdt_state(
    client: &tokio_postgres::Client,
    timeout: Duration,
) -> Result<String, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT crdt_state('{COLL}', '{DOC}')"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                        return Ok(r.get(0).unwrap_or("").to_string());
                    }
                }
                return Ok(String::new());
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
async fn crdt_apply_replicates_and_survives_leader_loss() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // CRDT collection: default-engine document collection (the CRDT doc is one
    // Loro doc per tenant; the collection is the root map). Matches the create
    // syntax used by the single-node CRDT snapshot round-trip test.
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION");

    // Apply ONE CRDT delta through node 0's pgwire gateway. With the fix this
    // proposes through the data group's Raft log; without it the delta would
    // only ever exist on node 0.
    let delta_hex = build_delta_hex();
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "SELECT crdt_apply('{COLL}', '{DOC}', '{delta_hex}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("crdt_apply on node 0: {}", pg_detail(&e)));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Sanity: the delta is readable on node 0 (the write landed somewhere).
    let landed = read_crdt_state(&cluster.nodes[0].client, Duration::from_secs(10))
        .await
        .expect("crdt_state on node 0");
    assert!(
        !landed.is_empty(),
        "crdt_apply produced no readable state on the receiving node"
    );

    // Resolve the collection's data group and its leader from node 0's shared
    // routing view (same idiom as multi_replica_data_groups).
    let vshard = nodedb_cluster::routing::vshard_for_collection(DatabaseId::DEFAULT, COLL);
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
            .expect("collection vshard mapped to a group");
        let leader = routing.group_info(gid).map(|i| i.leader).unwrap_or(0);
        (gid, leader)
    };
    assert!(
        group_id != 0,
        "collection must map to a data group, not metadata"
    );
    assert!(group_leader != 0, "data group {group_id} has no leader");

    // Kill the data group's LEADER. Reading the delta from a survivor afterward
    // is the confound-free proof of replication: had the delta lived only on the
    // receiving node (the pre-fix local-only dispatch), killing the leader that
    // held it would lose it, and the read on the survivor would be empty.
    let mut nodes = cluster.nodes;
    let leader_idx = nodes
        .iter()
        .position(|n| n.node_id == group_leader)
        .expect("leader node present");
    nodes.remove(leader_idx).shutdown().await;

    // Survivors re-elect a new leader; give the group a moment to settle.
    tokio::time::sleep(Duration::from_secs(3)).await;

    for node in &nodes {
        let state = read_crdt_state(&node.client, Duration::from_secs(20))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "survivor node {} could not read crdt_state after leader death: {e}",
                    node.node_id
                )
            });
        // `crdt_state` returns the exported Loro snapshot bytes (binary), which
        // exist only if the row is present in this node's tenant doc. A non-empty
        // result therefore proves the delta replicated to this survivor.
        assert!(
            !state.is_empty(),
            "BUG: survivor node {} read EMPTY crdt_state after the data-group leader \
             was killed — the crdt_apply was dispatched LOCAL-ONLY and never proposed \
             through Raft, so the delta was lost on failover (silent write-loss under RF>1)",
            node.node_id
        );
    }

    for node in nodes {
        node.shutdown().await;
    }
}
