// SPDX-License-Identifier: BUSL-1.1

//! Cross-node routed-surrogate-exchange test (F1b).
//!
//! Brings up a 3-node cluster, creates a distributed collection, then exercises
//! [`assign_surrogate_routed`]: a coordinator resolves the AUTHORITATIVE global
//! surrogate for a `(collection, pk)` endpoint key whose home vShard is owned by
//! a DIFFERENT node. The coordinator routes an `AssignSurrogateRequest` to the
//! home vShard's leader, which runs a LOCAL assign and returns the authoritative
//! value.
//!
//! The assertions are:
//! - the resolved surrogate is non-zero with no error (the leader has a wired
//!   catalog);
//! - a SECOND routed call for the same key returns the SAME surrogate
//!   (first-wins / idempotent), proving the home leader is the single
//!   authoritative source rather than each coordinator allocating its own.

use std::time::Duration;

use nodedb::control::server::surrogate_exchange::assign_surrogate_routed;
use nodedb_types::TraceId;
use nodedb_types::id::{DatabaseId, TenantId, VShardId};

use crate::common::cluster_harness::{TestCluster, wait_for};

/// The default database / tenant the harness writes under.
const DB: DatabaseId = DatabaseId::DEFAULT;
const TENANT: TenantId = TenantId::new(0);

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn assign_remote_surrogate_is_authoritative_and_idempotent() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION people \
             (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION people");

    wait_for(
        "all 3 nodes see the collection",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    // A key's home is its collection's home vShard (`VShardId::from_collection`).
    // Find a coordinator whose snapshot names a DIFFERENT node as that vShard's
    // leader, so the routed assign crosses to a remote leader (not a local short
    // circuit). The leader node's own snapshot sees the home as local, so each
    // node is tried as the coordinator and the first follower is picked. The
    // owner is resolved exactly like the production helper: `leader_for_vshard`
    // on the coordinator's snapshot.
    let collection = "people".to_string();
    let home = VShardId::from_collection(nodedb_types::CollectionKey::from_bare(DB, &collection));
    let pk = "person:0".to_string();
    let mut picked: Option<(u64, u64)> = None;
    for cand in &cluster.nodes {
        let cand_id = cand.shared.node_id;
        let Some(routing) = cand.shared.cluster_routing.as_ref() else {
            continue;
        };
        let guard = routing.read().unwrap_or_else(|p| p.into_inner());
        if let Ok(leader) = guard.leader_for_vshard(home.as_u32())
            && leader != 0
            && leader != cand_id
        {
            picked = Some((cand_id, leader));
            break;
        }
    }
    let (coordinator_id, owner) =
        picked.expect("some node's snapshot names a remote leader for the collection's home");

    let coordinator = cluster
        .nodes
        .iter()
        .find(|n| n.shared.node_id == coordinator_id)
        .expect("chosen coordinator node is in the cluster");

    assert_ne!(
        owner, coordinator_id,
        "test requires a remote owner; got owner == coordinator ({owner})"
    );

    // First routed assign: crosses to the remote leader, which allocates the
    // authoritative surrogate.
    let s1 = assign_surrogate_routed(
        &coordinator.shared,
        nodedb_types::CollectionKey::from_bare(DB, &collection),
        TENANT,
        pk.as_bytes(),
        TraceId([0u8; 16]),
    )
    .await
    .expect("first routed assign succeeds");
    assert_ne!(
        s1.as_u32(),
        0,
        "authoritative surrogate must be non-zero (leader has a wired catalog)"
    );

    // Second routed assign for the SAME key: must return the SAME surrogate
    // (first-wins / idempotent) — the home leader is the single authoritative
    // source, so a repeat resolves the already-bound value.
    let s2 = assign_surrogate_routed(
        &coordinator.shared,
        nodedb_types::CollectionKey::from_bare(DB, &collection),
        TENANT,
        pk.as_bytes(),
        TraceId([0u8; 16]),
    )
    .await
    .expect("second routed assign succeeds");
    assert_eq!(
        s1, s2,
        "repeat routed assign for the same key must return the same authoritative surrogate \
         (first-wins / idempotent); got {s1:?} then {s2:?}"
    );

    cluster.shutdown().await;
}
