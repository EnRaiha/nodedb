// SPDX-License-Identifier: BUSL-1.1

//! A learner caught up by InstallSnapshot resolves every edge endpoint.
//!
//! A key's surrogate is minted once, at its collection home `G_c`, and every
//! `G_c` replica binds it. In a collection that holds edges the bind also
//! lives on the group of `from_key(endpoint)`: a live edge write carries each
//! endpoint's surrogate, and every replica of the endpoint's group binds it on
//! apply. The snapshot of an endpoint's group must carry that bind, or the
//! caught-up node holds edges whose endpoints it cannot resolve.
//!
//! The test fails a snapshot that ships binds by collection home only. On 3 nodes with RF 2 and [`GROUPS`] data groups it picks a collection
//! whose group `G_c` the learner's 4-node placement leaves out, and an
//! endpoint group `G_e` that placement names the learner in. It writes edges
//! whose endpoints all home on `G_e`, then adds the learner. The learner hosts
//! no `G_c` replica.
//!
//! An edge write runs as a Calvin transaction: the sequencer log orders it,
//! and each endpoint home applies it from its scheduler, outside its data
//! group's log. Before the learner joins, the test therefore compacts two
//! logs past the edge writes: `G_e`'s, with filler documents of a collection
//! homed on `G_e`, and the sequencer's, with filler edges. The learner then
//! can learn the edges and binds only from a `G_e` snapshot, which a
//! collection-home-only snapshot ships without any of the binds. The learner's local `G_e` snapshot
//! index at or above `G_e`'s index after the edge writes proves it caught up
//! by snapshot install.

use std::time::Duration;

use nodedb::types::{DatabaseId, TenantId};
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_types::CollectionKey;

use crate::common::cluster_harness::shared_steps::{
    db_detail, group_members, group_of_key, group_status,
};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};

const COMPACTION_THRESHOLD: u64 = 4;
const RF: usize = 2;
/// Data groups of the cluster.
const GROUPS: u64 = 4;
/// Node ids after the learner joins.
const NODES_WITH_LEARNER: [u64; 4] = [1, 2, 3, 4];
/// Prefix of the candidate collection names. The test picks the first one
/// whose group the learner never joins.
const COLLECTION_PREFIX: &str = "snap_edge_ep_";
/// Prefix of the candidate filler collection names. The test picks the first
/// one homed on the endpoint group.
const FILL_PREFIX: &str = "snap_edge_fill_";
const PAIRS: usize = 6;
const MAX_FILLER: usize = 400;
const TENANT: u64 = 1;

/// The surrogate `node` binds `endpoint` to in its own catalog.
fn local_bind(node: &TestClusterNode, collection: &str, endpoint: &str) -> Option<u32> {
    node.shared
        .surrogate_assigner
        .lookup_bound(
            CollectionKey::from_bare(DatabaseId::DEFAULT, collection),
            TenantId::new(TENANT),
            endpoint.as_bytes(),
        )
        .ok()
        .flatten()
        .map(|s| s.as_u32())
}

/// `count` node keys homed on `group_id`, named `{prefix}{i}`.
fn keys_on_group(node: &TestClusterNode, group_id: u64, prefix: &str, count: usize) -> Vec<String> {
    (0..1_000_000)
        .map(|i| format!("{prefix}{i}"))
        .filter(|k| group_of_key(node, k) == group_id)
        .take(count)
        .collect()
}

async fn insert_edge(node: &TestClusterNode, collection: &str, src: &str, dst: &str) {
    node.client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN '{collection}' FROM '{src}' TO '{dst}' TYPE 'l'"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {}", db_detail(&e)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn learner_caught_up_by_snapshot_resolves_every_edge_endpoint() {
    let mut cluster = TestCluster::spawn_three_with_groups_compaction_threshold_and_rf(
        GROUPS,
        COMPACTION_THRESHOLD,
        RF,
    )
    .await
    .expect("3-node cluster with 4 groups, low compaction threshold and rf=2");

    // The 4-node placement the learner's join leads to, from the same rules
    // the cluster runs.
    let view = &cluster.nodes[0];
    let data_groups: Vec<u64> = (1..=GROUPS).collect();
    let placement_with_learner = nodedb_cluster::rebalancer::placement::compute_placement(
        &NODES_WITH_LEARNER,
        &data_groups,
        RF as u32,
    );
    let learner_id = NODES_WITH_LEARNER[3];
    let learner_joins = |g: u64| {
        placement_with_learner
            .get(&g)
            .is_some_and(|p| p.contains(&learner_id))
    };
    // G_c must not place the learner: a member of G_c binds every endpoint
    // of the collection through G_c, and the test cannot fail on a
    // collection-home-only snapshot. G_e must place it, so it catches up on G_e by snapshot.
    let (collection, collection_group) = (0..10_000)
        .map(|i| format!("{COLLECTION_PREFIX}{i}"))
        .find_map(|name| {
            let group = view.group_id_for_collection(&name)?;
            (!learner_joins(group)).then_some((name, group))
        })
        .expect("a collection name whose group the learner never joins");
    let endpoint_group = data_groups
        .iter()
        .copied()
        .find(|g| *g != collection_group && learner_joins(*g))
        .expect("placement offers an endpoint group the learner joins");
    let endpoint_members = group_members(view, endpoint_group);
    // A document collection homed on G_e. Its single-shard autocommit
    // inserts go through G_e's Raft log and grow it.
    let fill_collection = (0..10_000)
        .map(|i| format!("{FILL_PREFIX}{i}"))
        .find(|name| view.group_id_for_collection(name) == Some(endpoint_group))
        .expect("a filler collection name homed on the endpoint group");

    for name in [&collection, &fill_collection] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {name}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {name}: {e}"));
    }
    wait_for(
        "all nodes see both collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 2)
        },
    )
    .await;

    // Edges whose endpoints all home on G_e, written from a G_e member.
    let writer = cluster
        .nodes
        .iter()
        .find(|n| endpoint_members.contains(&n.node_id))
        .expect("a member of the endpoint group");
    let endpoints = keys_on_group(view, endpoint_group, "ep_", PAIRS * 2);
    assert_eq!(
        endpoints.len(),
        PAIRS * 2,
        "enough keys home on the endpoint group"
    );
    for pair in endpoints.chunks(2) {
        insert_edge(writer, &collection, &pair[0], &pair[1]).await;
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let edges_applied = cluster
        .nodes
        .iter()
        .filter_map(|n| group_status(n, endpoint_group))
        .map(|s| s.last_applied)
        .max()
        .expect("the endpoint group is hosted");
    // The collection home mints every key's surrogate, so each G_c replica
    // binds every endpoint.
    let collection_members = group_members(view, collection_group);
    for ep in &endpoints {
        for node in cluster
            .nodes
            .iter()
            .filter(|n| collection_members.contains(&n.node_id))
        {
            assert!(
                local_bind(node, &collection, ep).is_some(),
                "G_c replica {} binds {ep}: the collection home mints it",
                node.node_id
            );
        }
    }

    // An edge write runs as a Calvin transaction: it lands in the sequencer
    // log, and each endpoint home applies it from its scheduler, not from
    // its data group's log. A node that later joins G_e can learn the edges
    // two ways: from a G_e snapshot, or by replaying the sequencer log for
    // G_e's vShards. The test must leave only the snapshot, so it compacts
    // both logs past the edge writes:
    // - G_e's log, with filler documents homed on G_e (single-shard
    //   autocommit writes, which go through the data group);
    // - the sequencer log, with filler edges (Calvin transactions).
    let sequencer_after_edges = cluster
        .nodes
        .iter()
        .filter_map(|n| group_status(n, SEQUENCER_GROUP_ID))
        .map(|s| s.last_applied)
        .max()
        .expect("the sequencer group is hosted");
    let filler = keys_on_group(view, endpoint_group, "fill_", MAX_FILLER * 2);
    // Every voter of `group_id` compacted past `index`. A node that left the
    // group can still host its replica until it unmounts it. That replica
    // gets no entries and never compacts, and it never sends the learner a
    // snapshot, so it is not part of the premise.
    let voters_compacted = |cluster: &TestCluster, group_id: u64, index: u64| {
        let voters: Vec<u64> = match group_members(&cluster.nodes[0], group_id) {
            voters if !voters.is_empty() => voters,
            // A group the routing table does not list: every host votes.
            _ => cluster
                .nodes
                .iter()
                .filter(|n| group_status(n, group_id).is_some())
                .map(|n| n.node_id)
                .collect(),
        };
        !voters.is_empty()
            && voters.iter().all(|voter| {
                cluster
                    .nodes
                    .iter()
                    .find(|n| n.node_id == *voter)
                    .and_then(|n| group_status(n, group_id))
                    .is_some_and(|s| s.snapshot_index >= index)
            })
    };
    let compacted = |cluster: &TestCluster| {
        voters_compacted(cluster, endpoint_group, edges_applied)
            && voters_compacted(cluster, SEQUENCER_GROUP_ID, sequencer_after_edges)
    };
    // Every node's view of a group, for the failure message.
    let replicas = |cluster: &TestCluster, group_id: u64| -> Vec<(u64, u64, u64)> {
        cluster
            .nodes
            .iter()
            .filter_map(|n| {
                group_status(n, group_id).map(|s| (n.node_id, s.snapshot_index, s.last_applied))
            })
            .collect()
    };
    let mut written = 0;
    for (i, pair) in filler.chunks(2).enumerate() {
        if compacted(&cluster) {
            break;
        }
        writer
            .client
            .simple_query(&format!(
                "INSERT INTO {fill_collection} {{ id: 'f{i}', n: {i} }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("filler document f{i}: {}", db_detail(&e)));
        insert_edge(writer, &collection, &pair[0], &pair[1]).await;
        written += 1;
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    assert!(
        compacted(&cluster),
        "G_e voters compacted past {edges_applied} and sequencer voters past \
         {sequencer_after_edges} after {written} filler rounds; (node, snapshot_index, \
         last_applied) of G_e: {:?}, of the sequencer: {:?}",
        replicas(&cluster, endpoint_group),
        replicas(&cluster, SEQUENCER_GROUP_ID)
    );
    let expected: Vec<(String, u32)> = endpoints
        .iter()
        .map(|ep| {
            let s = cluster
                .nodes
                .iter()
                .find_map(|n| local_bind(n, &collection, ep))
                .unwrap_or_else(|| panic!("a G_e replica binds {ep}"));
            (ep.clone(), s)
        })
        .collect();

    let learner_id = {
        let joined = cluster
            .add_learner_node()
            .await
            .expect("add learner node")
            .node_id;
        assert_eq!(joined, learner_id, "the learner joins as the placed node");
        joined
    };
    let learner = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == learner_id)
        .expect("learner present");

    wait_for_async(
        "the learner installs a G_e snapshot covering the edge writes",
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async move {
            group_status(learner, endpoint_group).is_some_and(|s| s.snapshot_index >= edges_applied)
        },
    )
    .await;
    assert!(
        group_status(learner, collection_group).is_none(),
        "the learner hosts G_c {collection_group}; its binds could come from G_c and the old \
         rule would pass"
    );

    for (ep, s) in &expected {
        assert_eq!(
            local_bind(learner, &collection, ep),
            Some(*s),
            "learner {learner_id} resolves endpoint {ep} from the G_e snapshot"
        );
    }

    cluster.shutdown().await;
}
