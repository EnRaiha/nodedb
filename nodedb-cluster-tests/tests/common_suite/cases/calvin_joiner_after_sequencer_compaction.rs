// SPDX-License-Identifier: BUSL-1.1

//! A node that joins a data group after the sequencer compacted its log holds
//! every Calvin-written row and edge of the group.
//!
//! A Calvin transaction runs outside its data groups' logs: the sequencer
//! orders it, and each participant's scheduler installs its slice. Calvin
//! writes barely grow a data group's log, so the group need not compact, and
//! a new replica can catch up by log replay. Its scheduler catches up from
//! the first index the sequencer log still holds, so a transaction the
//! sequencer compacted away never reaches it.
//!
//! On 3 nodes with RF 2 the test picks a data group `G_e` that a fourth
//! node's placement names. It writes edges and cross-shard rows homed on
//! `G_e`, then compacts the sequencer log past them with filler edges homed
//! on other groups. The fourth node joins, and its replica of `G_e` must hold
//! every edge and row: the group's snapshot carries them with the Calvin cut
//! they were captured at.

use std::time::Duration;

use nodedb::control::security::catalog::calvin_base::CalvinBase;
use nodedb::types::{DatabaseId, TenantDataSnapshot, TenantId, VShardId};
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;

use crate::common::cluster_harness::shared_steps::{
    db_detail, group_members, group_of_key, group_status, key_collection,
};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

const COMPACTION_THRESHOLD: u64 = 4;
const RF: usize = 2;
const GROUPS: u64 = 4;
const NODES_WITH_JOINER: [u64; 4] = [1, 2, 3, 4];
const PAIRS: usize = 4;
const ROWS: usize = 4;
const MAX_FILLER: usize = 400;
const TENANT: u64 = 1;
/// SQLSTATE `serialization_failure`: the transaction aborted and a client
/// runs it again.
const SERIALIZATION_FAILURE: &str = "40001";
/// How long a write retries a `40001` before the test fails.
const RETRY_DEADLINE: Duration = Duration::from_secs(30);
/// The pause between two attempts of a write.
const RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// `count` node keys whose group satisfies `on`, named `{prefix}{i}`.
fn keys_where(
    node: &TestClusterNode,
    prefix: &str,
    count: usize,
    on: impl Fn(u64) -> bool,
) -> Vec<String> {
    (0..1_000_000)
        .map(|i| format!("{prefix}{i}"))
        .filter(|k| on(group_of_key(node, k)))
        .take(count)
        .collect()
}

/// Run `sql` on `node` as one client request. A `40001` failure aborted the
/// transaction and wrote nothing: a real client runs it again, and so does
/// this until [`RETRY_DEADLINE`]. Any other error, or a `40001` past the
/// deadline, panics with `what`, the SQLSTATE and the detail.
async fn run_retrying(node: &TestClusterNode, what: &str, sql: &str) {
    let deadline = tokio::time::Instant::now() + RETRY_DEADLINE;
    loop {
        let error = match node.client.simple_query(sql).await {
            Ok(_) => return,
            Err(error) => error,
        };
        let retryable = error
            .as_db_error()
            .is_some_and(|db| db.code().code() == SERIALIZATION_FAILURE);
        if !retryable || tokio::time::Instant::now() >= deadline {
            panic!("{what}: {}", db_detail(&error));
        }
        // A failed block can leave the session in an aborted transaction.
        // Outside one, ROLLBACK only warns, so its result does not matter.
        let _ = node.client.simple_query("ROLLBACK").await;
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
}

async fn insert_edge(node: &TestClusterNode, collection: &str, src: &str, dst: &str) {
    run_retrying(
        node,
        &format!("insert {src} -> {dst}"),
        &format!("GRAPH INSERT EDGE IN '{collection}' FROM '{src}' TO '{dst}' TYPE 'l'"),
    )
    .await;
}

/// Every edge key `node` stores locally, from its own tenant snapshot.
async fn local_edge_keys(node: &TestClusterNode) -> Vec<String> {
    let bytes = node.create_tenant_snapshot(TenantId::new(TENANT)).await;
    let snapshot: TenantDataSnapshot = zerompk::from_msgpack(&bytes).unwrap_or_default();
    snapshot.edges.into_iter().map(|(key, _)| key).collect()
}

/// Whether `edges` holds an edge from `src` to `dst`.
fn holds_edge(edges: &[String], src: &str, dst: &str) -> bool {
    let from = format!("\u{0}{src}\u{0}");
    let to = format!("\u{0}{dst}\u{0}");
    edges
        .iter()
        .any(|key| key.contains(&from) && key.contains(&to))
}

/// The number of documents of `collection` `node` stores locally.
async fn local_rows(node: &TestClusterNode, collection: &str) -> usize {
    let mut count = 0;
    for core in 0..node.num_cores() {
        count += node
            .document_keys_on_core(core, TenantId::new(TENANT))
            .await
            .iter()
            .filter(|key| key_collection(key) == Some(collection))
            .count();
    }
    count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_joiner_after_sequencer_compaction_holds_every_calvin_write() {
    let mut cluster = TestCluster::spawn_three_with_groups_compaction_threshold_and_rf(
        GROUPS,
        COMPACTION_THRESHOLD,
        RF,
    )
    .await
    .expect("3-node cluster with 4 groups, low compaction threshold and rf=2");

    let view = &cluster.nodes[0];
    let data_groups: Vec<u64> = (1..=GROUPS).collect();
    let placement = nodedb_cluster::rebalancer::placement::compute_placement(
        &NODES_WITH_JOINER,
        &data_groups,
        RF as u32,
    );
    let joiner_id = NODES_WITH_JOINER[3];
    let joins = |g: u64| placement.get(&g).is_some_and(|p| p.contains(&joiner_id));
    let endpoint_group = data_groups
        .iter()
        .copied()
        .find(|g| joins(*g))
        .expect("placement names the joiner in a group");
    let named = |prefix: &str, on: &dyn Fn(u64) -> bool| {
        (0..10_000)
            .map(|i| format!("{prefix}{i}"))
            .find(|name| view.group_id_for_collection(name).is_some_and(on))
            .unwrap_or_else(|| panic!("a {prefix} collection name on the wanted group"))
    };
    // The edge collection homes off G_e: its filler edges' binds then grow
    // no G_e log.
    let edges = named("cj_edges_", &|g| g != endpoint_group);
    let rows_here = named("cj_rows_e_", &|g| g == endpoint_group);
    let rows_there = named("cj_rows_o_", &|g| g != endpoint_group);

    for name in [&edges, &rows_here, &rows_there] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {name}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {name}: {e}"));
    }
    wait_for(
        "all nodes see the collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 3)
        },
    )
    .await;

    // Edges whose endpoints home on G_e, and cross-shard rows with one
    // slice on G_e. Each is a Calvin transaction.
    let writer = &cluster.nodes[0];
    let endpoints = keys_where(view, "cj_ep_", PAIRS * 2, |g| g == endpoint_group);
    for pair in endpoints.chunks(2) {
        insert_edge(writer, &edges, &pair[0], &pair[1]).await;
    }
    writer
        .client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");
    for i in 0..ROWS {
        run_retrying(
            writer,
            &format!("cross-shard COMMIT {i}"),
            &format!(
                "BEGIN; \
                 INSERT INTO {rows_here} {{ id: 'r{i}', v: 'here' }}; \
                 INSERT INTO {rows_there} {{ id: 'r{i}', v: 'there' }}; \
                 COMMIT"
            ),
        )
        .await;
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let sequencer_after_writes = cluster
        .nodes
        .iter()
        .filter_map(|n| group_status(n, SEQUENCER_GROUP_ID))
        .map(|s| s.last_applied)
        .max()
        .expect("the sequencer group is hosted");

    // Compact the sequencer log past the writes with Calvin edges homed on
    // other groups, so G_e's own log does not grow.
    let sequencer_compacted = |cluster: &TestCluster| {
        cluster
            .nodes
            .iter()
            .filter_map(|n| group_status(n, SEQUENCER_GROUP_ID))
            .all(|s| s.snapshot_index >= sequencer_after_writes)
    };
    let filler = keys_where(view, "cj_fill_", MAX_FILLER * 2, |g| g != endpoint_group);
    let mut written = 0;
    for pair in filler.chunks(2) {
        if sequencer_compacted(&cluster) {
            break;
        }
        insert_edge(writer, &edges, &pair[0], &pair[1]).await;
        written += 1;
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    assert!(
        sequencer_compacted(&cluster),
        "every sequencer replica compacted past {sequencer_after_writes} after {written} \
         filler edges"
    );

    let joined = cluster
        .add_learner_node()
        .await
        .expect("add the fourth node")
        .node_id;
    assert_eq!(
        joined, joiner_id,
        "the fourth node joins as the placed node"
    );
    let joiner = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == joiner_id)
        .expect("joiner present");

    // The joiner's scheduler of every G_e vShard started from a Calvin base
    // that reaches its sequencer log.
    let endpoint_vshards: Vec<u32> = endpoints
        .iter()
        .map(|k| VShardId::from_key(k.as_bytes()).as_u32())
        .chain(std::iter::once(
            nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &rows_here)
                .vshard()
                .as_u32(),
        ))
        .collect();
    wait_for(
        "the joiner keeps the Calvin state of every G_e vShard",
        Duration::from_secs(60),
        Duration::from_millis(100),
        || {
            group_members(&cluster.nodes[0], endpoint_group).contains(&joiner_id)
                && endpoint_vshards
                    .iter()
                    .all(|v| CalvinBase::is_kept(joiner.shared.calvin.bases.base(*v)))
        },
    )
    .await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let held = local_edge_keys(joiner).await;
    for pair in endpoints.chunks(2) {
        assert!(
            holds_edge(&held, &pair[0], &pair[1]),
            "joiner {joiner_id} holds the edge {} -> {} written before the sequencer \
             compacted",
            pair[0],
            pair[1]
        );
    }
    assert_eq!(
        local_rows(joiner, &rows_here).await,
        ROWS,
        "joiner {joiner_id} holds every cross-shard row written on G_e"
    );

    cluster.shutdown().await;
}
