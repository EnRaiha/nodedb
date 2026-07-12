// SPDX-License-Identifier: BUSL-1.1

//! Multi-shard Calvin commits survive a WAL-only restart.
//!
//! Before the `TransactionRedo` WAL record, a committed multi-shard (Calvin)
//! transaction journalled only a non-replayable `CalvinApplied` marker. On a
//! WAL-only restart (no snapshot/checkpoint) that marker carried nothing to
//! replay, so a Calvin-committed write did not come back at all — neither its
//! base row nor any in-memory-only secondary index built on top of it. The
//! production change makes a committed Calvin transaction journal a
//! replayable `TransactionRedo` record instead, so its writes (base KV row,
//! vector-indexed document row) rebuild on restart exactly like a
//! single-shard transaction's already do (see `vector_index_txn_restart.rs`).
//!
//! This test proves the multi-shard case end to end:
//!
//! 1. Two collections — a KV collection and a vector-indexed document
//!    collection — are created on DIFFERENT vShards (`two_distinct_vshard_
//!    collections`, same technique as `calvin_cluster_pgwire_e2e.rs`).
//! 2. `BEGIN; INSERT INTO <kv>; INSERT INTO <vecdocs>; COMMIT` is sent as ONE
//!    `simple_query` call. tokio-postgres ships this as a single wire
//!    message; the server buffers the two INSERTs during the transaction and,
//!    on COMMIT, `classify_dispatch` sees writes on two vShards → MultiShard
//!    → submits the whole batch atomically to the Calvin sequencer. This is
//!    the exact mechanism `calvin_cluster_pgwire_e2e.rs` uses to force
//!    Calvin routing for a plain (non-graph) multi-collection transaction —
//!    sending BEGIN/INSERT/INSERT/COMMIT as three SEPARATE `simple_query`
//!    calls instead does NOT work here: the per-statement dispatch path
//!    rejects a cross-shard write inside an explicit transaction block with
//!    `CrossShardInExplicitTransaction` (only `GRAPH INSERT EDGE`'s dual-home
//!    staging is exempt from that rejection).
//! 3. The node shuts down via `graceful_shutdown_wal_only` (flushes the WAL,
//!    awaits every background task, triggers NO checkpoint) and a second node
//!    reopens the SAME data directory — a pure WAL-only restart.
//! 4. Post-restart: the KV row's value survives (base-row durability), AND a
//!    vector search near the inserted embedding returns the document's id —
//!    THE key signal, since it proves the in-memory HNSW graph was rebuilt
//!    from the replayed `TransactionRedo` record's engine-native sub-records,
//!    not merely that the underlying redb row survived.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};
use tokio_postgres::SimpleQueryMessage;

use common::cluster_harness::{TestClusterNode, wait_for};

/// Observed sequencer-group leader id from a node's local Raft status, or `0`
/// if no leader is known yet. Same shape as the sibling `single_node_calvin_*`
/// suite.
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

/// Count of transactions the single-node sequencer has admitted to an epoch,
/// or `0` if the sequencer metrics handle is not installed yet.
fn admitted_total(node: &TestClusterNode) -> u64 {
    node.shared
        .sequencer_metrics
        .get()
        .map(|m| m.admitted_total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// A `(kv_name, vec_name)` pair of collection names whose vShard ids differ,
/// so a transaction writing to both is genuinely multi-shard. Deterministic:
/// `VShardId::from_collection_in_database` is a pure function of the database
/// id + collection name bytes. Same technique as
/// `calvin_cluster_pgwire_e2e.rs::two_distinct_vshard_collections`.
fn distinct_vshard_collections() -> (String, String) {
    let kv_name = "cmr_kv".to_string();
    let vkv = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &kv_name).as_u32();
    for i in 0u32..512 {
        let vec_name = format!("cmr_vecdocs_{i}");
        if VShardId::from_collection_in_database(DatabaseId::DEFAULT, &vec_name).as_u32() != vkv {
            return (kv_name, vec_name);
        }
    }
    panic!(
        "could not find a vector-doc collection name on a distinct vShard from \
         the KV collection in 512 tries"
    );
}

/// The first `v` column value from `SELECT v FROM <kv> WHERE id = '<id>'`, if
/// the row exists.
async fn kv_value(client: &tokio_postgres::Client, kv: &str, id: &str) -> Option<String> {
    let msgs = client
        .simple_query(&format!("SELECT v FROM {kv} WHERE id = '{id}'"))
        .await
        .expect("SELECT v FROM kv");
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
        _ => None,
    })
}

/// Nearest-neighbour `id` on `<vecdocs>`'s vector index to `axis`, or `None`
/// when the index has no reachable rows.
async fn nearest_doc(
    client: &tokio_postgres::Client,
    vecdocs: &str,
    axis: [f32; 3],
) -> Option<String> {
    let msgs = client
        .simple_query(&format!(
            "SELECT id FROM {vecdocs} ORDER BY vector_distance(embedding, ARRAY[{},{},{}]) LIMIT 1",
            axis[0], axis[1], axis[2]
        ))
        .await
        .expect("SELECT id ... ORDER BY vector_distance");
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
        _ => None,
    })
}

/// A committed multi-shard Calvin transaction writing a KV base row AND a
/// vector-indexed document row survives a WAL-only restart — the base row via
/// redb, the vector row's presence in the HNSW via the replayed
/// `TransactionRedo` record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_multi_shard_kv_and_vector_write_survives_wal_only_restart() {
    // The node's own data directory. Kept alive across BOTH spawns so the
    // second `spawn_single_node_calvin_on_path` call reopens the exact same
    // WAL / redb stores the first node wrote.
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_dir_path = data_dir.path().to_path_buf();

    // 4 Data-Plane cores so distinct vShards land on distinct cores — a
    // genuine cross-core, cross-shard commit.
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir_path.clone())
        .await
        .expect("spawn standalone single-node-calvin server on path");

    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    let (kv, vecdocs) = distinct_vshard_collections();

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {kv} (id TEXT PRIMARY KEY, v INT) WITH (engine='kv')"
        ))
        .await
        .expect("CREATE COLLECTION kv");
    node.client
        .simple_query(&format!("CREATE COLLECTION {vecdocs} TYPE document"))
        .await
        .expect("CREATE COLLECTION vecdocs");
    node.client
        .simple_query(&format!(
            "CREATE VECTOR INDEX idx_{vecdocs} ON {vecdocs} (embedding) METRIC cosine DIM 3"
        ))
        .await
        .expect("CREATE VECTOR INDEX");
    wait_for(
        "both collections visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 2,
    )
    .await;

    // Strict cross-shard mode so COMMIT's multi-shard path routes through
    // Calvin (mirrors `calvin_cluster_pgwire_e2e.rs`).
    node.client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    let admitted_before = admitted_total(&node);

    // ONE `simple_query` call carrying the whole transaction. Sending BEGIN,
    // the two INSERTs, and COMMIT as separate `simple_query` calls instead
    // would hit the per-statement `CrossShardInExplicitTransaction` rejection
    // (see module doc) — the server must see all statements together so it
    // can buffer them and classify the COMMIT-time task set as MultiShard.
    let txn_sql = format!(
        "BEGIN; \
         INSERT INTO {kv} (id, v) VALUES ('k1', 42); \
         INSERT INTO {vecdocs} (id, body, embedding) VALUES ('d1', 'hello', ARRAY[0.1,0.2,0.3]); \
         COMMIT"
    );
    node.client
        .simple_query(&txn_sql)
        .await
        .expect("multi-shard Calvin transaction (KV + vector-indexed write) must commit");

    // Proof it traversed the sequencer (genuinely Calvin, not a fast path
    // that never touches the sequencer): the batch was admitted to an epoch.
    wait_for(
        "multi-shard transaction admitted to a Calvin epoch",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    // Pre-restart sanity: both writes are visible through the live node.
    assert_eq!(
        kv_value(&node.client, &kv, "k1").await.as_deref(),
        Some("42"),
        "PRE-RESTART: kv row 'k1' must read back v=42"
    );
    assert_eq!(
        nearest_doc(&node.client, &vecdocs, [0.1, 0.2, 0.3])
            .await
            .as_deref(),
        Some("d1"),
        "PRE-RESTART: vector search near the inserted embedding must return 'd1'"
    );

    // WAL-only restart: flush + await every background task (no checkpoint),
    // then reopen the SAME data directory.
    node.graceful_shutdown_wal_only().await;

    let node2 = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir_path.clone())
        .await
        .expect("reopen standalone single-node-calvin server on the same path");

    wait_for(
        "single-node sequencer leader re-elected after restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node2) == node2.node_id,
    )
    .await;
    wait_for(
        "both collections visible again after restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node2.cached_collection_count() >= 2,
    )
    .await;

    // (a) The KV base row survived (redb durability).
    assert_eq!(
        kv_value(&node2.client, &kv, "k1").await.as_deref(),
        Some("42"),
        "POST-RESTART: kv row 'k1' must still read back v=42"
    );

    // (b) THE CRUX: a vector search still returns the document — the HNSW
    // was rebuilt from the replayed `TransactionRedo` record. Before the
    // redo switch this would return `None`: the old non-replayable
    // `CalvinApplied` marker carried nothing to rebuild the index from.
    assert_eq!(
        nearest_doc(&node2.client, &vecdocs, [0.1, 0.2, 0.3])
            .await
            .as_deref(),
        Some("d1"),
        "POST-RESTART: vector search near the inserted embedding must return 'd1' — \
         the HNSW must be rebuilt from the multi-shard commit's TransactionRedo record"
    );

    node2.shutdown().await;
}
