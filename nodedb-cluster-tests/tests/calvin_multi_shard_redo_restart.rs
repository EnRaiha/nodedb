// SPDX-License-Identifier: BUSL-1.1

//! An interactive `BEGIN; <cross-shard KV + vector writes>; COMMIT` block is
//! rejected — Calvin cross-shard atomicity currently requires auto-commit
//! (single-statement) writes.
//!
//! This test originally proved that a committed multi-shard Calvin
//! transaction's writes (base KV row, vector-indexed document row) survive a
//! WAL-only restart via the replayable `TransactionRedo` WAL record (see
//! `vector_index_txn_restart.rs` for the single-shard analogue). That still
//! requires committing the multi-shard write, and the write path used to
//! reach it — buffering the writes inside an explicit `BEGIN ... COMMIT`
//! block — is itself now rejected before it ever commits:
//!
//! 1. Two collections — a KV collection and a vector-indexed document
//!    collection — are created on DIFFERENT vShards (`distinct_vshard_
//!    collections`, same technique as `calvin_cluster_pgwire_e2e.rs`).
//! 2. `BEGIN; INSERT INTO <kv>; INSERT INTO <vecdocs>; COMMIT` is sent as ONE
//!    `simple_query` call. tokio-postgres ships this as a single wire
//!    message; the server buffers the two INSERTs during the transaction and,
//!    on COMMIT, `classify_dispatch` sees writes on two vShards → MultiShard.
//!    Because the writes were buffered inside an explicit transaction block
//!    rather than sent auto-commit, the COMMIT is rejected with
//!    `Error::CrossShardInExplicitTransaction` instead of being submitted to
//!    the Calvin sequencer.
//! 3. Nothing was committed, so there is nothing to restart-and-verify: the
//!    WAL-only restart and post-restart survival checks are not reachable
//!    from this path and are not exercised here.
//!
//! Interactive cross-shard COMMIT inside an explicit block is not yet
//! supported; when that capability lands, this test should be flipped back
//! to assert the COMMIT succeeds, restore the WAL-only restart, and re-assert
//! that both the KV base row and the vector-indexed document row (and its
//! HNSW entry) survive the restart.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};

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

/// A multi-shard write (KV row + vector-indexed document row) buffered
/// inside an explicit `BEGIN ... COMMIT` block is rejected at COMMIT time —
/// Calvin cross-shard atomicity currently requires auto-commit
/// (single-statement) writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_multi_shard_write_in_explicit_block_is_rejected() {
    // The node's own data directory (kept alive for the node's lifetime).
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_dir_path = data_dir.path().to_path_buf();

    // 4 Data-Plane cores so distinct vShards land on distinct cores — a
    // genuine cross-core, cross-shard write.
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir_path)
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
    // the two INSERTs, and COMMIT as separate `simple_query` calls would ALSO
    // hit the per-statement `CrossShardInExplicitTransaction` rejection; this
    // form buffers the writes and lets `classify_dispatch` see the full
    // COMMIT-time task set as MultiShard before rejecting it.
    //
    // NOTE: interactive cross-shard COMMIT inside an explicit `BEGIN` block
    // is not yet supported — Calvin cross-shard atomicity currently requires
    // auto-commit (single-statement) writes. When that capability lands,
    // this test should be flipped back to assert the COMMIT succeeds,
    // restore the WAL-only restart below it, and re-assert that both the KV
    // base row and the vector-indexed document row (and its HNSW entry)
    // survive the restart.
    let txn_sql = format!(
        "BEGIN; \
         INSERT INTO {kv} (id, v) VALUES ('k1', 42); \
         INSERT INTO {vecdocs} (id, body, embedding) VALUES ('d1', 'hello', ARRAY[0.1,0.2,0.3]); \
         COMMIT"
    );
    let err = node
        .client
        .simple_query(&txn_sql)
        .await
        .expect_err("multi-shard write inside an explicit transaction block must be rejected");
    assert!(
        err.as_db_error()
            .map(|db| db.message())
            .unwrap_or_default()
            .contains("cross-shard write inside explicit transaction block is not supported"),
        "expected CrossShardInExplicitTransaction error text, got: {err:?}"
    );

    // Nothing was ever submitted to the Calvin sequencer inbox — the reject
    // fires at COMMIT-time classification, before dispatch. There is nothing
    // committed to restart-and-verify, so the WAL-only restart + survival
    // checks that used to follow here are not exercised.
    let admitted_after = admitted_total(&node);
    assert_eq!(
        admitted_after, admitted_before,
        "admitted_total must not advance for a rejected transaction: \
         before={admitted_before} after={admitted_after}"
    );

    node.shutdown().await;
}
