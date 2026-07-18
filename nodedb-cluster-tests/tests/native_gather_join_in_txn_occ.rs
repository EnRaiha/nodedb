// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard OCC read validation for an in-transaction DISTRIBUTED
//! GATHER-JOIN read driven over the NATIVE (MessagePack) transport.
//!
//! ## What this pins (and how it differs from the pgwire gather suite)
//!
//! This is the native-transport mirror of `gather_join_in_txn_occ.rs`. A
//! cross-vShard equi-join lowers to `Exchange{Gather}` wrapping a `HashJoin` BY
//! DEFAULT — no forced shuffle. The coordinator routes the `HashJoin` to the
//! LEFT (probe) collection's owning vShard and GATHERS the RIGHT (build)
//! collection across all vShards. The Gather resolver emits a per-collection
//! `DistributedReadCapture` for each side.
//!
//! The pgwire transport already threaded those captures into the shared
//! read-set recorder. The native dispatch loop previously DROPPED them, so an
//! in-txn distributed read on native recorded only the plan's collapsed left
//! collection — the build side's gathered read-version was DISCARDED and its
//! vShard was never revalidated at the Calvin commit barrier. A concurrent
//! write to the build collection between the in-txn read and COMMIT went
//! UNDETECTED and the transaction silently committed a stale join. Native is the
//! canonical transport; this suite proves the hole is now closed on native by
//! feeding the SAME captures into the SAME shared recorder pgwire uses.
//!
//! Three cases bound the fixed behavior:
//!
//!  * `commits_when_neither_side_concurrently_written` — neither join side is
//!    written during the txn, so both recorded real per-collection versions
//!    still validate and COMMIT must SUCCEED (proves the captures carry the
//!    sound `coll_write_lsn`, not an inflated global watermark that over-aborts).
//!  * `occ_aborts_on_stale_probe_read` — a confirmed-visible concurrent write to
//!    the LEFT/probe collection advances it past the captured probe version, so
//!    COMMIT must abort with a serialization failure.
//!  * `occ_aborts_on_stale_build_read` — THE LOAD-BEARING CASE: a
//!    confirmed-visible concurrent write to the RIGHT/build collection ONLY must
//!    STILL abort. Pre-fix, native recorded no read-set entry for the build
//!    collection, so this would have SILENTLY committed. This case proves the
//!    native build-side hole is closed on the NATURAL Gather path.
//!
//! ## Determinism
//!
//! No fixed sleep governs correctness. Each concurrent write is issued on a
//! separate `NativeClient` and confirmed VISIBLE (via an autocommit `SELECT`)
//! BEFORE the coordinator's COMMIT is issued, so the join read is provably stale
//! at commit time — the abort is guaranteed, never racy.
//!
//! The native protocol surfaces the abort as an error MESSAGE (not a SQLSTATE),
//! so the stale cases detect it via the stable text
//! "could not serialize access due to concurrent update".

mod common;

use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};
use nodedb_client::NativeClient;
use nodedb_client::native::pool::PoolConfig;
use nodedb_types::error::NodeDbError;

use common::cluster_harness::{TestClusterNode, wait_for, wait_for_async};
use common::occ_shuffle::{admitted_total, count_rows, has_id, pg_detail, sequencer_leader};

/// Stable message the native transport emits when a cross-shard COMMIT aborts
/// because a read-only participant's captured slice went stale at the barrier.
const SERIALIZATION_ABORT: &str = "could not serialize access due to concurrent update";

/// Four collection names whose vShard ids are pairwise distinct, so a transaction
/// that reads two of them (the join sides) and writes the other two is genuinely
/// multi-vShard on both its read set and its write set. Deterministic:
/// `VShardId::from_collection_in_database` is a pure function of the database id +
/// collection-name bytes.
fn distinct_vshard_quad() -> (String, String, String, String) {
    let mut chosen: Vec<(String, u32)> = Vec::new();
    for i in 0u32..2048 {
        let name = format!("native_gather_join_occ_{i}");
        let v = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
        if chosen.iter().all(|(_, cv)| *cv != v) {
            chosen.push((name, v));
            if chosen.len() == 4 {
                let mut it = chosen.into_iter().map(|(n, _)| n);
                return (
                    it.next().expect("left name"),
                    it.next().expect("right name"),
                    it.next().expect("w1 name"),
                    it.next().expect("w2 name"),
                );
            }
        }
    }
    panic!("could not find four pairwise-distinct-vShard collection names in 2048 tries");
}

/// The distributed equi-join over the seeded `left`/`right` collections. `left` is
/// the probe side (its column is the LEFT of the `on` pair), `right` is the build
/// side. Lowers to the default `Exchange{Gather{HashJoin}}` plan — NO forced
/// shuffle.
fn join_sql(left: &str, right: &str) -> String {
    format!("SELECT l.id AS lid, r.id AS rid FROM {left} l JOIN {right} r ON l.jk = r.jk")
}

/// `true` if a native-client error is the cross-shard serialization abort.
fn is_serialization_abort(e: &NodeDbError) -> bool {
    e.message().contains(SERIALIZATION_ABORT)
}

/// A single-connection-pinned `NativeClient` on the node's native listener. A
/// `max_size` of 1 means every `begin`/`query`/`commit`/`set_parameter` call rides
/// the SAME socket and server session, so `BEGIN → read → writes → COMMIT` all
/// share one transaction context.
fn pinned_native_client(node: &TestClusterNode) -> NativeClient {
    NativeClient::new(PoolConfig {
        addr: format!("127.0.0.1:{}", node.native_port),
        max_size: 1,
        ..Default::default()
    })
}

/// Concatenate a native `EXPLAIN` result's plan cells into one string.
async fn explain_text_native(client: &NativeClient, sql: &str) -> String {
    let result = client
        .query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("native EXPLAIN {sql}: {e}"));
    result
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Spawn a 4-core single-node-calvin node, create `left`/`right` (document_strict,
/// each seeded with three matching join keys) plus `w1`/`w2` on distinct vShards,
/// select strict cross-shard mode on a single-connection-pinned native driver, and
/// confirm the join lowers to the DEFAULT distributed gather plan (`Exchange{Gather}`,
/// NOT shuffle). Returns the node, its data-dir guard, the pinned native driver,
/// and the four collection names `(left, right, w1, w2)`.
async fn spawn_node_with_collections() -> (
    TestClusterNode,
    tempfile::TempDir,
    NativeClient,
    String,
    String,
    String,
    String,
) {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir.path().to_path_buf())
        .await
        .expect("spawn standalone single-node-calvin server on path");

    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    let (left, right, w1, w2) = distinct_vshard_quad();

    // left/right carry the join rows (document_strict); w1/w2 are the two
    // buffered-write collections that push COMMIT onto the multi-participant
    // Calvin barrier. Collections + seed data are created over pgwire — the
    // transport under test is the native DRIVER of the transaction below, not DDL.
    for coll in [&left, &right] {
        node.client
            .simple_query(&format!(
                "CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, jk TEXT, val BIGINT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {coll}: {}", pg_detail(&e)));
    }
    for coll in [&w1, &w2] {
        node.client
            .simple_query(&format!(
                "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, value STRING) \
                 WITH (engine='document_schemaless')"
            ))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {coll}: {}", pg_detail(&e)));
    }
    wait_for(
        "all four collections visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 4,
    )
    .await;

    // Seed left and right with three matching join keys so the join returns real
    // rows AND each side's committed `coll_write_lsn` is non-zero (gives the
    // stale-read cases a real baseline version to advance past, and the
    // commits-clean case a non-zero version that must still validate).
    node.client
        .simple_query(&format!(
            "INSERT INTO {left} (id, jk, val) VALUES \
             ('l1', 'k1', 10), ('l2', 'k2', 20), ('l3', 'k3', 30)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {left}: {}", pg_detail(&e)));
    node.client
        .simple_query(&format!(
            "INSERT INTO {right} (id, jk, val) VALUES \
             ('r1', 'k1', 1), ('r2', 'k2', 2), ('r3', 'k3', 3)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {right}: {}", pg_detail(&e)));

    // The single-connection-pinned native driver used for the whole transaction.
    let driver = pinned_native_client(&node);

    // Strict mode so COMMIT's multi-shard path routes through the Calvin barrier.
    // Set via the native session-SET mechanism on the pinned connection — NO
    // force_shuffle knob, so the join stays a NATURAL Gather.
    driver
        .set_parameter("cross_shard_txn", "strict")
        .await
        .expect("native SET cross_shard_txn = strict");

    // The join MUST default to the distributed GATHER plan (not shuffle): confirm
    // the physical plan carries an `Exchange{Gather}` and NOT a `Shuffle`. If this
    // ever regresses to shuffle in the harness, the test is exercising the wrong
    // path and must fail loudly here rather than silently passing.
    let plan = explain_text_native(&driver, &join_sql(&left, &right)).await;
    assert!(
        plan.contains("Exchange") && plan.contains("Gather"),
        "cross-vShard join must lower to Exchange{{Gather}} by default; EXPLAIN was:\n{plan}"
    );
    assert!(
        !plan.contains("Shuffle"),
        "cross-vShard join must NOT be a shuffle join in this test (no force override); \
         EXPLAIN was:\n{plan}"
    );

    // Sanity: the distributed gather join resolves and returns the 3 matched rows
    // in autocommit over native before any transaction — localizes a plan/resolve
    // regression away from the OCC assertions below. (A read; writes nothing.)
    let joined = driver
        .query(&join_sql(&left, &right))
        .await
        .unwrap_or_else(|e| panic!("autocommit native distributed gather join: {e}"));
    assert_eq!(
        joined.rows.len(),
        3,
        "distributed gather join must return the 3 matched rows"
    );

    (node, data_dir, driver, left, right, w1, w2)
}

/// Open a cross-shard txn on the pinned native driver: BEGIN + the distributed
/// gather join read of `left`/`right` + two buffered writes on distinct vShards,
/// leaving the transaction OPEN. The two INSERTs buffer on the coordinator, so at
/// COMMIT the write set spans two vShards → the Calvin barrier; the join SELECT
/// registers BOTH join sides' vShards as read-only participants with captured real
/// read versions.
async fn begin_join_read_and_buffer_writes(
    driver: &NativeClient,
    left: &str,
    right: &str,
    w1: &str,
    w2: &str,
) {
    driver.begin().await.expect("native BEGIN");
    driver
        .query(&join_sql(left, right))
        .await
        .expect("in-txn native distributed gather join read");
    driver
        .query(&format!("INSERT INTO {w1} (id, value) VALUES ('a', '1')"))
        .await
        .expect("buffer write into w1");
    driver
        .query(&format!("INSERT INTO {w2} (id, value) VALUES ('b', '2')"))
        .await
        .expect("buffer write into w2");
}

/// Issue an autocommit INSERT from a SEPARATE (external) concurrent writer and
/// confirm the new row is VISIBLE before returning — makes the native driver's
/// in-txn read provably stale so its COMMIT must fail OCC validation.
///
/// The concurrent writer here uses the pgwire connection: what this test bounds
/// is the NATIVE reader's in-transaction distributed-read OCC (recording +
/// commit-time revalidation), for which the writer's transport is incidental.
/// The native-writer-vs-native-reader cell is covered separately by the unit
/// that fixes native autocommit writes advancing the per-collection OCC floor.
async fn concurrent_write_and_confirm(node: &TestClusterNode, coll: &str) {
    node.client
        .simple_query(&format!(
            "INSERT INTO {coll} (id, jk, val) VALUES ('rival', 'k1', 999)"
        ))
        .await
        .unwrap_or_else(|e| panic!("concurrent write to {coll}: {e}"));

    wait_for_async(
        "rival row visible before COMMIT",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || async {
            let msgs = node
                .client
                .simple_query(&format!("SELECT id FROM {coll}"))
                .await
                .expect("autocommit read confirming rival visibility");
            has_id(&msgs, "rival")
        },
    )
    .await;
}

/// Neither join side is concurrently written, so both recorded real read versions
/// still validate at the barrier and COMMIT must SUCCEED. Proves the captured
/// read-versions are the sound per-collection `coll_write_lsn` — an inflated
/// global watermark would over-abort this clean commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_gather_join_commits_when_neither_side_concurrently_written() {
    let (node, _data_dir, driver, left, right, w1, w2) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    begin_join_read_and_buffer_writes(&driver, &left, &right, &w1, &w2).await;

    // No concurrent write to either join side: both read slices stay current.
    // COMMIT must succeed through the multi-participant Calvin barrier.
    driver.commit().await.unwrap_or_else(|e| {
        panic!(
            "COMMIT of an in-txn native distributed gather join whose sides were NOT \
             concurrently written must succeed; got: {e}"
        )
    });

    // The batch reached the sequencer — proving the commit went through the
    // multi-participant barrier (which validates read-only participants), not a
    // no-op / single-shard path.
    wait_for(
        "calvin admitted the committed cross-shard transaction",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    // Both committed writes become visible (Calvin flush lands asynchronously).
    wait_for_async(
        "both committed writes visible after COMMIT",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || async {
            let rows_w1 = node
                .client
                .simple_query(&format!("SELECT id FROM {w1} WHERE id = 'a'"))
                .await
                .expect("SELECT w1 after commit");
            let rows_w2 = node
                .client
                .simple_query(&format!("SELECT id FROM {w2} WHERE id = 'b'"))
                .await
                .expect("SELECT w2 after commit");
            count_rows(&rows_w1) == 1 && count_rows(&rows_w2) == 1
        },
    )
    .await;

    node.shutdown().await;
}

/// A confirmed-visible concurrent write to the LEFT/probe collection advances it
/// past the captured probe read version. COMMIT must abort with a serialization
/// failure — the barrier revalidates the probe side's read-only slice and finds it
/// stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_gather_join_occ_aborts_on_stale_probe_read() {
    let (node, _data_dir, driver, left, right, w1, w2) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    begin_join_read_and_buffer_writes(&driver, &left, &right, &w1, &w2).await;

    // Concurrent writer advances the LEFT (probe) collection past the captured
    // probe read version; confirmed visible before COMMIT for determinism.
    concurrent_write_and_confirm(&node, &left).await;

    let err = driver
        .commit()
        .await
        .expect_err("COMMIT of a stale in-txn native gather join (probe side) must abort");
    assert!(
        is_serialization_abort(&err),
        "expected serialization abort for the stale probe-side read, got: {err}"
    );

    wait_for(
        "calvin admitted the aborted cross-shard transaction",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    node.shutdown().await;
}

/// THE LOAD-BEARING CASE. A confirmed-visible concurrent write to the RIGHT/build
/// collection ONLY must STILL abort the COMMIT with a serialization failure.
/// Pre-fix, the native dispatch loop DROPPED the Gather resolver's build-side
/// capture and `extract_collection(Exchange{Gather{HashJoin}})` returned only the
/// left collection, so NO read-set entry existed for the build collection — its
/// vShard was never validated at the barrier and this transaction would have
/// SILENTLY committed a stale join result. The abort here proves the build side is
/// now recorded on the NATURAL gather path over native and the hole is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_gather_join_occ_aborts_on_stale_build_read() {
    let (node, _data_dir, driver, left, right, w1, w2) = spawn_node_with_collections().await;

    let admitted_before = admitted_total(&node);

    begin_join_read_and_buffer_writes(&driver, &left, &right, &w1, &w2).await;

    // Concurrent writer advances the RIGHT (build) collection ONLY — the LEFT side
    // stays current. Pre-fix this write went undetected at commit on native.
    concurrent_write_and_confirm(&node, &right).await;

    let err = driver.commit().await.expect_err(
        "COMMIT of a stale in-txn native gather join (BUILD side) must abort — the hole this \
         closes; pre-fix native silently committed",
    );
    assert!(
        is_serialization_abort(&err),
        "expected serialization abort for the stale BUILD-side read (hole closed), got: {err}"
    );

    // The aborted transaction reached the sequencer via the multi-participant
    // barrier (the only path that revalidates read-only participants), proving the
    // abort is the barrier finding the build side stale — not an unrelated reason.
    wait_for(
        "calvin admitted the aborted cross-shard transaction",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    // Belt-and-suspenders: the aborted transaction's buffered write into w1 must
    // have rolled back — an autocommit read shows no 'a' row.
    let rows_w1 = node
        .client
        .simple_query(&format!("SELECT id FROM {w1} WHERE id = 'a'"))
        .await
        .expect("SELECT w1 after abort");
    assert_eq!(
        count_rows(&rows_w1),
        0,
        "the aborted transaction's buffered write into w1 must not be visible"
    );

    node.shutdown().await;
}
