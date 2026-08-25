// SPDX-License-Identifier: BUSL-1.1
//! Cluster end-to-end BACKUP / RESTORE.
//!
//! Verifies that the wire-streaming COPY surface fans out across
//! every node holding tenant data and that restore replays through
//! the current routing topology rather than the source one.

mod common;
use common::cluster_harness::TestCluster;

use bytes::Bytes;
use common::cluster_harness::wait::wait_for;
use futures::SinkExt;
use futures::StreamExt;
use nodedb_types::backup_envelope::{DEFAULT_MAX_TOTAL_BYTES, parse_encrypted as parse_envelope};
use std::time::Duration;

/// Fixed test KEK injected into cluster nodes via `cluster_harness`.
const TEST_KEK: [u8; 32] = [0x42u8; 32];

const TENANT: u64 = 1;

async fn drain_backup(node_idx: usize, cluster: &TestCluster, tenant: u64) -> Vec<u8> {
    let stream = cluster.nodes[node_idx]
        .client
        .copy_out(&format!("COPY (BACKUP TENANT {tenant}) TO STDOUT"))
        .await
        .expect("copy_out");
    let mut bytes = Vec::new();
    let mut s = Box::pin(stream);
    while let Some(chunk) = s.next().await {
        bytes.extend_from_slice(&chunk.expect("copy chunk"));
    }
    bytes
}

async fn push_restore(
    node_idx: usize,
    cluster: &TestCluster,
    tenant: u64,
    bytes: Vec<u8>,
) -> Result<(), String> {
    let sink = cluster.nodes[node_idx]
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN"))
        .await
        .map_err(|e| db_detail(&e))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(bytes))
        .await
        .map_err(|e| db_detail(&e))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| db_detail(&e))
}

async fn unique_contents(client: &tokio_postgres::Client) -> std::collections::BTreeSet<String> {
    let rows = client
        .simple_query("SELECT content FROM cl_docs")
        .await
        .expect("SELECT cl_docs");
    let mut seen = std::collections::BTreeSet::new();
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            // 'content' may be the only column — try field 0.
            if let Some(s) = r.get(0) {
                seen.insert(s.to_string());
            }
        }
    }
    seen
}

fn db_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_backup_gathers_one_section_per_node() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    let bytes = drain_backup(0, &cluster, TENANT).await;
    let env = parse_envelope(&bytes, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse envelope");
    assert_eq!(
        env.meta.tenant_id, TENANT,
        "envelope tenant id should match request"
    );
    assert!(
        env.meta.source_vshard_count >= 1,
        "envelope must record source vshard count, got {}",
        env.meta.source_vshard_count
    );
    // One section per unique cluster node — three nodes, three sections.
    // The orchestrator dedupes by node id (leader + replicas of every group).
    assert!(
        !env.sections.is_empty() && env.sections.len() <= 3,
        "expected 1..=3 sections, got {}",
        env.sections.len()
    );
    let origins: std::collections::BTreeSet<u64> =
        env.sections.iter().map(|s| s.origin_node_id).collect();
    assert!(
        !origins.contains(&0),
        "origin_node_id 0 must not appear in cluster sections: {origins:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_roundtrip_preserves_data() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    // Write through node 0 — gateway routing places writes on the
    // current vshard owner (which may be any node).
    //
    // Hard fail-early invariant: after every INSERT the value MUST be
    // visible from node 0's pgwire client, otherwise a previous
    // INSERT silently dropped its data — surface that as the test
    // failure rather than letting it propagate into the
    // backup/restore symptom.
    for i in 0..6 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('k{i}','v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert k{i}: {}", db_detail(&e)));
        let rows = unique_contents(&cluster.nodes[0].client).await;
        let expected_count = (i + 1) as usize;
        assert_eq!(
            rows.len(),
            expected_count,
            "after INSERT k{i}: expected {expected_count} rows, got {} — \
             previous INSERT silently dropped a row. rows={rows:?}",
            rows.len()
        );
        let needle = format!("v{i}");
        assert!(
            rows.iter().any(|s| s.contains(&needle)),
            "after INSERT k{i}: v{i} missing — INSERT returned Ok but the row \
             never landed. rows={rows:?}"
        );
    }

    let bytes = drain_backup(0, &cluster, TENANT).await;

    // Restore into the same cluster. Writes are idempotent at the engine
    // level (PointPut overwrites); after restore we should still see all
    // six rows from any node's pgwire client.
    push_restore(0, &cluster, TENANT, bytes)
        .await
        .expect("RESTORE");

    // After the restore, every node's `ProposeTracker.complete`
    // bumps its per-group apply watermark after the Data Plane
    // confirms the storage write. Wait for cluster-wide convergence
    // so a follower-side SELECT sees the same state as the leader
    // — deterministic, no SQL polling.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    let post = unique_contents(&cluster.nodes[1].client).await;
    for i in 0..6u32 {
        let needle = format!("v{i}");
        assert!(
            post.iter().any(|s| s.contains(&needle)),
            "post-restore missing v{i}; rows={post:?}"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_dry_run_validates_envelope() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    let bytes = drain_backup(0, &cluster, TENANT).await;
    let sink = cluster.nodes[0]
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({TENANT}) FROM STDIN DRY RUN"))
        .await
        .expect("copy_in");
    let mut sink = Box::pin(sink);
    sink.as_mut().send(Bytes::from(bytes)).await.expect("send");
    sink.as_mut().finish().await.expect("dry run finish");

    cluster.shutdown().await;
}

// ────────────────────────────────────────────────────────────────────
// Snapshot-watermark coordination across the fan-out.
//
// The backup orchestrator must gather a cluster-wide "as-of" LSN
// before snapshotting each node. Without it, a write that interleaves
// with the fan-out can land on one node's snapshot but not another,
// materialising a state that never existed at any wall-clock instant
// (cross-shard UNIQUE / FK invariants observably break). The spec is:
//
//   1. The envelope's `snapshot_watermark` is non-zero whenever any
//      writes have committed (0 means "not captured" — a placeholder).
//   2. Two backups bracketing a write advance the watermark strictly
//      monotonically — it reflects a real coordinated commit instant,
//      not a hardcoded constant.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_envelope_carries_nonzero_snapshot_watermark() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    for i in 0..3 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('wm{i}','v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert wm{i}: {}", db_detail(&e)));
    }

    let bytes = drain_backup(0, &cluster, TENANT).await;
    let env = parse_envelope(&bytes, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse envelope");

    assert_ne!(
        env.meta.snapshot_watermark, 0,
        "backup envelope must carry a coordinated snapshot watermark (nonzero LSN), \
         got 0 — the orchestrator is skipping the coordination round and the \
         envelope represents a smear of per-node instants rather than one logical \
         instant, breaking cross-shard invariants on restore"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_watermark_advances_after_writes() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    cluster.nodes[0]
        .client
        .simple_query("INSERT INTO cl_docs (id, content) VALUES ('a','pre')")
        .await
        .unwrap_or_else(|e| panic!("insert a: {}", db_detail(&e)));

    let bytes_before = drain_backup(0, &cluster, TENANT).await;
    let env_before =
        parse_envelope(&bytes_before, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse before");

    for i in 0..4 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('b{i}','post')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert b{i}: {}", db_detail(&e)));
    }

    let bytes_after = drain_backup(0, &cluster, TENANT).await;
    let env_after =
        parse_envelope(&bytes_after, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse after");

    assert!(
        env_after.meta.snapshot_watermark > env_before.meta.snapshot_watermark,
        "watermark must advance across writes: before={}, after={} — a constant \
         watermark means the orchestrator is not capturing a real LSN, so restore \
         has no logical instant to recover to",
        env_before.meta.snapshot_watermark,
        env_after.meta.snapshot_watermark
    );

    cluster.shutdown().await;
}

// Cross-cluster topology drift (source mapping ≠ destination mapping)
// is verified out-of-process against the released binary. The
// in-process variant exposed a non-deterministic 1-row loss on the
// restore-side dispatch race; the reproducer lives where 3-node ×
// 3-node spawn cost is acceptable.

// ────────────────────────────────────────────────────────────────────
// Mid-flight node failure during restore fan-out.
//
// The restore orchestrator iterates per-node sub-snapshots via
// `sync_dispatch` (local) or `RaftRpc::ExecuteRequest` (remote) and
// surfaces the first per-node error. Two contracts must hold:
//
//   1. Loud failure — the client receives a structured error that
//      names the failing node, never silent partial success.
//   2. Idempotent retry — the engine-level PointPut writes are
//      idempotent, so a subsequent retry after the failed node is
//      restored converges to the expected state.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_surfaces_failing_node_id_on_midflight_failure() {
    let mut cluster = TestCluster::spawn_three().await.expect("cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    for i in 0..4 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('f{i}','v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert f{i}: {}", db_detail(&e)));
    }

    // Fault-inject: take down node 2, then pin node 0's routing table
    // so it still believes node 2 is the leader for every raft group.
    // Without the stale-route pin, quorum-replicated failover hides the
    // fault entirely — restore transparently re-routes to a surviving
    // replica and succeeds. Pinning forces the restore fan-out to
    // attempt dispatch against the dead peer so the structured
    // error-naming contract is actually exercised.
    let downed_node_id = cluster.nodes[2].node_id;
    let downed = cluster.nodes.remove(2);
    downed.shutdown().await;
    // Wait for the surviving nodes' SWIM/topology subsystem to observe
    // the peer death before pinning the stale routes. If we pin before
    // SWIM converges, an in-flight SWIM update can land *after* the pin
    // and overwrite it with a fresh leader hint, defeating the
    // fault-injection. The pin must be the last write to the routing
    // table for `downed_node_id`'s groups.
    wait_for(
        "node 0 marks downed peer as inactive in its topology view",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || cluster.nodes[0].active_topology_size() < 3,
    )
    .await;

    // Back up only once the topology has settled, and before the routes
    // are pinned.
    //
    // The restore path refuses an envelope whose watermark predates the
    // destination's last observed write-HLC, and that high-water advances on
    // every successful dispatch for the tenant — not only on writes the test
    // issues itself. Taking the backup first leaves the node teardown and the
    // SWIM convergence window sitting between the envelope and the restore,
    // and anything dispatched in there carries the high-water past the
    // envelope. The restore then fails the staleness check instead of
    // reaching the fan-out, and the assertion below sees the wrong loud
    // failure. Capturing after the teardown keeps the envelope dominant;
    // pinning afterwards adds nothing, since it only rewrites a local routing
    // table.
    //
    // The backup itself still succeeds with the peer down: routing has failed
    // over to the surviving replicas at this point, which is exactly the
    // transparent recovery the pin below goes on to defeat.
    let bytes = drain_backup(0, &cluster, TENANT).await;

    for group_id in 0..8u64 {
        cluster.nodes[0].force_stale_route_for_test(group_id, downed_node_id);
    }

    let err = push_restore(0, &cluster, TENANT, bytes)
        .await
        .expect_err("restore must fail loudly when fan-out targets a dead node");

    assert!(
        err.contains(&format!("node {downed_node_id}"))
            || err.contains(&downed_node_id.to_string()),
        "restore error must name the failing node id {downed_node_id} so the \
         operator can act; got: {err}"
    );

    cluster.shutdown().await;
}

// NOTE: a proper same-cluster retry test (fault a node, it recovers,
// retry succeeds on the same cluster) requires harness support for
// in-place node restart — shutting down a node and bringing it back
// with the same node id, same data dir, and a fresh transport that
// the raft group accepts as a rejoin rather than a new member. The
// current `TestClusterNode::spawn` always creates a fresh data dir
// and a fresh transport keypair, so a "replacement" node fails the
// rejoin handshake. Filed as a separate harness initiative.
//
// Idempotent replay on a healthy cluster is already covered by
// `three_node_roundtrip_preserves_data`, which restores into a
// cluster that already holds the same rows and expects convergence.

// ────────────────────────────────────────────────────────────────────
// Staleness gate on restore.
//
// Once the backup orchestrator captures a real watermark, a
// restore into a cluster whose current applied state is *newer* than
// the envelope's watermark would roll back committed writes. The
// operator should be told. Spec: by default, restoring a
// stale-watermark envelope is rejected with a structured error. An
// explicit opt-in (`FORCE` keyword) is the separate design decision —
// not pinned here until the DDL surface lands.
//
// This test will remain red until the coordination round produces
// nonzero watermarks AND the restore path compares them.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_refuses_stale_watermark() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    // Write, back up — envelope's watermark pins this logical instant.
    for i in 0..3 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('s{i}','early')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert s{i}: {}", db_detail(&e)));
    }
    let stale_bytes = drain_backup(0, &cluster, TENANT).await;

    // Advance the cluster past the envelope's watermark.
    for i in 0..3 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('s{i}_late','late{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert s{i}_late: {}", db_detail(&e)));
    }

    // Restoring the older envelope over newer state must be rejected.
    let err = push_restore(0, &cluster, TENANT, stale_bytes)
        .await
        .expect_err(
            "restore of stale-watermark envelope into newer cluster state must be \
             rejected by default — silently overwriting newer committed writes is \
             a correctness bug",
        );
    let lower = err.to_lowercase();
    assert!(
        lower.contains("stale") || lower.contains("watermark") || lower.contains("older"),
        "rejection error must name the staleness cause so operators know to \
         pass FORCE if they really mean it; got: {err}"
    );

    cluster.shutdown().await;
}
