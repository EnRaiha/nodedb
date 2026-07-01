// SPDX-License-Identifier: BUSL-1.1

//! End-to-end InstallSnapshot recovery through the FULL server stack.
//!
//! Unlike `install_snapshot_basic.rs` (which pokes `MultiRaft` directly) and
//! `install_snapshot_chunked.rs` (which exercises the chunk framing), this
//! test drives the REAL snapshot round-trip across a running cluster:
//!
//!   1. Spawn a 3-node cluster with a LOW `log_compaction_threshold` AND
//!      `replication_factor` set to the POST-JOIN node count (4). The
//!      cluster boots via `start_raft`, so the production
//!      `DataPlaneSnapshotBuilder` (leader) and `DataPlaneSnapshotApplier`
//!      (follower) hooks are installed and active. HRW placement assigns
//!      `take = min(replication_factor, node_count)` nodes to each Raft
//!      group — with the default `replication_factor = 3`, a 4th node
//!      added later is NOT guaranteed to be placed on the collection's
//!      data group at all, which would make any assertion about it
//!      vacuous. `replication_factor = 4` makes `take = min(4, 4) = 4` at
//!      every node count from 3 (pre-join) through 4 (post-join), so
//!      placement deterministically assigns the learner to the group too.
//!   2. Write enough rows that the leader's data-group Raft log compacts
//!      past the start (its `snapshot_index` advances). Wait for the whole
//!      cluster to converge on the data.
//!   3. ASSERT compaction actually happened on the leader BEFORE any new
//!      node joins — this is what makes `AppendEntries` catch-up impossible
//!      for a fresh peer, forcing the leader down the `InstallSnapshot`
//!      path. Resolve and record the collection's data group id here too.
//!   4. Add a FRESH 4th node as a learner via the production join /
//!      `AddLearner` conf-change path (`TestCluster::add_learner_node`).
//!      Because the leader's log is already compacted, the only way the
//!      learner can be made whole is a real `InstallSnapshot` built by the
//!      `DataPlaneSnapshotBuilder` and applied by the
//!      `DataPlaneSnapshotApplier`.
//!   5. PRIMARY ASSERTION: poll the learner's OWN local Raft state
//!      (`hosts_data_group` / `local_snapshot_index_for_group`) until it
//!      LOCALLY mounts the collection's data group AND its local
//!      `snapshot_index` is non-zero. This is deliberately NOT a pgwire
//!      `SELECT COUNT(*)` — the pgwire gateway on a cluster node FORWARDS
//!      reads to whichever node actually hosts the group whenever the
//!      local node isn't a member, so a `SELECT COUNT(*)` against the
//!      learner would return the right answer whether or not the learner
//!      itself ever mounted the group or applied a snapshot. It is a
//!      pure forwarding artifact and proves nothing about InstallSnapshot.
//!      Reading the learner's own local Raft state cannot be satisfied by
//!      forwarding: it either mounted the group and applied a snapshot, or
//!      it didn't.
//!   6. SECONDARY (kept as functional confirmation, not proof): the
//!      existing pgwire `COUNT(*)` and PK point-lookup checks against the
//!      learner's own client. These still pass, and remain useful as an
//!      end-user-visible confirmation of the data — but the local-hosting
//!      assertion above is what actually proves the InstallSnapshot path
//!      ran, since these queries could pass via forwarding alone.

use std::time::{Duration, Instant};

mod common;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Low enough that a couple dozen single-row inserts (each one Raft entry on
/// the data group) compacts the leader's data-group log past the start.
const COMPACTION_THRESHOLD: u64 = 4;

/// Number of rows to write. Comfortably more than the compaction threshold so
/// the data group compacts well before the learner joins.
const ROW_COUNT: usize = 40;

const COLLECTION: &str = "snap_e2e";

/// Render the human-readable detail of a pgwire error.
fn db_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!(
            "{}: {} (SQLSTATE {})",
            db.severity(),
            db.message(),
            db.code().code()
        )
    } else {
        format!("{e:?}")
    }
}

/// True for errors that are transient during catch-up and SHOULD be retried:
/// catalog/replication lag ("table not found") and the snapshot-apply window
/// where the catalog is mutating under the query ("schema changed during
/// execution ... please retry", SQLSTATE XX000 — the server explicitly asks
/// the client to retry). Any other error is a real failure.
fn is_retryable_query_err(e: &tokio_postgres::Error) -> bool {
    e.as_db_error()
        .map(|db| {
            let msg = db.message();
            db.code().code() == "42601"
                || msg.contains("table not found")
                || msg.contains("collection not found")
                || msg.contains("schema changed during execution")
                || msg.contains("please retry")
        })
        .unwrap_or(false)
}

/// Poll `SELECT COUNT(*)` on `client` until the collection is queryable AND
/// reports `>= expected` rows, or the deadline expires (then panic). Returns
/// the observed count. Transient catch-up errors (see [`is_retryable_query_err`])
/// are retried; any other error fails loudly and immediately.
async fn count_rows_when_ready(
    client: &tokio_postgres::Client,
    table: &str,
    expected: usize,
    timeout: Duration,
) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
            .await
        {
            Ok(rows) => {
                let mut count = None;
                for msg in &rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
                        && let Some(s) = r.get(0)
                    {
                        count = Some(s.parse::<usize>().expect("COUNT(*) parse"));
                    }
                }
                let count = count.expect("COUNT(*) returned no row");
                if count >= expected {
                    return count;
                }
                if Instant::now() >= deadline {
                    panic!(
                        "collection `{table}` reached only {count}/{expected} rows within {timeout:?}"
                    );
                }
            }
            Err(ref e) => {
                if !is_retryable_query_err(e) {
                    panic!(
                        "SELECT COUNT(*) FROM {table} failed unexpectedly: {}",
                        db_detail(e)
                    );
                }
                if Instant::now() >= deadline {
                    panic!("collection `{table}` never became queryable within {timeout:?}");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// cluster/install_snapshot_e2e
///
/// A freshly-added learner is made whole purely by a real Raft
/// `InstallSnapshot` (the leader's log is already compacted past the writes),
/// and ends up with the complete dataset it never saw as log entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn learner_caught_up_via_real_install_snapshot() {
    // 1. Cluster with a low compaction threshold — production snapshot
    //    builder/applier hooks are wired by `start_raft`. `replication_factor
    //    = 4` (the post-join node count) so HRW placement deterministically
    //    assigns every node, including the learner added in step 5, to the
    //    collection's data group.
    let mut cluster =
        TestCluster::spawn_three_with_compaction_threshold_and_rf(COMPACTION_THRESHOLD, 4)
            .await
            .expect("3-node cluster with low compaction threshold and rf=4");

    // 2. Create a strict-document collection (queryable via SELECT, carries a
    //    primary key, survives a snapshot round-trip).
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} \
             (id TEXT PRIMARY KEY, payload TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION");

    wait_for(
        "all nodes see the collection",
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

    // 3. Write enough rows that the data-group log compacts past the start.
    //    Each INSERT is one Raft entry on the data group.
    for i in 0..ROW_COUNT {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {COLLECTION} (id, payload) VALUES ('row-{i}', 'val-{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert row-{i}: {}", db_detail(&e)));
    }

    // Wait for the writes to fully propagate to all three original members.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        // Generous per-node visibility budget: this 4-node test is heavy, and
        // when it runs back-to-back after another cluster test (serialized by
        // the nextest `cluster` group) the machine is under port/process
        // pressure that slows the first pgwire round-trips. Standalone this
        // resolves in <1s.
        let n = count_rows_when_ready(&node.client, COLLECTION, ROW_COUNT, Duration::from_secs(30))
            .await;
        assert_eq!(n, ROW_COUNT, "node {} must see all rows", node.node_id);
    }

    // 4. ASSERT (a): the leader's data-group log compacted BEFORE the learner
    //    joins. With auto-compaction gated on the applied watermark, the
    //    leader's `snapshot_index` advances once it has more than
    //    `COMPACTION_THRESHOLD` applied entries past the snapshot. A non-zero
    //    value across the data groups means a fresh peer below it CANNOT be
    //    caught up by `AppendEntries` — only `InstallSnapshot`.
    let max_snap_before = cluster
        .nodes
        .iter()
        .map(|n| n.max_data_group_snapshot_index())
        .max()
        .unwrap_or(0);
    assert!(
        max_snap_before > 0,
        "expected a data group's log to have compacted (snapshot_index > 0) before the \
         learner joins, so catch-up cannot be via AppendEntries; saw 0 on every node"
    );

    // Resolve the collection's data group id from an original node's own
    // routing view. This is the group we'll assert the learner LOCALLY
    // mounts and snapshot-applies below.
    let gid = cluster.nodes[0]
        .group_id_for_collection(COLLECTION)
        .expect("collection maps to a data group");
    assert!(
        gid != 0,
        "collection must map to a data group, not metadata"
    );

    // 5. Add a brand-new node as a learner via the production join /
    //    AddLearner conf-change path. The leader must InstallSnapshot it.
    let learner_id = {
        let learner = cluster.add_learner_node().await.expect("add learner node");
        learner.node_id
    };

    let learner = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == learner_id)
        .expect("learner present in cluster");

    // PRIMARY ASSERTION: poll the learner's OWN local Raft state until it
    // LOCALLY mounts the collection's data group AND its local
    // `snapshot_index` for that group is non-zero. Unlike a pgwire query,
    // this cannot be satisfied by the gateway forwarding reads to some
    // other hosting node — `hosts_data_group` / `local_snapshot_index_for_group`
    // only ever reflect this node's own Raft state. A non-zero local
    // snapshot_index on a node whose log starts beyond the compacted region
    // (asserted above via `max_snap_before > 0`) can ONLY be explained by a
    // real `InstallSnapshot`: `AppendEntries` alone cannot advance a
    // compacted-past log. A timeout here means the joined over-RF node
    // never mounted the group and/or never applied a snapshot for it — a
    // real regression, not a flake, since `replication_factor = 4` makes
    // placement of every node on every group deterministic.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let hosts = learner.hosts_data_group(gid);
        let snap = learner.local_snapshot_index_for_group(gid);
        if hosts && snap > 0 {
            break;
        }
        if Instant::now() >= deadline {
            let dump: Vec<String> = cluster
                .nodes
                .iter()
                .map(|n| n.group_status_line(gid))
                .collect();
            panic!(
                "learner node {learner_id} never locally mounted data group {gid} with a \
                 non-zero local snapshot_index within 30s (hosts_data_group={hosts}, \
                 local_snapshot_index_for_group={snap}); this proves the joined node never \
                 received a real InstallSnapshot for the collection's group — a regression, \
                 since replication_factor=4 makes placement on this group deterministic for \
                 every node.\nGROUP {gid} STATUS DUMP:\n{}",
                dump.join("\n")
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 6. SECONDARY (functional confirmation, not proof — see module docs):
    //    the learner — which never received the original writes as
    //    log entries — returns the FULL dataset through its OWN pgwire client.
    //    Its data-group log starts beyond the compacted region, so the only
    //    way it has this data is the applied InstallSnapshot.
    let learner_count = count_rows_when_ready(
        &learner.client,
        COLLECTION,
        ROW_COUNT,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        learner_count, ROW_COUNT,
        "learner node {learner_id} must hold the full dataset restored via InstallSnapshot"
    );

    // Spot-check a specific row round-tripped through the snapshot, not just
    // the count: a PK point-lookup (`WHERE id = pk`) resolves the pk→surrogate
    // binding the snapshot apply rebound into the catalog — the thing that was
    // broken before the fix. Poll briefly: transient catch-up errors are
    // retried, but a successful query returning the wrong/no value fails.
    let deadline = Instant::now() + Duration::from_secs(10);
    let payload = loop {
        match learner
            .client
            .simple_query(&format!(
                "SELECT payload FROM {COLLECTION} WHERE id = 'row-0'"
            ))
            .await
        {
            Ok(rows) => {
                let payload = rows.iter().find_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                    _ => None,
                });
                if payload.is_some() || Instant::now() >= deadline {
                    break payload;
                }
            }
            Err(ref e) => {
                if !is_retryable_query_err(e) {
                    panic!("learner SELECT row-0: {}", db_detail(e));
                }
                if Instant::now() >= deadline {
                    break None;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        payload.as_deref(),
        Some("val-0"),
        "learner must return the snapshot-restored value for row-0 (pk→surrogate binding \
         must be rebound on snapshot apply)"
    );

    cluster.shutdown().await;
}
