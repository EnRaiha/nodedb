// SPDX-License-Identifier: BUSL-1.1

//! End-to-end InstallSnapshot recovery through the FULL server stack.
//!
//! Unlike `install_snapshot_basic.rs` (which pokes `MultiRaft` directly) and
//! `install_snapshot_chunked.rs` (which exercises the chunk framing), this
//! test drives the REAL snapshot round-trip across a running cluster:
//!
//!   1. Spawn a 3-node cluster with a LOW `log_compaction_threshold`. The
//!      cluster boots via `start_raft`, so the production
//!      `DataPlaneSnapshotBuilder` (leader) and `DataPlaneSnapshotApplier`
//!      (follower) hooks are installed and active.
//!   2. Write enough rows that the leader's data-group Raft log compacts
//!      past the start (its `snapshot_index` advances). Wait for the whole
//!      cluster to converge on the data.
//!   3. ASSERT compaction actually happened on the leader BEFORE any new
//!      node joins — this is what makes `AppendEntries` catch-up impossible
//!      for a fresh peer, forcing the leader down the `InstallSnapshot`
//!      path.
//!   4. Add a FRESH 4th node as a learner via the production join /
//!      `AddLearner` conf-change path (`TestCluster::add_learner_node`).
//!      Because the leader's log is already compacted, the only way the
//!      learner can be made whole is a real `InstallSnapshot` built by the
//!      `DataPlaneSnapshotBuilder` and applied by the
//!      `DataPlaneSnapshotApplier`.
//!   5. ASSERT the learner — which never saw the original writes as log
//!      entries — returns the FULL dataset when queried through its own
//!      pgwire client. That proves the snapshot path restored real engine
//!      state on a node caught up purely by snapshot.

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
    //    builder/applier hooks are wired by `start_raft`.
    let mut cluster = TestCluster::spawn_three_with_compaction_threshold(COMPACTION_THRESHOLD)
        .await
        .expect("3-node cluster with low compaction threshold");

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
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;
    for node in &cluster.nodes {
        let n = count_rows_when_ready(&node.client, COLLECTION, ROW_COUNT, Duration::from_secs(15))
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

    // 5. Add a brand-new node as a learner via the production join /
    //    AddLearner conf-change path. The leader must InstallSnapshot it.
    let learner_id = {
        let learner = cluster.add_learner_node().await.expect("add learner node");
        learner.node_id
    };

    // 6. ASSERT (b): the learner — which never received the original writes as
    //    log entries — returns the FULL dataset through its OWN pgwire client.
    //    Its data-group log starts beyond the compacted region, so the only
    //    way it has this data is the applied InstallSnapshot.
    let learner = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == learner_id)
        .expect("learner present in cluster");

    let learner_count = count_rows_when_ready(
        &learner.client,
        COLLECTION,
        ROW_COUNT,
        Duration::from_secs(20),
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
