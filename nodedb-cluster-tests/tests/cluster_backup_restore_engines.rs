// SPDX-License-Identifier: BUSL-1.1
//! Cluster end-to-end BACKUP / RESTORE for engine-specific data paths.
//!
//! Validates that BACKUP TENANT faithfully captures BOTH flushed (on-disk
//! segment) data AND memtable (in-memory, not yet flushed) data, and that
//! RESTORE TENANT replays both into a genuinely CLEAN target: a FRESH second
//! cluster that has never seen the collection.
//!
//! ## Why a fresh second cluster (not DROP-PURGE on the same cluster)
//!
//! Two restore bugs were fixed that require a truly clean restore target to
//! test faithfully:
//!   (1) Columnar/flushed-TS data was silently dropped on restore.
//!   (2) Catalog recreation was coordinator-local; now it propagates
//!       cluster-wide via metadata Raft.
//!
//! Restoring onto the SAME cluster after DROP … PURGE is an imperfect
//! substitute for two reasons:
//!   - The fail-closed "refuse to overwrite live data" guard fires if any
//!     engine state survives the purge window, making the test fragile.
//!   - DROP … PURGE advances the tenant write-HLC, which can trip the restore
//!     staleness gate (backup snapshot_watermark < destination tenant_write_hlc).
//!
//! A FRESH cluster B has tenant_write_hlc == 0, so the staleness gate always
//! passes, and there is no live data to collide with. The test backup KEK is a
//! fixed constant across all harness instances, so cluster B can decrypt
//! cluster A's envelope without key exchange. Cluster ports are ephemeral, so
//! two clusters never conflict.
//!
//! ## Flow for each test
//!
//!  1. Spawn SOURCE cluster A, CREATE collection, INSERT rows (triggering
//!     flush where relevant), then BACKUP from node 0 into `bytes`.
//!  2. Shut down cluster A — `bytes` is owned by the test and survives.
//!  3. Spawn a FRESH TARGET cluster B with the SAME spawn config as A (so any
//!     flush thresholds match if RESTORE re-ingests). Do NOT create the
//!     collection on B — RESTORE must recreate the catalog cluster-wide (that
//!     is part of what we are testing).
//!  4. `push_restore` into cluster B node 0. B's tenant_write_hlc is 0, so
//!     the staleness gate passes; no live data, so no fail-closed collision.
//!  5. `wait_for_full_apply_convergence` on cluster B.
//!  6. Final assertions query NODE 1 of cluster B (not the coordinator node 0)
//!     to prove the catalog propagated cluster-wide AND data is routable from
//!     a non-coordinator node.
//!  7. Shut down cluster B.
//!
//! NOTE: Cluster A is fully shut down before cluster B spawns (sequential, not
//! concurrent) to limit resource use and avoid port/file-descriptor exhaustion.

mod common;
use common::cluster_harness::TestCluster;

use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;
use std::time::Duration;
use std::time::Instant;

const TENANT: u64 = 1;

// ── Shared helpers copied verbatim from cluster_backup_restore.rs ────────────

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

fn db_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// Poll `SELECT COUNT(*) FROM <table>` on `client` until the collection is
/// queryable (i.e., the catalog has propagated to this node after a restore),
/// then return the row count.
///
/// Retry policy:
/// - "table not found" (sqlstate 42601 or message contains "table not found"):
///   catalog not yet applied on this node — sleep 100 ms and retry.
/// - Any OTHER query error: panic immediately (real failure, not lag).
/// - Timeout exceeded without the collection becoming queryable: panic with a
///   clear message naming the table and timeout.
async fn count_rows_when_ready(
    client: &tokio_postgres::Client,
    table: &str,
    timeout: Duration,
) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
            .await
        {
            Ok(rows) => {
                // Collection is queryable — parse and return the count.
                for msg in &rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
                        && let Some(s) = r.get(0)
                    {
                        return s.parse::<usize>().expect("COUNT(*) parse");
                    }
                }
                panic!("COUNT(*) returned no rows for {table}");
            }
            Err(ref e) => {
                let is_not_found = e
                    .as_db_error()
                    .map(|db| {
                        db.code().code() == "42601"
                            || db.message().contains("table not found")
                            || db.message().contains("collection not found")
                    })
                    .unwrap_or(false);

                if !is_not_found {
                    // Real error — fail loudly immediately.
                    panic!(
                        "SELECT COUNT(*) FROM {table} failed with unexpected error: {}",
                        db_detail(e)
                    );
                }

                // "table not found" — catalog propagation lag; retry if time remains.
                if Instant::now() >= deadline {
                    panic!(
                        "collection `{table}` never became queryable on this node within \
                         {timeout:?}: last error: {}",
                        db_detail(e)
                    );
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Collect the first column of every data row returned by `simple_query`.
async fn collect_first_col(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let rows = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {}", db_detail(&e)));
    let mut out = Vec::new();
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
            && let Some(s) = r.get(0)
        {
            out.push(s.to_string());
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Timeseries: flushed segments + memtable rows survive restore into
//           a FRESH second cluster
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_restore_preserves_timeseries_flushed_and_memtable() {
    // ── SOURCE cluster A ─────────────────────────────────────────────────────
    let cluster_a = TestCluster::spawn_three().await.expect("cluster A");

    // DDL — copied from nodedb/tests/engine_surface_timeseries.rs
    // `ingest_and_time_range_scan`. COLUMNS keyword, no PRIMARY KEY (timeseries
    // uses TIME_KEY as the ordering axis, not a primary key constraint).
    cluster_a
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION ts_br \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("CREATE COLLECTION ts_br on cluster A");

    // Insert 5 rows with DISTINCT timestamps (no dedup risk). Written through
    // node 0; the gateway routes to the correct vshard owner.
    let pre_flush_ids = ["p1", "p2", "p3", "p4", "p5"];
    let pre_flush_ts: [u64; 5] = [1000, 2000, 3000, 4000, 5000];
    for (id, ts) in pre_flush_ids.iter().zip(pre_flush_ts.iter()) {
        cluster_a.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO ts_br (id, ts, metric, value) \
                 VALUES ('{id}', {ts}, 'cpu', 1.0)"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {}", db_detail(&e)));
    }

    // The timeseries engine idle-flushes to on-disk segments after ~5 s of
    // ingest quiescence (maintenance loop). Sleep 8 s to let the flush fire,
    // converting the five rows above from memtable entries to segment data.
    // There is no manual FLUSH SQL for the timeseries engine.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Insert 2 more rows AFTER the sleep. These land in the fresh memtable at
    // backup time and exercise the memtable capture path of BACKUP TENANT.
    let post_flush_ids = ["p6", "p7"];
    let post_flush_ts: [u64; 2] = [6000, 7000];
    for (id, ts) in post_flush_ids.iter().zip(post_flush_ts.iter()) {
        cluster_a.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO ts_br (id, ts, metric, value) \
                 VALUES ('{id}', {ts}, 'cpu', 2.0)"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert post-flush {id}: {}", db_detail(&e)));
    }

    // Capture the backup from cluster A. `bytes` is owned by the test and
    // survives cluster A's shutdown.
    let bytes = drain_backup(0, &cluster_a, TENANT).await;
    assert!(!bytes.is_empty(), "backup must produce non-empty bytes");

    // Shut down cluster A before spawning cluster B to limit resource use and
    // avoid port / file-descriptor exhaustion between the two clusters.
    cluster_a.shutdown().await;

    // ── TARGET cluster B (fresh — no ts_br collection, no data) ─────────────
    // Spawn with the same config as cluster A so that engine parameters match
    // if RESTORE re-ingests rows. Do NOT create ts_br here: RESTORE must
    // recreate the catalog cluster-wide via metadata Raft — that is the
    // behavior under test.
    //
    // cluster B's tenant_write_hlc starts at 0, so the restore staleness gate
    // (snapshot_watermark >= tenant_write_hlc) always passes. There is no live
    // data, so the fail-closed "refuse to overwrite live data" guard does not
    // fire.
    let cluster_b = TestCluster::spawn_three().await.expect("cluster B");

    // ── Restore into the fresh target ────────────────────────────────────────
    push_restore(0, &cluster_b, TENANT, bytes)
        .await
        .expect("RESTORE ts_br into fresh cluster B");

    // Wait for all Raft groups on cluster B to apply the restored catalog +
    // data entries before asserting.
    cluster_b
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // ── Final assertions on NODE 1 of cluster B ──────────────────────────────
    // Querying node 1 (not the coordinator node 0 that received the RESTORE)
    // proves:
    //   (a) the restored catalog propagated cluster-wide via metadata Raft, and
    //   (b) data is routable from a non-coordinator node.
    let total =
        count_rows_when_ready(&cluster_b.nodes[1].client, "ts_br", Duration::from_secs(15)).await;
    assert_eq!(
        total, 7,
        "post-restore SELECT COUNT(*) from cluster B node 1 must be 7 \
         (5 flushed + 2 memtable), got {total}"
    );

    // Assert the full set of IDs is present, in timestamp order.
    let ids = collect_first_col(
        &cluster_b.nodes[1].client,
        "SELECT id FROM ts_br ORDER BY ts",
    )
    .await;
    let expected: Vec<&str> = vec!["p1", "p2", "p3", "p4", "p5", "p6", "p7"];
    assert_eq!(
        ids, expected,
        "post-restore row IDs from cluster B node 1 must match all 7 rows in ts order; \
         got {ids:?}"
    );

    cluster_b.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1b — CRDT: an applied delta survives BACKUP → RESTORE into a FRESH
//           second cluster, and is readable on every node of the target.
//
// Closes a declared backup/restore gap: the prior engine-coverage tests pinned
// timeseries and columnar but never exercised the `tenant_crdt_state` section of
// the backup envelope. A `crdt_apply` populates one Loro doc per tenant; BACKUP
// must capture it and RESTORE must fan it out to all nodes of cluster B.
// ─────────────────────────────────────────────────────────────────────────────

const CRDT_COLL: &str = "crdt_br";
const CRDT_DOC: &str = "doc1";

/// Build a real Loro snapshot delta for `CRDT_COLL`/`CRDT_DOC` with field
/// `name=alice`, exactly as the single-node CRDT snapshot test does. Returns the
/// hex `crdt_apply` decodes before merging into the tenant doc.
fn build_crdt_delta_hex() -> String {
    let doc = loro::LoroDoc::new();
    let coll = doc.get_map(CRDT_COLL);
    let row = coll
        .insert_container(CRDT_DOC, loro::LoroMap::new())
        .expect("row container");
    row.insert("name", "alice").expect("field");
    doc.commit();
    let delta = doc
        .export(loro::ExportMode::Snapshot)
        .expect("export loro snapshot");
    hex::encode(delta)
}

/// Read `crdt_state(CRDT_COLL, CRDT_DOC)` on `client`, retrying transient
/// catch-up errors until `timeout`. Returns the (possibly empty) payload text.
async fn read_crdt_state_when_ready(client: &tokio_postgres::Client, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT crdt_state('{CRDT_COLL}', '{CRDT_DOC}')"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                        let text = r.get(0).unwrap_or("").to_string();
                        if !text.is_empty() {
                            return text;
                        }
                    }
                }
                if Instant::now() >= deadline {
                    return String::new();
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(ref e) => {
                if Instant::now() >= deadline {
                    panic!(
                        "crdt_state never became readable within {timeout:?}: {}",
                        db_detail(e)
                    );
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_restore_preserves_crdt_state() {
    // ── SOURCE cluster A ─────────────────────────────────────────────────────
    let cluster_a = TestCluster::spawn_three().await.expect("cluster A");

    // CRDT collection: default-engine document collection (the CRDT doc is one
    // Loro doc per tenant). Matches the single-node CRDT snapshot round-trip test.
    cluster_a
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {CRDT_COLL}"))
        .await
        .expect("CREATE CRDT COLLECTION on cluster A");

    // Apply one CRDT delta through node 0's gateway (proposes through Raft).
    let delta_hex = build_crdt_delta_hex();
    cluster_a.nodes[0]
        .client
        .simple_query(&format!(
            "SELECT crdt_apply('{CRDT_COLL}', '{CRDT_DOC}', '{delta_hex}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("crdt_apply on cluster A: {}", db_detail(&e)));

    cluster_a
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Sanity: the delta is readable on cluster A before backup.
    let src_state =
        read_crdt_state_when_ready(&cluster_a.nodes[0].client, Duration::from_secs(10)).await;
    assert!(
        !src_state.is_empty(),
        "source cluster A must read back the applied CRDT row before backup"
    );

    // Capture the backup. `bytes` is owned by the test and survives A's shutdown.
    let bytes = drain_backup(0, &cluster_a, TENANT).await;
    assert!(!bytes.is_empty(), "backup must produce non-empty bytes");

    cluster_a.shutdown().await;

    // ── TARGET cluster B (fresh — no CRDT collection, no data) ───────────────
    let cluster_b = TestCluster::spawn_three().await.expect("cluster B");

    push_restore(0, &cluster_b, TENANT, bytes)
        .await
        .expect("RESTORE CRDT state into fresh cluster B");

    cluster_b
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // ── Final assertion on EVERY node of cluster B ───────────────────────────
    // The restore fan-out replicates the tenant CRDT doc to all nodes; reading
    // any node must return the row. `crdt_state` returns the exported Loro
    // snapshot bytes, which exist only if the row is present in that node's
    // tenant doc — a non-empty result proves the `tenant_crdt_state` section
    // round-tripped through BACKUP → RESTORE.
    for node in &cluster_b.nodes {
        let state = read_crdt_state_when_ready(&node.client, Duration::from_secs(15)).await;
        assert!(
            !state.is_empty(),
            "BUG: cluster B node {} read EMPTY crdt_state after restore — the \
             `tenant_crdt_state` section was NOT carried through BACKUP/RESTORE",
            node.node_id
        );
    }

    cluster_b.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Plain-Columnar: flushed segment + memtable rows survive restore
//           into a FRESH second cluster
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_restore_preserves_columnar_flushed_and_memtable() {
    // ── SOURCE cluster A ─────────────────────────────────────────────────────
    // Low flush threshold (4 rows) so that inserting 5 rows triggers a flush
    // to a segment deterministically — no sleep required.
    let cluster_a = TestCluster::spawn_three_with_columnar_flush_threshold(4)
        .await
        .expect("cluster A");

    // DDL — copied from nodedb/tests/engine_surface_columnar.rs
    // `ingest_and_select`. Plain columnar uses COLUMNS, no TIME_KEY.
    cluster_a
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION col_br \
             COLUMNS (id TEXT, region TEXT, revenue FLOAT, ts BIGINT) \
             WITH (engine='columnar')",
        )
        .await
        .expect("CREATE COLLECTION col_br on cluster A");

    // Insert 5 rows (> threshold of 4) — after the 5th insert the columnar
    // engine will have flushed a segment containing at least the first 4 rows,
    // with the 5th row either in the new memtable or already in the segment.
    let pre_rows = [
        ("c1", "us", 100.0_f64, 1_i64),
        ("c2", "eu", 200.0, 2),
        ("c3", "us", 150.0, 3),
        ("c4", "eu", 120.0, 4),
        ("c5", "us", 180.0, 5),
    ];
    for (id, region, revenue, ts) in &pre_rows {
        cluster_a.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO col_br (id, region, revenue, ts) \
                 VALUES ('{id}', '{region}', {revenue}, {ts})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {}", db_detail(&e)));
    }

    // Insert 3 more rows — these land in the memtable at backup time and
    // exercise the memtable capture path of BACKUP TENANT.
    let post_rows = [
        ("c6", "ap", 300.0_f64, 6_i64),
        ("c7", "us", 250.0, 7),
        ("c8", "eu", 175.0, 8),
    ];
    for (id, region, revenue, ts) in &post_rows {
        cluster_a.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO col_br (id, region, revenue, ts) \
                 VALUES ('{id}', '{region}', {revenue}, {ts})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert post-flush {id}: {}", db_detail(&e)));
    }

    // Capture the backup from cluster A. `bytes` is owned by the test and
    // survives cluster A's shutdown.
    let bytes = drain_backup(0, &cluster_a, TENANT).await;
    assert!(!bytes.is_empty(), "backup must produce non-empty bytes");

    // Shut down cluster A before spawning cluster B to limit resource use and
    // avoid port / file-descriptor exhaustion between the two clusters.
    cluster_a.shutdown().await;

    // ── TARGET cluster B (fresh — no col_br collection, no data) ─────────────
    // Spawn with the SAME columnar flush threshold as cluster A so that engine
    // parameters match if RESTORE re-ingests rows. Do NOT create col_br here:
    // RESTORE must recreate the catalog cluster-wide via metadata Raft — that
    // is the behavior under test (bug 2 listed in the module doc).
    //
    // cluster B's tenant_write_hlc starts at 0, so the restore staleness gate
    // (snapshot_watermark >= tenant_write_hlc) always passes. There is no live
    // data, so the fail-closed "refuse to overwrite live data" guard does not
    // fire.
    let cluster_b = TestCluster::spawn_three_with_columnar_flush_threshold(4)
        .await
        .expect("cluster B");

    // ── Restore into the fresh target ────────────────────────────────────────
    push_restore(0, &cluster_b, TENANT, bytes)
        .await
        .expect("RESTORE col_br into fresh cluster B");

    // Wait for all Raft groups on cluster B to apply the restored catalog +
    // data entries before asserting.
    cluster_b
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // ── Final assertions on NODE 1 of cluster B ──────────────────────────────
    // Querying node 1 (not the coordinator node 0 that received the RESTORE)
    // proves:
    //   (a) the restored catalog propagated cluster-wide via metadata Raft, and
    //   (b) data is routable from a non-coordinator node.
    let total = count_rows_when_ready(
        &cluster_b.nodes[1].client,
        "col_br",
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(
        total, 8,
        "post-restore SELECT COUNT(*) from cluster B node 1 must be 8 \
         (5 flushed + 3 memtable), got {total}"
    );

    // Assert the full PK set is present, ordered by ts.
    let ids = collect_first_col(
        &cluster_b.nodes[1].client,
        "SELECT id FROM col_br ORDER BY ts",
    )
    .await;
    let expected: Vec<&str> = vec!["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"];
    assert_eq!(
        ids, expected,
        "post-restore row IDs from cluster B node 1 must match all 8 rows in ts order; \
         got {ids:?}"
    );

    cluster_b.shutdown().await;
}
