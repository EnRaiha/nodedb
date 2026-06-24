// SPDX-License-Identifier: BUSL-1.1
//! Cluster end-to-end BACKUP / RESTORE for engine-specific data paths.
//!
//! Validates that BACKUP TENANT captures BOTH flushed (on-disk segment) data
//! AND memtable (in-memory, not yet flushed) data, and that RESTORE TENANT
//! replays both through the cluster, making them visible from a different node
//! than the backup source.
//!
//! NOTE: Validates same-process restore→query. Restart-survival is NOT
//! covered: the cluster harness cannot restart a node against the same
//! data_dir (documented harness limitation, filed separately).

mod common;
use common::cluster_harness::TestCluster;

use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;
use std::time::Duration;

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

// ── Helper: count rows via simple_query and extract the first column value ───

async fn count_rows(client: &tokio_postgres::Client, table: &str) -> usize {
    let rows = client
        .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT COUNT(*) FROM {table}: {}", db_detail(&e)));
    for msg in &rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            if let Some(s) = r.get(0) {
                return s.parse::<usize>().expect("COUNT(*) parse");
            }
        }
    }
    panic!("COUNT(*) returned no rows for {table}");
}

/// Collect the first column of every data row returned by `simple_query`.
async fn collect_first_col(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let rows = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {}", db_detail(&e)));
    let mut out = Vec::new();
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            if let Some(s) = r.get(0) {
                out.push(s.to_string());
            }
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Timeseries: flushed segments + memtable rows are both captured
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_restore_preserves_timeseries_flushed_and_memtable() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    // DDL — copied from nodedb/tests/engine_surface_timeseries.rs
    // `ingest_and_time_range_scan`. COLUMNS keyword, no PRIMARY KEY (timeseries
    // uses TIME_KEY as the ordering axis, not a primary key constraint).
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION ts_br \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("CREATE COLLECTION ts_br");

    // Insert 5 rows with DISTINCT timestamps (no dedup risk). Written through
    // node 0; the gateway routes to the correct vshard owner.
    let pre_flush_ids = ["p1", "p2", "p3", "p4", "p5"];
    let pre_flush_ts: [u64; 5] = [1000, 2000, 3000, 4000, 5000];
    for (id, ts) in pre_flush_ids.iter().zip(pre_flush_ts.iter()) {
        cluster.nodes[0]
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
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO ts_br (id, ts, metric, value) \
                 VALUES ('{id}', {ts}, 'cpu', 2.0)"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert post-flush {id}: {}", db_detail(&e)));
    }

    // Backup from node 0, restore into the same cluster via node 0.
    let bytes = drain_backup(0, &cluster, TENANT).await;
    push_restore(0, &cluster, TENANT, bytes)
        .await
        .expect("RESTORE ts_br");

    // Wait for all Raft groups to apply the restore entries on every node.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // Assert count from node 1 (different from the backup source, node 0).
    // This proves cross-node restore visibility.
    let total = count_rows(&cluster.nodes[1].client, "ts_br").await;
    assert_eq!(
        total, 7,
        "post-restore SELECT COUNT(*) from node 1 must be 7 (5 flushed + 2 memtable), \
         got {total}"
    );

    // Assert the full set of IDs is present (ORDER BY ts).
    let ids = collect_first_col(&cluster.nodes[1].client, "SELECT id FROM ts_br ORDER BY ts").await;
    let expected: Vec<&str> = vec!["p1", "p2", "p3", "p4", "p5", "p6", "p7"];
    assert_eq!(
        ids, expected,
        "post-restore row IDs from node 1 must match all 7 rows in ts order; \
         got {ids:?}"
    );

    cluster.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Plain-Columnar: flushed segment + memtable rows are both captured
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_restore_preserves_columnar_flushed_and_memtable() {
    // Low flush threshold (4 rows) so that inserting 5 rows triggers a flush
    // to a segment deterministically — no sleep required.
    let cluster = TestCluster::spawn_three_with_columnar_flush_threshold(4)
        .await
        .expect("cluster");

    // DDL — copied from nodedb/tests/engine_surface_columnar.rs
    // `ingest_and_select`. Plain columnar uses COLUMNS, no TIME_KEY.
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION col_br \
             COLUMNS (id TEXT, region TEXT, revenue FLOAT, ts BIGINT) \
             WITH (engine='columnar')",
        )
        .await
        .expect("CREATE COLLECTION col_br");

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
        cluster.nodes[0]
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
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO col_br (id, region, revenue, ts) \
                 VALUES ('{id}', '{region}', {revenue}, {ts})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert post-flush {id}: {}", db_detail(&e)));
    }

    // Backup from node 0, restore into the same cluster via node 0.
    let bytes = drain_backup(0, &cluster, TENANT).await;
    push_restore(0, &cluster, TENANT, bytes)
        .await
        .expect("RESTORE col_br");

    // Wait for all Raft groups to apply the restore entries on every node.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // Assert count from node 1 (different from the backup source, node 0).
    let total = count_rows(&cluster.nodes[1].client, "col_br").await;
    assert_eq!(
        total, 8,
        "post-restore SELECT COUNT(*) from node 1 must be 8 (5 flushed + 3 memtable), \
         got {total}"
    );

    // Assert the full PK set is present (ORDER BY ts).
    let ids = collect_first_col(
        &cluster.nodes[1].client,
        "SELECT id FROM col_br ORDER BY ts",
    )
    .await;
    let expected: Vec<&str> = vec!["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"];
    assert_eq!(
        ids, expected,
        "post-restore row IDs from node 1 must match all 8 rows in ts order; \
         got {ids:?}"
    );

    cluster.shutdown().await;
}
