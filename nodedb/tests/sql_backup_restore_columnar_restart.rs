// SPDX-License-Identifier: BUSL-1.1

//! Single-node RESTORE durability for plain-columnar data ACROSS A RESTART.
//!
//! Regression for the silent-data-loss bug where RESTORE installed plain-
//! columnar engine state into in-memory-only Data Plane maps (via the snapshot
//! install) with NO WAL record. On single-node recovery — which is WAL replay —
//! the restored columnar rows were lost on the next restart.
//!
//! The fix re-issues each restored columnar collection's rows as a durable
//! `ColumnarOp::Insert`: the single-node path WAL-appends the insert before
//! installing it live, so a restart replays it.
//!
//! # Flush strategy
//!
//! The source server is started with `columnar_flush_threshold = 4`. Inserting
//! 8 rows forces the first 4 to flush to a segment (flushed-segment path) and
//! leaves rows 5–8 in the fresh memtable (unflushed-memtable path) — so the
//! restore + restart exercises BOTH paths.

mod common;
use common::pgwire_harness::TestServer;

use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;

const TENANT: u64 = 1;
const FLUSH_THRESHOLD: usize = 4;
const TOTAL_ROWS: usize = FLUSH_THRESHOLD * 2;

async fn drain_backup(server: &TestServer, tenant: u64) -> Vec<u8> {
    let stream = server
        .client
        .copy_out(&format!("COPY (BACKUP TENANT {tenant}) TO STDOUT"))
        .await
        .expect("copy_out: BACKUP TENANT");
    let mut bytes = Vec::new();
    let mut s = Box::pin(stream);
    while let Some(chunk) = s.next().await {
        bytes.extend_from_slice(&chunk.expect("copy_out chunk"));
    }
    bytes
}

async fn push_restore(server: &TestServer, tenant: u64, bytes: Vec<u8>) {
    let sink = server
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN"))
        .await
        .expect("copy_in: RESTORE TENANT");
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(bytes))
        .await
        .expect("send backup bytes");
    sink.as_mut()
        .finish()
        .await
        .expect("finish copy_in: RESTORE TENANT");
}

fn detail(e: tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

async fn count_rows(server: &TestServer, table: &str) -> usize {
    let msgs = server
        .client
        .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT COUNT(*) FROM {table}: {}", detail(e)));
    for msg in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
            && let Some(s) = r.get(0)
        {
            return s.parse::<usize>().expect("COUNT(*) parse");
        }
    }
    0
}

async fn collect_ids(server: &TestServer, sql: &str) -> Vec<String> {
    let msgs = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {}", detail(e)));
    let mut ids = Vec::new();
    for msg in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
            && let Some(s) = r.get(0)
        {
            ids.push(s.to_string());
        }
    }
    ids
}

#[tokio::test]
async fn backup_restore_columnar_survives_restart() {
    // ── Step 1: source server — create collection, insert 8 rows ──────────────
    let srv_a = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    srv_a
        .exec(
            "CREATE COLLECTION col_restart \
             COLUMNS (id TEXT, region TEXT, value FLOAT, ts BIGINT) \
             WITH (engine='columnar')",
        )
        .await
        .expect("CREATE COLLECTION col_restart on srvA");

    for i in 0..TOTAL_ROWS {
        let region = if i % 2 == 0 { "us" } else { "eu" };
        srv_a
            .exec(&format!(
                "INSERT INTO col_restart (id, region, value, ts) \
                 VALUES ('r{i}', '{region}', {i}.0, {i})"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT row {i} into col_restart on srvA: {e}"));
    }
    assert_eq!(count_rows(&srv_a, "col_restart").await, TOTAL_ROWS);

    // ── Step 2: BACKUP from srvA ──────────────────────────────────────────────
    let backup_bytes = drain_backup(&srv_a, TENANT).await;
    assert!(
        !backup_bytes.is_empty(),
        "backup envelope must not be empty"
    );
    drop(srv_a);

    // ── Step 3: fresh srvB — RESTORE into a clean target ──────────────────────
    let srv_b = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;
    push_restore(&srv_b, TENANT, backup_bytes).await;

    assert_eq!(
        count_rows(&srv_b, "col_restart").await,
        TOTAL_ROWS,
        "RESTORE must carry columnar data into the clean target before restart"
    );

    // ── Step 4: graceful shutdown + reopen the SAME data dir (WAL replay) ──────
    let (srv_b, dir) = srv_b.take_dir();
    srv_b.graceful_shutdown().await;

    let (srv_c, _dir) =
        TestServer::open_on_path_with_columnar_flush_threshold(dir, FLUSH_THRESHOLD).await;

    // ── Step 5: assert restored columnar data SURVIVED the restart ────────────
    let post_restart_count = count_rows(&srv_c, "col_restart").await;
    assert_eq!(
        post_restart_count, TOTAL_ROWS,
        "restored columnar rows must survive a restart (WAL replay): expected {TOTAL_ROWS}, \
         got {post_restart_count}"
    );

    let ids = collect_ids(&srv_c, "SELECT id FROM col_restart ORDER BY ts").await;
    let expected: Vec<String> = (0..TOTAL_ROWS).map(|i| format!("r{i}")).collect();
    assert_eq!(
        ids, expected,
        "every restored row id must be intact after restart; got {ids:?}"
    );
}
