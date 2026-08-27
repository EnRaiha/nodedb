// SPDX-License-Identifier: BUSL-1.1

//! Single-node RESTORE durability for vector-engine data ACROSS A RESTART.
//!
//! Regression for the silent-data-loss bug where RESTORE installed vector
//! engine state into in-memory-only Data Plane maps (via the snapshot
//! install) with NO WAL record. On single-node recovery — which is WAL
//! replay — the restored vectors were lost on the next restart.
//!
//! The fix re-issues each restored vector as a durable `VectorOp::Insert`:
//! the single-node path WAL-appends the insert before installing it live, so
//! a restart replays it. This also proves the reissue is exactly-once: the
//! restored row count must match before AND after restart — no duplication
//! from a straddling WAL record being replayed on top of an already-applied
//! insert.

mod common;
use common::pgwire_harness::TestServer;

use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;

const TENANT: u64 = 1;
const TOTAL_VECTORS: usize = 6;

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

/// Nearest-neighbor id for the query vector `[1,0,0,0]`, which the source
/// data places closest to `v0`.
async fn nearest_id(server: &TestServer) -> String {
    let rows = server
        .query_rows(
            "SELECT id FROM vec_restart \
             ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) \
             LIMIT 1",
        )
        .await
        .unwrap_or_else(|e| panic!("ANN search on vec_restart: {e}"));
    rows.first()
        .and_then(|r| r.first())
        .cloned()
        .unwrap_or_default()
}

#[tokio::test]
async fn backup_restore_vector_survives_restart() {
    // ── Step 1: source server — create collection + HNSW index, insert vectors ─
    let srv_a = TestServer::start().await;
    srv_a
        .exec("CREATE COLLECTION vec_restart WITH (engine='vector')")
        .await
        .expect("CREATE COLLECTION vec_restart on srvA");
    srv_a
        .exec("CREATE INDEX ON vec_restart (embedding)")
        .await
        .expect("CREATE INDEX on srvA");

    for i in 0..TOTAL_VECTORS {
        // v0 sits exactly on the query vector [1,0,0,0]; every other vector is
        // pushed away along a distinct axis so nearest-neighbor is unambiguous.
        let mut v = [0.0f32; 4];
        v[i % 4] = 1.0;
        v[(i % 4 + 1) % 4] += i as f32;
        let arr = format!("[{},{},{},{}]", v[0], v[1], v[2], v[3]);
        srv_a
            .exec(&format!(
                "INSERT INTO vec_restart {{ id: 'v{i}', embedding: {arr} }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT vector {i} into vec_restart on srvA: {e}"));
    }
    assert_eq!(count_rows(&srv_a, "vec_restart").await, TOTAL_VECTORS);
    assert_eq!(nearest_id(&srv_a).await, "v0");

    // ── Step 2: BACKUP from srvA ──────────────────────────────────────────────
    let backup_bytes = drain_backup(&srv_a, TENANT).await;
    assert!(
        !backup_bytes.is_empty(),
        "backup envelope must not be empty"
    );
    drop(srv_a);

    // ── Step 3: fresh srvB — RESTORE into a clean target ──────────────────────
    let srv_b = TestServer::start().await;
    push_restore(&srv_b, TENANT, backup_bytes).await;

    assert_eq!(
        count_rows(&srv_b, "vec_restart").await,
        TOTAL_VECTORS,
        "RESTORE must carry vector data into the clean target before restart"
    );
    assert_eq!(
        nearest_id(&srv_b).await,
        "v0",
        "restored HNSW index must answer ANN search correctly before restart"
    );

    // ── Step 4: graceful shutdown + reopen the SAME data dir (WAL replay) ──────
    let (srv_b, dir) = srv_b.take_dir();
    srv_b.graceful_shutdown().await;

    let (srv_c, _dir) = TestServer::open_on_path(dir).await;

    // ── Step 5: assert restored vector data SURVIVED the restart, exactly once ─
    let post_restart_count = count_rows(&srv_c, "vec_restart").await;
    assert_eq!(
        post_restart_count, TOTAL_VECTORS,
        "restored vectors must survive a restart (WAL replay) with no loss and no \
         duplication: expected {TOTAL_VECTORS}, got {post_restart_count}"
    );
    assert_eq!(
        nearest_id(&srv_c).await,
        "v0",
        "restored HNSW index must still answer ANN search correctly after restart"
    );
}
