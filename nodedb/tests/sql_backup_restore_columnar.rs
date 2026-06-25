// SPDX-License-Identifier: BUSL-1.1

//! Single-node RESTORE regression for plain-columnar data.
//!
//! Proves that RESTORE carries columnar data — both flushed segments AND the
//! unflushed memtable — into a server that does NOT already hold the
//! collection's columnar engine (a genuinely CLEAN target).
//!
//! This is the regression for the silent-data-loss bug where `merge_sections`
//! and `split_by_current_topology` dropped `columnar_engines` /
//! `flushed_ts_segments`, so a restore into a target without pre-existing
//! columnar data reported success while silently losing every columnar row.
//!
//! # Flush strategy
//!
//! The source server is started with `columnar_flush_threshold = 4` via
//! `TestServer::start_with_columnar_flush_threshold`. Inserting 8 rows
//! guarantees that the first 4 trigger a segment flush (flushed-segment path)
//! and rows 5–8 stay in the fresh in-memory memtable at backup time
//! (unflushed-memtable path) — covering BOTH code paths.

mod common;
use common::pgwire_harness::TestServer;

use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;

const TENANT: u64 = 1;
/// Columnar flush threshold used for all servers in this test.
const FLUSH_THRESHOLD: usize = 4;
/// Total rows inserted: first FLUSH_THRESHOLD rows force a segment flush;
/// the remaining FLUSH_THRESHOLD rows stay in the fresh memtable.
const TOTAL_ROWS: usize = FLUSH_THRESHOLD * 2;

// ── Helpers ───────────────────────────────────────────────────────────────────

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
    // An empty columnar collection can yield no COUNT(*) row; treat as zero
    // so callers get a clean numeric assertion (expected N, got 0).
    0
}

// ─────────────────────────────────────────────────────────────────────────────
// Clean-target restore test (no restart): proves RESTORE carries columnar data
// — both flushed segments AND the unflushed memtable — into a server that does
// NOT already hold the collection's columnar engine.
//
// # Clean-target determination (verified by reading the source)
//
// (a) The BACKUP envelope emits a `SECTION_ORIGIN_CATALOG_ROWS` section
//     (`control/backup/orchestrator.rs`), and the RESTORE path's
//     `apply_metadata_sections` (`control/backup/restore/sections.rs`) proposes
//     each collection catalog row via the metadata-Raft group (single-node
//     falls back to a local `put_collection`) — so RESTORE RECREATES the
//     collection catalog on a fresh server.
// (b) The columnar `MutationEngine` is created LAZILY — only on first insert
//     (`data/executor/handlers/columnar_write/insert.rs`) or first scan
//     (`data/executor/handlers/columnar_read/scan.rs`); `CREATE COLLECTION`
//     DDL never populates `columnar_engines`.
//
// Because RESTORE recreates the catalog (a) AND the columnar engine is created
// lazily (b), the cleanest clean target is a FRESH `TestServer` with NO
// pre-create: the catalog arrives via the restored metadata section, and
// `columnar_engines` is empty at restore time so the fail-closed
// `restore_columnar_engines` collision check (which errors if a live engine
// already holds the key) does not trip. We therefore do NOT pre-create the
// collection on srvB, and we do NOT query it before RESTORE (a pre-restore
// scan would lazily materialise an engine and trip the fail-closed check).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn restore_carries_columnar_into_clean_target() {
    // ── Step 1: source server srvA — create collection and insert rows ────────
    //
    // flush_threshold=4 → after the 5th insert the first 4 rows flush to a
    // segment; rows 5–8 stay in the fresh memtable at backup time. This covers
    // BOTH the flushed-segment and unflushed-memtable restore paths.

    let srv_a = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;

    srv_a
        .exec(
            "CREATE COLLECTION col_clean \
             COLUMNS (id TEXT, region TEXT, value FLOAT, ts BIGINT) \
             WITH (engine='columnar')",
        )
        .await
        .expect("CREATE COLLECTION col_clean on srvA");

    for i in 0..TOTAL_ROWS {
        let region = if i % 2 == 0 { "us" } else { "eu" };
        srv_a
            .exec(&format!(
                "INSERT INTO col_clean (id, region, value, ts) \
                 VALUES ('r{i}', '{region}', {i}.0, {i})"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT row {i} into col_clean on srvA: {e}"));
    }

    let pre_backup_count = count_rows(&srv_a, "col_clean").await;
    assert_eq!(
        pre_backup_count, TOTAL_ROWS,
        "pre-backup COUNT(*) on srvA must equal {TOTAL_ROWS}, got {pre_backup_count}"
    );

    // ── Step 2: BACKUP from srvA ──────────────────────────────────────────────

    let backup_bytes = drain_backup(&srv_a, TENANT).await;
    assert!(
        !backup_bytes.is_empty(),
        "backup envelope must not be empty"
    );
    drop(srv_a);

    // ── Step 3: fresh srvB — CLEAN target, NO pre-create, NO pre-query ────────
    //
    // RESTORE recreates the catalog from the envelope's catalog-rows section,
    // and the columnar engine is created lazily, so srvB holds NO columnar
    // engine for the collection at restore time — the fail-closed collision
    // check is satisfied.

    let srv_b = TestServer::start_with_columnar_flush_threshold(FLUSH_THRESHOLD).await;

    push_restore(&srv_b, TENANT, backup_bytes).await;

    // ── Step 4: assert rows carried into the clean target (no restart) ────────

    let post_restore_count = count_rows(&srv_b, "col_clean").await;
    assert_eq!(
        post_restore_count, TOTAL_ROWS,
        "RESTORE must carry columnar data into a clean target: expected {TOTAL_ROWS} \
         rows on srvB after restore, got {post_restore_count}"
    );

    // Verify ordered row ids survived (ORDER BY ts).
    let msgs = srv_b
        .client
        .simple_query("SELECT id FROM col_clean ORDER BY ts LIMIT 3")
        .await
        .expect("SELECT id FROM col_clean on srvB");
    let mut ids: Vec<String> = Vec::new();
    for msg in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
            && let Some(s) = r.get(0)
        {
            ids.push(s.to_string());
        }
    }
    assert_eq!(
        ids,
        vec!["r0", "r1", "r2"],
        "first three row ids after clean-target restore must match originals; got {ids:?}"
    );
}
