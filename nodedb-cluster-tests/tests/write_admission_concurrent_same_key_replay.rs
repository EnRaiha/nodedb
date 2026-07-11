// SPDX-License-Identifier: BUSL-1.1

//! Concurrent same-key writes must leave the SAME final state after a WAL
//! replay as they did live.
//!
//! The write-admission fix this covers: with no Calvin scheduler registered
//! for a vShard (the plain single-node path exercised here), a POINT write is
//! handed the global keyed order-lock (`SharedState::write_order_locks`) so
//! concurrent same-key writers still serialize FIFO in arrival order — closing
//! the hole where WAL-LSN order and Data-Plane apply order per key could
//! diverge. `nodedb/src/data/executor/wal_replay_kv_atomic.rs` documents the
//! matching contract on the replay side: `wal_append_if_write` appends the WAL
//! record BEFORE dispatch, so replaying the WAL in LSN order must reproduce
//! exactly the same final value a live run converged to — never a torn or
//! reordered write.
//!
//! This test drives many concurrent connections at ONE key with a
//! non-commutative op (`INSERT ... ON CONFLICT DO UPDATE`, last-writer-wins),
//! captures the live final value, restarts the server against the same WAL,
//! and asserts the replayed final value is bit-for-bit identical. A
//! commutative op (e.g. `KV_INCR`) would pass even if replay silently
//! reordered writes, since any order sums to the same total; last-writer-wins
//! is the property that actually pins ordering.

mod common;

use common::pgwire_harness::TestServer;

/// Concurrent writers racing on the same key. Deliberately > 1 core's worth so
/// the write-admission fence has real contention to serialize, not just two
/// writes that happen not to overlap.
const WRITERS: usize = 12;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_key_writes_survive_wal_replay_consistently() {
    let server = TestServer::start().await;

    // Typed `(key, n INT)` KV shape (not the RESP `(key, value)` opaque-blob
    // form): the typed column reliably round-trips through `INSERT ... ON
    // CONFLICT DO UPDATE SET n = EXCLUDED.n` and a plain `SELECT n`, mirroring
    // the passing `kv_insert_on_conflict_do_update_overwrites` test. The RESP
    // two-column value form does NOT project back through a column SELECT, which
    // is what made an earlier revision read an empty value.
    server
        .exec("CREATE COLLECTION cskr_kv (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION cskr_kv");
    server
        .exec("INSERT INTO cskr_kv (key, n) VALUES ('k', -1)")
        .await
        .expect("seed row");

    // Each writer gets its OWN pgwire connection: the simple-query protocol
    // serializes on one connection, so genuine concurrency needs distinct
    // sockets racing the write-admission fence for real. Each writer sets `n`
    // to its own distinct index, so the final value pins WHICH write landed
    // last — a non-commutative, last-writer-wins property (unlike KV_INCR,
    // which any apply order would sum to the same total).
    let mut handles = Vec::with_capacity(WRITERS);
    for i in 0..WRITERS {
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
            server.pg_port
        );
        handles.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                .await
                .unwrap_or_else(|e| panic!("writer {i} connect failed: {e}"));
            let conn_handle = tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .simple_query(&format!(
                    "INSERT INTO cskr_kv (key, n) VALUES ('k', {i}) \
                     ON CONFLICT (key) DO UPDATE SET n = EXCLUDED.n"
                ))
                .await
                .unwrap_or_else(|e| panic!("writer {i} write failed: {e}"));
            conn_handle.abort();
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        h.await
            .unwrap_or_else(|e| panic!("writer {i} task panicked: {e}"));
    }

    // Live final state: whichever writer's `n` the write-admission-fenced apply
    // order left as current.
    let live_rows = server
        .query_text("SELECT n FROM cskr_kv WHERE key = 'k'")
        .await
        .expect("SELECT live final value");
    assert_eq!(live_rows.len(), 1, "the row must still be a single key");
    let live_value = live_rows[0].clone();
    let winner: i64 = live_value
        .parse()
        .unwrap_or_else(|e| panic!("final n must parse as an integer, got {live_value:?}: {e}"));
    assert!(
        (0..WRITERS as i64).contains(&winner),
        "live final value must be exactly one writer's index, not a torn write; got {winner}"
    );

    // Reopen from the same WAL. Replay must reproduce the identical final
    // state — the whole point of appending the WAL record before dispatch is
    // that WAL-LSN order equals Data-Plane apply order per key, so a fresh
    // replay of that same order lands on the same value.
    let (server, data_dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server2, _data_dir2) = TestServer::open_on_path(data_dir).await;

    let replayed_rows = server2
        .query_text("SELECT n FROM cskr_kv WHERE key = 'k'")
        .await
        .expect("SELECT replayed final value");
    assert_eq!(
        replayed_rows.len(),
        1,
        "the row must survive replay as a single key"
    );
    let replayed_value = replayed_rows[0].clone();

    assert_eq!(
        replayed_value, live_value,
        "WAL replay must reproduce the exact live final state for concurrent same-key writes \
         (live={live_value}, replayed={replayed_value})"
    );
}
