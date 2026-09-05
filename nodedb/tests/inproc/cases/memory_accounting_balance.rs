// SPDX-License-Identifier: BUSL-1.1

//! Engine memory accounting must remain balanced across a full
//! lifecycle of `CREATE → INSERT → DROP`.
//!
//! `ReservationToken::drop` saturates each layer's counter to zero on
//! over-release and emits a `tracing::warn!` — silent in test output, but
//! the real symptom on origin deployments is a continuous "memory release
//! exceeds allocation" log every ~40 s. The dual call-site bug — releasing
//! the wrong size or skipping `acquire` — surfaces as drift in the global
//! `total_allocated()` counter once a workload's resources are fully
//! released.
//!
//! This test pins the invariant: after a known ingest is fully torn
//! down, the governor's accumulated allocation must return to its
//! pre-workload baseline (within a small tolerance for background
//! metadata).

use std::time::Duration;

use nodedb_test_support::pgwire_harness::TestServer;

/// Drain pending Data Plane releases by repeatedly sleeping until the
/// allocated counter is stable for two consecutive observations, or
/// the deadline expires. Returns the final settled value.
async fn settle_allocated(srv: &TestServer, max_wait: Duration) -> usize {
    let gov = srv.shared.governor.clone();
    let start = std::time::Instant::now();
    let mut prev = gov.total_allocated();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let next = gov.total_allocated();
        if next == prev || start.elapsed() >= max_wait {
            return next;
        }
        prev = next;
    }
}

/// After a full `CREATE → INSERT (n=200) → DROP COLLECTION` cycle the
/// governor's total allocated bytes must return to within a small
/// tolerance of the pre-workload baseline.
///
/// Drift in either direction is a bug:
/// - Drift **up** = a call site acquired without a matching release.
/// - Drift **down** would saturate `Budget` to zero on a per-engine
///   basis (the over-release case), so the global counter never goes
///   negative — but residual upward drift from one engine compensating
///   for another's over-release shows up here too.
///
/// The tolerance is generous (16 KiB) because background metadata
/// caches may still hold a handful of bytes after teardown — the
/// originally-reported bench drift was multiple megabytes (~6× over
/// the engine budget), so the tolerance has plenty of headroom.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_memory_returns_to_baseline_after_create_insert_drop() {
    let srv = TestServer::start().await;

    // Baseline: governor at rest, before any user workload.
    let baseline = settle_allocated(&srv, Duration::from_secs(2)).await;

    srv.exec(
        "CREATE COLLECTION mem_balance_check \
         COLUMNS (id TEXT PRIMARY KEY, payload TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();

    // Two hundred small rows — representative of a steady ingest burst
    // and large enough that a per-row leak would dominate the tolerance.
    for i in 0..200u32 {
        srv.exec(&format!(
            "INSERT INTO mem_balance_check (id, payload) \
             VALUES ('r{i}', 'payload-{i}')"
        ))
        .await
        .unwrap();
    }

    srv.exec("DROP COLLECTION mem_balance_check PURGE")
        .await
        .unwrap();

    let settled = settle_allocated(&srv, Duration::from_secs(5)).await;

    // Tolerance: 16 KiB above baseline. The reported drift was
    // multiple-megabyte, so a per-row leak would blow well past this.
    const TOLERANCE_BYTES: usize = 16 * 1024;
    let drift = settled.saturating_sub(baseline);
    assert!(
        drift <= TOLERANCE_BYTES,
        "post-teardown memory drift = {drift} B (settled = {settled}, \
         baseline = {baseline}); tolerance = {TOLERANCE_BYTES} B. \
         Persistent drift after a complete CREATE/INSERT/DROP cycle is \
         a call-site accounting bug — `acquire` without a matching \
         `release`, or the wrong size on either side."
    );

    // The drift check above only catches *under-release* (acquire
    // without matching release). The bench-reported symptom is the
    // dual case: `release(size)` where `size > current`, which
    // saturates the per-engine counter to zero and so is invisible
    // in `allocated()`. Assert directly on the over-release event
    // counter exposed by `Budget` so any call-site that crosses the
    // wrong direction is caught here.
    let gov = srv.shared.governor.clone();
    let over_releases = gov.total_over_release_count();
    assert_eq!(
        over_releases, 0,
        "memory accounting reported {over_releases} over-release event(s) \
         during the CREATE/INSERT/DROP cycle. Over-release saturates the \
         per-engine counter to zero so it doesn't show up in drift, but \
         each event is the production-reported \"memory release exceeds \
         allocation\" warning — a call-site is releasing more bytes than \
         it reserved."
    );
}

/// Restarting on a populated data directory (WAL replay re-applying
/// recovered rows across every engine) must leave the memory governor
/// in a sane state — no over-release events, and an allocation that
/// reflects the data actually resident, not a phantom multi-GB figure.
///
/// This is the crash-recovery / restart half of the accounting
/// contract. On boot, each engine's WAL-replay path reconstructs its
/// in-memory state; if any of those paths releases bytes it never
/// reserved, or reserves bytes it never releases, the post-restart
/// governor view detaches from reality — and a budget that reads in
/// the multi-GB range against a GB-scale limit reports permanent
/// Emergency pressure, suspends the Data Plane core's SPSC reads, and
/// deadlocks every subsequent DDL on the schema-register barrier (the
/// reported "healthy /healthz, every query fails" state). The
/// tolerance is deliberately huge (256 MiB) so this only fires on a
/// genuine accounting blow-up, not on legitimately-resident replayed
/// data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_replay_restart_keeps_governor_accounting_sane() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION replay_kv (k TEXT PRIMARY KEY, v TEXT) \
         WITH (engine='kv')",
    )
    .await
    .unwrap();
    srv.exec(
        "CREATE COLLECTION replay_ts \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, val FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec(
        "CREATE COLLECTION replay_doc (id TEXT PRIMARY KEY, payload TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();

    for i in 0..300u32 {
        srv.exec(&format!(
            "INSERT INTO replay_kv (k, v) VALUES ('k{i}', 'v{i}')"
        ))
        .await
        .unwrap();
        srv.exec(&format!(
            "INSERT INTO replay_ts (id, ts, val) VALUES ('s{}', {}, {}.5)",
            i % 8,
            1_000_000 + i as i64,
            i
        ))
        .await
        .unwrap();
        srv.exec(&format!(
            "INSERT INTO replay_doc (id, payload) VALUES ('d{i}', 'payload-{i}')"
        ))
        .await
        .unwrap();
    }

    // Restart against the same data directory — this drives every
    // engine's WAL-replay path on the new server's Data Plane core.
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // Sanity: the replayed data is actually queryable (so we know
    // replay ran, not that it silently dropped everything).
    let kv_rows = srv2
        .query_rows("SELECT v FROM replay_kv WHERE k = 'k42'")
        .await
        .unwrap();
    assert_eq!(kv_rows.len(), 1, "replayed KV row must be queryable");

    let settled = settle_allocated(&srv2, Duration::from_secs(5)).await;
    let gov = srv2.shared.governor.clone();

    let over_releases = gov.total_over_release_count();
    assert_eq!(
        over_releases, 0,
        "WAL replay produced {over_releases} over-release event(s): some \
         engine's replay path released budget it never reserved. Each event \
         is the production \"memory release exceeds allocation (WAL replay or \
         accounting drift)\" warning, and it can corrupt the per-engine \
         counter into the range that reads as permanent Emergency pressure."
    );

    const MAX_SANE_ALLOCATED: usize = 256 * 1024 * 1024;
    assert!(
        settled <= MAX_SANE_ALLOCATED,
        "after WAL replay the governor reports {settled} B allocated — far \
         beyond what the replayed data could occupy. A replay path is \
         reserving bytes it never releases (or a counter underflowed and \
         wrapped); the governor's view of memory has detached from reality, \
         which is what drives the post-upgrade Emergency-pressure cascade."
    );
}
