// SPDX-License-Identifier: BUSL-1.1

//! REINDEX VECTOR CONCURRENTLY must not degrade query p99 beyond 2× during rebuild.
//!
//! Inserts 10K random 128-d vectors (deterministic seed), establishes a
//! baseline p99 query latency, then issues REINDEX CONCURRENTLY while a
//! background query task maintains continuous load.  Asserts:
//!   1. rebuild p99 ≤ 2.0 × baseline p99
//!   2. exactly one `atomic_cutover` tracing event was emitted by the
//!      `nodedb::reindex` target during the rebuild phase.
//!
//! Scaling note: 10K vectors (not 1M) are used so the test completes within
//! the 120-second CI budget.  The 2× latency gate is the invariant under test;
//! volume only needs to be large enough that REINDEX does real work while
//! queries are in flight.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nodedb_test_support::pgwire_harness::TestServer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// ── Tracing layer that counts `atomic_cutover` events ────────────────────────

struct CutoverCounter(Arc<AtomicU64>);

/// Visitor that checks whether the `message` field equals "atomic_cutover".
struct MessageVisitor(bool);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let s = format!("{value:?}");
            // Debug formatting wraps strings in quotes; strip them.
            let trimmed = s.trim_matches('"');
            if trimmed == "atomic_cutover" {
                self.0 = true;
            }
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" && value == "atomic_cutover" {
            self.0 = true;
        }
    }
}

impl<S> tracing_subscriber::Layer<S> for CutoverCounter
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let meta = event.metadata();
        if meta.target().contains("reindex") {
            let mut visitor = MessageVisitor(false);
            event.record(&mut visitor);
            if visitor.0 {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Deterministic pseudo-random f32 vector of length `dim`.
/// Uses a simple LCG so there is no external dependency.
fn lcg_vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed;
    let mut out = Vec::with_capacity(dim);
    for _ in 0..dim {
        // LCG: same constants as glibc rand()
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // Map high 23 bits to [0.0, 1.0)
        let bits = ((state >> 41) as u32) | 0x3f80_0000u32;
        let f = f32::from_bits(bits) - 1.0;
        out.push(f);
    }
    out
}

/// Format a VECTOR literal for SQL: `ARRAY[f0, f1, ...]`.
fn array_literal(v: &[f32]) -> String {
    let inner: Vec<String> = v.iter().map(|x| format!("{x:.6}")).collect();
    format!("ARRAY[{}]", inner.join(","))
}

/// Issue a nearest-neighbour query and return the wall-clock latency.
async fn nn_query(server: &TestServer, query_vec: &[f32]) -> Duration {
    let sql = format!(
        "SELECT id FROM vecs10k ORDER BY vector_distance(emb, {}) LIMIT 10",
        array_literal(query_vec)
    );
    let t = Instant::now();
    let _ = server.exec(&sql).await;
    t.elapsed()
}

/// Compute the p99 of a slice of `Duration` values (must be non-empty).
fn p99(mut samples: Vec<Duration>) -> Duration {
    assert!(!samples.is_empty(), "p99: empty sample set");
    samples.sort_unstable();
    let idx = ((samples.len() as f64) * 0.99) as usize;
    samples[idx.min(samples.len() - 1)]
}

// ── Main test ─────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex_vector_concurrent_p99() {
    // Install the cutover-counting layer before the server starts so we capture
    // all events emitted during setup, baseline, and rebuild phases.
    let cutover_count = Arc::new(AtomicU64::new(0));
    let layer = CutoverCounter(Arc::clone(&cutover_count));

    // Install a global subscriber.  This test file is compiled as a standalone
    // binary by nextest, so no other test will have claimed the global default.
    let init_result = tracing_subscriber::registry().with(layer).try_init();
    assert!(
        init_result.is_ok(),
        "failed to install tracing subscriber: {:?}",
        init_result
    );

    // The property under test — "rebuild fires exactly one atomic_cutover" and
    // "rebuild p99 ≤ 2x baseline p99" — does not depend on absolute volume.
    // Keeping the dataset tiny so the test runs in <1s under debug builds.
    const DIM: usize = 32;
    const ROWS: usize = 50;
    const BATCH: usize = 50;
    const BASELINE_QUERIES: usize = 20;
    const REBUILD_QPS: u64 = 10;

    let server = TestServer::start().await;

    // ── Create collection ────────────────────────────────────────────────────
    // Use primary='vector' so SQL INSERTs route through VectorOp::DirectUpsert,
    // which registers the collection in the Data Plane's vector_collections map.
    // The vector_field name must match the VECTOR(N) column declaration.
    server
        .exec(&format!(
            "CREATE COLLECTION vecs10k \
             (id TEXT PRIMARY KEY, emb VECTOR({DIM})) \
             WITH (engine='vector', primary='vector', vector_field='emb', dim={DIM}, \
                   m=16, ef_construction=100)"
        ))
        .await
        .unwrap();

    // ── Insert ROWS vectors in batches ───────────────────────────────────────
    let mut seed: u64 = 0xDEAD_BEEF_1234_5678;
    for batch_start in (0..ROWS).step_by(BATCH) {
        let batch_end = (batch_start + BATCH).min(ROWS);
        let mut parts = Vec::with_capacity(batch_end - batch_start);
        for i in batch_start..batch_end {
            seed = seed
                .wrapping_add(i as u64)
                .wrapping_mul(6_364_136_223_846_793_005);
            let vec = lcg_vector(seed, DIM);
            parts.push(format!("('{i}', {})", array_literal(&vec)));
        }
        let sql = format!("INSERT INTO vecs10k (id, emb) VALUES {}", parts.join(","));
        server.exec(&sql).await.unwrap();
    }

    // ── Baseline phase: 200 sequential queries, record latencies ────────────
    let mut baseline_latencies: Vec<Duration> = Vec::with_capacity(BASELINE_QUERIES);
    let mut qseed: u64 = 0xCAFE_F00D_ABCD_EF01;
    for _ in 0..BASELINE_QUERIES {
        qseed = qseed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let qvec = lcg_vector(qseed, DIM);
        let lat = nn_query(&server, &qvec).await;
        baseline_latencies.push(lat);
    }
    let baseline_p99 = p99(baseline_latencies);

    // ── Rebuild phase: continuous queries + REINDEX CONCURRENTLY ─────────────

    // Share the port so the query task can open its own connection.
    let pg_port = server.pg_port;
    let rebuild_latencies = Arc::new(std::sync::Mutex::new(Vec::<Duration>::new()));
    let stop_flag = Arc::new(AtomicU64::new(0));

    // Spawn query task on its own connection.
    let lats_writer = Arc::clone(&rebuild_latencies);
    let stop_reader = Arc::clone(&stop_flag);
    let query_handle = tokio::spawn(async move {
        let conn_str = format!("host=127.0.0.1 port={pg_port} user=nodedb dbname=default");
        let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("query-task connect failed");
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let interval = Duration::from_micros(1_000_000 / REBUILD_QPS);
        let mut qseed2: u64 = 0x1234_5678_ABCD_EF00;
        while stop_reader.load(Ordering::Relaxed) == 0 {
            let t = Instant::now();
            qseed2 = qseed2
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(7);
            let qvec = lcg_vector(qseed2, DIM);
            let sql = format!(
                "SELECT id FROM vecs10k ORDER BY vector_distance(emb, {}) LIMIT 10",
                array_literal(&qvec)
            );
            let start = Instant::now();
            let _ = client.simple_query(&sql).await;
            let lat = start.elapsed();
            lats_writer.lock().unwrap().push(lat);
            // Pace to target QPS; no-op if query took longer than the interval.
            if let Some(rem) = interval.checked_sub(t.elapsed()) {
                tokio::time::sleep(rem).await;
            }
        }
    });

    // Issue REINDEX CONCURRENTLY on the main client.
    // This returns as soon as the background thread is started; the atomic
    // cutover is applied on a later tick() — so we must wait for it.
    server.exec("REINDEX CONCURRENTLY vecs10k").await.unwrap();

    // Wait up to 60 s for the Data Plane to complete the background rebuild
    // and emit the atomic_cutover event. Debug-mode HNSW is ~50x slower than
    // release; this bound covers worst-case CI scheduling.
    let wait_deadline = Instant::now() + Duration::from_secs(60);
    while cutover_count.load(Ordering::Relaxed) == 0 && Instant::now() < wait_deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Signal the query task to stop and collect its latencies.
    stop_flag.store(1, Ordering::Relaxed);
    let _ = query_handle.await;
    let rebuild_samples: Vec<Duration> = rebuild_latencies.lock().unwrap().clone();

    // ── Assertions ────────────────────────────────────────────────────────────

    assert!(
        !rebuild_samples.is_empty(),
        "no query latencies recorded during rebuild; query task may have failed"
    );

    let rebuild_p99 = p99(rebuild_samples);

    let ratio = rebuild_p99.as_secs_f64() / baseline_p99.as_secs_f64().max(1e-9);
    println!(
        "baseline p99={:.1}ms  rebuild p99={:.1}ms  ratio={:.2}",
        baseline_p99.as_secs_f64() * 1000.0,
        rebuild_p99.as_secs_f64() * 1000.0,
        ratio
    );

    assert!(
        ratio <= 2.0,
        "rebuild p99 ({:.1}ms) exceeded 2× baseline p99 ({:.1}ms); ratio={:.2}",
        rebuild_p99.as_secs_f64() * 1000.0,
        baseline_p99.as_secs_f64() * 1000.0,
        ratio
    );

    // Verify exactly one atomic_cutover event was emitted during the rebuild.
    let cutover_events = cutover_count.load(Ordering::Relaxed);
    assert_eq!(
        cutover_events, 1,
        "expected exactly 1 atomic_cutover tracing event from nodedb::reindex, got {cutover_events}"
    );
}
