// SPDX-License-Identifier: BUSL-1.1

//! REINDEX VECTOR CONCURRENTLY must keep serving queries while it rebuilds.
//!
//! Inserts a small deterministic vector set, measures a baseline query
//! latency, then issues REINDEX CONCURRENTLY while a background task keeps
//! querying.  Asserts:
//!   1. every query issued during the rebuild returned rows — the gate that
//!      actually holds the rebuild to being correct, not merely quick
//!   2. no query errored during the rebuild
//!   3. queries kept completing throughout the rebuild, not just before it
//!   4. no query stalled past `STALL_BOUND` — the signature of a rebuild
//!      that took an exclusive lock instead of running concurrently
//!   5. exactly one `atomic_cutover` tracing event was emitted by the
//!      `nodedb::reindex` target during the rebuild phase
//!
//! Why no p99 ratio: this test asserted `rebuild_p99 <= 2.0 * baseline_p99`,
//! and the dataset had been shrunk to the point where the rebuild finished
//! before a second query could start. So "rebuild p99" was ONE query, compared
//! against the maximum of twenty baseline samples — a coin flip that failed on
//! an unmodified tree whenever the machine was busy. The dataset is now sized
//! so the rebuild window holds a few hundred samples, and the assertions below
//! are orders of magnitude away from scheduler noise rather than a 2× multiple
//! of it. Latencies are still printed, so a real slowdown stays visible in the
//! log without gating CI on wall-clock.
//!
//! Assertion 1 is the load-bearing one and is the only one whose failure mode
//! was verified by deliberately breaking it: a query answered out of a
//! half-swapped index returns Ok, fast, and empty, which every latency- or
//! error-based check reads as the healthiest possible result.

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
///
/// Panics if the query fails: a failing query returns almost instantly, so
/// swallowing the error here would silently establish a baseline out of error
/// responses and make every later comparison meaningless.
async fn nn_query(server: &TestServer, query_vec: &[f32]) -> Duration {
    let sql = format!(
        "SELECT id FROM vecs10k ORDER BY vector_distance(emb, {}) LIMIT 10",
        array_literal(query_vec)
    );
    let t = Instant::now();
    server.exec(&sql).await.expect("baseline query failed");
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

    const DIM: usize = 32;
    // Sized so the HNSW rebuild outlives the query interval below, which is
    // what makes "served concurrently" observable at all. At the previous
    // ROWS=50 the rebuild finished before a second query could start, so
    // exactly one sample landed in the window and the old ratio was comparing
    // that single query against the baseline maximum. 1_000 rows yields a few
    // hundred in-window samples for ~8s of debug-build runtime.
    const ROWS: usize = 1_000;
    const BATCH: usize = 500;
    const BASELINE_QUERIES: usize = 20;
    // One query per millisecond, so the rebuild window holds many samples
    // instead of one. The loop paces itself and skips the sleep when a query
    // already took longer than the interval.
    const REBUILD_QPS: u64 = 1_000;

    /// Longest a single query may take during the rebuild.
    ///
    /// A concurrent rebuild leaves queries in the low milliseconds; one that
    /// takes an exclusive lock blocks them for the whole rebuild, which this
    /// test already allows up to 60s for. Two seconds sits between those two
    /// regimes by orders of magnitude, so a loaded machine cannot cross it but
    /// a lock-holding rebuild cannot avoid it.
    const STALL_BOUND: Duration = Duration::from_secs(2);

    /// Fewest queries that must complete during the rebuild window.
    ///
    /// Guards the case where the read path fails so fast, or blocks so early,
    /// that almost nothing is sampled — which would otherwise leave the
    /// latency assertions vacuously true over one or two rows.
    const MIN_REBUILD_SAMPLES: usize = 5;

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
    let rebuild_errors = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let rebuild_empty = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicU64::new(0));

    // Spawn query task on its own connection.
    let lats_writer = Arc::clone(&rebuild_latencies);
    let errors_writer = Arc::clone(&rebuild_errors);
    let empty_writer = Arc::clone(&rebuild_empty);
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
            let outcome = client.simple_query(&sql).await;
            let lat = start.elapsed();
            // Rows are counted, not just errors: a rebuild that drops the index
            // out from under the read path answers Ok with zero rows, and it
            // answers fast, so both a latency-only and an error-only assertion
            // would read that as the healthiest possible result.
            match outcome {
                Ok(messages) => {
                    let rows = messages
                        .iter()
                        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
                        .count();
                    if rows == 0 {
                        empty_writer.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(error) => errors_writer.lock().unwrap().push(error.to_string()),
            }
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
    let query_errors: Vec<String> = rebuild_errors.lock().unwrap().clone();

    // ── Assertions ────────────────────────────────────────────────────────────

    // Reported, never gated: the numbers make a real slowdown visible in the
    // log, but this machine cannot hold a wall-clock threshold (see the module
    // doc for why the old p99 ratio was removed).
    let slowest = rebuild_samples.iter().copied().max().unwrap_or_default();
    println!(
        "baseline p99={:.1}ms  rebuild p99={:.1}ms  slowest={:.1}ms  samples={}",
        baseline_p99.as_secs_f64() * 1000.0,
        p99(rebuild_samples.clone()).as_secs_f64() * 1000.0,
        slowest.as_secs_f64() * 1000.0,
        rebuild_samples.len()
    );

    assert!(
        query_errors.is_empty(),
        "{} of {} queries failed during the rebuild; a rebuild that breaks the \
         read path returns instantly and would otherwise look fast. First: {}",
        query_errors.len(),
        rebuild_samples.len(),
        query_errors.first().map_or("<none>", String::as_str)
    );

    // The load-bearing correctness gate. `simple_query` reports Ok for a query
    // the rebuild answered out of a half-swapped index, so only the row count
    // separates "served concurrently" from "answered with nothing".
    let empty_responses = rebuild_empty.load(Ordering::Relaxed);
    assert_eq!(
        empty_responses,
        0,
        "{empty_responses} of {} queries returned zero rows during the rebuild; \
         every one must still find neighbours among the {ROWS} indexed vectors",
        rebuild_samples.len()
    );

    assert!(
        rebuild_samples.len() >= MIN_REBUILD_SAMPLES,
        "only {} queries completed during the rebuild (want at least \
         {MIN_REBUILD_SAMPLES}); too few samples to claim anything about \
         concurrency",
        rebuild_samples.len()
    );

    assert!(
        slowest < STALL_BOUND,
        "a query stalled {:.1}ms during the rebuild (bound {:.0}ms) — the \
         signature of a rebuild holding an exclusive lock rather than running \
         concurrently",
        slowest.as_secs_f64() * 1000.0,
        STALL_BOUND.as_secs_f64() * 1000.0
    );

    // Verify exactly one atomic_cutover event was emitted during the rebuild.
    let cutover_events = cutover_count.load(Ordering::Relaxed);
    assert_eq!(
        cutover_events, 1,
        "expected exactly 1 atomic_cutover tracing event from nodedb::reindex, got {cutover_events}"
    );
}
