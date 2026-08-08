// SPDX-License-Identifier: BUSL-1.1

//! Timeseries memory-budget accounting must stay balanced across the
//! ingest → memtable → flush cycle — the same `flush_ts_collection`
//! path that WAL replay drives when it re-ingests recovered rows on
//! boot.
//!
//! The bug class: the ingest side charges the governor a tiny
//! per-batch row estimate (`lines * 24`) and drops that reservation
//! before it returns, while `flush_ts_collection` releases the
//! *resident memtable footprint* (`memory_bytes()`, up to the 64 MiB
//! soft limit). The two sides of the same `EngineId::Timeseries`
//! budget therefore track different quantities: every flush calls
//! `Budget::release(memtable_bytes)` against an `allocated` counter
//! that only ever saw the small estimate, so it saturates to zero and
//! bumps `over_release_count` — the production "memory release exceeds
//! allocation (WAL replay or accounting drift)" warning — and the
//! engine's reported allocation no longer reflects the memory it is
//! actually holding.
//!
//! Worse: the over-release saturates the per-engine `Budget` to zero
//! while the still-alive per-batch `ReservationToken` is holding a
//! reservation, so the token's `Drop` (a raw `fetch_sub`) underflows
//! the engine counter to ~`usize::MAX`. In debug builds the next
//! `apply_spsc_pressure` tick panics computing `utilization_percent`
//! (`allocated * 100` overflows); in release builds it reads as 100 %
//! utilization → Emergency pressure → suspended SPSC reads →
//! schema-register barrier deadlock — the issue's "healthy /healthz,
//! every DDL and query fails" state. Either way, the assertion below
//! (or the panic before it) marks the run red until ingest and flush
//! agree on what the budget is tracking.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_mem::{EngineId, GovernorConfig, MemoryGovernor};
use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};

use crate::helpers::*;

/// A governor with generous per-engine and global limits — large
/// enough that no reservation is ever rejected, so the only way
/// `over_release_count` moves is a genuine release-without-reserve.
fn generous_governor() -> Arc<MemoryGovernor> {
    let per_engine: usize = 1 << 30; // 1 GiB
    let mut engine_limits = HashMap::new();
    for id in EngineId::ALL {
        engine_limits.insert(*id, per_engine);
    }
    let global_ceiling = per_engine * EngineId::ALL.len();
    Arc::new(
        MemoryGovernor::new(GovernorConfig {
            global_ceiling,
            engine_limits,
        })
        .expect("governor config valid"),
    )
}

/// Build an ILP payload with `count` wide rows for `collection`,
/// timestamps `start_ts_ns` and 1 ms apart. Mirrors the row shape used
/// by the timeseries query-engine tests so the ~64 MiB memtable soft
/// limit is reached at a comparable row count.
fn ilp_lines(collection: &str, count: usize, start_ts_ns: i64) -> String {
    let mut lines = String::with_capacity(count * 96);
    let qtypes = ["A", "AAAA", "MX", "CNAME"];
    let rcodes = ["NOERROR", "NXDOMAIN", "SERVFAIL", "REFUSED"];
    for i in 0..count {
        let ts_ns = start_ts_ns + i as i64 * 1_000_000;
        let qtype = qtypes[i % qtypes.len()];
        let rcode = rcodes[i % rcodes.len()];
        let qname = format!("host-{}.example.com", i % 50);
        let client_ip = format!("10.0.{}.{}", (i / 256) % 256, i % 256);
        lines.push_str(&format!(
            "{collection},qtype={qtype},rcode={rcode},qname={qname},client_ip={client_ip} elapsed_ms={}.0 {ts_ns}\n",
            (i % 1000) as f64
        ));
    }
    lines
}

fn ingest_ilp(ctx: &mut TestCtx, collection: &str, payload: &str) -> serde_json::Value {
    let raw = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.to_string(),
            payload: payload.as_bytes().to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        }),
    );
    let json = nodedb::data::executor::response_codec::decode_payload_to_json(&raw);
    serde_json::from_str(&json).unwrap_or(serde_json::Value::Null)
}

/// Ingest enough wide rows to force at least two memtable flush cycles,
/// leaving a partially-filled memtable resident afterwards. Returns the
/// context (memtable still live) and its governor.
///
/// `count_star_sees_flushed_partitions` uses the same 3 M-row volume to
/// guarantee ≥ 2 flushes; we reuse that so these tests fail for the
/// accounting bug and not because nothing flushed.
fn run_ts_flush_workload() -> (TestCtx, Arc<MemoryGovernor>) {
    let mut ctx = make_ctx();
    let gov = generous_governor();
    ctx.core.set_governor(Arc::clone(&gov));

    let batch_size = 10_000usize;
    let num_batches = 300usize;
    let mut accepted: u64 = 0;
    let mut rejected: u64 = 0;
    for b in 0..num_batches {
        let start_ns = (b * batch_size) as i64 * 1_000_000;
        let payload = ilp_lines("budget_metrics", batch_size, start_ns);
        let resp = ingest_ilp(&mut ctx, "budget_metrics", &payload);
        accepted += resp["accepted"].as_u64().unwrap_or(0);
        rejected += resp["rejected"].as_u64().unwrap_or(0);
    }
    assert_eq!(rejected, 0, "no rows should be rejected by the hard limit");
    assert_eq!(
        accepted,
        (batch_size * num_batches) as u64,
        "all sent rows should be accepted — otherwise the workload didn't \
         exercise the flush path the way this test expects"
    );
    (ctx, gov)
}

/// The timeseries flush path must not release more bytes from the
/// engine budget than ingest ever reserved.
///
/// `flush_ts_collection` releases `memtable_bytes`; ingest only ever
/// reserved a tiny per-batch estimate (and dropped it). The release
/// therefore over-releases on every flush — the per-engine `Budget`
/// saturates to zero and increments `over_release_count`, which is the
/// production "memory release exceeds allocation" warning. WAL replay
/// re-ingest drives the same `flush_ts_collection`, so this is the
/// drift that detaches a recovered node's governor view from reality.
#[test]
fn timeseries_flush_does_not_over_release_engine_budget() {
    let (_ctx, gov) = run_ts_flush_workload();
    let over_releases = gov.total_over_release_count();
    assert_eq!(
        over_releases, 0,
        "timeseries ingest+flush produced {over_releases} over-release \
         event(s): `flush_ts_collection` releases the resident memtable \
         footprint from the Timeseries budget, but ingest only reserved a \
         small per-batch estimate. The two sides must track the same \
         quantity — charge the memtable's bytes on ingest (or release only \
         what was reserved), so the flush release is balanced and the \
         governor's view survives a WAL-replay restart."
    );
}

/// After ingesting far more than fits in two flush cycles, the
/// governor's engine-layer allocation must reflect the still-resident
/// memtable — it must not be pinned at zero by repeated over-releases.
///
/// With the asymmetric accounting in place every flush saturates the
/// Timeseries budget to zero and the per-batch reservation is dropped
/// the moment ingest returns, so `total_allocated()` reads ~0 even
/// though a partially-filled memtable holds hundreds of thousands of
/// rows: the governor believes the engine is idle and pressure
/// detection is blind to the real footprint. The resident memtable
/// must be charged for as long as it is resident.
#[test]
fn timeseries_governor_reflects_resident_memtable_after_flush() {
    let (_ctx, gov) = run_ts_flush_workload();
    let allocated = gov.total_allocated();
    assert!(
        allocated > 0,
        "after a 3 M-row ingest (≥ 2 flush cycles, ~600 K rows still \
         resident in the memtable) the governor reports {allocated} B \
         allocated. The resident memtable is unaccounted: the per-batch \
         reservation is released when ingest returns and each flush \
         saturates the Timeseries budget to zero. Pressure detection \
         cannot see memory the governor doesn't know about."
    );
}
