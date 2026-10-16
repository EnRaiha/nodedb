// SPDX-License-Identifier: BUSL-1.1

//! Measurement of one 1024 x 1024 x 6 array load through
//! `NativeClient::array_put_cells` and the server's bulk-load core.
//!
//! The server runs in this process, so this binary installs jemalloc as its
//! global allocator and the counters cover the server's allocations. The
//! test prints the jemalloc allocation count and bytes of the load alone.
//!
//! Judge a change by instruction and allocation counts, never wall time.
//! Run it ignored, then under `perf stat`, once with the load and once
//! without, and subtract:
//!
//! ```text
//! cargo nextest run -p nodedb-client-tests --test array_load_measure \
//!     --run-ignored only --no-capture
//! BIN=$(ls -t target/debug/deps/array_load_measure-* | grep -v '\.d$' | head -1)
//! perf stat -e instructions:u -- "$BIN" --ignored --exact \
//!     array_load_1024x1024x6 --nocapture
//! NODEDB_MEASURE_SKIP_LOAD=1 perf stat -e instructions:u -- "$BIN" --ignored \
//!     --exact array_load_1024x1024x6 --nocapture
//! ```
//!
//! The first `perf stat` minus the second is the load's instruction count.
//! Rebuild both sides of an A/B before measuring, and never measure while a
//! build runs.

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::protocol::ArrayCellWrite;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SIDE: i64 = 1024;
const BANDS: usize = 6;

/// jemalloc's totals over every arena: allocation calls and bytes allocated.
fn alloc_counters() -> (u64, u64) {
    tikv_jemalloc_ctl::epoch::advance().expect("advance the jemalloc epoch");
    // SAFETY: each name is a NUL-terminated jemalloc ctl key of type u64.
    let calls = unsafe {
        tikv_jemalloc_ctl::raw::read::<u64>(b"stats.arenas.4096.small.nmalloc\0")
            .expect("small nmalloc")
            + tikv_jemalloc_ctl::raw::read::<u64>(b"stats.arenas.4096.large.nmalloc\0")
                .expect("large nmalloc")
    };
    let bytes = tikv_jemalloc_ctl::stats::allocated::read().expect("stats.allocated") as u64;
    (calls, bytes)
}

fn scene() -> Vec<ArrayCellWrite> {
    (0..SIDE)
        .flat_map(|y| {
            (0..SIDE).map(move |x| ArrayCellWrite {
                coords: vec![Value::Integer(y), Value::Integer(x)],
                attrs: (1..=BANDS)
                    .map(|k| Value::Float(((y * SIDE + x) * k as i64) as f64 + 0.5))
                    .collect(),
            })
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: run with --run-ignored only under perf stat"]
async fn array_load_1024x1024x6() {
    let server = TestServer::start().await;
    let client = NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ));
    client
        .execute_sql(
            "CREATE ARRAY scene DIMS (y INT64 [0..1023], x INT64 [0..1023]) \
             ATTRS (b1 FLOAT64, b2 FLOAT64, b3 FLOAT64, b4 FLOAT64, b5 FLOAT64, b6 FLOAT64) \
             TILE_EXTENTS (128, 128)",
            &[],
        )
        .await
        .expect("CREATE ARRAY scene");
    let cells = scene();
    if std::env::var_os("NODEDB_MEASURE_SKIP_LOAD").is_some() {
        return;
    }

    let (calls_before, bytes_before) = alloc_counters();
    let loaded = client
        .array_put_cells("scene", cells)
        .await
        .expect("array_put_cells");
    let (calls_after, bytes_after) = alloc_counters();

    assert_eq!(loaded, (SIDE * SIDE) as u64);
    println!(
        "array_load_1024x1024x6: {loaded} cells, {} jemalloc allocations, \
         {} bytes allocated at the end versus the start",
        calls_after - calls_before,
        bytes_after as i64 - bytes_before as i64
    );
}
