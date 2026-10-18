// SPDX-License-Identifier: BUSL-1.1

//! Cost of reading a flushed 512x512 array: 262,144 cells, three FLOAT64
//! attrs, loaded by `INSERT` batches so the memtable auto-flushes them into
//! segments.
//!
//! Both tests are `#[ignore]`: they measure, they do not gate. One loads the
//! array. The other loads it and then runs `ARRAY_AGG` count and sum plus a
//! full `ARRAY_SLICE`. The read cost is the second test's count minus the
//! first's. Judge a change by instructions and allocations, never wall time.
//!
//! Recipe, from the workspace root:
//!
//! 1. Build the binary: `cargo nextest run -p nodedb --test inproc --no-run`.
//!    Take the path of `target/debug/deps/inproc-<hash>` from its output.
//! 2. Count instructions for each test:
//!    `perf stat -e instructions:u <bin> --ignored --exact cases::array_scan_cost::load_only`
//!    and the same with `cases::array_scan_cost::load_and_read`.
//! 3. Count allocations by running the same two commands under jemalloc:
//!    `LD_PRELOAD=/usr/lib/libjemalloc.so.2 MALLOC_CONF=stats_print:true <bin> ...`.
//!    Add the `nmalloc` totals of the `small` and `large` rows it prints at
//!    exit.
//!
//! Rebuild before each side of an A/B, and never measure while a build runs.

use nodedb_test_support::pgwire_harness::TestServer;

const SIDE: i64 = 512;
const CELLS: i64 = SIDE * SIDE;
const BATCH: i64 = 1024;

async fn load(srv: &TestServer) {
    srv.exec(
        "CREATE ARRAY scene \
         DIMS (y INT64 [0..511], x INT64 [0..511]) \
         ATTRS (b1 FLOAT64, b2 FLOAT64, b3 FLOAT64) \
         TILE_EXTENTS (128, 128)",
    )
    .await
    .expect("CREATE ARRAY scene");
    for start in (0..CELLS).step_by(BATCH as usize) {
        let cells: Vec<String> = (start..start + BATCH)
            .map(|i| {
                format!(
                    "COORDS ({}, {}) VALUES ({i}, {}, {})",
                    i / SIDE,
                    i % SIDE,
                    i * 2,
                    i * 3
                )
            })
            .collect();
        srv.exec(&format!("INSERT INTO ARRAY scene {}", cells.join(", ")))
            .await
            .unwrap_or_else(|e| panic!("INSERT batch at {start}: {e}"));
    }
}

async fn read(srv: &TestServer) {
    let count = srv
        .query_text("SELECT * FROM ARRAY_AGG('scene', 'b1', 'count')")
        .await
        .expect("ARRAY_AGG count");
    let count: f64 = count[0].parse().expect("count");
    assert_eq!(count, CELLS as f64, "ARRAY_AGG count");

    let sum = srv
        .query_text("SELECT * FROM ARRAY_AGG('scene', 'b2', 'sum')")
        .await
        .expect("ARRAY_AGG sum");
    let sum: f64 = sum[0].parse().expect("sum");
    assert_eq!(sum, ((CELLS - 1) * CELLS) as f64, "ARRAY_AGG sum");

    let rows = srv
        .query_rows(
            "SELECT y, x, b3 FROM ARRAY_SLICE('scene', \
             '{\"y\":[0,511],\"x\":[0,511]}', '*', 300000)",
        )
        .await
        .expect("ARRAY_SLICE");
    assert_eq!(rows.len() as i64, CELLS, "ARRAY_SLICE row count");
}

#[tokio::test]
#[ignore = "measurement: run by hand under perf stat"]
async fn load_only() {
    let srv = TestServer::start().await;
    load(&srv).await;
}

#[tokio::test]
#[ignore = "measurement: run by hand under perf stat"]
async fn load_and_read() {
    let srv = TestServer::start().await;
    load(&srv).await;
    read(&srv).await;
}
