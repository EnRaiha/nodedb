// SPDX-License-Identifier: BUSL-1.1

//! Cells a threshold flush moves from the memtable into segments stay
//! readable.
//!
//! The memtable flushes once it holds the engine's cell threshold of 4096
//! cells. The load writes 136 rows of 128 cells in batches of 1024, so four
//! flushes move the first 128 rows into segments and the last 8 rows stay in
//! the memtable. Each flush holds 32 rows, so every tile of 64x64 cells is
//! split over two segment versions, and every tile uses the structural
//! codec. Every cell must read back through `ARRAY_SLICE` and `ARRAY_AGG`,
//! before and after a restart.

use nodedb_test_support::pgwire_harness::TestServer;

const WIDTH: i64 = 128;
const ROWS: i64 = 136;
const CELLS: i64 = WIDTH * ROWS;
const BATCH: i64 = 1024;

async fn load(srv: &TestServer) {
    srv.exec(
        "CREATE ARRAY big \
         DIMS (y INT64 [0..255], x INT64 [0..127]) \
         ATTRS (b1 FLOAT64, b2 FLOAT64, b3 FLOAT64) \
         TILE_EXTENTS (64, 64)",
    )
    .await
    .expect("CREATE ARRAY big");
    for start in (0..CELLS).step_by(BATCH as usize) {
        let cells: Vec<String> = (start..start + BATCH)
            .map(|i| {
                format!(
                    "COORDS ({}, {}) VALUES ({i}, {}, {})",
                    i / WIDTH,
                    i % WIDTH,
                    i * 2,
                    i * 3
                )
            })
            .collect();
        srv.exec(&format!("INSERT INTO ARRAY big {}", cells.join(", ")))
            .await
            .unwrap_or_else(|e| panic!("INSERT batch at {start}: {e}"));
    }
}

async fn assert_every_cell_reads_back(srv: &TestServer, when: &str) {
    let rows = srv
        .query_rows(
            "SELECT y, x, b1 FROM ARRAY_SLICE('big', '{\"y\":[0,255],\"x\":[0,127]}', '*', 100000)",
        )
        .await
        .unwrap_or_else(|e| panic!("{when}: ARRAY_SLICE: {e}"));
    assert_eq!(rows.len() as i64, CELLS, "{when}: ARRAY_SLICE row count");
    let mut seen = vec![false; CELLS as usize];
    for row in &rows {
        let y: i64 = row[0].parse().expect("y");
        let x: i64 = row[1].parse().expect("x");
        let b1: f64 = row[2].parse().expect("b1");
        let i = y * WIDTH + x;
        assert_eq!(b1, i as f64, "{when}: b1 of ({y}, {x})");
        assert!(!seen[i as usize], "{when}: ({y}, {x}) read twice");
        seen[i as usize] = true;
    }

    // A slice cutting through tiles keeps exactly the cells inside it.
    let part = srv
        .query_rows(
            "SELECT y, x FROM ARRAY_SLICE('big', '{\"y\":[60,131],\"x\":[10,69]}', '*', 100000)",
        )
        .await
        .unwrap_or_else(|e| panic!("{when}: partial ARRAY_SLICE: {e}"));
    assert_eq!(part.len(), 72 * 60, "{when}: partial ARRAY_SLICE row count");

    let count = srv
        .query_text("SELECT * FROM ARRAY_AGG('big', 'b1', 'count')")
        .await
        .unwrap_or_else(|e| panic!("{when}: ARRAY_AGG count: {e}"));
    assert_eq!(count.len(), 1, "{when}: one count row: {count:?}");
    let count: f64 = count[0].parse().expect("count");
    assert_eq!(count, CELLS as f64, "{when}: ARRAY_AGG count");

    let sum = srv
        .query_text("SELECT * FROM ARRAY_AGG('big', 'b1', 'sum')")
        .await
        .unwrap_or_else(|e| panic!("{when}: ARRAY_AGG sum: {e}"));
    let sum: f64 = sum[0].parse().expect("sum");
    assert_eq!(
        sum,
        ((CELLS - 1) * CELLS / 2) as f64,
        "{when}: ARRAY_AGG sum"
    );
}

#[tokio::test]
async fn cells_past_the_flush_threshold_read_back_before_and_after_restart() {
    let srv = TestServer::start().await;
    load(&srv).await;
    assert_every_cell_reads_back(&srv, "before restart").await;

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) = TestServer::open_on_path(dir).await;
    assert_every_cell_reads_back(&srv, "after restart").await;
}
