// SPDX-License-Identifier: BUSL-1.1

//! End-to-end guard for the single-node pgwire streaming SELECT path. A scan
//! whose row count exceeds the Data-Plane `stream_chunk_size` (default 1000)
//! is emitted as several frames per core; the streaming path must surface
//! every row, never truncating to the first chunk.

use crate::harness::TestServer;

use std::collections::HashSet;

/// Number of rows to insert — comfortably above the default 1000-row
/// `stream_chunk_size` so the scan streams as multiple frames per core.
const ROW_COUNT: usize = 2_500;

/// A multi-chunk `SELECT n FROM coll` (no ORDER BY) over a multi-core
/// single-node server must return every inserted row through the streaming
/// path — not a truncated first chunk.
#[tokio::test]
async fn streaming_select_returns_all_rows_across_chunks() {
    // Multiple cores exercise the fan-out + interleave in `gather_all_cores_stream`.
    let srv = TestServer::start_multicores(4).await;
    srv.exec("CREATE COLLECTION stream_doc WITH (engine='document_schemaless')")
        .await
        .unwrap();

    for i in 0..ROW_COUNT {
        srv.exec(&format!("INSERT INTO stream_doc {{ id: 'r{i}', n: {i} }}"))
            .await
            .unwrap_or_else(|e| panic!("insert {i} failed: {e}"));
    }

    // No ORDER BY / DISTINCT / OFFSET / aggregate → streamable unordered scan.
    let rows = srv
        .query_rows("SELECT n FROM stream_doc")
        .await
        .expect("streaming SELECT should succeed");

    assert_eq!(
        rows.len(),
        ROW_COUNT,
        "streaming SELECT must return all {ROW_COUNT} rows, not a truncated chunk"
    );

    // Unordered; assert the full set of values is present exactly.
    let seen: HashSet<i64> = rows
        .iter()
        .filter_map(|cols| cols.first())
        .filter_map(|s| s.trim().parse::<i64>().ok())
        .collect();

    assert_eq!(
        seen.len(),
        ROW_COUNT,
        "every distinct value 0..{ROW_COUNT} must appear exactly once in the streamed result"
    );
    for i in 0..ROW_COUNT as i64 {
        assert!(seen.contains(&i), "missing streamed row with n = {i}");
    }
}
