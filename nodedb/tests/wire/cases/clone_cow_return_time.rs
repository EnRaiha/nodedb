// SPDX-License-Identifier: BUSL-1.1

//! Acceptance gate: `CLONE DATABASE` must complete in ≤ 2 000 ms (wall time)
//! regardless of source row count, provided the source fits in a single-node
//! test server.
//!
//! CoW semantics mean no data is physically copied at clone time; the clone
//! merely records a lineage pointer. This test verifies the property holds
//! at the integration level.

use crate::harness::TestServer;

/// Clone a database with a modest number of rows and assert the wall-clock
/// time is well below 2 seconds.
#[tokio::test]
async fn clone_completes_under_two_seconds() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Seed the source with rows.
    client
        .simple_query("CREATE DATABASE cow_src")
        .await
        .expect("CREATE DATABASE cow_src");
    client
        .simple_query("USE DATABASE cow_src")
        .await
        .expect("USE cow_src");
    client
        .simple_query("CREATE COLLECTION rows (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION rows");

    for i in 0..50u32 {
        client
            .simple_query(&format!("INSERT INTO rows (k, v) VALUES ('k{i}', 'v{i}')"))
            .await
            .expect("INSERT row");
    }

    // Time the clone.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    let t0 = std::time::Instant::now();
    client
        .simple_query("CLONE DATABASE cow_clone FROM cow_src")
        .await
        .expect("CLONE cow_src");
    let elapsed = t0.elapsed();

    assert!(
        elapsed.as_millis() < 2_000,
        "CLONE DATABASE must complete in < 2 000 ms (CoW — no physical copy); took {} ms",
        elapsed.as_millis()
    );
}
