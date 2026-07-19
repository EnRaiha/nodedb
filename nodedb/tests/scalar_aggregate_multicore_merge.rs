// SPDX-License-Identifier: BUSL-1.1

//! Regression: a no-`GROUP BY` scalar aggregate on a single-vShard-homed
//! collection must merge to ONE row on a multi-core server.
//!
//! A scalar aggregate with no `GROUP BY` plans as
//! `QueryOp::Aggregate { input: None }` wrapped in `Exchange{Gather}`. In
//! single-node mode the gather formerly BROADCAST the plan to every core; the
//! collection's rows live on ONE owning core, so every other (empty) core
//! seeded its own scalar-aggregate identity row and the coordinator merge is a
//! passthrough — yielding N rows (N-1 identity rows + 1 real row) instead of
//! one. The fix routes single-vShard-homed plans to their one owning core.
//! A single-core harness masks the bug, so these tests drive an 8-core server.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_count_star_merges_to_one_row_multicore() {
    let srv = TestServer::start_multicores(8).await;
    srv.exec(
        "CREATE COLLECTION t \
         COLUMNS (id TEXT PRIMARY KEY, v INTEGER) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO t (id, v) VALUES ('a',1),('b',2),('c',3)")
        .await
        .unwrap();

    let rows = srv.query_rows("SELECT count(*) FROM t").await.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "scalar count(*) must merge to ONE row across cores, got {rows:?}"
    );
    assert_eq!(rows[0][0], "3");

    let rows = srv
        .query_rows("SELECT count(*) AS c, sum(v) AS s FROM t")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "aliased scalar aggregate must be one row, got {rows:?}"
    );
    assert_eq!(rows[0][0], "3");
    // `sum` over an INTEGER column renders as a float ("6.0"); the merge
    // correctness this test guards is the single row + right value, so compare
    // the value numerically rather than pinning its textual formatting.
    assert_eq!(
        rows[0][1].parse::<f64>().expect("numeric sum"),
        6.0,
        "aliased sum must carry the merged value, got {:?}",
        rows[0][1]
    );
}
