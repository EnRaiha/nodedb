// SPDX-License-Identifier: BUSL-1.1

//! Regression: a no-`GROUP BY` scalar aggregate on a single-vShard-homed
//! collection must merge to one row on a multi-core server. A gather that
//! broadcasts to every core would seed identity rows on the empty cores and
//! pass them through unmerged, yielding N rows instead of one. A single-core
//! harness masks the bug, so these tests drive an 8-core server.

use crate::harness::TestServer;

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
    // `sum` over an INTEGER column renders as a float; compare numerically
    // rather than pinning textual formatting.
    assert_eq!(
        rows[0][1].parse::<f64>().expect("numeric sum"),
        6.0,
        "aliased sum must carry the merged value, got {:?}",
        rows[0][1]
    );
}
