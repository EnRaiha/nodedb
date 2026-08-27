// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT durability for vector-index HNSW parameters.
//!
//! Regression: `TenantDataSnapshot::vector_params` / `::index_configs` were
//! captured at BACKUP but never re-issued at RESTORE, so the first restored
//! `VectorOp::Insert` lazily created the Data Plane HNSW index with
//! `HnswParams::default()` (metric=Cosine) — silently wrong nearest-neighbor
//! results for any collection created with a non-default metric.
//!
//! Proof strategy: the two seed vectors are placed so the nearest neighbor
//! of the query vector FLIPS depending on which metric is active (cosine
//! picks `va`, l2 picks `vb`). The source collection uses `metric='l2'`; if
//! RESTORE silently reverted to the cosine default, the nearest neighbor
//! would flip to `va`.

use crate::harness::TestServer;

use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;

const TENANT: u64 = 1;

async fn drain_backup(server: &TestServer, tenant: u64) -> Vec<u8> {
    let stream = server
        .client
        .copy_out(&format!("COPY (BACKUP TENANT {tenant}) TO STDOUT"))
        .await
        .expect("copy_out: BACKUP TENANT");
    let mut bytes = Vec::new();
    let mut s = Box::pin(stream);
    while let Some(chunk) = s.next().await {
        bytes.extend_from_slice(&chunk.expect("copy_out chunk"));
    }
    bytes
}

async fn push_restore(server: &TestServer, tenant: u64, bytes: Vec<u8>) {
    let sink = server
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN"))
        .await
        .expect("copy_in: RESTORE TENANT");
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(bytes))
        .await
        .expect("send backup bytes");
    sink.as_mut()
        .finish()
        .await
        .expect("finish copy_in: RESTORE TENANT");
}

/// Nearest-neighbor id for query vector `[1,0,0,0]` against the two seeded
/// vectors:
/// - `va = [3,0,0,0]` — same direction as the query (cosine distance 0), but
///   far away in Euclidean space (L2 squared distance 4).
/// - `vb = [1,0.5,0,0]` — slightly off-direction (cosine distance ~0.106),
///   but close in Euclidean space (L2 squared distance 0.25).
///
/// Under `metric='cosine'` the nearest neighbor is `va`; under
/// `metric='l2'` it is `vb` — the two metrics disagree, which makes this a
/// valid proof for which metric is actually active on the Data Plane index.
async fn nearest_id(server: &TestServer) -> String {
    let rows = server
        .query_rows(
            "SELECT id FROM vec_params \
             ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) \
             LIMIT 1",
        )
        .await
        .unwrap_or_else(|e| panic!("ANN search on vec_params: {e}"));
    rows.first()
        .and_then(|r| r.first())
        .cloned()
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_restore_preserves_vector_index_metric() {
    // Source server: collection with a NON-DEFAULT metric.
    let srv_a = TestServer::start().await;
    srv_a
        .exec("CREATE COLLECTION vec_params WITH (engine='vector')")
        .await
        .expect("CREATE COLLECTION vec_params on srvA");
    // `CREATE VECTOR INDEX ... METRIC ...` dispatches `VectorOp::SetParams`
    // BEFORE any insert, so the params it sets are what the Data Plane's
    // `get_or_create_vector_index` lazily creates the HNSW index with on the
    // first `VectorOp::Insert` below.
    srv_a
        .exec(
            "CREATE VECTOR INDEX idx_vec_params ON vec_params (embedding) \
             METRIC l2 M 32 EF_CONSTRUCTION 64 DIM 4",
        )
        .await
        .expect("CREATE VECTOR INDEX on srvA");

    srv_a
        .exec("INSERT INTO vec_params { id: 'va', embedding: [3.0,0.0,0.0,0.0] }")
        .await
        .expect("insert va on srvA");
    srv_a
        .exec("INSERT INTO vec_params { id: 'vb', embedding: [1.0,0.5,0.0,0.0] }")
        .await
        .expect("insert vb on srvA");

    // Sanity: the source collection really is using L2 (nearest = vb), not
    // the cosine default (which would pick va).
    assert_eq!(
        nearest_id(&srv_a).await,
        "vb",
        "source collection must be using metric='l2' as configured"
    );

    let backup_bytes = drain_backup(&srv_a, TENANT).await;
    assert!(
        !backup_bytes.is_empty(),
        "backup envelope must not be empty"
    );
    drop(srv_a);

    // Fresh target: RESTORE into a clean server.
    let srv_b = TestServer::start().await;
    push_restore(&srv_b, TENANT, backup_bytes).await;

    // The restored index must still answer with metric='l2'.
    assert_eq!(
        nearest_id(&srv_b).await,
        "vb",
        "RESTORE must preserve the collection's configured metric (l2); a \
         nearest-neighbor result of 'va' means the restored HNSW index \
         silently reverted to the cosine default"
    );
}
