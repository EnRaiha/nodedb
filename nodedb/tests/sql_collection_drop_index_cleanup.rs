// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the index cleanup owed by `DROP COLLECTION`.
//!
//! An index exists only to serve the collection it is attached to, so its
//! lifecycle follows the collection's: a soft drop hides its indexes,
//! `UNDROP COLLECTION` brings them back, and a purge removes them along with
//! every engine-side build parameter they registered. An index that outlives
//! its collection names something that no longer exists, accumulates in
//! `SHOW INDEXES` without bound, and hands its stale configuration to the next
//! collection created under the same name.

mod common;

use common::pgwire_harness::TestServer;

/// Index names listed by `SHOW INDEXES` for the current tenant.
async fn listed_indexes(server: &TestServer) -> Vec<String> {
    server
        .query_text("SHOW INDEXES")
        .await
        .expect("SHOW INDEXES must succeed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_collection_removes_its_vector_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vidx_orphan (id TEXT PRIMARY KEY, embedding FLOAT[])")
        .await
        .unwrap();
    server
        .exec(
            "CREATE VECTOR INDEX vidx_orphan_idx ON vidx_orphan (embedding) \
             METRIC cosine DIM 3",
        )
        .await
        .unwrap();

    server
        .exec("DROP COLLECTION vidx_orphan")
        .await
        .expect("DROP COLLECTION must succeed");

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n == "vidx_orphan_idx"),
        "an index cannot outlive the collection it indexes; it is still listed \
         after the collection was dropped: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_collection_removes_its_secondary_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION sec_orphan (id TEXT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX idx_sec_orphan_email ON sec_orphan (email)")
        .await
        .unwrap();

    server.exec("DROP COLLECTION sec_orphan").await.unwrap();

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n == "idx_sec_orphan_email"),
        "the secondary index outlived its collection: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_collection_removes_its_fulltext_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION fts_orphan (id TEXT PRIMARY KEY, body TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE FULLTEXT INDEX idx_fts_orphan_body ON fts_orphan (body)")
        .await
        .unwrap();

    server.exec("DROP COLLECTION fts_orphan").await.unwrap();

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n.contains("fts_orphan")),
        "the full-text index outlived its collection: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_collection_removes_its_spatial_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION geo_orphan (id TEXT PRIMARY KEY, location TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE SPATIAL INDEX geo_orphan_idx ON geo_orphan (location) USING RTREE")
        .await
        .unwrap();

    server.exec("DROP COLLECTION geo_orphan").await.unwrap();

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n == "geo_orphan_idx"),
        "the spatial index outlived its collection: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undrop_collection_restores_its_indexes() {
    // A soft drop hides the collection's indexes rather than destroying them,
    // because the collection is recoverable until retention expires. UNDROP
    // must bring back a collection that still has its indexes.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION undrop_idx (id TEXT PRIMARY KEY, embedding FLOAT[])")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX undrop_idx_vec ON undrop_idx (embedding) METRIC cosine DIM 3")
        .await
        .unwrap();
    server.exec("DROP COLLECTION undrop_idx").await.unwrap();
    assert!(
        !listed_indexes(&server)
            .await
            .iter()
            .any(|n| n == "undrop_idx_vec"),
        "a soft-dropped collection must not list its indexes"
    );

    server
        .exec("UNDROP COLLECTION undrop_idx")
        .await
        .expect("UNDROP COLLECTION must succeed");

    let after = listed_indexes(&server).await;
    assert!(
        after.iter().any(|n| n == "undrop_idx_vec"),
        "UNDROP must restore the collection's indexes, not just its rows: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soft_dropped_collection_hides_its_indexes_from_drop_index() {
    // While a collection is soft-dropped its indexes are not observable, so
    // DROP INDEX must not resolve them either — dropping one individually
    // would leave UNDROP restoring a collection with a hole in it.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION hidden_idx (id TEXT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX hidden_idx_email ON hidden_idx (email)")
        .await
        .unwrap();
    server.exec("DROP COLLECTION hidden_idx").await.unwrap();

    server
        .expect_error("DROP INDEX hidden_idx_email", "does not exist")
        .await;

    server.exec("UNDROP COLLECTION hidden_idx").await.unwrap();
    assert!(
        listed_indexes(&server)
            .await
            .iter()
            .any(|n| n == "hidden_idx_email"),
        "the hidden index must be intact after UNDROP"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recreated_collection_accepts_a_fresh_vector_index() {
    // Dropping and re-creating a collection under the same name is the
    // documented way to start over. If the old index registration survives the
    // drop, the new collection is born with a vector configuration it never
    // asked for and cannot replace — the reported workaround of "use fresh
    // names forever" is the shape of this leak.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vidx_reuse (id TEXT PRIMARY KEY, embedding FLOAT[])")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX vidx_reuse_idx ON vidx_reuse (embedding) METRIC cosine DIM 3")
        .await
        .unwrap();
    server.exec("DROP COLLECTION vidx_reuse").await.unwrap();

    server
        .exec("CREATE COLLECTION vidx_reuse (id TEXT PRIMARY KEY, embedding FLOAT[])")
        .await
        .expect("re-creating the dropped collection must succeed");
    server
        .exec("CREATE VECTOR INDEX vidx_reuse_idx ON vidx_reuse (embedding) METRIC l2 DIM 8")
        .await
        .expect("a re-created collection must accept a freshly configured vector index");
}
